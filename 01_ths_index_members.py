# -*- coding: utf-8 -*-
from common import *
import tushare as ts

# ================================= 读取配置文件 =================================
config = load_config()
token = config.get('tushare', 'token')
pro = ts.pro_api(token)
engine = create_engine(get_pg_connection_string(config))


def upsert_data(df: pd.DataFrame, table_name: str, temp_table: str, insert_sql: str, engine) -> None:
    """使用临时表进行批量更新"""
    with engine.begin() as conn:
        if engine.url.drivername.startswith('postgresql'):
            from io import StringIO
            import csv
            logger.info(f'开始导入数据到 {temp_table}')
            df.head(0).to_sql(temp_table, conn, if_exists='replace', index=False)
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False, quoting=csv.QUOTE_MINIMAL)
            output.seek(0)
            cur = conn.connection.cursor()
            cur.copy_from(output, temp_table, sep='\t', null='')
        else:
            chunk_size = 100000  # 每批处理的行数
            total_rows = len(df)
            for i in tqdm(range(0, total_rows, chunk_size), desc="导入进度"):
                chunk_df = df.iloc[i:i + chunk_size]
                chunk_df.to_sql(temp_table, conn, if_exists='append' if i > 0 else 'replace', index=False, method='multi')
        conn.execute(text(insert_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))


# ================================= 概念指数,行业指数成分股 =================================
def get_ths_index_members():
    df_indices = pro.ths_index(exchange='A')
    df_indices = df_indices[
        ~df_indices['name'].isin(['2024三季报预增', '2024年报预增']) &  # 剔除指定指数
        df_indices['type'].isin(['N', 'I'])  # 只保留概念指数和行业指数
    ]
    
    df_indices['name'] = df_indices['name'].str.replace('(A股)', '', regex=False)
    df_indices['type'] = df_indices['type'].map({'N': '概念指数', 'I': '行业指数'})
    df_indices = df_indices.rename(columns={
        'ts_code': 'index_code',
        'name': 'index_name',
        'count': 'ts_count',
        'type': 'index_type'
    })
    
    all_members = []
    for _, row in df_indices.iterrows():
        print(f"正在获取指数 {row['index_name']} ({row['index_code']}) 的成分股...")
        df_members = pro.ths_member(ts_code=row['index_code'])
        if not df_members.empty:
            df_members = df_members.rename(columns={'ts_code': 'index_code', 'con_code': 'ts_code', 'con_name': 'ts_name'})
            df_members['index_code'] = row['index_code']
            df_members['index_name'] = row['index_name']
            df_members['ts_count'] = row['ts_count']
            df_members['list_date'] = row['list_date']
            df_members['index_type'] = row['index_type']
            df_members = df_members[['index_code', 'index_name', 'index_type', 'ts_count', 'list_date', 'ts_code', 'ts_name']]
            all_members.append(df_members)
    
    # 合并所有数据并保存
    if all_members:
        final_df = pd.concat(all_members, ignore_index=True)
        final_df.to_csv('同花顺概念行业指数成分.csv', encoding='utf-8-sig', index=False)
        table_name = 'ths_index_members'
        tmp_table = 'tmp_ths_index_members'
        insert_sql = f"""
            INSERT INTO {table_name} (index_code, index_name, index_type, ts_count, list_date, ts_code, ts_name)
            SELECT index_code, index_name, index_type, ts_count, list_date, ts_code, ts_name
            FROM {tmp_table} ON CONFLICT (index_code,ts_code) DO UPDATE 
            SET index_name = EXCLUDED.index_name,
                index_type = EXCLUDED.index_type,
                ts_count   = EXCLUDED.ts_count,
                list_date  = EXCLUDED.list_date,
                ts_code    = EXCLUDED.ts_code,
                ts_name    = EXCLUDED.ts_name;
        """
        upsert_data(final_df, table_name, tmp_table, insert_sql, engine)
        return final_df
    return None

# ================================= 行业指数成分股聚合 =================================
def process_industry_index_members(df):
    industry_df = df[df['index_type'] == '行业指数'].copy()
    def aggregate_index_names(group):
        sorted_group = group.sort_values('list_date', ascending=False)
        return pd.Series({
            'ts_name': sorted_group['ts_name'].iloc[0],  # 保持一个ts_name
            'industry_agg': ','.join(sorted_group['index_name']),
        })
    
    industry_grouped = industry_df.groupby('ts_code').apply(aggregate_index_names).reset_index()
    table_name = 'ths_ts_code_industry_agg'
    tmp_table = 'tmp_ths_ts_code_industry_agg'
    insert_sql = f"""
        INSERT INTO {table_name} (ts_code, ts_name, industry_agg)
        SELECT ts_code, ts_name, industry_agg
        FROM {tmp_table} ON CONFLICT (ts_code) DO UPDATE 
        SET ts_name = EXCLUDED.ts_name,
            industry_agg = EXCLUDED.industry_agg;
    """
    upsert_data(industry_grouped, table_name, tmp_table, insert_sql, engine)
    return industry_grouped

# ================================= 概念指数成分股聚合 =================================
def process_concept_index_members(df):
    concept_df = df[df['index_type'] == '概念指数'].copy()
    def aggregate_index_names(group):
        sorted_group = group.sort_values('list_date', ascending=False)
        return pd.Series({
            'ts_name': sorted_group['ts_name'].iloc[0],  # 保持一个ts_name
            'concept_agg': ','.join(sorted_group['index_name']),
        })
    
    concept_grouped = concept_df.groupby('ts_code').apply(aggregate_index_names).reset_index()
    table_name = 'ths_ts_code_concept_agg'
    tmp_table = 'tmp_ths_ts_code_concept_agg'
    insert_sql = f"""
        INSERT INTO {table_name} (ts_code, ts_name, concept_agg)
        SELECT ts_code, ts_name, concept_agg
        FROM {tmp_table} ON CONFLICT (ts_code) DO UPDATE 
        SET ts_name     = EXCLUDED.ts_name,
            concept_agg = EXCLUDED.concept_agg;
    """
    upsert_data(concept_grouped, table_name, tmp_table, insert_sql, engine)
    return concept_grouped

if __name__ == "__main__":
    df = get_ths_index_members()
    # df = pd.read_csv('同花顺概念行业指数成分.csv')
    industry_grouped = process_industry_index_members(df)
    concept_grouped = process_concept_index_members(df)

