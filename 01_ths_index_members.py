# -*- coding: utf-8 -*-
from common import *

# ================================= 初始化日志和配置 =================================
logger = setup_logger()
config = load_config()
engine = create_engine(get_pg_connection_string(config))

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
        logger.info(f"正在获取指数 {row['index_name']} ({row['index_code']}) 的成分股...")
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
        table_name = 'ths_index_members'
        update_columns = ['index_name', 'index_type', 'ts_count', 'list_date', 'ts_code', 'ts_name']
        save_to_database(final_df, table_name, ['index_code', 'ts_code'], '同花顺指数成分股', engine, update_columns)
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
    save_to_database(industry_grouped, table_name, ['ts_code'], '同花顺行业指数聚合', engine, ['ts_name', 'industry_agg'])
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
    save_to_database(concept_grouped, table_name, ['ts_code'], '同花顺概念指数聚合', engine, ['ts_name', 'concept_agg'])
    return concept_grouped

if __name__ == "__main__":
    df = get_ths_index_members()
    industry_grouped = process_industry_index_members(df)
    concept_grouped = process_concept_index_members(df)

