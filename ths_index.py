# -*- coding: utf-8 -*-
from common import *
import tushare as ts


# ================================= 读取配置文件 =================================
config = load_config()
token = config.get('tushare', 'token')
pro = ts.pro_api(token)

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
        'type': '指数类型'
    })
    
    all_members = []
    for _, row in df_indices.iterrows():
        print(f"正在获取指数 {row['index_name']} ({row['index_code']}) 的成分股...")
        df_members = pro.ths_member(ts_code=row['index_code'])
        if not df_members.empty:
            df_members = df_members.rename(columns={'con_code': '成分股代码', 'con_name': '成分股名称'})
            df_members['指数代码'] = row['index_code']
            df_members['指数名称'] = row['index_name']
            df_members['成分股数量'] = row['ts_count']
            df_members['编制日期'] = row['list_date']
            df_members['指数类型'] = row['指数类型']
            df_members = df_members[['指数代码', '指数名称', '指数类型', '成分股数量', '编制日期', '成分股代码', '成分股名称']]
            all_members.append(df_members)
    
    # 合并所有数据并保存
    if all_members:
        final_df = pd.concat(all_members, ignore_index=True)
        final_df.to_csv('同花顺概念行业指数成分.csv', encoding='utf-8-sig', index=False)
        return final_df
    return None

# ================================= 行业指数成分股聚合 =================================
def process_industry_index_members(df):
    industry_df = df[df['指数类型'] == '行业指数'].copy()
    def aggregate_index_names(group):
        sorted_group = group.sort_values('编制日期', ascending=False)
        return pd.Series({
            '股票名称': sorted_group['成分股名称'].iloc[0],  # 保持一个成分股名称
            '行业名称聚合': ','.join(sorted_group['指数名称']),
        })
    
    industry_grouped = industry_df.groupby('成分股代码').apply(aggregate_index_names).reset_index().rename(columns={'成分股代码': '股票代码'})
    industry_grouped.to_csv('同花顺行业指数成分股聚合.csv', encoding='utf-8-sig', index=False)
    return industry_grouped

# ================================= 概念指数成分股聚合 =================================
def process_concept_index_members(df):
    concept_df = df[df['指数类型'] == '概念指数'].copy()
    def aggregate_index_names(group):
        sorted_group = group.sort_values('编制日期', ascending=False)
        return pd.Series({
            '股票名称': sorted_group['成分股名称'].iloc[0],  # 保持一个成分股名称
            '概念名称聚合': ','.join(sorted_group['指数名称']),
        })
    
    concept_grouped = concept_df.groupby('成分股代码').apply(aggregate_index_names).reset_index().rename(columns={'成分股代码': '股票代码'})
    concept_grouped.to_csv('同花顺概念指数成分股聚合.csv', encoding='utf-8-sig', index=False)
    return concept_grouped

if __name__ == "__main__":
    df = get_ths_index_members()
    # df = pd.read_csv('同花顺概念行业指数成分.csv')
    industry_grouped = process_industry_index_members(df)
    concept_grouped = process_concept_index_members(df)

