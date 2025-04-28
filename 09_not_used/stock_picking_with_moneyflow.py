# -*- coding: utf-8 -*-
from common import *
import tushare as ts

# ================================= 定义初始变量 =================================
n_stock = 3        # 分析个股N个交易日资金流向
n_industry = 365   # 分析行业N个交易日净额分位数

# ================================= 读取配置文件 =================================
config = load_config()
token = config.get('tushare', 'token')
pro = ts.pro_api(token)
engine = create_engine(get_pg_connection_string(config))
def send_notification_pushplus(subject, content):
    """使用PushPlus发送微信通知"""
    try:
        config = load_config()
        token = config.get('pushplus', 'token')
        url = "http://www.pushplus.plus/send"
        data = {
            "token": token,
            "title": subject,
            "content": content,
            "template": "html"
            }
        response = requests.post(url, json=data)
        if response.status_code == 200:
            result = response.json()
            if result.get('code') == 200:
                return True
            else:
                logger.error(f"微信通知发送失败: {result.get('msg')}")
                return False
        else:
            logger.error(f"微信通知发送失败，HTTP状态码: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"微信通知发送失败: {str(e)}")
        return False

# ================================= 获取N日交易日期 =================================
def get_recent_trade_days(days):
    """获取最近N个交易日的日期（排除非交易日）"""
    now = datetime.now()
    start_date = (now - timedelta(days=400)).strftime('%Y%m%d')
    include_today = now.time() > datetime.strptime('16:00:00', '%H:%M:%S').time()
    end_date = now.strftime('%Y%m%d') if include_today else (now - timedelta(1)).strftime('%Y%m%d')
    cal = pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, is_open=1)
    return cal.sort_values('cal_date', ascending=True).tail(days)['cal_date'].tolist()
trade_dates = get_recent_trade_days(n_stock)

# ================================= 获取多日全量股票资金流向数据 =================================
all_moneyflow = []
for date in trade_dates:
    df = pro.moneyflow(trade_date=date)
    # 计算各类单子的净流入
    net_flows = {
        "特大单净流入": df["buy_elg_amount"] - df["sell_elg_amount"],
        "大单净流入": df["buy_lg_amount"] - df["sell_lg_amount"],
        "中单净流入": df["buy_md_amount"] - df["sell_md_amount"],
        "小单净流入": df["buy_sm_amount"] - df["sell_sm_amount"]
    }
    df = df.assign(**net_flows)
    all_moneyflow.append(df)
moneyflow_df = pd.concat(all_moneyflow)

# ================================= 获取多日全量股票最新市值数据 =================================
all_basic = []
for date in trade_dates:
    df = pro.daily_basic(trade_date=date, fields='ts_code,trade_date,turnover_rate,circ_mv,volume_ratio')
    all_basic.append(df)
basic_df = pd.concat(all_basic)

# ================================= 合并资金流向和市值数据 =================================
moneyflow_merged_df = pd.merge(moneyflow_df, basic_df, on=['ts_code','trade_date'], how='left')

# ================================= 按股票代码分组,聚合计算多日指标 =================================
agg_dict = {
    "特大单净流入": "sum",
    "大单净流入": "sum",
    "中单净流入": "sum",
    "小单净流入": "sum",
    "circ_mv": "mean",
    "volume_ratio": "mean",
    "turnover_rate": "mean"
}
moneyflow_grouped = moneyflow_merged_df.groupby("ts_code").agg(agg_dict).reset_index()
moneyflow_grouped.columns = ["ts_code", "特大单净流入总和", "大单净流入总和", "中单净流入总和", "小单净流入总和", "市值均值", "量比均值", "换手率均值"]

# ================================= 按流通市值均分5组,并在组内标准化各指标 =================================
# 定义市值区间边界（单位：亿元）
市值区间边界 = [0, 20, 50, 100, 500, 1000, 10000, float('inf')]
市值区间标签 = [
    '0-20亿', 
    '20-50亿', 
    '50-100亿',
    '100-500亿',
    '500-1000亿',
    '1000-10000亿',
    '10000亿以上'
]

# 将市值从万元转换为亿元单位进行分组
moneyflow_grouped['市值(亿)'] = moneyflow_grouped['市值均值'] / 10000
moneyflow_grouped['市值分位'] = pd.cut(
    moneyflow_grouped['市值(亿)'], 
    bins=市值区间边界,
    labels=市值区间标签,
    right=False  # 左闭右开区间
)

def normalize_group(group):
    if len(group) <= 1:  # 如果分组中只有一个样本，跳过标准化
        for col in ['特大单/流通市值', '大单/流通市值', '中单/流通市值', '小单/流通市值', '换手率均值', '量比均值']:
            group[f'{col}_Z'] = 50  # 单个样本赋予中间值
        return group
        
    group['特大单/流通市值'] = group['特大单净流入总和'] / group['市值均值']
    group['大单/流通市值']   = group['大单净流入总和']   / group['市值均值']
    group['中单/流通市值']   = group['中单净流入总和']   / group['市值均值']
    group['小单/流通市值']   = group['小单净流入总和']   / group['市值均值']
    # Z-score标准化
    for col in ['特大单/流通市值', '大单/流通市值', '中单/流通市值', '小单/流通市值', '换手率均值', '量比均值']:
        # 先进行Z-score标准化
        group[f'{col}_Z'] = (group[col] - group[col].mean()) / group[col].std()
        # 将Z-score转换到0-100范围
        min_z = group[f'{col}_Z'].min()
        max_z = group[f'{col}_Z'].max()
        if min_z != max_z:  # 避免除以零
            group[f'{col}_Z'] = ((group[f'{col}_Z'] - min_z) / (max_z - min_z) * 100).round(2)
        else:
            group[f'{col}_Z'] = 50  # 如果所有值相同，则赋予中间值
    return group
moneyflow_grouped = moneyflow_grouped.groupby('市值分位').apply(normalize_group)

# ================================= 定义权重并计算综合得分,并归一化到0-100的范围 =================================
weights = {
    '特大单/流通市值_Z': 0.30,  # 主力资金权重最高
    '大单/流通市值_Z':   0.25,
    '中单/流通市值_Z':   0.20,
    '小单/流通市值_Z':   0.05,  # 小单噪音多，权重最低
    '换手率均值_Z':      0.10,  # 换手率反映活跃度
    '量比均值_Z':        0.10   # 量比反映成交活跃度
}

moneyflow_grouped['综合得分'] = sum(moneyflow_grouped[col] * weight for col, weight in weights.items())
min_score = moneyflow_grouped['综合得分'].min()
max_score = moneyflow_grouped['综合得分'].max()
moneyflow_grouped['综合得分'] = ((moneyflow_grouped['综合得分'] - min_score) / (max_score - min_score) * 100).round(2)

# ================================= 个股按得分排序并输出 =================================
moneyflow_rank = moneyflow_grouped.sort_values('综合得分', ascending=False).round(2)
moneyflow_rank['交易日期'] = trade_dates[-1]
moneyflow_rank = moneyflow_rank.reset_index(drop=True)  # 重置索引
moneyflow_rank.insert(0, '排名', range(1, len(moneyflow_rank) + 1))  # 添加排名列
result_columns = ['交易日期', 'ts_code', '排名', '市值分位', '特大单/流通市值_Z', '大单/流通市值_Z', '中单/流通市值_Z', '小单/流通市值_Z', '换手率均值_Z', '量比均值_Z', '综合得分']
moneyflow_rank = moneyflow_rank[result_columns]
moneyflow_rank.to_sql('stock_moneyflow_rank', engine, if_exists='replace', index=False)
# moneyflow_rank.to_csv('个股资金流向得分.csv', encoding='utf-8-sig', index=False)
print(moneyflow_rank.head(10))



# ================================= 获取较长时间的行业资金流向数据(THS)  =================================
long_trade_dates = get_recent_trade_days(n_industry)
start_date = long_trade_dates[0]
end_date = long_trade_dates[-1]
total_days = len(long_trade_dates)

# 每个交易日约有90条记录，计算预计的总记录数
estimated_records = total_days * 90
all_ind_data = []

# 分段获取数据，避免超过API限制
max_days_per_chunk = 55  # 5000/90≈55.5，取55保守一些
if estimated_records > 5000:
    chunks = math.ceil(total_days / max_days_per_chunk)    
    for i in range(chunks):
        start_idx = i * max_days_per_chunk
        end_idx = min((i + 1) * max_days_per_chunk - 1, total_days - 1)
        chunk_start_date = long_trade_dates[start_idx]
        chunk_end_date = long_trade_dates[end_idx]        
        chunk_data = pro.moneyflow_ind_ths(start_date=chunk_start_date, end_date=chunk_end_date)
        if not chunk_data.empty:
            all_ind_data.append(chunk_data)
else:
    # 如果总记录数不超过5000，直接获取所有数据
    all_ind_data.append(pro.moneyflow_ind_ths(start_date=start_date, end_date=end_date))
full_ind_data = pd.concat(all_ind_data, ignore_index=True) if all_ind_data else pd.DataFrame()

# ================================= 获取最新交易日行业资金流向数据(THS)  =================================
latest_date = max(full_ind_data['trade_date']) if not full_ind_data.empty else None
latest_data = full_ind_data[full_ind_data['trade_date'] == latest_date] if latest_date else pd.DataFrame()

# 获取过去5个交易日的日期（不含当日）
if not full_ind_data.empty:
    all_dates = sorted(full_ind_data['trade_date'].unique(), reverse=True)
    # 从第二个日期开始取（跳过当日）
    tm1_date = all_dates[1] if len(all_dates) > 1 else None  # T-1日
    tm2_date = all_dates[2] if len(all_dates) > 2 else None  # T-2日
    tm3_date = all_dates[3] if len(all_dates) > 3 else None  # T-3日
    tm4_date = all_dates[4] if len(all_dates) > 4 else None  # T-4日
    tm5_date = all_dates[5] if len(all_dates) > 5 else None  # T-5日
else:
    tm1_date = tm2_date = tm3_date = tm4_date = tm5_date = None

# ================================= 按行业计算net_amount(净买入额)的分位数  =================================
ind_results = {}

# 计算指定日期的资金流向区间
def calculate_range_for_date(industry_data, date):
    if date is None:
        return "N/A"
    
    # 获取该日期的资金流向数据
    date_data = industry_data[industry_data['trade_date'] == date]
    if date_data.empty:
        return "N/A"
    
    date_net_amount = date_data['net_amount'].values[0]
    all_net_amounts = industry_data['net_amount'].tolist()
    percentile_rank = sum(amount < date_net_amount for amount in all_net_amounts) / len(all_net_amounts) * 100
    percentile_bins = list(range(0, 101, 10))
    range_end = min([p for p in percentile_bins if p >= percentile_rank])
    return str(range_end)

# 遍历每个行业
for industry in latest_data['industry'].unique():
    industry_data = full_ind_data[full_ind_data['industry'] == industry]
    if industry_data.empty:
        continue
        
    ts_code = industry_data['ts_code'].iloc[0]
    industry_history = industry_data['net_amount'].tolist()
    latest_industry_data = latest_data[latest_data['industry'] == industry]
    if latest_industry_data.empty:
        continue
        
    latest_net_amount = latest_industry_data['net_amount'].values[0]
    percentiles = {f'p{p}': np.percentile(industry_history, p) for p in range(10, 101, 10)}
    percentile_rank = sum(amount < latest_net_amount for amount in industry_history) / len(industry_history) * 100
    percentile_bins = list(range(0, 101, 10))
    range_end = min([p for p in percentile_bins if p >= percentile_rank])
    percentile_range = str(range_end)
    
    # 计算过去5个交易日的资金流向区间（不含当日）
    tm5_range = calculate_range_for_date(industry_data, tm5_date)
    tm4_range = calculate_range_for_date(industry_data, tm4_date)
    tm3_range = calculate_range_for_date(industry_data, tm3_date)
    tm2_range = calculate_range_for_date(industry_data, tm2_date)
    tm1_range = calculate_range_for_date(industry_data, tm1_date)
    
    # 存储结果
    ind_results[industry] = {
        'latest_date': latest_date,
        'ts_code': ts_code,
        'latest_net_amount': latest_net_amount,
        'percentile_rank': percentile_rank,
        'percentile_range': percentile_range,
        'past_5d_range': tm5_range,
        'past_4d_range': tm4_range,
        'past_3d_range': tm3_range,
        'past_2d_range': tm2_range,
        'past_1d_range': tm1_range,
        'percentiles': percentiles
    }

# 转换为DataFrame便于展示，使用中文字段名
ind_rank = pd.DataFrame([
    {
        '交易日期': data['latest_date'],
        '板块代码': data['ts_code'],
        '行业': industry,
        '净额(亿元)': data['latest_net_amount'],
        '百分位排名': data['percentile_rank'],
        '所处区间': data['percentile_range'],
        '过去5日所处区间': data['past_5d_range'],
        '过去4日所处区间': data['past_4d_range'],
        '过去3日所处区间': data['past_3d_range'],
        '过去2日所处区间': data['past_2d_range'],
        '过去1日所处区间': data['past_1d_range'],
        '过去5日区间均值': pd.Series([
            float(data['past_5d_range']) if data['past_5d_range'] != 'N/A' else None,
            float(data['past_4d_range']) if data['past_4d_range'] != 'N/A' else None,
            float(data['past_3d_range']) if data['past_3d_range'] != 'N/A' else None,
            float(data['past_2d_range']) if data['past_2d_range'] != 'N/A' else None,
            float(data['past_1d_range']) if data['past_1d_range'] != 'N/A' else None
        ]).mean().round(2)
    }
    for industry, data in ind_results.items()
])

# 按照分位数排序
ind_rank = ind_rank.sort_values('百分位排名', ascending=False).round(2)
ind_rank = ind_rank.reset_index(drop=True)  # 重置索引
ind_rank.insert(0, '排名', range(1, len(ind_rank) + 1))  # 添加排名列
# ind_rank.to_csv('行业资金流向得分.csv', encoding='utf-8-sig', index=False)
ind_rank.to_sql('industry_moneyflow_rank', engine, if_exists='replace', index=False)
print(ind_rank.head(10))



# ================================= 获取行业和个股的联合排名 =================================
print("\n================ 行业资金流向TOP10及其成分股资金流向TOP5 ================")

# 获取前10的行业
top_industries = ind_rank.head(10)

# 查询行业成分股
industry_members_sql = """
SELECT DISTINCT i.index_code, i.index_name, i.ts_code, i.ts_name 
FROM ths_index_members i 
WHERE i.index_type = '行业指数' 
AND i.index_code IN ({})
""".format(','.join([f"'{code}'" for code in top_industries['板块代码']]))

industry_members = pd.read_sql(industry_members_sql, engine)

# 创建结果列表用于存储所有数据
results = []

# 对每个行业找出其成分股中资金流向排名前5的股票
for idx, industry in top_industries.iterrows():
    # 获取该行业的所有成分股
    industry_stocks = industry_members[industry_members['index_code'] == industry['板块代码']]
    
    # 从moneyflow_rank中筛选出该行业的股票并取前5名
    industry_top_stocks = moneyflow_rank[moneyflow_rank['ts_code'].isin(industry_stocks['ts_code'])].head(5)
    
    if not industry_top_stocks.empty:
        for _, stock in industry_top_stocks.iterrows():
            stock_name = industry_stocks[industry_stocks['ts_code'] == stock['ts_code']]['ts_name'].iloc[0]
            results.append({
                '交易日期': stock['交易日期'],
                '行业名称': industry['行业'],
                '行业排名': idx + 1,
                '净额(亿元)': industry['净额(亿元)'],
                '净额所处区间': industry['所处区间'],
                '股票代码': stock['ts_code'],
                '股票名称': stock_name,
                '股票排名': stock['排名'],
                '个股资金流综合得分': stock['综合得分']
            })

# 转换为DataFrame并显示结果
results_df = pd.DataFrame(results)
print("\n最终筛选结果：")
print("=" * 100)
print(results_df.to_string(index=False))
print("=" * 100)

# 保存结果到数据库
results_df.to_sql('industry_stock_top_rank', engine, if_exists='replace', index=False)

# 格式化结果为HTML表格
html_content = """
<h3>行业资金流向TOP3及其成分股资金流向TOP5</h3>
<table border="1" cellspacing="0" cellpadding="5" style="border-collapse: collapse; width: 100%;">
    <tr style="background-color: #f2f2f2;">
        <th>行业名称</th>
        <th>行业排名</th>
        <th>净额(亿元)</th>
        <th>净额所处区间</th>
        <th>股票代码</th>
        <th>股票名称</th>
        <th>股票排名</th>
        <th>个股资金流综合得分</th>
    </tr>
"""

# 添加数据行
for _, row in results_df.iterrows():
    html_content += f"""
    <tr>
        <td>{row['行业名称']}</td>
        <td>{row['行业排名']}</td>
        <td>{row['净额(亿元)']:.2f}</td>
        <td>{row['净额所处区间']}</td>
        <td>{row['股票代码']}</td>
        <td>{row['股票名称']}</td>
        <td>{row['股票排名']}</td>
        <td>{row['个股资金流综合得分']:.2f}</td>
    </tr>
    """

html_content += "</table>"

# 发送通知
today = datetime.now().strftime('%Y-%m-%d')
subject = f"{today} 行业及个股资金流向分析"
send_notification_pushplus(subject, html_content)

