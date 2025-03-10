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
moneyflow_grouped['市值分位'] = pd.qcut(moneyflow_grouped['市值均值'], q=5, labels=False)

def normalize_group(group):
    group['特大单/流通市值'] = group['特大单净流入总和'] / group['市值均值']
    group['大单/流通市值']   = group['大单净流入总和']   / group['市值均值']
    group['中单/流通市值']   = group['中单净流入总和']   / group['市值均值']
    group['小单/流通市值']   = group['小单净流入总和']   / group['市值均值']
    # Z-score标准化
    for col in ['特大单/流通市值', '大单/流通市值', '中单/流通市值', '小单/流通市值', '换手率均值', '量比均值']:
        group[f'{col}_Z'] = (group[col] - group[col].mean()) / group[col].std()
    return group
moneyflow_grouped = moneyflow_grouped.groupby('市值分位').apply(normalize_group)

# ================================= 定义权重并计算综合得分（可调参数） =================================
weights = {
    '特大单/流通市值_Z': 0.35,  # 主力资金权重最高
    '大单/流通市值_Z':   0.25,
    '中单/流通市值_Z':   0.15,
    '小单/流通市值_Z':   0.05,  # 小单噪音多，权重最低
    '换手率均值_Z':      0.10,  # 换手率反映活跃度
    '量比均值_Z':        0.10   # 量比反映成交活跃度
}

# 计算综合得分
moneyflow_grouped['综合得分'] = sum(moneyflow_grouped[col] * weight for col, weight in weights.items())

# ================================= 个股按得分排序并输出 =================================
moneyflow_rank = moneyflow_grouped.sort_values('综合得分', ascending=False).round(2)
moneyflow_rank['交易日期'] = trade_dates[-1]
result_columns = ['ts_code', '交易日期', '市值分位', '特大单/流通市值_Z', '大单/流通市值_Z', '中单/流通市值_Z', '小单/流通市值_Z', '换手率均值_Z', '量比均值_Z', '综合得分']
moneyflow_rank = moneyflow_rank[result_columns]





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

# 合并所有获取的数据
full_ind_data = pd.concat(all_ind_data, ignore_index=True) if all_ind_data else pd.DataFrame()

# ================================= 获取最新交易日行业资金流向数据(THS)  =================================
latest_date = max(full_ind_data['trade_date']) if not full_ind_data.empty else None
latest_data = full_ind_data[full_ind_data['trade_date'] == latest_date] if latest_date else pd.DataFrame()

# 获取过去第3日、第5日和第10日的日期（单日）
if not full_ind_data.empty:
    all_dates = sorted(full_ind_data['trade_date'].unique(), reverse=True)
    past_3d_date = all_dates[2] if len(all_dates) > 2 else None
    past_5d_date = all_dates[4] if len(all_dates) > 4 else None
    past_10d_date = all_dates[9] if len(all_dates) > 9 else None
else:
    past_3d_date = past_5d_date = past_10d_date = None

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
    
    # 获取该日期的净额
    date_net_amount = date_data['net_amount'].values[0]
    
    # 获取历史上所有日期的净额
    all_net_amounts = industry_data['net_amount'].tolist()
    
    # 计算该日期净额在历史上的分位数
    percentile_rank = sum(amount < date_net_amount for amount in all_net_amounts) / len(all_net_amounts) * 100
    
    # 确定净额所处的区间范围
    percentile_bins = list(range(0, 101, 10))
    range_start = max([p for p in percentile_bins if p <= percentile_rank])
    range_end = min([p for p in percentile_bins if p >= percentile_rank])
    return f"[{range_start}, {range_end}]"

# 遍历每个行业
for industry in latest_data['industry'].unique():
    # 获取该行业一年内的所有资金流数据
    industry_data = full_ind_data[full_ind_data['industry'] == industry]
    
    # 确保有足够的历史数据
    if industry_data.empty:
        continue
        
    # 获取该行业的板块代码和历史净额数据
    ts_code = industry_data['ts_code'].iloc[0]
    industry_history = industry_data['net_amount'].tolist()
    
    # 获取该行业最新交易日的净额
    latest_industry_data = latest_data[latest_data['industry'] == industry]
    if latest_industry_data.empty:
        continue
        
    latest_net_amount = latest_industry_data['net_amount'].values[0]
    
    # 计算分位数值和最新净额的分位值
    percentiles = {f'p{p}': np.percentile(industry_history, p) for p in range(10, 101, 10)}
    percentile_rank = sum(amount < latest_net_amount for amount in industry_history) / len(industry_history) * 100
    
    # 确定净额所处的区间范围
    percentile_bins = list(range(0, 101, 10))
    range_start = max([p for p in percentile_bins if p <= percentile_rank])
    range_end = min([p for p in percentile_bins if p >= percentile_rank])
    percentile_range = f"[{range_start}, {range_end}]"
    
    # 计算过去第3日、第5日和第10日的资金流向区间
    past_3d_range = calculate_range_for_date(industry_data, past_3d_date)
    past_5d_range = calculate_range_for_date(industry_data, past_5d_date)
    past_10d_range = calculate_range_for_date(industry_data, past_10d_date)
    
    # 存储结果
    ind_results[industry] = {
        'latest_date': latest_date,
        'ts_code': ts_code,
        'latest_net_amount': latest_net_amount,
        'percentile_rank': percentile_rank,
        'percentile_range': percentile_range,
        'past_3d_range': past_3d_range,
        'past_5d_range': past_5d_range,
        'past_10d_range': past_10d_range,
        'percentiles': percentiles
    }

# 转换为DataFrame便于展示，使用中文字段名
ind_rank = pd.DataFrame([
    {
        '行业': industry,
        '板块代码': data['ts_code'],
        '交易日期': data['latest_date'],
        '净额(亿元)': data['latest_net_amount'],
        '百分位排名': data['percentile_rank'],
        '所处区间': data['percentile_range'],
        '过去3日所处区间': data['past_3d_range'],
        '过去5日所处区间': data['past_5d_range'],
        '过去10日所处区间': data['past_10d_range'],
    }
    for industry, data in ind_results.items()
])

# 按照分位数排序
ind_rank = ind_rank.sort_values('百分位排名', ascending=False).round(2)


