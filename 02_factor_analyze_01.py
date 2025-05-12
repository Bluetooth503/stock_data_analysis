# -*- coding: utf-8 -*-
from common import *
from scipy.stats import spearmanr
import statsmodels.api as sm

'''
因子描述：
1.3秒快照平均bid_ask_ratio
2.一分钟累计bid_ask_ratio
'''

# ================================= 读取配置文件 =================================
logger = setup_logger()
config = load_config()
engine = create_engine(get_pg_connection_string(config))
path = r"D:\a_stock_data\factor01\parquet"
os.chdir(path)

# ================================= 计算指标 =================================
def process_per_stock():
    """按股票循环处理，将数据保存为parquet文件"""
    os.makedirs(path, exist_ok=True)
    output_file = os.path.join(path, 'factor01.parquet')
    sql_stocks = text("""SELECT DISTINCT ts_code FROM public.a_stock_qmt_sector WHERE index_name = '沪深A股' ORDER BY ts_code""")
    
    all_data = []
    with engine.connect() as conn:
        stocks = pd.read_sql(sql_stocks, conn)['ts_code'].tolist()
        total_stocks = len(stocks)
        
        for idx, stock in enumerate(stocks, 1):
            logger.info(f"处理进度: {idx}/{total_stocks} - {stock}")
            # 查询单个股票的数据
            sql = text("""
                SELECT 
                    trade_time,
                    ts_code,
                    avg_bid_ask_ratio,
                    max_bid_ask_ratio,
                    min_bid_ask_ratio,
                    total_bid_ask_ratio,
                    close
                FROM
                (
                    SELECT
                        time_bucket('1 minutes', "trade_time") AS trade_time,
                        ts_code,
                        FIRST(open, "trade_time") AS open,
                        MAX(high) AS high,
                        MIN(low) AS low,
                        LAST(last_price, "trade_time") AS close,
                        LAST(volume, "trade_time") - FIRST(volume, "trade_time") AS volume,
                        LAST(amount, "trade_time") - FIRST(amount, "trade_time") AS amount,
                        LAST(transaction_num, "trade_time") - FIRST(transaction_num, "trade_time") AS transaction_num,
                        AVG((bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5)*1.0 / 
                            NULLIF(ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5, 0)*1.0) AS avg_bid_ask_ratio,
                        MAX((bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5)*1.0 / 
                            NULLIF(ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5, 0)*1.0) AS max_bid_ask_ratio,
                        MIN((bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5)*1.0 / 
                            NULLIF(ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5, 0)*1.0) AS min_bid_ask_ratio,
                        CASE WHEN SUM(ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5) = 0 THEN 0
                            ELSE SUM(bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5)*1.0 /
                                SUM(ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5)*1.0
                        END AS total_bid_ask_ratio
                    FROM public.ods_a_stock_level1_data
                    WHERE ts_code = :stock
                    GROUP BY time_bucket('1 minutes', "trade_time"), ts_code
                    ORDER BY trade_time
                ) t1
            """)
            # 读取数据到DataFrame
            df = pd.read_sql(sql, conn, params={'stock': stock})
            if not df.empty:
                # 确保trade_time列是datetime类型，并设置正确的时区
                df['trade_time'] = pd.to_datetime(df['trade_time'])
                all_data.append(df)
            else:
                logger.warning(f"{stock} 没有数据")
        
        if all_data:
            # 合并所有数据
            final_df = pd.concat(all_data, ignore_index=True)
            final_df = final_df.sort_values(by=['ts_code', 'trade_time'])
            final_df.to_parquet(output_file, compression='zstd', index=False)
            logger.info("所有股票数据处理完成并保存为parquet文件")
        else:
            logger.warning("没有数据需要保存")

# ================================= 数据读取功能 =================================
def read_factor_data(file='factor01.parquet', stock_codes=None):
    """读取parquet文件并进行初步排序"""
    df = pd.read_parquet(file)
    df = df.sort_values(by=['ts_code', 'trade_time'])
    return df

# ================================= 横截面 IC 分析 =================================
def calc_cross_sectional_ic(df, factor_col, return_col='future_return'):
    """计算指定因子的横截面 IC 序列（Spearman rank correlation）"""
    ic_list = []
    for dt, group in df.groupby('trade_time'):
        # 确保因子列和收益率列都存在
        if factor_col not in group.columns or return_col not in group.columns:
            logger.warning(f"在时间点 {dt}，因子列 {factor_col} 或收益率列 {return_col} 不存在，跳过IC计算。")
            ic_list.append(np.nan)
            continue

        # 提取因子和收益率数据，并移除包含NaN的行对
        valid_data = group[[factor_col, return_col]].dropna()
        
        # 检查是否有足够的数据点进行相关性计算
        if len(valid_data) < 2 or valid_data[factor_col].nunique() < 2 or valid_data[return_col].nunique() < 2:
            ic_list.append(np.nan)
            continue
        
        # 计算Spearman相关系数
        ic, _ = spearmanr(valid_data[factor_col], valid_data[return_col])
        ic_list.append(ic)
    return pd.Series(ic_list, name=factor_col)

def eval_ic_stats(ic_series):
    """计算 IC 均值、IC 标准差和 IR（信息比率）"""
    valid_series = ic_series.dropna()
    if len(valid_series) == 0:
        return np.nan, np.nan, np.nan
    mean_ic = valid_series.mean()
    std_ic = valid_series.std()
    ir = mean_ic / std_ic if std_ic != 0 else np.nan
    return mean_ic, std_ic, ir

def eval_ic_decay(df, factor_col, periods, base_return_col='close'):
    """计算不同未来周期（如1,5,15分钟）的平均 IC（IC 衰减分析）"""
    decay = {}
    for p in periods:
        ret_col = f'future_return_{p}min'
        # 确保收益率列已存在，由 evaluate_factors 函数准备
        if ret_col not in df.columns:
            logger.error(f"收益率列 {ret_col} 未在DataFrame中找到，请确保 evaluate_factors 函数已正确计算。")
            decay[p] = np.nan
            continue
        ic_series = calc_cross_sectional_ic(df, factor_col, return_col=ret_col)
        decay[p] = ic_series.mean()
    return decay

def eval_group_return_and_monotonicity(df, factor_col, return_col='future_return', n_quantiles=5):
    """计算分组后 Q1/Q5 平均收益，并判断分组收益序列是否单调"""
    group_matrix = []
    for dt, group in df.groupby('trade_time'):
        if group[factor_col].nunique() < n_quantiles:
            continue
        g = group.copy()
        try:
            g['group'] = pd.qcut(g[factor_col], q=n_quantiles, labels=False, duplicates='drop')
            means = g.groupby('group')[return_col].mean().values
            if len(means) == n_quantiles:
                group_matrix.append(means)
        except ValueError as e:
            logger.warning(f"时间点 {dt} 的分组失败: {str(e)}")
            continue
    if not group_matrix:
        return np.nan, np.nan, False
    arr = np.vstack(group_matrix)  # shape: (num_slices, n_quantiles)
    avg_returns = np.nanmean(arr, axis=0)
    q1, q5 = avg_returns[0], avg_returns[-1]
    # 判断是否严格单调增或单调减
    diff = np.diff(avg_returns)
    monotonic = bool(np.all(diff > 0) or np.all(diff < 0))
    return q1, q5, monotonic

def eval_regression(df, factor_col, return_col='future_return'):
    """对整个样本做单因子回归，返回：coef（回归系数），pval（p-value）"""
    data = df.dropna(subset=[factor_col, return_col])
    X = sm.add_constant(data[factor_col])
    y = data[return_col]
    model = sm.OLS(y, X).fit()
    return model.params[factor_col], model.pvalues[factor_col]

def evaluate_factors(df, factor_cols, periods=[1, 5, 15]):
    """批量评估多个因子，结果写入 CSV"""
    # 默认的5分钟未来收益
    df['future_return'] = df.groupby('ts_code')['close'].shift(-5) / df['close'] - 1
    df = df.dropna(subset=['future_return']) # 在计算完默认收益后进行dropna

    # 用于IC衰减分析的未来收益率
    for p in periods:
        ret_col = f'future_return_{p}min'
        if ret_col not in df.columns: # 避免重复计算，如果已存在则跳过（例如 p=5 时）
            df[ret_col] = df.groupby('ts_code')['close'].shift(-p) / df['close'] - 1

    results = []
    for f in factor_cols:
        # 1. IC 序列及统计
        ic_series = calc_cross_sectional_ic(df, f)
        ic_mean, ic_std, ir = eval_ic_stats(ic_series)
        # 2. IC 衰减
        decay = eval_ic_decay(df, f, periods)
        # 3. 分组收益与单调性
        q1, q5, mono = eval_group_return_and_monotonicity(df, f)
        # 4. 回归系数与 p 值
        coef, pval = eval_regression(df, f)

        row = {
            '因子名称':            f,
            'IC均值':             ic_mean,
            'IC标准差':           ic_std,
            'IR': ir,
            **{f'IC_{p}min': decay[p] for p in periods},
            '分组收益_Q1':         q1,
            '分组收益_Q5':         q5,
            '单调性（Y/N）':       'Y' if mono else 'N',
            '回归系数':           coef,
            'p值':                pval
        }
        results.append(row)

    res_df = pd.DataFrame(results)
    res_df.to_csv('factor_evaluation.csv', index=False, encoding='utf-8-sig')
    return res_df



if __name__ == '__main__':
    # 1.计算因子
    process_per_stock()

    # 2.读取因子数据集
    df = read_factor_data()

    # 3.执行评估
    factor_list = [
        'avg_bid_ask_ratio',
        'max_bid_ask_ratio',
        'min_bid_ask_ratio',
        'total_bid_ask_ratio'
    ]
    result_df = evaluate_factors(df, factor_list)

    # 4. 查看结果
    print(result_df)
    print("已保存至 factor01_evaluation.csv")
