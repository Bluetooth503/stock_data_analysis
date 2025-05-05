# -*- coding: utf-8 -*-
from common import *
import backtrader as bt
import quantstats_lumi as qs
import multiprocessing as mp
import optuna
from numba import jit, prange
import pandas_ta as ta

# ================================= 读取配置文件 =================================
config = load_config()
engine = create_engine(get_pg_connection_string(config))

# ================================= 配置日志 =================================
logger = setup_logger()

# ================================= 参数设置 =================================
START_DATE = '2000-01-01'
END_DATE   = '2025-04-30'
PERIOD_RANGE = np.arange(8, 168, 8)    # 20个离散点
MULTIPLIER_RANGE = np.arange(2, 7.5, 0.5)  # 12个离散点
MAX_PROCESSES = max(1, mp.cpu_count() - 1)  # 保留一个CPU核心
N_TRIALS = 200  # Optuna优化的试验次数
N_STARTUP_TRIALS = 50  # 随机采样的试验次数
N_WARMUP_STEPS = 5  # 预热步数，用于修剪机制

# ================================= ha_st计算 =================================
def ha_st_pandas_ta(df, length, multiplier):
    '''pandas-ta计算'''
    df = df.copy(deep=False)
    length = int(round(length))
    multiplier = float(multiplier)
    df.ta.ha(append=True)
    ha_ohlc = {"HA_open": "ha_open", "HA_high": "ha_high", "HA_low": "ha_low", "HA_close": "ha_close"}
    df.rename(columns=ha_ohlc, inplace=True)
    supertrend_df = ta.supertrend(df['ha_high'], df['ha_low'], df['ha_close'], length, multiplier)
    supert_col = f'SUPERT_{length}_{multiplier}'
    direction_col = f'SUPERTd_{length}_{multiplier}'
    df['supertrend'] = supertrend_df[supert_col]
    df['direction'] = supertrend_df[direction_col]
    return df

# 使用Numba加速计算日度收益率
@jit(nopython=True, parallel=True, fastmath=True)
def _calculate_daily_returns_numba(dates, values):
    """Numba加速版本的日度收益率计算"""
    unique_dates = np.unique(dates)
    result = np.zeros(len(unique_dates))

    for i in prange(len(unique_dates)):  # 使用并行循环
        date = unique_dates[i]
        # 找出当天的所有收益率
        day_indices = np.where(dates == date)[0]
        day_returns = values[day_indices]

        # 计算当天的复合收益率 - 使用向量化操作
        if len(day_returns) > 0:
            day_compound = np.prod(1.0 + day_returns) - 1.0
            result[i] = day_compound
        else:
            result[i] = 0.0

    return unique_dates, result

# ================================= 取数 =================================
def get_30m_kline_data(ts_code, start_date=None, end_date=None):
    """从PostgreSQL数据库获取股票数据"""
    query = f"""
        SELECT trade_time, ts_code, open, high, low, close, volume, amount
        FROM a_stock_30m_kline_wfq_baostock
        WHERE ts_code = '{ts_code}'
        AND trade_time >= '{start_date}' AND trade_time <= '{end_date}'
        ORDER BY trade_time
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    # 删除任何包含 NaN 的行
    df = df.dropna()
    df['trade_time'] = pd.to_datetime(df['trade_time'])
    df.set_index('trade_time', inplace=True)
    return df

# ================================= 函数定义 =================================
class HeikinAshiData(bt.feeds.PandasData):
    lines = ('direction', 'supertrend',)
    params = (
        ('datetime', None),
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'volume'),
        ('direction', 'direction'),
        ('supertrend', 'supertrend'),
        ('openinterest', None),
    )

class HeikinAshiSuperTrendStrategy(bt.Strategy):
    params = (
        ('supertrend_period', 10),
        ('supertrend_multiplier', 3),
    )

    def __init__(self):
        self.direction = self.data.lines.direction
        self.in_position = False

    def next(self):
        if not self.in_position and self.direction[0] == 1:
            size = int((self.broker.get_cash() * 0.95) / self.data.close[0])
            self.buy(size=size)
            self.in_position = True
        elif self.in_position and self.direction[0] == -1:
            self.close()
            self.in_position = False

def calculate_daily_returns(returns):
    """计算日度收益率"""
    returns.index = pd.to_datetime(returns.index)

    try:
        # 准备数据
        dates = np.array([d.date().toordinal() for d in returns.index])
        values = returns.values

        # 调用Numba加速函数
        unique_dates, result = _calculate_daily_returns_numba(dates, values)

        # 转换回pandas
        daily_returns = pd.Series(
            result,
            index=[datetime.fromordinal(int(d)) for d in unique_dates]
        )
        daily_returns.index = pd.to_datetime(daily_returns.index)
        return daily_returns
    except Exception as e:
        # 如果Numba版本失败，回退到原始版本
        logger.warning(f"Numba加速失败，使用原始方法: {str(e)}")
        daily_returns = (1 + returns).groupby(returns.index.date).prod() - 1
        daily_returns.index = pd.to_datetime(daily_returns.index)
        return daily_returns

def objective(trial, df):
    """Optuna优化目标函数"""
    # 从离散参数空间中选择参数
    period_idx = trial.suggest_int('period_idx', 0, len(PERIOD_RANGE)-1)
    multiplier_idx = trial.suggest_int('multiplier_idx', 0, len(MULTIPLIER_RANGE)-1)
    period = PERIOD_RANGE[period_idx]
    multiplier = MULTIPLIER_RANGE[multiplier_idx]

    # 运行回测 - 避免不必要的深拷贝
    df_copy = df.copy(deep=False)  # 使用浅拷贝提高性能
    df_copy = ha_st_pandas_ta(df_copy, period, multiplier)

    # 配置回测引擎 - 使用最小化配置
    cerebro = bt.Cerebro(stdstats=False)  # 禁用标准统计以提高性能
    data = HeikinAshiData(dataname=df_copy)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000)
    cerebro.broker.setcommission(commission=0.0003)
    cerebro.addstrategy(HeikinAshiSuperTrendStrategy,
                      supertrend_period=period,
                      supertrend_multiplier=multiplier)
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn')

    # 运行回测
    results = cerebro.run(runonce=True, quicknotify=True)  # 使用runonce和quicknotify提高性能
    strat = results[0]
    returns = pd.Series(strat.analyzers.timereturn.get_analysis())

    # 计算日度收益率
    daily_returns = calculate_daily_returns(returns)

    # 计算Calmar比率
    try:
        calmar = qs.stats.calmar(daily_returns)
        return calmar if not np.isnan(calmar) else float('-inf')
    except:
        return float('-inf')

def run_backtest(df, period, multiplier):
    """运行回测并返回收益序列"""
    df = ha_st_pandas_ta(df, period, multiplier)

    cerebro = bt.Cerebro()
    data = HeikinAshiData(dataname=df)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000)
    cerebro.broker.setcommission(commission=0.0003)
    cerebro.addstrategy(HeikinAshiSuperTrendStrategy,
                       supertrend_period=period,
                       supertrend_multiplier=multiplier)
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn')

    results = cerebro.run()
    strat = results[0]
    returns = pd.Series(strat.analyzers.timereturn.get_analysis())
    daily_returns = calculate_daily_returns(returns)
    return daily_returns

def optimize_parameters(df):
    """使用Optuna进行参数优化"""
    # 创建修剪器 - 使用更激进的修剪策略
    pruner = optuna.pruners.HyperbandPruner(
        min_resource=N_WARMUP_STEPS,
        max_resource=N_TRIALS,
        reduction_factor=5
    )

    # 创建学习器 - 使用更高效的采样器
    study = optuna.create_study(
        direction='maximize',
        pruner=pruner,
        sampler=optuna.samplers.TPESampler(
            seed=42, 
            n_startup_trials=N_STARTUP_TRIALS,
            multivariate=True)
    )

    study.optimize(
        lambda trial: objective(trial, df),
        n_trials=N_TRIALS,
        n_jobs=1  # 外层已使用多进程，内层使用单线程避免资源竞争
    )

    # 获取最佳参数
    period_idx = study.best_params['period_idx']
    multiplier_idx = study.best_params['multiplier_idx']
    best_params = {
        'supertrend_period': int(PERIOD_RANGE[period_idx]),
        'supertrend_multiplier': float(MULTIPLIER_RANGE[multiplier_idx])
    }
    best_score = study.best_value

    return best_params, best_score

def optimize_stock(stock_code):
    """对单个股票进行参数优化"""
    try:
        logger.info(f'开始处理股票: {stock_code}')
        os.makedirs('reports', exist_ok=True)
        df = get_30m_kline_data(stock_code, START_DATE, END_DATE)

        # 使用Optuna进行参数优化
        best_params, best_score = optimize_parameters(df)
        logger.info(f'股票 {stock_code} 最佳参数: {best_params}, score={best_score:.4f}')

        # 使用最佳参数进行最终回测
        daily_returns = run_backtest(df, best_params['supertrend_period'], best_params['supertrend_multiplier'])
        daily_returns.name = 'SuperTrend'

        # 获取基准数据
        benchmark_sql = f"""
        SELECT trade_date as trade_time, close
        FROM a_index_1day_kline_baostock
        WHERE ts_code = '000300.SH'
        AND trade_date >= '{START_DATE}' AND trade_date <= '{END_DATE}'
        ORDER BY trade_date
        """
        benchmark_df = pd.read_sql(benchmark_sql, engine)
        benchmark_df['trade_time'] = pd.to_datetime(benchmark_df['trade_time'])
        benchmark_df.set_index('trade_time', inplace=True)

        # 计算基准日度收益率
        benchmark_returns = benchmark_df['close'].pct_change()
        benchmark_returns.name = '000300.SH'

        # 确保两个时间序列的索引对齐
        aligned_dates = daily_returns.index.intersection(benchmark_returns.index)
        daily_returns = daily_returns[aligned_dates]
        benchmark_returns = benchmark_returns[aligned_dates]

        # 删除任何包含NaN的数据
        valid_data = ~(daily_returns.isna() | benchmark_returns.isna())
        daily_returns = daily_returns[valid_data]
        benchmark_returns = benchmark_returns[valid_data]

        # 确保数据类型一致
        daily_returns = daily_returns.astype(float)
        benchmark_returns = benchmark_returns.astype(float)

        # 生成quantstats报告
        report_title = f'{stock_code} 策略回测报告(基准:沪深300)'
        report_file = os.path.join('reports', f'{stock_code}_report.html')

        qs.reports.html(
            daily_returns,
            benchmark=benchmark_returns,
            output=report_file,
            title=report_title,
            download_filename=f'{stock_code}_report.html'
            # sharpe=True,  # 启用夏普比率计算
            # periods=252,  # 设置年化周期
            # rf=0.0,      # 设置无风险利率
            # annualize=True  # 启用年化
        )

        # 计算指标 - 确保所有值都是标量
        try:
            metrics = {
                'ts_code': stock_code,
                'period': best_params["supertrend_period"],
                'multiplier': best_params["supertrend_multiplier"],
                # 风险调整收益
                'sharpe': float(qs.stats.sharpe(daily_returns)),
                'sortino': float(qs.stats.sortino(daily_returns)),
                'calmar': float(qs.stats.calmar(daily_returns)),
                'adjusted_sortino': float(qs.stats.adjusted_sortino(daily_returns)),
                'gain_to_pain_ratio': float(qs.stats.gain_to_pain_ratio(daily_returns)),
                'risk_of_ruin': float(qs.stats.risk_of_ruin(daily_returns)),
                'risk_return_ratio': float(qs.stats.risk_return_ratio(daily_returns)),
                # 胜率 & 盈亏比
                'win_rate': float(qs.stats.win_rate(daily_returns) * 100),
                'profit_factor': float(qs.stats.profit_factor(daily_returns)),
                'profit_ratio': float(qs.stats.profit_ratio(daily_returns)),
                'win_loss_ratio': float(qs.stats.win_loss_ratio(daily_returns)),
                'payoff_ratio': float(qs.stats.payoff_ratio(daily_returns)),
                'consecutive_losses': int(qs.stats.consecutive_losses(daily_returns)),
                'consecutive_wins': int(qs.stats.consecutive_wins(daily_returns)),
                # 收益相关指标
                'avg_return': float(qs.stats.avg_return(daily_returns) * 100),
                'avg_win': float(qs.stats.avg_win(daily_returns) * 100),
                'avg_loss': float(qs.stats.avg_loss(daily_returns) * 100),
                'best': float(qs.stats.best(daily_returns) * 100),
                'worst': float(qs.stats.worst(daily_returns) * 100),
                'expected_return': float(qs.stats.expected_return(daily_returns) * 100),
                'expected_shortfall': float(qs.stats.expected_shortfall(daily_returns)),
                'rar': float(qs.stats.rar(daily_returns)),
                # 风险 & 回撤
                'volatility': float(qs.stats.volatility(daily_returns) * 100),
                'max_drawdown': float(qs.stats.max_drawdown(daily_returns) * 100),
                'ulcer_index': float(qs.stats.ulcer_index(daily_returns)),
                'ulcer_performance_index': float(qs.stats.ulcer_performance_index(daily_returns)),
                'value_at_risk': float(qs.stats.value_at_risk(daily_returns)),
                'tail_ratio': float(qs.stats.tail_ratio(daily_returns)),
                'recovery_factor': float(qs.stats.recovery_factor(daily_returns)),
                # 年化收益 & 复利相关
                'cagr': float(qs.stats.cagr(daily_returns) * 100),
                # 确保正确处理compsum返回的Series
                'compsum': float(qs.stats.compsum(daily_returns).iloc[-1] * 100 if len(qs.stats.compsum(daily_returns)) > 0 else 0.0),
                # 统计指标
                'skew': float(qs.stats.skew(daily_returns)),
                'kurtosis': float(qs.stats.kurtosis(daily_returns)),
                'outlier_loss_ratio': float(qs.stats.outlier_loss_ratio(daily_returns)),
                'outlier_win_ratio': float(qs.stats.outlier_win_ratio(daily_returns)),
                'geometric_mean': float(qs.stats.geometric_mean(daily_returns) * 100),
                # 组合管理 & 风险衡量
                'cpc_index': float(qs.stats.cpc_index(daily_returns)),
                'kelly_criterion': float(qs.stats.kelly_criterion(daily_returns)),
                'common_sense_ratio': float(qs.stats.common_sense_ratio(daily_returns)),
                'exposure': float(qs.stats.exposure(daily_returns) * 100),
                'ghpr': float(qs.stats.ghpr(daily_returns)),
                # 需要基准数据的指标
                'information_ratio': float(qs.stats.information_ratio(daily_returns, benchmark_returns)),
                'r_squared': float(qs.stats.r_squared(daily_returns, benchmark_returns))
            }
        except Exception as e:
            logger.warning(f"计算指标时出现错误: {str(e)}，使用简化指标")
            # 如果出现错误，使用简化的指标集
            metrics = {
                'ts_code': stock_code,
                'period': best_params["supertrend_period"],
                'multiplier': best_params["supertrend_multiplier"],
                'calmar': float(best_score)
            }

        return metrics

    except Exception as e:
        logger.error(f'处理股票 {stock_code} 时发生错误: {str(e)}')
        return None

def get_stock_codes() -> list:
    """获取需要处理的股票代码列表"""
    try:
        query = """
            SELECT DISTINCT ts_code 
            FROM a_stock_qmt_sector 
            WHERE index_name IN ('上证50', '沪深300', '中证500', '中证1000')
        """
        with engine.connect() as conn:
            result = conn.execute(text(query)).mappings()
            return [row['ts_code'] for row in result]
    except Exception as e:
        logger.error(f'获取股票代码失败: {str(e)}')
        raise


def run_parallel_processing(stock_codes: list) -> list:
    """并行处理股票优化任务"""
    with mp.Pool(processes=MAX_PROCESSES) as pool:
        return [result for result in pool.imap_unordered(optimize_stock, stock_codes)
                if isinstance(result, dict)]


def save_results(results: list) -> None:
    """保存优化结果到文件"""
    if not results:
        logger.warning('没有成功处理任何股票')
        return

    try:
        results_df = pd.DataFrame(results)
        results_file = os.path.join('reports', 'Heikin_Ashi_SuperTrend_Metrics.csv')
        results_df.to_csv(results_file, index=False)
        logger.info(f'结果已保存到 {results_file}，共处理成功 {len(results)} 只股票')
    except Exception as e:
        logger.error(f'保存结果时发生错误: {str(e)}')
        with open(os.path.join('reports', 'raw_results.txt'), 'w') as f:
            f.writelines(str(r) + '\n' for r in results)


def main():
    """主流程控制函数"""
    try:
        stock_codes = get_stock_codes()
        logger.info(f'从a_stock_qmt_sector表中读取到 {len(stock_codes)} 只股票')
        
        results = run_parallel_processing(stock_codes)
        save_results(results)

    except Exception as e:
        logger.error(f'处理过程中发生错误: {str(e)}')

if __name__ == '__main__':
    main()