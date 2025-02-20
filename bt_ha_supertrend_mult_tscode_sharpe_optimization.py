# -*- coding: utf-8 -*-
from common import *
import backtrader as bt
import quantstats_lumi as qs
import multiprocessing as mp
from functools import partial
import itertools
from scipy import stats


#################################
# 参数设置
#################################
# 回测时间范围
START_DATE = '2000-01-01'
END_DATE   = '2024-12-31'

# SuperTrend参数优化范围
PERIOD_RANGE = np.arange(8, 88, 8)     # [8, 16, 24, 32, 40, 48, 56, 64, 72, 80]
MULTIPLIER_RANGE = np.arange(2, 7, 1)  # [2, 3, 4, 5, 6]

# 并行处理参数
MAX_PROCESSES = max(1, mp.cpu_count() - 1)  # 保留一个CPU核心

#################################

# 创建数据库连接
config = load_config()
engine = create_engine(get_pg_connection_string(config))

# 新的heikin_ashi函数
def heikin_ashi(df):
    df.ta.ha(append=True)
    ha_ohlc = {"HA_open": "ha_open", "HA_high": "ha_high", "HA_low": "ha_low", "HA_close": "ha_close"}
    df.rename(columns=ha_ohlc, inplace=True)
    return df

def supertrend(df, length, multiplier):
    '''direction=1上涨，-1下跌'''
    supertrend_df = ta.supertrend(df['ha_high'], df['ha_low'], df['ha_close'], length, multiplier)
    df['supertrend'] = supertrend_df[f'SUPERT_{length}_{multiplier}.0']
    df['direction'] = supertrend_df[f'SUPERTd_{length}_{multiplier}.0']
    return df

# HeikinAshiData类
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

# 交易策略
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

def evaluate_parameters(stock_code, period, multiplier, start_date, end_date):
    """评估单个参数组合的表现"""
    try:
        # 获取数据
        df = get_30m_kline_data('wfq', stock_code, start_date, end_date)
        df['trade_time'] = pd.to_datetime(df['trade_time'])
        df.set_index('trade_time', inplace=True)
        df = heikin_ashi(df)
        df = supertrend(df, period, multiplier)
        
        # 运行回测
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
        
        # 将30分钟收益聚合为日度收益
        returns.index = pd.to_datetime(returns.index)
        daily_returns = (1 + returns).groupby(returns.index.date).prod() - 1
        daily_returns.index = pd.to_datetime(daily_returns.index)
        
        # 计算累积收益率
        cumulative_returns = (1 + daily_returns).cumprod() - 1
        
        # 计算累积收益率的斜率
        x = np.arange(len(cumulative_returns))
        slope, _, _, _, _ = stats.linregress(x, cumulative_returns.values)
        
        # 年化斜率（假设252个交易日）
        annualized_slope = slope * 252
        
        return {
            'period': period,
            'multiplier': multiplier,
            'slope': annualized_slope
        }
        
    except Exception as e:
        print(f'评估参数 (period={period}, multiplier={multiplier}) 时发生错误: {str(e)}')
        return None

def grid_search(stock_code, param_grid, start_date, end_date):
    """
    执行网格搜索来找到最优参数
    
    参数:
    - stock_code: 股票代码
    - param_grid: 参数网格，包含所有需要搜索的参数组合
    - start_date: 回测开始日期
    - end_date: 回测结束日期
    
    返回:
    - best_params: 最优参数组合
    - best_metrics: 最优参数组合对应的评估指标
    """
    print(f'开始网格搜索最优参数 - {stock_code}')
    print(f'参数空间大小: {len(param_grid)} 种组合')
    
    results = []
    for params in param_grid:
        result = evaluate_parameters(
            stock_code=stock_code,
            period=params[0],
            multiplier=params[1],
            start_date=start_date,
            end_date=end_date
        )
        if result is not None:
            results.append(result)
            print(f'评估参数: period={params[0]}, multiplier={params[1]}, slope={result["slope"]:.4f}')
    
    if not results:
        return None, None
    
    # 找到最佳参数组合
    best_result = max(results, key=lambda x: x['slope'])
    best_params = {
        'supertrend_period': best_result['period'],
        'supertrend_multiplier': best_result['multiplier']
    }
    
    print('\n=== 网格搜索结果 ===')
    print(f'最佳参数组合:')
    print(f'Period: {best_params["supertrend_period"]}')
    print(f'Multiplier: {best_params["supertrend_multiplier"]}')
    print(f'Slope: {best_result["slope"]:.4f}')
    
    return best_params, best_result

def optimize_stock(stock_code):
    """对单个股票进行参数优化"""
    try:
        print(f'开始处理股票: {stock_code}')
        
        # 创建reports目录（如果不存在）
        os.makedirs('reports', exist_ok=True)
        
        # 生成参数网格
        param_grid = list(itertools.product(PERIOD_RANGE, MULTIPLIER_RANGE))
        
        # 执行网格搜索
        best_params, best_result = grid_search(
            stock_code=stock_code,
            param_grid=param_grid,
            start_date=START_DATE,
            end_date=END_DATE
        )
        
        if best_params is None:
            print(f'股票 {stock_code} 没有找到有效的参数组合')
            return None
        
        # 使用最佳参数进行最终回测
        df = get_30m_kline_data('wfq', stock_code, START_DATE, END_DATE)
        df['trade_time'] = pd.to_datetime(df['trade_time'])
        df.set_index('trade_time', inplace=True)
        df = heikin_ashi(df)
        df = supertrend(df, best_params['supertrend_period'], best_params['supertrend_multiplier'])
        
        cerebro = bt.Cerebro()
        data = HeikinAshiData(dataname=df)
        cerebro.adddata(data)
        cerebro.broker.setcash(100000)
        cerebro.broker.setcommission(commission=0.0003)
        cerebro.addstrategy(HeikinAshiSuperTrendStrategy,
                           supertrend_period=best_params['supertrend_period'],
                           supertrend_multiplier=best_params['supertrend_multiplier'])
        cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn')
        
        results = cerebro.run()
        strat = results[0]
        returns = pd.Series(strat.analyzers.timereturn.get_analysis())
        
        # 将30分钟收益聚合为日度收益
        returns.index = pd.to_datetime(returns.index)
        daily_returns = (1 + returns).groupby(returns.index.date).prod() - 1
        daily_returns.index = pd.to_datetime(daily_returns.index)
        daily_returns.name = 'SuperTrend'
        
        # 获取基准数据
        benchmark_sql = """
        SELECT trade_time, close 
        FROM a_index_1day_kline_baostock 
        WHERE ts_code = '000300.SH' 
        AND trade_time BETWEEN %s AND %s 
        ORDER BY trade_time
        """
        benchmark_df = pd.read_sql(benchmark_sql, engine, params=(START_DATE, END_DATE))
        benchmark_df['trade_time'] = pd.to_datetime(benchmark_df['trade_time'])
        benchmark_df.set_index('trade_time', inplace=True)
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
        report_title = f'{stock_code} 策略回测报告 (基准: 沪深300)'
        report_file = os.path.join('reports', f'{stock_code}_report.html')
        
        try:
            qs.reports.html(
                daily_returns, 
                benchmark=benchmark_returns,
                output=report_file,
                title=report_title,
                download_filename=f'{stock_code}_report.html'
            )
            print(f'已生成 {stock_code} 的策略报告: {report_file}')
        except Exception as e:
            print(f'生成 {stock_code} 的策略报告时发生错误: {str(e)}')
        
        # 计算指标
        metrics = {
            'ts_code': stock_code,
            'period': best_params["supertrend_period"],
            'multiplier': best_params["supertrend_multiplier"],
            # 风险调整收益
            'sharpe': qs.stats.sharpe(daily_returns, rf=0.0, periods=252, annualize=True),
            'sortino': qs.stats.sortino(daily_returns),
            'calmar': qs.stats.calmar(daily_returns),
            'adjusted_sortino': qs.stats.adjusted_sortino(daily_returns),
            'gain_to_pain_ratio': qs.stats.gain_to_pain_ratio(daily_returns),
            'risk_of_ruin': qs.stats.risk_of_ruin(daily_returns),
            'risk_return_ratio': qs.stats.risk_return_ratio(daily_returns),
            # 胜率 & 盈亏比
            'win_rate': qs.stats.win_rate(daily_returns) * 100,
            'profit_factor': qs.stats.profit_factor(daily_returns),
            'profit_ratio': qs.stats.profit_ratio(daily_returns),
            'win_loss_ratio': qs.stats.win_loss_ratio(daily_returns),
            'payoff_ratio': qs.stats.payoff_ratio(daily_returns),
            'consecutive_losses': qs.stats.consecutive_losses(daily_returns),
            'consecutive_wins': qs.stats.consecutive_wins(daily_returns),
            # 收益相关指标
            'avg_return': qs.stats.avg_return(daily_returns) * 100,
            'avg_win': qs.stats.avg_win(daily_returns) * 100,
            'avg_loss': qs.stats.avg_loss(daily_returns) * 100,
            'best': qs.stats.best(daily_returns) * 100,
            'worst': qs.stats.worst(daily_returns) * 100,
            'expected_return': qs.stats.expected_return(daily_returns) * 100,
            'expected_shortfall': qs.stats.expected_shortfall(daily_returns),
            'rar': qs.stats.rar(daily_returns),
            # 风险 & 回撤
            'volatility': qs.stats.volatility(daily_returns) * 100,
            'max_drawdown': qs.stats.max_drawdown(daily_returns) * 100,
            'ulcer_index': qs.stats.ulcer_index(daily_returns),
            'ulcer_performance_index': qs.stats.ulcer_performance_index(daily_returns),
            'value_at_risk': qs.stats.value_at_risk(daily_returns),
            'tail_ratio': qs.stats.tail_ratio(daily_returns),
            'recovery_factor': qs.stats.recovery_factor(daily_returns),
            # 年化收益 & 复利相关
            'cagr': qs.stats.cagr(daily_returns) * 100,
            'compsum': qs.stats.compsum(daily_returns).iloc[-1] * 100,
            # 统计指标
            'skew': qs.stats.skew(daily_returns),
            'kurtosis': qs.stats.kurtosis(daily_returns),
            'outlier_loss_ratio': qs.stats.outlier_loss_ratio(daily_returns),
            'outlier_win_ratio': qs.stats.outlier_win_ratio(daily_returns),
            'geometric_mean': qs.stats.geometric_mean(daily_returns) * 100,
            # 组合管理 & 风险衡量
            'cpc_index': qs.stats.cpc_index(daily_returns),
            'kelly_criterion': qs.stats.kelly_criterion(daily_returns),
            'common_sense_ratio': qs.stats.common_sense_ratio(daily_returns),
            'exposure': qs.stats.exposure(daily_returns) * 100,
            'ghpr': qs.stats.ghpr(daily_returns),
            # 需要基准数据的指标
            'information_ratio': qs.stats.information_ratio(daily_returns, benchmark_returns),
            'r_squared': qs.stats.r_squared(daily_returns, benchmark_returns)
        }
        
        print(f'股票 {stock_code} 处理完成')
        return metrics
        
    except Exception as e:
        print(f'处理股票 {stock_code} 时发生错误: {str(e)}')
        return None

def main():
    # 读取股票列表
    try:
        stock_list_df = pd.read_csv('中证1000_stock_list.csv', header=None, names=['ts_code'])
        stock_codes = stock_list_df['ts_code'].tolist()
        print(f'共读取到 {len(stock_codes)} 只股票')
    except Exception as e:
        print(f'读取股票列表时发生错误: {str(e)}')
        return

    # 生成所有可能的参数组合
    param_grid = list(itertools.product(PERIOD_RANGE, MULTIPLIER_RANGE))
    
    # 创建进程池
    pool = mp.Pool(processes=MAX_PROCESSES)
    
    try:
        # 并行处理所有股票
        results = []
        for result in pool.imap_unordered(optimize_stock, stock_codes):
            if result is not None:
                results.append(result)
                
        # 关闭进程池
        pool.close()
        pool.join()
        
        # 将结果保存到CSV
        if results:
            results_df = pd.DataFrame(results)
            results_df.to_csv('Heikin_Ashi_SuperTrend_Metrics.csv', index=False)
            print(f'结果已保存到 Heikin_Ashi_SuperTrend_Metrics.csv，共处理成功 {len(results)} 只股票')
        else:
            print('没有成功处理任何股票')
            
    except Exception as e:
        print(f'处理过程中发生错误: {str(e)}')
        pool.close()
        pool.join()

if __name__ == '__main__':
    main()
