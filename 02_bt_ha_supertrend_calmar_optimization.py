# -*- coding: utf-8 -*-
from common import *
import backtrader as bt
import quantstats_lumi as qs
import multiprocessing as mp
from sklearn.base import BaseEstimator
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import Matern
from scipy.stats import norm

# ================================= 读取配置文件 =================================
config = load_config()
engine = create_engine(get_pg_connection_string(config))

# ================================= 配置日志 =================================
logger = setup_logger()

# ================================= 参数设置 =================================
START_DATE = '2000-01-01'
END_DATE   = '2025-03-01'
PERIOD_RANGE = np.arange(8, 168, 8)    # 20个离散点
MULTIPLIER_RANGE = np.arange(2, 7.5, 0.5)  # 12个离散点
MAX_PROCESSES = max(1, mp.cpu_count() - 1)  # 保留一个CPU核心
N_ITERATIONS = 50  # 贝叶斯优化的迭代次数
N_CANDIDATES = 200  # 每次迭代生成的候选点数量
UCB_KAPPA = 1.5  # UCB采集函数的置信区间参数

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
    # 将收益率转换为日度收益率
    daily_returns = (1 + returns).groupby(returns.index.date).prod() - 1
    daily_returns.index = pd.to_datetime(daily_returns.index)
    return daily_returns

class SuperTrendEstimator(BaseEstimator):
    def __init__(self, supertrend_period=10, supertrend_multiplier=3):
        self.supertrend_period = supertrend_period
        self.supertrend_multiplier = supertrend_multiplier
        self.df = None

    def fit(self, X, y=None):
        """设置数据"""
        self.df = X
        return self

    def score(self, X, y=None):
        """评估参数表现"""
        if self.df is None:
            self.df = X
            
        df = self.df.copy()
        df = ha_st_pandas_ta(df, self.supertrend_period, self.supertrend_multiplier)
        
        cerebro = bt.Cerebro()
        data = HeikinAshiData(dataname=df)
        cerebro.adddata(data)
        cerebro.broker.setcash(100000)
        cerebro.broker.setcommission(commission=0.0003)
        cerebro.addstrategy(HeikinAshiSuperTrendStrategy,
                          supertrend_period=self.supertrend_period,
                          supertrend_multiplier=self.supertrend_multiplier)
        cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn')
        
        results = cerebro.run()
        strat = results[0]
        returns = pd.Series(strat.analyzers.timereturn.get_analysis())
        daily_returns = calculate_daily_returns(returns)
        
        calmar = qs.stats.calmar(daily_returns)
        return calmar if not np.isnan(calmar) else float('-inf')

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

def bayesian_optimization(df, n_iterations=N_ITERATIONS):
    """使用贝叶斯优化寻找最佳参数"""
    kernel = Matern(nu=2.5)
    gpr = GaussianProcessRegressor(kernel=kernel, random_state=42)
    
    X = np.array([]).reshape(0, 2)
    y = np.array([])
    
    # 随机选择初始点
    x_init = np.array([
        [np.random.choice(PERIOD_RANGE),
         np.random.choice(MULTIPLIER_RANGE)]
    ])
    
    # 评估初始点
    estimator = SuperTrendEstimator(x_init[0][0], x_init[0][1])
    score = estimator.score(df)
    X = np.vstack((X, x_init))
    y = np.append(y, score)
    
    # 迭代优化
    for i in range(n_iterations - 1):
        gpr.fit(X, y)
        
        # 生成候选点
        x_candidates = np.array([
            [np.random.choice(PERIOD_RANGE),
             np.random.choice(MULTIPLIER_RANGE)]
            for _ in range(N_CANDIDATES)
        ])
        
        mu, sigma = gpr.predict(x_candidates, return_std=True)
        ucb = mu + UCB_KAPPA * sigma
        
        best_idx = np.argmax(ucb)
        x_next = x_candidates[best_idx]
        
        estimator = SuperTrendEstimator(x_next[0], x_next[1])
        score = estimator.score(df)
        
        X = np.vstack((X, x_next))
        y = np.append(y, score)
        
        # logger.info(f'迭代 {i+1}/{n_iterations}: period={x_next[0]}, multiplier={x_next[1]:.1f}, score={score:.4f}')
    
    best_idx = np.argmax(y)
    best_params = {
        'supertrend_period': int(X[best_idx][0]),
        'supertrend_multiplier': float(X[best_idx][1])
    }
    best_score = y[best_idx]
    
    return best_params, best_score

def optimize_stock(stock_code):
    """对单个股票进行参数优化"""
    try:
        logger.info(f'开始处理股票: {stock_code}')
        
        os.makedirs('reports', exist_ok=True)
        
        # 获取数据
        df = get_30m_kline_data('wfq', stock_code, START_DATE, END_DATE)
        df['trade_time'] = pd.to_datetime(df['trade_time'])
        df.set_index('trade_time', inplace=True)
        
        # 使用贝叶斯优化进行参数优化
        best_params, best_score = bayesian_optimization(df)
        logger.info(f'股票 {stock_code} 最佳参数: {best_params}, score={best_score:.4f}')
        
        # 使用最佳参数进行最终回测
        daily_returns = run_backtest(df, best_params['supertrend_period'], best_params['supertrend_multiplier'])
        daily_returns.name = 'SuperTrend'
        
        # 获取基准数据
        start_date_fmt = START_DATE.replace('-', '')
        end_date_fmt = END_DATE.replace('-', '')
        benchmark_sql = """
        SELECT trade_date AS trade_time, close 
        FROM a_index_1day_kline_baostock 
        WHERE ts_code = '000300.SH' 
        AND trade_date >= %s AND trade_date <= %s 
        ORDER BY trade_date
        """
        benchmark_df = pd.read_sql(benchmark_sql, engine, params=(start_date_fmt, end_date_fmt))
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
        
        # 计算指标
        metrics = {
            'ts_code': stock_code,
            'period': best_params["supertrend_period"],
            'multiplier': best_params["supertrend_multiplier"],
            # 风险调整收益
            # 'sharpe': qs.stats.sharpe(daily_returns, rf=0.0, periods=252, annualize=True),
            'sharpe': qs.stats.sharpe(daily_returns),
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
        
        return metrics
        
    except Exception as e:
        logger.error(f'处理股票 {stock_code} 时发生错误: {str(e)}')
        return None

def main():
    try:
        # 读取多个指数成分股列表并合并去重
        stock_list_dfs = []
        index_files = [
            '上证50_stock_list.csv',
            '沪深300_stock_list.csv', 
            '中证500_stock_list.csv',
            '中证1000_stock_list.csv'
        ]
        
        for file in index_files:
            df = pd.read_csv(file, header=None, names=['ts_code'])
            stock_list_dfs.append(df)
            
        stock_list_df = pd.concat(stock_list_dfs).drop_duplicates()
        stock_codes = stock_list_df['ts_code'].tolist()
        logger.info(f'共读取到 {len(stock_codes)} 只股票')

        # 创建进程池
        pool = mp.Pool(processes=MAX_PROCESSES)
        
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
            results_file = os.path.join('reports', 'Heikin_Ashi_SuperTrend_Metrics.csv')
            results_df.to_csv(results_file, index=False)
            logger.info(f'结果已保存到 {results_file}，共处理成功 {len(results)} 只股票')
        else:
            logger.warning('没有成功处理任何股票')
            
    except Exception as e:
        logger.error(f'处理过程中发生错误: {str(e)}')
        if 'pool' in locals():
            pool.close()
            pool.join()

if __name__ == '__main__':
    main()