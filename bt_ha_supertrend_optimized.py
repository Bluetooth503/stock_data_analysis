# -*- coding: utf-8 -*-
from common import *
import backtrader as bt
# import optuna
import quantstats_lumi as qs
from pymoo.core.problem import Problem
from pymoo.algorithms.moo.nsga2 import NSGA2
from pymoo.optimize import minimize
from pymoo.termination import get_termination
from pymoo.operators.sampling.rnd import FloatRandomSampling
from pymoo.operators.crossover.sbx import SBX
from pymoo.operators.mutation.pm import PM
from pymoo.config import Config


#################################
# 参数设置
#################################
# 回测标的
STOCK_CODE = '000858.SZ'

# 回测区间
START_DATE = '2000-01-01'
END_DATE   = '2024-12-31'

# SuperTrend参数优化范围
PERIOD_RANGE = np.arange(10, 101, 10)    # [10, 20, 30, 40, 50]
MULTIPLIER_RANGE = np.arange(1, 11, 1)    # [2, 3, 4, 5, 6]

# NSGA2算法参数
POPULATION_SIZE = 20
OFFSPRING_SIZE = 10
N_GENERATIONS = 10

#################################

# 创建数据库连接
config = load_config()
engine = create_engine(get_pg_connection_string(config))

# 禁用PyMoo的编译警告
Config.warnings['not_compiled'] = False

# Heikin-Ashi数据生成
class HeikinAshiData(bt.feeds.PandasData):
    params = (
        ('datetime', None),
        ('open', 'ha_open'),
        ('high', 'ha_high'),
        ('low', 'ha_low'),
        ('close', 'ha_close'),
        ('volume', 'volume'),
        ('openinterest', None),
    )

# SuperTrend指标
class SuperTrend(bt.Indicator):
    lines = ('supertrend', 'upband', 'downband')
    params = (
        ('period', 10),
        ('multiplier', 3),
    )

    def __init__(self):
        self.atr = bt.indicators.ATR(period=self.p.period)
        hl2 = (self.data.high + self.data.low) / 2
        self.upband = hl2 + (self.atr * self.p.multiplier)
        self.downband = hl2 - (self.atr * self.p.multiplier)
        self.uptrend = True
        self.l.supertrend = self.upband

    def next(self):
        curr_close = self.data.close[0]
        prev_supertrend = self.l.supertrend[-1]
        curr_upband = self.upband[0]
        curr_downband = self.downband[0]
        if self.uptrend:
            self.l.supertrend[0] = max(curr_downband, prev_supertrend)
            if curr_close < self.l.supertrend[0]:
                self.uptrend = False
        else:
            self.l.supertrend[0] = min(curr_upband, prev_supertrend)
            if curr_close > self.l.supertrend[0]:
                self.uptrend = True

# 交易策略
class HeikinAshiSuperTrendStrategy(bt.Strategy):
    params = (
        ('supertrend_period', 10),
        ('supertrend_multiplier', 3),
    )

    def __init__(self):
        self.supertrend = SuperTrend(
            period=self.p.supertrend_period,
            multiplier=self.p.supertrend_multiplier
        )
        self.in_position = False

    def next(self):
        if not self.in_position and self.data.close[0] > self.supertrend.supertrend[0]:
            size = int((self.broker.get_cash() * 0.95) / self.data.close[0])
            self.buy(size=size)
            self.in_position = True
        elif self.in_position and self.data.close[0] < self.supertrend.supertrend[0]:
            self.close()
            self.in_position = False

# 定义多目标优化问题
class TradingProblem(Problem):
    def __init__(self, stock_code, start_date, end_date):
        # 先设置参数范围
        self.period_values = PERIOD_RANGE
        self.multiplier_values = MULTIPLIER_RANGE
        
        # 根据参数范围的长度自动设置上下界
        super().__init__(
            n_var=2,  # 两个变量：period和multiplier
            n_obj=2,  # 两个目标：胜率和盈亏比
            n_constr=0,  # 没有约束
            xl=np.array([0, 0]),  # 下界固定为0
            xu=np.array([len(self.period_values)-1, len(self.multiplier_values)-1]),  # 上界根据参数范围长度自动设置
            vtype=np.int32  # 设置变量类型为整数
        )
        self.stock_code = stock_code
        self.start_date = start_date
        self.end_date = end_date
        
        # 在初始化时获取数据，避免重复请求
        print('正在获取股票数据...')
        self.df = get_30m_kline_data('qfq', self.stock_code, self.start_date, self.end_date)
        self.df = prepare_heikin_ashi_data(self.df)
        
        # 在初始化时获取基准数据
        print('正在获取基准数据...')
        benchmark_sql = """
        SELECT trade_date, close 
        FROM a_index_1day_kline_baostock 
        WHERE ts_code = '000300.SH' 
        AND trade_date BETWEEN %s AND %s 
        ORDER BY trade_date
        """
        self.benchmark_df = pd.read_sql(benchmark_sql, engine, params=(start_date, end_date))
        self.benchmark_df.set_index('trade_date', inplace=True)
        self.benchmark_returns = self.benchmark_df['close'].pct_change()
        self.benchmark_returns.index = pd.to_datetime(self.benchmark_returns.index)
        self.benchmark_returns.name = '000300.SH'

    def _evaluate(self, x, out, *args, **kwargs):
        F = np.zeros((x.shape[0], 2))
        
        for i in range(x.shape[0]):
            # 从索引获取实际参数值
            period = self.period_values[int(x[i, 0])]
            multiplier = self.multiplier_values[int(x[i, 1])]
            
            cerebro = bt.Cerebro()
            data = HeikinAshiData(dataname=self.df)  # 使用已获取的数据
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
            
            # 计算目标值
            win_rate = qs.stats.win_rate(daily_returns)
            profit_factor = qs.stats.profit_factor(daily_returns)
            
            # 由于pymoo是最小化问题，所以取负值
            F[i, 0] = -win_rate
            F[i, 1] = -profit_factor
        
        out["F"] = F

def prepare_heikin_ashi_data(df):
    """计算Heikin-Ashi数据并返回更新后的DataFrame"""
    df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    df['ha_open'] = df['open'].copy()
    for i in range(1, len(df)):
        df.loc[df.index[i], 'ha_open'] = (df.loc[df.index[i-1], 'ha_open'] + 
                                         df.loc[df.index[i-1], 'ha_close']) / 2
    df['ha_high'] = df[['high', 'ha_open', 'ha_close']].max(axis=1)
    df['ha_low'] = df[['low', 'ha_open', 'ha_close']].min(axis=1)
    return df

if __name__ == '__main__':
    # 创建问题实例
    problem = TradingProblem(STOCK_CODE, START_DATE, END_DATE)
    
    # 创建算法实例，添加采样、交叉和变异操作
    algorithm = NSGA2(
        pop_size=POPULATION_SIZE,
        n_offsprings=OFFSPRING_SIZE,
        sampling=FloatRandomSampling(),
        crossover=SBX(prob=0.9, eta=15),
        mutation=PM(eta=20),
        eliminate_duplicates=True
    )
    
    # 设置终止条件
    termination = get_termination("n_gen", N_GENERATIONS)
    
    # 运行优化
    print('正在运行多目标参数优化...')
    res = minimize(
        problem,
        algorithm,
        termination,
        seed=1,
        verbose=True
    )
    
    # 输出结果
    print("\n=== 优化结果 ===")
    print("最优解集合:")
    for i in range(len(res.X)):
        period_idx = int(res.X[i, 0])
        multiplier_idx = int(res.X[i, 1])
        period = problem.period_values[period_idx]
        multiplier = problem.multiplier_values[multiplier_idx]
        win_rate = -res.F[i, 0] * 100
        profit_factor = -res.F[i, 1]
        print(f"\n解 {i+1}:")
        print(f"参数: period={period}, multiplier={multiplier}")
        print(f"胜率: {win_rate:.2f}%")
        print(f"盈亏比: {profit_factor:.2f}")
    
    # 选择一个平衡的解进行回测
    balanced_idx = len(res.X) // 2  # 选择中间的解
    period_idx = int(res.X[balanced_idx, 0])
    multiplier_idx = int(res.X[balanced_idx, 1])
    best_params = {
        'supertrend_period': problem.period_values[period_idx],
        'supertrend_multiplier': problem.multiplier_values[multiplier_idx]
    }
    
    # 使用选定的参数进行回测
    cerebro = bt.Cerebro()
    data = HeikinAshiData(dataname=problem.df)  # 使用已获取的数据
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
    
    # 使用已获取的基准数据
    benchmark_returns = problem.benchmark_returns
    
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
    
    # 最后检查确保长度一致
    if len(daily_returns) != len(benchmark_returns):
        raise ValueError(f"数据长度不一致: 策略={len(daily_returns)}, 基准={len(benchmark_returns)}")
    
    # 计算并打印最终结果
    metrics = {
        'stock_code': STOCK_CODE,
        'period': best_params["supertrend_period"],
        'multiplier': best_params["supertrend_multiplier"],
        'win_rate': qs.stats.win_rate(daily_returns) * 100,
        'win_loss_ratio': qs.stats.win_loss_ratio(daily_returns),
        'avg_return': qs.stats.avg_return(daily_returns) * 100,
        'avg_win': qs.stats.avg_win(daily_returns) * 100,
        'avg_loss': qs.stats.avg_loss(daily_returns) * 100,
        'best': qs.stats.best(daily_returns) * 100,
        'worst': qs.stats.worst(daily_returns) * 100,
        'cagr': qs.stats.cagr(daily_returns) * 100,
        'calmar': qs.stats.calmar(daily_returns),
        'common_sense_ratio': qs.stats.common_sense_ratio(daily_returns),
        'compsum': qs.stats.compsum(daily_returns).iloc[-1] * 100,
        'consecutive_losses': qs.stats.consecutive_losses(daily_returns),
        'consecutive_wins': qs.stats.consecutive_wins(daily_returns),
        'cpc_index': qs.stats.cpc_index(daily_returns),
        'expected_return': qs.stats.expected_return(daily_returns) * 100,
        'expected_shortfall': qs.stats.expected_shortfall(daily_returns),
        'exposure': qs.stats.exposure(daily_returns) * 100,
        'gain_to_pain_ratio': qs.stats.gain_to_pain_ratio(daily_returns),
        'geometric_mean': qs.stats.geometric_mean(daily_returns) * 100,
        'ghpr': qs.stats.ghpr(daily_returns),
        'kelly_criterion': qs.stats.kelly_criterion(daily_returns),
        'kurtosis': qs.stats.kurtosis(daily_returns),
        'max_drawdown': qs.stats.max_drawdown(daily_returns) * 100,
        'outlier_loss_ratio': qs.stats.outlier_loss_ratio(daily_returns),
        'outlier_win_ratio': qs.stats.outlier_win_ratio(daily_returns),
        'payoff_ratio': qs.stats.payoff_ratio(daily_returns),
        'profit_factor': qs.stats.profit_factor(daily_returns),
        'profit_ratio': qs.stats.profit_ratio(daily_returns),
        'rar': qs.stats.rar(daily_returns),
        'recovery_factor': qs.stats.recovery_factor(daily_returns),
        'risk_of_ruin': qs.stats.risk_of_ruin(daily_returns),
        'risk_return_ratio': qs.stats.risk_return_ratio(daily_returns),
        'sharpe': qs.stats.sharpe(daily_returns),
        'skew': qs.stats.skew(daily_returns),
        'sortino': qs.stats.sortino(daily_returns),
        'adjusted_sortino': qs.stats.adjusted_sortino(daily_returns),
        'tail_ratio': qs.stats.tail_ratio(daily_returns),
        'ulcer_index': qs.stats.ulcer_index(daily_returns),
        'ulcer_performance_index': qs.stats.ulcer_performance_index(daily_returns),
        'value_at_risk': qs.stats.value_at_risk(daily_returns),
        'volatility': qs.stats.volatility(daily_returns) * 100,
        # 需要基准数据的指标
        'information_ratio': qs.stats.information_ratio(daily_returns, benchmark_returns),
        'r_squared': qs.stats.r_squared(daily_returns, benchmark_returns)
    }
    
    # 将指标保存为DataFrame并输出到CSV
    metrics_df = pd.DataFrame([metrics])
    csv_filename = f'{STOCK_CODE}_metrics.csv'
    metrics_df.to_csv(csv_filename, index=False)
    print(f'\n指标已保存到：{csv_filename}')
    
    # 打印主要指标
    print('\n=== 选定参数的回测结果 ===')
    print(f'参数: period={best_params["supertrend_period"]}, multiplier={best_params["supertrend_multiplier"]}')
    print(f'Sortino Ratio: {metrics["sortino"]:.2f}')
    print(f'累积收益率: {metrics["compsum"]:.2f}%')
    print(f'胜率: {metrics["win_rate"]:.2f}%')
    print(f'盈亏比: {metrics["profit_factor"]:.2f}')
    
    # 生成报告
    qs.reports.html(
        daily_returns, 
        benchmark=benchmark_returns,
        output=f'{STOCK_CODE}.html',
        download_filename=f'{STOCK_CODE}.html',
        title=f'{STOCK_CODE} 策略回测报告 (基准: 沪深300)'
    )
    print(f'\n已生成业绩报告：{STOCK_CODE}.html') 