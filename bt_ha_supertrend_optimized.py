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
PERIOD_RANGE = np.arange(10, 101, 10)     # [10, 20, 30, 40, 50]
MULTIPLIER_RANGE = np.arange(1, 11, 1)    # [2, 3, 4, 5, 6]

# NSGA2算法参数
POPULATION_SIZE = 10  # 20
OFFSPRING_SIZE  = 5   # 10
N_GENERATIONS   = 5   # 10

#################################

# 创建数据库连接
config = load_config()
engine = create_engine(get_pg_connection_string(config))

# 禁用PyMoo的编译警告
Config.warnings['not_compiled'] = False

# 准备Heikin-Ashi数据
def prepare_heikin_ashi_data(df):
    """计算Heikin-Ashi数据并返回更新后的DataFrame"""
    df['trade_time'] = pd.to_datetime(df['trade_time'])
    df.set_index('trade_time', inplace=True)
    df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    df['ha_open'] = (df['open'].shift(1) + df['close'].shift(1)) / 2
    df['ha_open'].iloc[0] = df['open'].iloc[0]  # 第一行的 ha_open 等于 open
    df['ha_high'] = df[['high', 'ha_open', 'ha_close']].max(axis=1)
    df['ha_low'] = df[['low', 'ha_open', 'ha_close']].min(axis=1)
    return df

# Heikin-Ashi数据的Data Feed
class HeikinAshiData(bt.feeds.PandasData):
    lines = ('ha_open', 'ha_high', 'ha_low', 'ha_close')
    params = (
        ('datetime', None),  # 使用索引作为datetime
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'volume'),
        ('ha_open', 'ha_open'),
        ('ha_high', 'ha_high'),
        ('ha_low', 'ha_low'),
        ('ha_close', 'ha_close'),
        ('openinterest', None),
    )

# RMA计算
class PineRMA(bt.Indicator):
    lines = ('rma',)
    params = (('period', 14),)

    def __init__(self):
        self.addminperiod(self.p.period)
        
    def next(self):
        if len(self) < self.p.period:
            self.lines.rma[0] = self.data[0]  # 当数据点还不够时，直接赋值为当前数据
        else:
            alpha = 1.0 / self.p.period
            self.lines.rma[0] = (alpha * self.data[0]) + (1 - alpha) * self.lines.rma[-1]

# Heikin-Ashi SuperTrend指标
class HeikinAshiSuperTrend(bt.Indicator):
    lines = ('supertrend', 'direction', 'upperband', 'lowerband', 'hl2')
    params = (
        ('factor', 3),
        ('atr_period', 10),
    )
    
    def __init__(self):
        # 计算ATR
        tr = bt.indicators.TR(self.data)
        self.atr = PineRMA(tr, period=self.p.atr_period)
        
        # 初始化方向为1（下降趋势）
        self.l.direction = bt.LineNum(1)
        
        # 存储上一个supertrend值
        self.prev_supertrend = None
        self.prev_direction = None

    def next(self):
        # 计算中间值
        self.l.hl2[0] = (self.data.ha_high[0] + self.data.ha_low[0]) / 2.0
        self.l.upperband[0] = self.l.hl2[0] + self.p.factor * self.atr[0]
        self.l.lowerband[0] = self.l.hl2[0] - self.p.factor * self.atr[0]
        
        curr_upperband = self.l.upperband[0]
        curr_lowerband = self.l.lowerband[0]
        curr_close = self.data.ha_close[0]
        
        if self.prev_supertrend is None:
            self.l.direction[0] = 1
            self.l.supertrend[0] = curr_upperband
        else:
            # 更新supertrend和方向
            if self.prev_supertrend == self.prev_upperband:
                if curr_close > curr_upperband:
                    self.l.direction[0] = -1  # 上升趋势
                else:
                    self.l.direction[0] = 1
            else:
                if curr_close < curr_lowerband:
                    self.l.direction[0] = 1  # 下降趋势
                else:
                    self.l.direction[0] = -1
            
            # 根据方向设置supertrend值
            self.l.supertrend[0] = curr_lowerband if self.l.direction[0] == -1 else curr_upperband
        
        # 保存当前值作为下一次迭代的上一个值
        self.prev_supertrend = self.l.supertrend[0]
        self.prev_upperband = curr_upperband
        self.prev_direction = self.l.direction[0]


# 交易策略
class HeikinAshiSuperTrendStrategy(bt.Strategy):
    params = (
        ('supertrend_period', 10),
        ('supertrend_multiplier', 3),
    )

    def __init__(self):
        self.supertrend = HeikinAshiSuperTrend(
            self.data,
            factor=self.p.supertrend_multiplier,
            atr_period=self.p.supertrend_period
        )
        self.prev_direction = None

    def next(self):
        if self.prev_direction is None:
            self.prev_direction = self.supertrend.direction[0]
            return
        
        curr_direction = self.supertrend.direction[0]
        
        if self.prev_direction != curr_direction:
            if curr_direction == -1:  # 由1变为-1，买入信号
                size = int((self.broker.get_cash() * 0.95) / self.data.close[0])
                self.buy(size=size)
            elif curr_direction == 1:  # 由-1变为1，卖出信号
                self.close()
        
        self.prev_direction = curr_direction


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
        self.df = get_30m_kline_data('wfq', self.stock_code, self.start_date, self.end_date)
        self.df = prepare_heikin_ashi_data(self.df)
        
        # 在初始化时获取基准数据
        print('正在获取基准数据...')
        benchmark_sql = """
        SELECT trade_time, close 
        FROM a_index_1day_kline_baostock 
        WHERE ts_code = '000300.SH' 
        AND trade_time BETWEEN %s AND %s 
        ORDER BY trade_time
        """
        self.benchmark_df = pd.read_sql(benchmark_sql, engine, params=(start_date, end_date))
        self.benchmark_df.set_index('trade_time', inplace=True)
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