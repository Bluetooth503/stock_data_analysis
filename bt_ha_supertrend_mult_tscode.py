# -*- coding: utf-8 -*-
from common import *
import backtrader as bt
import quantstats_lumi as qs
from pymoo.core.problem import Problem
from pymoo.algorithms.moo.nsga2 import NSGA2
from pymoo.optimize import minimize
from pymoo.termination import get_termination
from pymoo.operators.sampling.rnd import FloatRandomSampling
from pymoo.operators.crossover.sbx import SBX
from pymoo.operators.mutation.pm import PM
from pymoo.config import Config
import multiprocessing as mp
from functools import partial


#################################
# 参数设置
#################################
# 回测时间范围
START_DATE = '2000-01-01'
END_DATE   = '2024-12-31'

# SuperTrend参数优化范围
PERIOD_RANGE = np.arange(8, 88, 8)     # [ 8, 16, 24, 32, 40, 48, 56, 64, 72, 80]
MULTIPLIER_RANGE = np.arange(2, 7, 1)  # [2, 3, 4, 5, 6]

# NSGA2算法参数
POPULATION_SIZE = 20
OFFSPRING_SIZE = 10
N_GENERATIONS = 10

# 并行处理参数
MAX_PROCESSES = max(1, mp.cpu_count() - 1)  # 保留一个CPU核心

#################################

# 创建数据库连接
config = load_config()
engine = create_engine(get_pg_connection_string(config))

# 禁用PyMoo的编译警告
Config.warnings['not_compiled'] = False

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

class TradingProblem(Problem):
    def __init__(self, stock_code, start_date, end_date):
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
        self.end_date   = end_date
        
        print(f'正在获取股票数据: {stock_code}')
        self.df = get_30m_kline_data('wfq', self.stock_code, self.start_date, self.end_date)
        self.df['trade_time'] = pd.to_datetime(self.df['trade_time'])
        self.df.set_index('trade_time', inplace=True)
        self.df = heikin_ashi(self.df)
        
        # 对每个可能的参数组合预先计算SuperTrend信号
        self.parameter_data = {}
        for period in self.period_values:
            for multiplier in self.multiplier_values:
                df_copy = self.df.copy()
                df_copy = supertrend(df_copy, period, multiplier)
                self.parameter_data[(period, multiplier)] = df_copy
        
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
        self.benchmark_df['trade_time'] = pd.to_datetime(self.benchmark_df['trade_time'])
        self.benchmark_df.set_index('trade_time', inplace=True)
        self.benchmark_returns = self.benchmark_df['close'].pct_change()
        self.benchmark_returns.name = '000300.SH'

    def _evaluate(self, x, out, *args, **kwargs):
        F = np.zeros((x.shape[0], 2))
        
        for i in range(x.shape[0]):
            period = self.period_values[int(x[i, 0])]
            multiplier = self.multiplier_values[int(x[i, 1])]
            
            # 使用预先计算好的数据
            df = self.parameter_data[(period, multiplier)]
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
            
            # 计算目标值
            win_rate = qs.stats.win_rate(daily_returns)
            profit_factor = qs.stats.profit_factor(daily_returns)
            
            # 由于pymoo是最小化问题，所以取负值
            F[i, 0] = -win_rate
            F[i, 1] = -profit_factor
        
        out["F"] = F

def optimize_stock(stock_code):
    """对单个股票进行参数优化"""
    try:
        print(f'开始处理股票: {stock_code}')
        
        # 创建reports目录（如果不存在）
        os.makedirs('reports', exist_ok=True)
        
        # 创建问题实例
        problem = TradingProblem(stock_code, START_DATE, END_DATE)
        
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
        print(f'正在运行 {stock_code} 的多目标参数优化...')
        res = minimize(
            problem,
            algorithm,
            termination,
            seed=1,
            verbose=False
        )
        
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

        # 先计算最优参数的SuperTrend指标
        final_df = problem.df.copy()
        final_df = supertrend(final_df, 
                            best_params['supertrend_period'], 
                            best_params['supertrend_multiplier'])
        
        data = HeikinAshiData(dataname=final_df)  # 使用计算好指标的数据
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
            print.error(f'生成 {stock_code} 的策略报告时发生错误: {str(e)}')
        
        # 计算指标
        metrics = {
            'stock_code': stock_code,
            'period': best_params["supertrend_period"],
            'multiplier': best_params["supertrend_multiplier"],
            # 风险调整收益
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
        
        print(f'股票 {stock_code} 处理完成')
        return metrics
        
    except Exception as e:
        print.error(f'处理股票 {stock_code} 时发生错误: {str(e)}')
        return None

def main():
    # 读取股票列表
    try:
        stock_list_df = pd.read_csv('沪深A股_stock_list.csv', header=None, names=['ts_code']).head(20)
        stock_codes = stock_list_df['ts_code'].tolist()
        print(f'共读取到 {len(stock_codes)} 只股票')
    except Exception as e:
        print.error(f'读取股票列表时发生错误: {str(e)}')
        return

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
            print.error('没有成功处理任何股票')
            
    except Exception as e:
        print.error(f'处理过程中发生错误: {str(e)}')
        pool.close()
        pool.join()

if __name__ == '__main__':
    main()
