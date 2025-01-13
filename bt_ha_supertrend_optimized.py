# -*- coding: utf-8 -*-
from common import *
import backtrader as bt
import optuna
import quantstats_lumi as qs
from pymoo.core.problem import Problem
from pymoo.algorithms.moo.nsga2 import NSGA2
from pymoo.optimize import minimize
from pymoo.termination import get_termination
from pymoo.operators.sampling.rnd import FloatRandomSampling
from pymoo.operators.crossover.sbx import SBX
from pymoo.operators.mutation.pm import PM
from pymoo.config import Config
import numpy as np

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
        super().__init__(
            n_var=2,  # 两个变量：period和multiplier
            n_obj=2,  # 两个目标：胜率和盈亏比
            n_constr=0,  # 没有约束
            # period的可选值：[10, 20, 30, 40, 50]
            # multiplier的可选值：[2, 3, 4, 5, 6]
            xl=np.array([0, 0]),  # 下界（用索引表示）
            xu=np.array([4, 4]),  # 上界（用索引表示）
            vtype=np.int32  # 设置变量类型为整数
        )
        self.stock_code = stock_code
        self.start_date = start_date
        self.end_date = end_date
        # 定义参数的可选值
        self.period_values = np.arange(10, 51, 10)  # [10, 20, 30, 40, 50]
        self.multiplier_values = np.arange(2, 7, 1)  # [2, 3, 4, 5, 6]

    def _evaluate(self, x, out, *args, **kwargs):
        F = np.zeros((x.shape[0], 2))
        
        for i in range(x.shape[0]):
            # 从索引获取实际参数值
            period = self.period_values[int(x[i, 0])]
            multiplier = self.multiplier_values[int(x[i, 1])]
            
            df = get_stock_data('qfq', self.stock_code, self.start_date, self.end_date)
            df = prepare_heikin_ashi_data(df)
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
            
            # 计算目标值
            win_rate = qs.stats.win_rate(returns)
            profit_factor = qs.stats.profit_factor(returns)
            
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
    stock_code = '000858.SZ'
    start_date = '2000-01-01'
    end_date   = '2024-12-31'
    
    # 创建问题实例
    problem = TradingProblem(stock_code, start_date, end_date)
    
    # 创建算法实例，添加采样、交叉和变异操作
    algorithm = NSGA2(
        pop_size=20,
        n_offsprings=10,
        sampling=FloatRandomSampling(),
        crossover=SBX(prob=0.9, eta=15),
        mutation=PM(eta=20),
        eliminate_duplicates=True
    )
    
    # 设置终止条件
    termination = get_termination("n_gen", 10)
    
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
    df = get_stock_data('qfq', stock_code, start_date, end_date)
    df = prepare_heikin_ashi_data(df)
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
    
    # 计算并打印最终结果
    cumulative_returns = qs.stats.compsum(returns).iloc[-1] * 100
    win_rate = qs.stats.win_rate(returns) * 100
    profit_factor = qs.stats.profit_factor(returns)
    sortino_ratio = qs.stats.sortino(returns)
    
    print('\n=== 选定参数的回测结果 ===')
    print(f'参数: period={best_params["supertrend_period"]}, multiplier={best_params["supertrend_multiplier"]}')
    print(f'Sortino Ratio: {sortino_ratio:.2f}')
    print(f'累积收益率: {cumulative_returns:.2f}%')
    print(f'胜率: {win_rate:.2f}%')
    print(f'盈亏比: {profit_factor:.2f}')
    
    # 生成报告
    qs.reports.html(
        returns, 
        output=f'{stock_code}_optimized.html',
        download_filename=f'{stock_code}_optimized.html',
        title=f'{stock_code} 回测报告'
    )
    print(f'\n已生成业绩报告：{stock_code}_optimized.html') 