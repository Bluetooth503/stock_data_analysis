import backtrader as bt
import pandas as pd
import optuna
from common import get_stock_data
import quantstats_lumi as qs

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

# 定义优化函数
def optimize_strategy(trial):
    supertrend_period = trial.suggest_int('supertrend_period', 10, 50, step=10)
    supertrend_multiplier = trial.suggest_int('supertrend_multiplier', 2, 6, step=1)
    df = get_stock_data('600000.SH', '2000-01-01', '2024-12-31')
    df = prepare_heikin_ashi_data(df)
    cerebro = bt.Cerebro()
    data = HeikinAshiData(dataname=df)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000)
    cerebro.broker.setcommission(commission=0.0003)
    cerebro.addstrategy(HeikinAshiSuperTrendStrategy,
                        supertrend_period=supertrend_period,
                        supertrend_multiplier=supertrend_multiplier)
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    results = cerebro.run()
    strat = results[0]
    
    # 获取回测结果
    returns = pd.Series(strat.analyzers.timereturn.get_analysis())
    if returns.index.tz is None:
        returns.index = returns.index.tz_localize('Asia/Shanghai')
    returns.index = returns.index.tz_localize(None)
    
    # 计算指标
    sortino_ratio = qs.stats.sortino(returns)
    trades = strat.analyzers.trades.get_analysis()
    total_trades = trades.total.total if 'total' in trades and 'total' in trades.total else 0
    won_trades = trades.won.total if 'won' in trades and 'total' in trades.won else 0
    lost_trades = trades.lost.total if 'lost' in trades and 'total' in trades.lost else 0
    
    # 净利润率
    final_value = cerebro.broker.getvalue()
    net_profit = final_value - 100000
    net_profit_ratio = (net_profit / 100000) * 100
    
    # 胜率
    win_rate = (won_trades / total_trades) * 100 if total_trades > 0 else 0
    
    # 平均盈亏比
    avg_profit = trades.won.pnl.average if won_trades > 0 else 0
    avg_loss = trades.lost.pnl.average if lost_trades > 0 else 0
    profit_loss_ratio = (avg_profit / abs(avg_loss)) if avg_loss != 0 else float('inf')
    
    # 加权平均
    weights = {'sortino': 0.4, 'net_profit_ratio': 0.2, 'win_rate': 0.2, 'profit_loss_ratio': 0.2}
    weighted_score = (weights['sortino'] * sortino_ratio +
                      weights['net_profit_ratio'] * net_profit_ratio +
                      weights['win_rate'] * win_rate +
                      weights['profit_loss_ratio'] * profit_loss_ratio)
    
    return weighted_score

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
    print('正在运行参数优化...')
    study = optuna.create_study(direction='maximize')
    study.optimize(optimize_strategy, n_trials=50)
    best_params = study.best_params
    print('最佳参数:', best_params)
    df = get_stock_data('600000.SH', '2000-01-01', '2024-12-31')
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
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    results = cerebro.run()
    strat = results[0]
    returns = pd.Series(strat.analyzers.timereturn.get_analysis())
    if returns.index.tz is None:
        returns.index = returns.index.tz_localize('Asia/Shanghai')
    returns.index = returns.index.tz_localize(None)
    
    # Sortino Ratio
    sortino_ratio = qs.stats.sortino(returns)
    
    # 交易分析
    trades = strat.analyzers.trades.get_analysis()
    total_trades = trades.total.total if 'total' in trades and 'total' in trades.total else 0
    won_trades = trades.won.total if 'won' in trades and 'total' in trades.won else 0
    lost_trades = trades.lost.total if 'lost' in trades and 'total' in trades.lost else 0
    
    # 净利润率
    final_value = cerebro.broker.getvalue()
    net_profit = final_value - 100000
    net_profit_ratio = (net_profit / 100000) * 100
    
    # 胜率
    win_rate = (won_trades / total_trades) * 100 if total_trades > 0 else 0
    
    # 平均盈亏比
    avg_profit = trades.won.pnl.average if won_trades > 0 else 0
    avg_loss = trades.lost.pnl.average if lost_trades > 0 else 0
    profit_loss_ratio = (avg_profit / abs(avg_loss)) if avg_loss != 0 else float('inf')
    
    # 打印评价指标
    print('\n=== 最佳参数的评价指标 ===')
    print(f'Sortino Ratio: {sortino_ratio:.2f}')
    print(f'净利润率: {net_profit_ratio:.2f}%')
    print(f'胜率: {win_rate:.2f}%')
    print(f'平均盈亏比: {profit_loss_ratio:.2f}')

    # 生成quantstats报告
    qs.reports.html(
        returns, 
        output='600000_SH_optimized.html',
        download_filename='600000_SH_optimized.html',
        title='600000.SH 回测报告'
    )
    print('已生成业绩报告：600000_SH_optimized.html') 