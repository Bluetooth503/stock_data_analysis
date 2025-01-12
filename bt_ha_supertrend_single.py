import backtrader as bt
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import configparser
# import quantstats as qs
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
        
        # 基本上轨和下轨计算
        hl2 = (self.data.high + self.data.low) / 2
        self.upband = hl2 + (self.atr * self.p.multiplier)
        self.downband = hl2 - (self.atr * self.p.multiplier)
        
        # 初始化SuperTrend
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
        # 初始化SuperTrend指标
        self.supertrend = SuperTrend(
            period=self.p.supertrend_period,
            multiplier=self.p.supertrend_multiplier
        )
        
        # 用于追踪持仓状态
        self.position_size = 0
        self.in_position = False

    def next(self):
        # 如果没有持仓且出现做多信号
        if not self.in_position and self.data.close[0] > self.supertrend.supertrend[0]:
            # 计算仓位大小（这里用总资金的95%）
            size = int((self.broker.get_cash() * 0.95) / self.data.close[0])
            self.buy(size=size)
            self.in_position = True
            self.position_size = size
            
        # 如果持仓且出现平仓信号
        elif self.in_position and self.data.close[0] < self.supertrend.supertrend[0]:
            self.close()
            self.in_position = False
            self.position_size = 0

# 数据准备函数
def prepare_data(stock_code, start_date, end_date):
    # 读取配置文件
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    # 创建SQLAlchemy引擎
    db_url = f"postgresql+psycopg2://{config['postgresql']['user']}:{config['postgresql']['password']}@{config['postgresql']['host']}:{config['postgresql']['port']}/{config['postgresql']['database']}"
    engine = create_engine(db_url)
    
    # SQL查询
    query = f"""
    SELECT 
        trade_date + trade_time as datetime,
        open,
        high,
        low,
        close,
        volume
    FROM 
        a_stock_30m_kline_qfq_baostock
    WHERE 
        ts_code = '{stock_code}'
        AND trade_date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY 
        trade_date, trade_time
    """
    
    # 使用SQLAlchemy连接读取数据
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    # 计算Heikin-Ashi价格
    df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    df['ha_open'] = df['open'].copy()
    for i in range(1, len(df)):
        df.loc[df.index[i], 'ha_open'] = (df.loc[df.index[i-1], 'ha_open'] + 
                                         df.loc[df.index[i-1], 'ha_close']) / 2
    df['ha_high'] = df[['high', 'ha_open', 'ha_close']].max(axis=1)
    df['ha_low'] = df[['low', 'ha_open', 'ha_close']].min(axis=1)
    
    return df

def run_backtest(stock_code, start_date, end_date, initial_cash=100000, optimize=False, supertrend_period=10, supertrend_multiplier=3) -> bt.Strategy:
    # 准备数据
    df = prepare_data(stock_code, start_date, end_date)
    
    # 创建cerebro实例
    cerebro = bt.Cerebro()
    
    # 添加数据
    data = HeikinAshiData(
        dataname=df,
        datetime='datetime',
        open='ha_open',
        high='ha_high',
        low='ha_low',
        close='ha_close',
        volume='volume'
    )
    cerebro.adddata(data)
    
    # 设置初始资金
    cerebro.broker.setcash(initial_cash)
    
    # 设置手续费
    cerebro.broker.setcommission(commission=0.0003)
    
    # 添加策略
    if optimize:
        # 参数优化
        # 统一设置参数优化范围
        cerebro.optstrategy(
            HeikinAshiSuperTrendStrategy,
            supertrend_period=range(10, 51, 10),  # 10到50，步长10
            supertrend_multiplier=[x * 1.0 for x in range(2, 7)]  # 2到6，步长1
        )
    else:
        # 单次回测
        cerebro.addstrategy(
            HeikinAshiSuperTrendStrategy,
            supertrend_period=supertrend_period,
            supertrend_multiplier=supertrend_multiplier
        )
    
    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    cerebro.addanalyzer(bt.analyzers.Transactions, _name='transactions')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn')
    cerebro.addanalyzer(bt.analyzers.VWR, _name='vwr')
    cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn')
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
    # 删除未定义的trade_list分析器

    # 运行回测
    results = cerebro.run()
    if optimize:
        # 参数优化模式下返回最佳参数
        return results
    else:
        # 单次回测模式下返回策略实例
        result = results[0]
        pyfoliozer = result.analyzers.getbyname('pyfolio')
        returns, positions, transactions, gross_lev = pyfoliozer.get_pf_items()
        
    if optimize:
        # 输出优化结果
        print('\n=== 参数优化结果 ===')
        best_result = None
        best_value = -float('inf')
        
        for i, result in enumerate(results):
            strat = result[0]
            sharpe = strat.analyzers.sharpe.get_analysis()["sharperatio"]
            drawdown = strat.analyzers.drawdown.get_analysis()["max"]["drawdown"]
            returns = strat.analyzers.returns.get_analysis()["rnorm100"]
            
            # 综合评分（可根据需要调整权重）
            score = sharpe * 0.5 - drawdown * 0.3 + returns * 0.2
            
            print(f'\n组合 {i+1}:')
            print(f'参数: period={strat.params.supertrend_period}, multiplier={strat.params.supertrend_multiplier}')
            print(f'夏普比率: {sharpe:.2f}')
            print(f'最大回撤: {drawdown:.2f}%')
            print(f'年化收益率: {returns:.2f}%')
            print(f'综合评分: {score:.2f}')
            
            if score > best_value:
                best_value = score
                best_result = {
                    'period': strat.params.supertrend_period,
                    'multiplier': strat.params.supertrend_multiplier,
                    'sharpe': sharpe,
                    'drawdown': drawdown,
                    'returns': returns
                }
        
        print('\n=== 最佳参数组合 ===')
        print(f'周期: {best_result["period"]}')
        print(f'乘数: {best_result["multiplier"]}')
        print(f'夏普比率: {best_result["sharpe"]:.2f}')
        print(f'最大回撤: {best_result["drawdown"]:.2f}%')
        print(f'年化收益率: {best_result["returns"]:.2f}%')
        
        return best_result
    else:
        # 单次回测结果
        strat = results[0]
        
        # 输出结果
    print(f'\n=== 基本统计 ===')
    print(f'最终资产价值: {cerebro.broker.getvalue():.2f}')
    print(f'年化收益率: {strat.analyzers.returns.get_analysis()["rnorm100"]:.2f}%')
    
    print(f'\n=== 风险指标 ===')
    print(f'夏普比率: {strat.analyzers.sharpe.get_analysis()["sharperatio"]:.2f}')
    print(f'最大回撤: {strat.analyzers.drawdown.get_analysis()["max"]["drawdown"]:.2f}%')
    print(f'波动率调整回报: {strat.analyzers.vwr.get_analysis()["vwr"]:.2f}')
    print(f'系统质量指标: {strat.analyzers.sqn.get_analysis()["sqn"]:.2f}')
    
    print(f'\n=== 交易统计 ===')
    trades = strat.analyzers.trades.get_analysis()
    print(f'总交易次数: {trades.total.total}')
    print(f'盈利交易次数: {trades.won.total}')
    print(f'亏损交易次数: {trades.lost.total}')
    print(f'胜率: {trades.won.total/trades.total.total*100:.2f}%')
    print(f'平均盈利: {trades.won.pnl.average:.2f}')
    print(f'平均亏损: {trades.lost.pnl.average:.2f}')
    print(f'盈亏比: {trades.won.pnl.total/abs(trades.lost.pnl.total):.2f}')
    
    # 计算净利润率
    net_profit = cerebro.broker.getvalue() - initial_cash
    net_profit_ratio = net_profit / initial_cash * 100
    print(f'\n=== 盈利能力 ===')
    print(f'净利润: {net_profit:.2f}')
    print(f'净利润率: {net_profit_ratio:.2f}%')
    
    # 计算胜率
    win_rate = trades.won.total / trades.total.total * 100
    print(f'胜率: {win_rate:.2f}%')
    
    # 计算盈亏比
    profit_factor = trades.won.pnl.total / abs(trades.lost.pnl.total)
    print(f'盈亏比: {profit_factor:.2f}')
    
    # 添加ValueRecorder分析器记录资金曲线
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn', timeframe=bt.TimeFrame.NoTimeFrame)
    
    # 返回策略实例
    return strat
    
# 使用示例
if __name__ == '__main__':
    stock_code = '600000.SH'  # 浦发银行
    start_date = '2000-01-01'
    end_date = '2024-12-31'
    
    # 运行参数优化
    print('正在运行参数优化...')
    best_params = run_backtest(stock_code, start_date, end_date, optimize=True)
    
    # 使用最佳参数运行回测
    print('\n使用最佳参数运行回测...')
    if isinstance(best_params, list):
        # 提取最佳参数
        best_result = best_params[0][0]
        strat = run_backtest(
            stock_code, 
            start_date, 
            end_date,
            supertrend_period=best_result.params.supertrend_period,
            supertrend_multiplier=best_result.params.supertrend_multiplier
        )
    else:
        strat = run_backtest(
            stock_code, 
            start_date, 
            end_date,
            supertrend_period=best_params['period'],
            supertrend_multiplier=best_params['multiplier']
        )
    
    # 生成quantstats报告
    if strat is not None:
        returns = pd.Series(strat.analyzers.timereturn.get_analysis())
        # 如果时间索引没有时区信息，先添加本地时区
        if returns.index.tz is None:
            returns.index = returns.index.tz_localize('Asia/Shanghai')
        # 转换为无时区格式
        returns.index = returns.index.tz_localize(None)
        
        # 计算关键指标
        metrics = {
            'sharpe': round(qs.stats.sharpe(returns), 3),
            'smart_sharpe': round(qs.stats.smart_sharpe(returns), 3),
            'avg_return': round(qs.stats.avg_return(returns), 3),
            'win_rate': round(qs.stats.win_rate(returns), 3),
            'profit_factor': round(qs.stats.profit_factor(returns), 3),
            'risk_return_ratio': round(qs.stats.risk_return_ratio(returns), 3),
            'profit_ratio': round(qs.stats.profit_ratio(returns), 3),
        }
        
        # 输出指标
        print('\n=== 关键指标 ===')
        for metric, value in metrics.items():
            print(f'{metric}: {value}')
            
        # 生成简单HTML报告
        qs.reports.html(
            returns, 
            output=f'{stock_code}.html',
            download_filename=f'{stock_code}.html',
            title=f'{stock_code} 回测报告'
        )
        print(f'\n已生成业绩报告：{stock_code}.html')
    else:
        print('无法生成报告：策略实例为空')
