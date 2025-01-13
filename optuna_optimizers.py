# -*- coding: utf-8 -*-
import sys
sys.path.append('/data_01/data_stock_code')
from common_new import *
import backtrader as bt
import optuna
import gradient_free_optimizers as gfo

# 取数函数
def cal_indicators_crypto(stock_period, stock):
    df = query_sql("SELECT trade_time,ts_code,open,high,low,close,volume FROM public.stock_cryptocurrency_%s WHERE ts_code='%s' and volume>0 ORDER BY trade_time"%(stock_period,stock))
    df['trade_time'] = pd.to_datetime(df['trade_time'])
    df.set_index("trade_time", inplace=True)
    return df

# 扩展DataFeed
class PandasData_Ex(bt.feeds.PandasData):
    pass

class LongStrategy01(bt.Strategy):
    params = (('bbl_period', 35),('bbl_up_std', 2),('bbl_dn_std', 2),('mfi_period', 35),('mfi_up_th', 70),('mfi_dn_th', 30),('stop', 5),)
    
    def __init__(self):
        self.order  = None # 订单清空
        self.high   = self.datas[0].high
        self.low    = self.datas[0].low
        self.close  = self.datas[0].close
        self.volume = self.datas[0].volume
        self.hlc3   = (self.datas[0].high + self.datas[0].low + self.datas[0].close) / 3
        self.bbands = bt.talib.BBANDS(self.data, timeperiod=int(self.p.bbl_period), nbdevup=self.p.bbl_up_std, nbdevdn=self.p.bbl_dn_std, matype=0)
        self.higher = self.bbands.line0
        self.lower  = self.bbands.line2
        self.mfi    = bt.talib.MFI(self.high, self.low, self.close, self.volume, timeperiod=int(self.p.mfi_period))
        self.cross_hlc3_lower  = bt.ind.CrossOver(self.hlc3, self.lower)
        self.cross_hlc3_higher = bt.ind.CrossOver(self.hlc3, self.higher)
        self.cross_mfi_dnth    = bt.ind.CrossOver(self.mfi, self.p.mfi_dn_th)
        self.cross_mfi_upth    = bt.ind.CrossOver(self.mfi, self.p.mfi_up_th)
        
    def next(self):
        if not self.position:
            if self.cross_hlc3_lower<0 or self.mfi[0]<self.p.mfi_dn_th:
                self.buy()
        else:
            stop_price_long = self.position.price*(1-self.p.stop/100)
            if self.cross_hlc3_higher>0 or self.mfi[0]>self.p.mfi_up_th or self.low[0]<=stop_price_long:
                self.sell()


##################################################### optuna ################################################### 
def optimize(trial):
    stock="BTC/USDT"
    stock_period="4h"
    start="20200101"
    end="20230601"
    strategy_name = "Long_BBL_MFI_Stop_01"
    strategy_desc = "入:hlc3下穿lower or mfi<dn_th.出:hlc3上穿higher or mfi>up_th.止损:low<=stop_price."
    
    cerebro = bt.Cerebro()
    initial_cash = 100000.0  # 初始金额
    cerebro.broker.setcash(initial_cash)
    cerebro.broker.setcommission(commission=0.003)  # 手续费0.3%(现货:Maker0.1%,Taker0.3%)
    cerebro.broker.set_slippage_perc(perc=0.005)  # 滑点
    cerebro.addsizer(bt.sizers.AllInSizer, percents=90)  # 全仓买入
    cerebro.addstrategy(LongStrategy01,
                        bbl_period=trial.suggest_int('bbl_period',5,120,step=5),
                        bbl_up_std=trial.suggest_float('bbl_up_std',1,3,step=0.5),
                        bbl_dn_std=trial.suggest_float('bbl_dn_std',1,3,step=0.5),
                        mfi_period=trial.suggest_int('mfi_period',5,120,step=5),
                        mfi_up_th=trial.suggest_int('mfi_up_th',0,100,step=5),
                        mfi_dn_th=trial.suggest_int('mfi_dn_th',0,100,step=5),
                        stop=trial.suggest_int('stop',0,5,step=1))
    # 读数据
    df = cal_indicators_crypto(stock_period, stock)
    data = PandasData_Ex(dataname=df, fromdate = datetime.strptime(start,'%Y%m%d'), todate = datetime.strptime(end,'%Y%m%d'))
    cerebro.adddata(data, name=stock)  # 将数据传入回测系统
    # add analyzer
    cerebro.addanalyzer(bt.analyzers.Returns, _name='_Returns')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='_TradeAnalyzer')
    cerebro.addanalyzer(bt.analyzers.SQN, _name='_SQN')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio', timeframe=bt.TimeFrame.Days, riskfreerate=0.0, annualize=True, factor=250)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')
    # run
    results = cerebro.run(maxcpus=32,stdstats=False)
    result = results[0]
    # get analysis dict
    returns_ = result.analyzers._Returns.get_analysis()
    trade_analyzer_ = result.analyzers._TradeAnalyzer.get_analysis()
    sqn_ = result.analyzers._SQN.get_analysis()
    sharpe_ratio_ = result.analyzers._SharpeRatio.get_analysis()
    draw_down_ = result.analyzers._DrawDown.get_analysis()
    # get analysis value
    win_rate = trade_analyzer_['won']['total'] / trade_analyzer_["total"]["total"] if len(trade_analyzer_)>1 else 0
    avg_holding_k = trade_analyzer_['len']['average'] if len(trade_analyzer_)>1 else 0
    trade_cnt = sqn_['trades'] if len(sqn_)>0 else 0
    total_k = len(data)
    sharpe_ratio = sharpe_ratio_['sharperatio'] if len(sharpe_ratio_)>0 else 0
    sqn = sqn_['sqn'] if len(sqn_)>0 else 0
    rtot = returns_['rtot'] if len(returns_)>0 else 0
    rnorm = returns_['rnorm'] if len(returns_)>0 else 0
    ravg = returns_['ravg'] if len(returns_)>0 else 0
    avg_drawdown_len = draw_down_['len'] if len(draw_down_)>0 else 0
    avg_drawdown_rate = draw_down_['drawdown'] if len(draw_down_)>0 else 0
    max_drawdown_len = draw_down_['max']['len'] if len(draw_down_)>0 else 0
    max_drawdown_rate = draw_down_['max']['drawdown'] if len(draw_down_)>0 else 0
    final_cash = cerebro.broker.getvalue()
    final_initial_rate = final_cash / initial_cash
    # save analysis result
    df_result = pd.DataFrame({
        'stock': stock,
        'stock_period': stock_period,
        'strategy_name': strategy_name,
        'strategy_desc': strategy_desc,
        'para': str(trial.params),
        'final_initial_rate': final_initial_rate,
        'win_rate': win_rate,
        'avg_holding_k': avg_holding_k,
        'trade_cnt': trade_cnt,
        'total_k': total_k,
        'sharpe_ratio': sharpe_ratio,
        'sqn': sqn,
        'vwr': np.nan,
        'rtot': rtot,
        'rnorm': rnorm,
        'ravg': ravg,
        'avg_drawdown_len': avg_drawdown_len,
        'avg_drawdown_rate': avg_drawdown_rate,
        'max_drawdown_len': max_drawdown_len,
        'max_drawdown_rate': max_drawdown_rate,
        'initial_cash': initial_cash,
        'final_cash': final_cash,
        'draw_down': json.dumps(draw_down_),
        'start': start,
        'end': end,
        },index=[0])
    df_result = df_result.round(4)
    write_to_table(df_result, "public", "crypto_strategy_analysis_result_record")
    # write_to_table(df_result, "public", "aaaaaa")
    return final_initial_rate

starttime = datetime.now()
study = optuna.create_study(direction="maximize",sampler=optuna.samplers.TPESampler(),)
study.optimize(optimize, n_trials=2000)
endtime = datetime.now()
print("耗时: {}秒!".format((endtime-starttime)))


##################################################### gfo ################################################### 
'''
def runstrat(para):
    stock="BTC/USDT"
    stock_period="4h"
    start="20200101"
    end="20230601"
    strategy_name = "Long_BBL_MFI_Stop_08"
    strategy_desc = "入:hlc3下穿lower or mfi<dn_th.出:hlc3上穿higher or mfi>up_th.止损:low<=stop_price."
    
    cerebro = bt.Cerebro()
    initial_cash = 100000.0  # 初始金额
    cerebro.broker.setcash(initial_cash)
    cerebro.broker.setcommission(commission=0.003)  # 手续费0.3%(现货:Maker0.1%,Taker0.3%)
    cerebro.broker.set_slippage_perc(perc=0.005)  # 滑点
    cerebro.addsizer(bt.sizers.AllInSizer, percents=90)  # 全仓买入
    cerebro.addstrategy(LongStrategy06, bbl_period=para['bbl_period'], bbl_up_std=para['bbl_up_std'], bbl_dn_std=para['bbl_dn_std'], mfi_period=para['mfi_period'], mfi_up_th=para['mfi_up_th'], mfi_dn_th=para['mfi_dn_th'], stop=para['stop'])
    # 读数据
    df = cal_indicators_crypto(stock_period, stock)
    data = PandasData_Ex(dataname=df, fromdate = datetime.strptime(start,'%Y%m%d'), todate = datetime.strptime(end,'%Y%m%d'))
    cerebro.adddata(data, name=stock)  # 将数据传入回测系统
    # add analyzer
    cerebro.addanalyzer(bt.analyzers.Returns, _name='_Returns')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='_TradeAnalyzer')
    cerebro.addanalyzer(bt.analyzers.SQN, _name='_SQN')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio', timeframe=bt.TimeFrame.Days, riskfreerate=0.0, annualize=True, factor=250)
    # cerebro.addanalyzer(bt.analyzers.VWR, _name='_VWR')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')
    # run
    results = cerebro.run(maxcpus=32,stdstats=False)
    result = results[0]
    # get analysis dict
    returns_ = result.analyzers._Returns.get_analysis()
    trade_analyzer_ = result.analyzers._TradeAnalyzer.get_analysis()
    sqn_ = result.analyzers._SQN.get_analysis()
    sharpe_ratio_ = result.analyzers._SharpeRatio.get_analysis()
    # vwr_ = result.analyzers._VWR.get_analysis()
    draw_down_ = result.analyzers._DrawDown.get_analysis()
    # get analysis value
    win_rate = trade_analyzer_['won']['total'] / trade_analyzer_["total"]["total"] if len(trade_analyzer_)>1 else 0
    avg_holding_k = trade_analyzer_['len']['average'] if len(trade_analyzer_)>1 else 0
    trade_cnt = sqn_['trades'] if len(sqn_)>0 else 0
    total_k = len(data)
    sharpe_ratio = sharpe_ratio_['sharperatio'] if len(sharpe_ratio_)>0 else 0
    sqn = sqn_['sqn'] if len(sqn_)>0 else 0
    # vwr = vwr_['vwr'] if len(vwr_)>0 else 0
    rtot = returns_['rtot'] if len(returns_)>0 else 0
    rnorm = returns_['rnorm'] if len(returns_)>0 else 0
    ravg = returns_['ravg'] if len(returns_)>0 else 0
    avg_drawdown_len = draw_down_['len'] if len(draw_down_)>0 else 0
    avg_drawdown_rate = draw_down_['drawdown'] if len(draw_down_)>0 else 0
    max_drawdown_len = draw_down_['max']['len'] if len(draw_down_)>0 else 0
    max_drawdown_rate = draw_down_['max']['drawdown'] if len(draw_down_)>0 else 0
    final_cash = cerebro.broker.getvalue()
    final_initial_rate = final_cash / initial_cash
    # save analysis result
    df_result = pd.DataFrame({
        'stock': stock,
        'stock_period': stock_period,
        'strategy_name': strategy_name,
        'strategy_desc': strategy_desc,
        'para': str(para),
        'final_initial_rate': final_initial_rate,
        'win_rate': win_rate,
        'avg_holding_k': avg_holding_k,
        'trade_cnt': trade_cnt,
        'total_k': total_k,
        'sharpe_ratio': sharpe_ratio,
        'sqn': sqn,
        'vwr': np.nan,
        'rtot': rtot,
        'rnorm': rnorm,
        'ravg': ravg,
        'avg_drawdown_len': avg_drawdown_len,
        'avg_drawdown_rate': avg_drawdown_rate,
        'max_drawdown_len': max_drawdown_len,
        'max_drawdown_rate': max_drawdown_rate,
        'initial_cash': initial_cash,
        'final_cash': final_cash,
        'draw_down': json.dumps(draw_down_),
        'start': start,
        'end': end,
        },index=[0])
    df_result = df_result.round(4)
    write_to_table(df_result, "public", "crypto_strategy_analysis_result_record")
    # write_to_table(df_result, "public", "aaaaaa")
    return sqn

search_space = {
    "bbl_period": np.arange(5, 125, 5),
    "bbl_up_std": np.arange(1, 3.5, 0.5),
    "bbl_dn_std": np.arange(1, 3.5, 0.5),
    "mfi_period": np.arange(5, 125, 5),
    "mfi_up_th":  np.arange(5, 105, 5),
    "mfi_dn_th":  np.arange(5, 105, 5),
    "stop":       np.arange(0, 16, 1),
}

def constraint(para):
    return para["mfi_up_th"] > para["mfi_dn_th"]
constraints_list = [constraint]

starttime = datetime.now()
opt = gfo.EvolutionStrategyOptimizer(search_space, constraints=constraints_list)
opt.search(runstrat, n_iter=5000)
endtime = datetime.now()
print("耗时: {}秒!".format((endtime-starttime)))
'''
