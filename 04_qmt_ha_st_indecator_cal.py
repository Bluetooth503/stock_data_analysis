# -*- coding: utf-8 -*-
from datetime import datetime
import time
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import pandas_ta as ta
from xtquant import xtdata
xtdata.enable_hello = False
import argparse

# 设置命令行参数
parser = argparse.ArgumentParser(description='计算股票的Heikin-Ashi和Supertrend指标')
parser.add_argument('code', type=str, help='股票代码，例如：601136')
args = parser.parse_args()

# 输入参数
code = args.code
ts_code = f"{code}.SH" if code.startswith('6') else f"{code}.SZ"


# 读取配置
df_parm = pd.read_csv('qmt_monitor_stocks_calmar.csv', encoding='utf-8')
df_parm = df_parm[df_parm['ts_code'] == ts_code]

# ================================= Heikin-Ashi SuperTrend 计算 =================================
# direction=1上涨,-1下跌.
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

xtdata.download_history_data2([ts_code], period='30m', start_time='20240101', end_time='', incrementally=True)
df = xtdata.get_market_data_ex([], [ts_code], period='30m', start_time='20240101', end_time='')

# 计时开始
start_time = time.time()
df_ta = df[ts_code].copy()
df_ta['trade_time'] = pd.to_datetime(df_ta['time'].apply(lambda x: datetime.fromtimestamp(x / 1000.0)))
df_ta = ha_st_pandas_ta(df_ta, length=df_parm['period'].iloc[0], multiplier=df_parm['multiplier'].iloc[0])
df_ta['ts_code'] = ts_code
df_ta = df_ta.round(3)
print(df_ta[['trade_time','ts_code','direction','open','high','low','close','ha_close','supertrend']].tail(40))
elapsed_time = time.time() - start_time
print(f'[pandas_ta] 代码段执行耗时: {elapsed_time:.2f}秒')
print('shape: ', df_ta.shape)
