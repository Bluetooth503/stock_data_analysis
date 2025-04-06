# -*- coding: utf-8 -*-
from datetime import datetime
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import numpy as np
import talib
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

# 计算Heikin-Ashi和Supertrend指标
def ha_st_pine(df, length, multiplier):
    # ========== Heikin Ashi计算 ==========
    '''direction=1上涨，-1下跌'''
    df = df.copy()
    
    # 使用向量化操作计算HA价格
    ha_close = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    
    # 使用向量化操作计算HA开盘价
    ha_open = pd.Series(index=df.index, dtype=float)
    ha_open.iloc[0] = (df['open'].iloc[0] + df['close'].iloc[0]) / 2
    
    # 使用向量化操作计算HA高低价
    ha_high = pd.Series(index=df.index, dtype=float)
    ha_low = pd.Series(index=df.index, dtype=float)
    
    # 使用cumsum和shift进行向量化计算
    for i in range(1, len(df)):
        ha_open.iloc[i] = (ha_open.iloc[i-1] + ha_close.iloc[i-1]) / 2
    
    # 向量化计算HA高低价
    ha_high = df[['high']].join(pd.DataFrame({
        'ha_open': ha_open,
        'ha_close': ha_close
    })).max(axis=1)
    
    ha_low = df[['low']].join(pd.DataFrame({
        'ha_open': ha_open,
        'ha_close': ha_close
    })).min(axis=1)
    
    # ========== ATR计算 ==========
    # 使用向量化操作计算TR
    tr1 = ha_high - ha_low
    tr2 = (ha_high - ha_close.shift(1)).abs()
    tr3 = (ha_low - ha_close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    # 使用向量化操作计算RMA
    rma = pd.Series(index=df.index, dtype=float)
    alpha = 1.0 / length
    
    # 初始化RMA
    rma.iloc[length-1] = tr.iloc[:length].mean()
    
    # 使用向量化操作计算RMA
    for i in range(length, len(df)):
        rma.iloc[i] = alpha * tr.iloc[i] + (1 - alpha) * rma.iloc[i-1]
    
    # ========== SuperTrend计算 ==========
    src = (ha_high + ha_low) / 2
    upper_band = pd.Series(index=df.index, dtype=float)
    lower_band = pd.Series(index=df.index, dtype=float)
    super_trend = pd.Series(index=df.index, dtype=float)
    direction = pd.Series(0, index=df.index)
    
    # 初始化第一个有效值
    start_idx = length - 1
    upper_band.iloc[start_idx] = src.iloc[start_idx] + multiplier * rma.iloc[start_idx]
    lower_band.iloc[start_idx] = src.iloc[start_idx] - multiplier * rma.iloc[start_idx]
    super_trend.iloc[start_idx] = upper_band.iloc[start_idx]
    direction.iloc[start_idx] = 1
    
    # 使用向量化操作计算SuperTrend
    for i in range(start_idx+1, len(df)):
        current_upper = src.iloc[i] + multiplier * rma.iloc[i]
        current_lower = src.iloc[i] - multiplier * rma.iloc[i]
        
        lower_band.iloc[i] = current_lower if (current_lower > lower_band.iloc[i-1] or ha_close.iloc[i-1] < lower_band.iloc[i-1]) else lower_band.iloc[i-1]
        upper_band.iloc[i] = current_upper if (current_upper < upper_band.iloc[i-1] or ha_close.iloc[i-1] > upper_band.iloc[i-1]) else upper_band.iloc[i-1]
        
        if i == start_idx or pd.isna(rma.iloc[i-1]):
            direction.iloc[i] = -1
        elif super_trend.iloc[i-1] == upper_band.iloc[i-1]:
            direction.iloc[i] = 1 if ha_close.iloc[i] > upper_band.iloc[i] else -1
        else:
            direction.iloc[i] = -1 if ha_close.iloc[i] < lower_band.iloc[i] else 1
        
        super_trend.iloc[i] = lower_band.iloc[i] if direction.iloc[i] == 1 else upper_band.iloc[i]
    
    # 将计算结果添加到DataFrame中
    df['ha_open'] = ha_open
    df['ha_high'] = ha_high
    df['ha_low'] = ha_low
    df['ha_close'] = ha_close
    df['supertrend'] = super_trend
    df['direction'] = direction
    
    return df

xtdata.download_history_data(ts_code, period='5m', start_time='20240101')
xtdata.subscribe_quote(ts_code, '5m')
df = xtdata.get_market_data_ex([], [ts_code], period='30m', start_time='20240101')
df = df[ts_code]
df['trade_time'] = pd.to_datetime(df['time'].apply(lambda x: datetime.fromtimestamp(x / 1000.0)))
df = ha_st_pine(df, length=df_parm['period'].iloc[0], multiplier=df_parm['multiplier'].iloc[0])
df['ts_code'] = ts_code
df = df.round(3)
# if '0930' <= datetime.now().strftime('%H%M') <= '1500':
#     df = df.iloc[:-1]  # xtdata.subscribe_quote是秒级的,会产生未完结30mK线数据
print(df[['trade_time','ts_code','direction','open','high','low','close','ha_close','supertrend']].tail(16))