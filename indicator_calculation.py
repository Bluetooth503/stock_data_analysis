from common import *

# 用 Heikin-Ashi K线计算 SuperTrend 指标
def calculate_ha_supertrend(df: pd.DataFrame, factor: float = 3, atr_period: int = 10) -> pd.DataFrame:
    # 计算 Heikin-Ashi K线
    ha_close = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    ha_open = pd.Series(df['open'].iloc[0], index=df.index)
    for i in range(1, len(df)):
        ha_open.iloc[i] = (ha_open.iloc[i-1] + ha_close.iloc[i-1]) / 2
    # ha_high = df[['high', 'open', 'close']].max(axis=1)
    # ha_low = df[['low', 'open', 'close']].min(axis=1)
    ha_high = pd.concat([df['high'], ha_open, ha_close], axis=1).max(axis=1)
    ha_low = pd.concat([df['low'], ha_open, ha_close], axis=1).min(axis=1)
   
    # 计算 True Range
    tr = pd.Series(index=df.index)
    tr.iloc[0] = ha_high.iloc[0] - ha_low.iloc[0]
    for i in range(1, len(df)):
        tr.iloc[i] = max(
            ha_high.iloc[i] - ha_low.iloc[i],
            abs(ha_high.iloc[i] - ha_close.iloc[i-1]),
            abs(ha_low.iloc[i] - ha_close.iloc[i-1])
        )
    
    # 计算 RMA (类似EMA，但使用 alpha = 1/length)
    alpha = 1.0 / atr_period
    atr = pd.Series(index=df.index)
    atr.iloc[0] = tr.iloc[0:atr_period].mean()  # 第一个值使用SMA
    for i in range(1, len(df)):
        atr.iloc[i] = alpha * tr.iloc[i] + (1 - alpha) * atr.iloc[i-1]
    
    # 计算基础轨道
    hl2 = (ha_high + ha_low) / 2
    upperband = hl2 + factor * atr
    lowerband = hl2 - factor * atr
    
    # 初始化最终轨道和方向
    final_upperband = pd.Series(index=df.index)
    final_lowerband = pd.Series(index=df.index)
    direction = pd.Series(1, index=df.index)  # 1: 下跌, -1: 上涨
    supertrend = pd.Series(index=df.index)
    
    # 设置第一个值
    final_upperband.iloc[0] = upperband.iloc[0]
    final_lowerband.iloc[0] = lowerband.iloc[0]
    supertrend.iloc[0] = final_upperband.iloc[0]
    
    # 计算SuperTrend
    for i in range(1, len(df)):
        # 更新轨道
        if upperband.iloc[i] < final_upperband.iloc[i-1] or ha_close.iloc[i-1] > final_upperband.iloc[i-1]:
            final_upperband.iloc[i] = upperband.iloc[i]
        else:
            final_upperband.iloc[i] = final_upperband.iloc[i-1]
            
        if lowerband.iloc[i] > final_lowerband.iloc[i-1] or ha_close.iloc[i-1] < final_lowerband.iloc[i-1]:
            final_lowerband.iloc[i] = lowerband.iloc[i]
        else:
            final_lowerband.iloc[i] = final_lowerband.iloc[i-1]
        
        # 更新方向和SuperTrend值
        if pd.isna(atr.iloc[i-1]):
            direction.iloc[i] = 1  # 默认下跌趋势
        elif supertrend.iloc[i-1] == final_upperband.iloc[i-1]:
            direction.iloc[i] = -1 if ha_close.iloc[i] > final_upperband.iloc[i] else 1  # 突破上轨=-1（上涨）
        else:
            direction.iloc[i] = 1 if ha_close.iloc[i] < final_lowerband.iloc[i] else -1  # 跌破下轨=1（下跌）
        
        supertrend.iloc[i] = final_lowerband.iloc[i] if direction.iloc[i] == -1 else final_upperband.iloc[i]
    
    # 生成结果
    result_df = pd.DataFrame({
        # 'ha_open': ha_open,
        # 'ha_high': ha_high,
        # 'ha_low': ha_low,
        # 'ha_close': ha_close,
        'supertrend': supertrend,
        'direction': direction,
        # 'upperband': final_upperband,
        # 'lowerband': final_lowerband
    }, index=df.index)
    
    return result_df

df = get_stock_data('wfq', '000858.SZ', start_date='2000-01-01', end_date='2024-12-31')
result_df = calculate_ha_supertrend(df)
df = pd.concat([df, result_df], axis=1)

df.tail(200).to_csv('000858.SZ.csv', index=False)
print(df.tail(20))