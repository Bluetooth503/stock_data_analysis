from common import *

def calculate_ha_supertrend(df: pd.DataFrame, multiplier: float = 3, atr_period: int = 10) -> pd.DataFrame:
    """
    计算Heikin-Ashi K线和SuperTrend指标
    
    Parameters:
        df (pd.DataFrame): 包含OHLC数据的DataFrame
        multiplier (float): ATR乘数
        atr_period (int): ATR计算周期
        
    Returns:
        pd.DataFrame: 包含HA和Supertrend值的DataFrame
    """
    # 参数验证
    if multiplier <= 0:
        raise ValueError("multiplier must be positive")
    if atr_period <= 0:
        raise ValueError("atr_period must be positive")
    if not all(col in df.columns for col in ['open', 'high', 'low', 'close']):
        raise ValueError("DataFrame must contain open, high, low, and close columns")
    
    # 计算Heikin-Ashi K线
    ha_close = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    ha_open = pd.Series(0.0, index=df.index)
    ha_open.iloc[0] = df['open'].iloc[0]
    for i in range(1, len(df)):
        ha_open.iloc[i] = (ha_open.iloc[i-1] + ha_close.iloc[i-1]) / 2
    ha_high = df[['high', 'open', 'close']].max(axis=1)
    ha_low = df[['low', 'open', 'close']].min(axis=1)
    
    # 计算ATR
    tr = pd.DataFrame({
        'tr1': ha_high - ha_low,
        'tr2': abs(ha_high - ha_close.shift(1)),
        'tr3': abs(ha_low - ha_close.shift(1))
    }).max(axis=1)
    atr = tr.ewm(span=atr_period, adjust=False).mean()
    
    # 计算基础轨道
    hl2 = (ha_high + ha_low) / 2
    upperband = hl2 + multiplier * atr
    lowerband = hl2 - multiplier * atr
    
    # 初始化数组
    final_upperband = pd.Series(index=df.index, dtype=float)
    final_lowerband = pd.Series(index=df.index, dtype=float)
    supertrend = pd.Series(index=df.index, dtype=float)
    direction = pd.Series(index=df.index, dtype=int)
    
    # 设置第一个值
    final_upperband.iloc[0] = upperband.iloc[0]
    final_lowerband.iloc[0] = lowerband.iloc[0]
    direction.iloc[0] = 1
    supertrend.iloc[0] = final_upperband.iloc[0]
    
    # 计算SuperTrend
    for i in range(1, len(df)):
        # 更新上下轨
        if (lowerband.iloc[i] > final_lowerband.iloc[i-1] or 
            ha_close.iloc[i-1] < final_lowerband.iloc[i-1]):
            final_lowerband.iloc[i] = lowerband.iloc[i]
        else:
            final_lowerband.iloc[i] = final_lowerband.iloc[i-1]
            
        if (upperband.iloc[i] < final_upperband.iloc[i-1] or 
            ha_close.iloc[i-1] > final_upperband.iloc[i-1]):
            final_upperband.iloc[i] = upperband.iloc[i]
        else:
            final_upperband.iloc[i] = final_upperband.iloc[i-1]
        
        # 确定方向
        if np.isnan(atr.iloc[i-1]):
            direction.iloc[i] = 1
        elif supertrend.iloc[i-1] == final_upperband.iloc[i-1]:
            direction.iloc[i] = -1 if ha_close.iloc[i] > final_upperband.iloc[i] else 1
        else:
            direction.iloc[i] = 1 if ha_close.iloc[i] < final_lowerband.iloc[i] else -1
        
        # 设置SuperTrend值
        supertrend.iloc[i] = final_lowerband.iloc[i] if direction.iloc[i] == -1 else final_upperband.iloc[i]
    
    # 生成结果
    result_df = pd.DataFrame({
        # 'ha_open': ha_open,
        # 'ha_high': ha_high,
        # 'ha_low': ha_low,
        # 'ha_close': ha_close,
        'supertrend': supertrend,
        'direction': direction
    }, index=df.index)
    
    return result_df


df = get_stock_data('wfq', '000858.SZ', start_date='2000-01-01', end_date='2024-12-31')
result_df = calculate_ha_supertrend(df)
df = pd.concat([df, result_df], axis=1)

df.tail(100).to_csv('000858.SZ.csv', index=False)
print(df.tail(20))