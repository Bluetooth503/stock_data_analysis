import numpy as np
import pandas_ta as ta
from numba import njit
import pandas as pd


def ha_st_py(df, length, multiplier):
    '''Heikin Ashi计算'''
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
    # SuperTrend计算
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


def ha_st_numba(df: pd.DataFrame, length: int = 10, multiplier: float = 3.0) -> pd.DataFrame:
    """基于Numba加速的Heikin Ashi SuperTrend指标计算"""
    # 转换为numpy数组提升计算性能
    o, h, l, c = (df[col].values.astype(np.float64) for col in ['open', 'high', 'low', 'close'])
    n = len(o)
    
    @njit
    def calculate_ha(o, h, l, c):
        """计算Heikin-Ashi价格序列"""
        ha_close = (o + h + l + c) / 4
        ha_open = np.empty_like(ha_close)
        ha_open[0] = (o[0] + c[0]) / 2
        for i in range(1, n):
            ha_open[i] = (ha_open[i-1] + ha_close[i-1]) / 2
        return ha_open, ha_close

    @njit
    def calculate_supertrend(ha_open, ha_close, length, multiplier):
        """计算SuperTrend指标的核心逻辑"""
        # 计算HA最高价/最低价
        ha_high = np.maximum(h, np.maximum(ha_open, ha_close))
        ha_low = np.minimum(l, np.minimum(ha_open, ha_close))

        # 计算真实波动范围(True Range)
        tr = np.maximum(ha_high - ha_low, 
                      np.maximum(np.abs(ha_high - ha_close), 
                               np.abs(ha_low - ha_close)))

        # 计算RMA(滚动移动平均)
        rma = np.full(n, np.nan)
        rma[length-1] = np.mean(tr[:length])
        for i in range(length, n):
            rma[i] = (tr[i] + (length-1)*rma[i-1]) / length

        # SuperTrend核心计算逻辑
        src = (ha_high + ha_low) / 2
        direction = np.zeros(n)
        super_trend = np.zeros(n)
        
        upper = src + multiplier * rma
        lower = src - multiplier * rma
        
        for i in range(length-1, n):
            if i == length-1 or df.close[i-1] > upper[i-1]:
                direction[i] = 1
                super_trend[i] = lower[i]
            else:
                direction[i] = -1
                super_trend[i] = upper[i]
                
        return ha_high, ha_low, super_trend, direction

    # 执行核心计算过程
    ha_open, ha_close = calculate_ha(o, h, l, c)
    ha_high, ha_low, super_trend, direction = calculate_supertrend(
        ha_open, ha_close, length, multiplier
    )

    # 更新返回的DataFrame
    return df.assign(
        ha_open=ha_open,
        ha_high=ha_high,
        ha_low=ha_low,
        ha_close=ha_close,
        supertrend=super_trend,
        direction=direction
    )