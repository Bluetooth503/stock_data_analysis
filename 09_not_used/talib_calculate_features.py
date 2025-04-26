# -*- coding: utf-8 -*-
from common import *
import talib
from scipy.stats import percentileofscore
import swifter
swifter.config.progress_bar = False


# ================================= 配置日志 =================================
logger = setup_logger()

# ================================= 读取配置文件 =================================
config = load_config()
engine = create_engine(get_pg_connection_string(config))


# ================================= 获取原始数据 =================================
def get_stock_data(start_date, end_date):
    """获取股票数据，包括K线和资金流数据"""    
    # 获取K线和基础数据
    query_kline = f"""
    SELECT k.ts_code, k.trade_date, k.open, k.high, k.low, k.close, k.vol as volume, k.amount,
           b.turnover_rate, b.volume_ratio, b.total_share, b.float_share, b.free_share,
           b.total_mv, b.circ_mv
    FROM a_stock_daily_k k
    LEFT JOIN a_stock_daily_basic b ON k.ts_code = b.ts_code AND k.trade_date = b.trade_date
    WHERE k.trade_date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY k.ts_code, k.trade_date
    """
    
    # 获取资金流数据
    query_moneyflow = f"""
    SELECT ts_code, trade_date,
           buy_elg_vol, buy_elg_amount, sell_elg_vol, sell_elg_amount,
           buy_lg_vol, buy_lg_amount, sell_lg_vol, sell_lg_amount,
           buy_md_vol, buy_md_amount, sell_md_vol, sell_md_amount,
           buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount,
           net_mf_vol, net_mf_amount
    FROM a_stock_moneyflow
    WHERE trade_date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY ts_code, trade_date
    """
    
    df_kline = pd.read_sql(query_kline, engine)
    df_moneyflow = pd.read_sql(query_moneyflow, engine)
    
    # 合并数据
    df = pd.merge(df_kline, df_moneyflow, on=['ts_code', 'trade_date'], how='left')    
    
    return df

# ================================= 指标计算 =================================
def calculate_features(df: pd.DataFrame) -> pd.DataFrame:
    features = []
    
    # 计算委比和委差
    df['total_buy_vol']  = df['buy_elg_vol']  + df['buy_lg_vol']  + df['buy_md_vol']  + df['buy_sm_vol']
    df['total_sell_vol'] = df['sell_elg_vol'] + df['sell_lg_vol'] + df['sell_md_vol'] + df['sell_sm_vol']
    df['委差'] = df['total_buy_vol'] - df['total_sell_vol']
    df['委比'] = (df['total_buy_vol'] - df['total_sell_vol']) / (df['total_buy_vol'] + df['total_sell_vol']) * 100
    
    # 计算净流入
    df['特大单净流入'] = df['buy_elg_amount'] - df['sell_elg_amount']
    df['大单净流入']   = df['buy_lg_amount']  - df['sell_lg_amount']
    df['中单净流入']   = df['buy_md_amount']  - df['sell_md_amount']
    df['小单净流入']   = df['buy_sm_amount']  - df['sell_sm_amount']

    # 计算资金流/市值比率
    df['特大单净流入/市值'] = df['特大单净流入'] / df['total_mv']
    df['大单净流入/市值']   = df['大单净流入']   / df['total_mv']
    df['中单净流入/市值']   = df['中单净流入']   / df['total_mv']
    df['小单净流入/市值']   = df['小单净流入']   / df['total_mv']
    
    # 计算下一个交易日的涨幅
    df['next_day_return'] = df.groupby('ts_code')['close'].shift(-1) / df['close'] - 1
    df['target'] = (df['next_day_return'] >= 0.08).astype(int)

    # 按股票分组处理
    for ts_code, group in df.groupby('ts_code'):
        try:
            # 将trade_date转换为datetime类型并设置为索引
            group['trade_date'] = pd.to_datetime(group['trade_date'])
            group = group.set_index('trade_date').sort_index()
            
            # 确保数据类型正确
            for col in ['open', 'high', 'low', 'close', 'volume']:
                group[col] = group[col].astype(float)
            
            # 计算价格均线
            group['SMA_5']  = talib.SMA(group['close'], timeperiod=5)
            group['SMA_10'] = talib.SMA(group['close'], timeperiod=10)
            group['SMA_20'] = talib.SMA(group['close'], timeperiod=20)
            group['SMA_60'] = talib.SMA(group['close'], timeperiod=60)
            
            # 计算成交量均线
            group['volume_SMA_5']  = talib.SMA(group['volume'], timeperiod=5)
            group['volume_SMA_10'] = talib.SMA(group['volume'], timeperiod=10)
            group['volume_SMA_20'] = talib.SMA(group['volume'], timeperiod=20)
            group['volume_SMA_60'] = talib.SMA(group['volume'], timeperiod=60)
            
            # 计算动量指标
            group['RSI_10'] = talib.RSI(group['close'], timeperiod=10)
            group['MFI_10'] = talib.MFI(group['high'], group['low'], group['close'], group['volume'], timeperiod=10)
            slowk, slowd = talib.STOCH(group['high'], group['low'], group['close'], fastk_period=10, slowk_period=3, slowd_period=3)
            group['STOCH_K'] = slowk
            group['STOCH_D'] = slowd
            group['WILLR_10'] = talib.WILLR(group['high'], group['low'], group['close'], timeperiod=10)
            group['MOM_10'] = talib.MOM(group['close'], timeperiod=10)
            group['ROC_10'] = talib.ROC(group['close'], timeperiod=10)
            
            # 计算波动性指标
            upper, middle, lower = talib.BBANDS(group['close'], timeperiod=20)
            group['BB_UPPER'] = upper
            group['BB_MIDDLE'] = middle
            group['BB_LOWER'] = lower
            group['ATR_10'] = talib.ATR(group['high'], group['low'], group['close'], timeperiod=10)
            
            # 计算成交量指标
            group['OBV'] = talib.OBV(group['close'], group['volume'])
            group['OBV_SMA_5']  = talib.SMA(group['OBV'], timeperiod=5)
            group['OBV_SMA_10'] = talib.SMA(group['OBV'], timeperiod=10)
            group['OBV_SMA_20'] = talib.SMA(group['OBV'], timeperiod=20)
            group['OBV_SMA_60'] = talib.SMA(group['OBV'], timeperiod=60)
            group['AD'] = talib.AD(group['high'], group['low'], group['close'], group['volume'])
            group['ADOSC'] = talib.ADOSC(group['high'], group['low'], group['close'], group['volume'], fastperiod=3, slowperiod=10)
            
            # 计算MACD
            macd, macdsignal, macdhist = talib.MACD(group['close'], fastperiod=12, slowperiod=26, signalperiod=9)
            group['MACD'] = macd
            group['MACD_SIGNAL'] = macdsignal
            group['MACD_HIST'] = macdhist
            
            # 计算VWAP (Volume Weighted Average Price)
            group['VWAP'] = (group['close'] * group['volume']).cumsum() / group['volume'].cumsum()
            
            # 计算n日最大值和最小值
            group['close_05d_max'] = group['close'].rolling(window=5).max()
            group['close_05d_min'] = group['close'].rolling(window=5).min()
            group['close_10d_max'] = group['close'].rolling(window=10).max()
            group['close_10d_min'] = group['close'].rolling(window=10).min()
            group['close_20d_max'] = group['close'].rolling(window=20).max()
            group['close_20d_min'] = group['close'].rolling(window=20).min()
            group['close_60d_max'] = group['close'].rolling(window=60).max()
            group['close_60d_min'] = group['close'].rolling(window=60).min()

            group['volume_05d_max'] = group['volume'].rolling(window=5).max()
            group['volume_05d_min'] = group['volume'].rolling(window=5).min()
            group['volume_10d_max'] = group['volume'].rolling(window=10).max()
            group['volume_10d_min'] = group['volume'].rolling(window=10).min()
            group['volume_20d_max'] = group['volume'].rolling(window=20).max()
            group['volume_20d_min'] = group['volume'].rolling(window=20).min()
            group['volume_60d_max'] = group['volume'].rolling(window=60).max()
            group['volume_60d_min'] = group['volume'].rolling(window=60).min()

            # 计算分位数（优化后的高效方法）
            def calc_percentile(series):
                return percentileofscore(series.iloc[:-1], series.iloc[-1])/100.0
                
            # 使用swifter加速分位数计算
            window_size = 365
            min_period = 60
            rolling_config = {'window': window_size, 'min_periods': min_period}
            
            group['close_percentile']   = group['close'].swifter.rolling(**rolling_config).apply(calc_percentile)
            group['volume_percentile'] = group['volume'].swifter.rolling(**rolling_config).apply(calc_percentile)
            group['特大单净流入_percentile'] = group['特大单净流入'].swifter.rolling(**rolling_config).apply(calc_percentile)
            group['大单净流入_percentile'] = group['大单净流入'].swifter.rolling(**rolling_config).apply(calc_percentile)
            group['中单净流入_percentile'] = group['中单净流入'].swifter.rolling(**rolling_config).apply(calc_percentile)
            group['小单净流入_percentile'] = group['小单净流入'].swifter.rolling(**rolling_config).apply(calc_percentile)

            # 布林带位置
            group['bb_position'] = (group['close'] - group['BB_MIDDLE']) / (group['BB_UPPER'] - group['BB_LOWER'])            
            
            # 计算累计净流入与市值的比率
            group['特大单3日累计/市值'] = group['特大单净流入'].rolling(3).sum() / group['total_mv']
            group['大单3日累计/市值']   = group['大单净流入'].rolling(3).sum()   / group['total_mv']
            group['中单3日累计/市值']   = group['中单净流入'].rolling(3).sum()   / group['total_mv']
            group['小单3日累计/市值']   = group['小单净流入'].rolling(3).sum()   / group['total_mv']

            # 移除包含NaN的行
            group = group.replace([np.inf, -np.inf], np.nan)
            group = group.dropna()
            
            # 重置索引，保留trade_date列
            group = group.reset_index()
            
            features.append(group)
            
        except Exception as e:
            logger.error(f"处理股票{ts_code}时出错: {str(e)}")
            continue
    
    if not features:
        raise ValueError("没有成功处理任何股票数据")
    
    df_features = pd.concat(features)

    return df_features

def main():
    # 设置时间范围
    start_date = '20100101'
    end_date   = '20250301'
    
    # 获取数据
    logger.info("获取股票数据...")
    # df = get_stock_data(start_date, end_date)
    # df.to_parquet('tmp_stock_data.parquet', engine='pyarrow', index=False)
    df = pd.read_parquet('tmp_stock_data.parquet')

    logger.info("开始计算特征...")
    df_features = calculate_features(df)
    df_features.to_parquet('tmp_stock_features_df_test.parquet', engine='pyarrow', index=False)


if __name__ == "__main__":
    main()
