# -*- coding: utf-8 -*-
from common import *
import pytz

'''
因子描述：
1.3秒快照平均bid_ask_ratio
2.一分钟累计bid_ask_ratio
'''

# ================================= 读取配置文件 =================================
logger = setup_logger()
config = load_config()
engine = create_engine(get_pg_connection_string(config))
path = r"D:\a_stock_level1_data\factor01\parquet"
os.chdir(path)

# ================================= 计算指标 =================================
def process_per_stock():
    """按股票循环处理，将数据保存为parquet文件"""
    os.makedirs(path, exist_ok=True)
    output_file = os.path.join(path, 'factor01.parquet')
    sql_stocks = text("""SELECT DISTINCT ts_code FROM public.a_stock_qmt_sector WHERE index_name = '沪深A股' ORDER BY ts_code LIMIT 5""")
    
    all_data = []
    with engine.connect() as conn:
        stocks = pd.read_sql(sql_stocks, conn)['ts_code'].tolist()
        total_stocks = len(stocks)
        
        for idx, stock in enumerate(stocks, 1):
            logger.info(f"处理进度: {idx}/{total_stocks} - {stock}")
            # 查询单个股票的数据
            sql = text("""
                SELECT 
                    trade_time,
                    ts_code,
                    avg_bid_ask_ratio,
                    total_bid_ask_ratio,
                    close,
                    LEAD(close, 5)  OVER (PARTITION BY ts_code ORDER BY trade_time) as close_5min,
                    LEAD(close, 15) OVER (PARTITION BY ts_code ORDER BY trade_time) as close_15min,
                    LEAD(close, 30) OVER (PARTITION BY ts_code ORDER BY trade_time) as close_30min
                FROM
                (
                    SELECT
                        time_bucket('1 minutes', "trade_time") AS trade_time,
                        ts_code,
                        FIRST(open, "trade_time") AS open,
                        MAX(high) AS high,
                        MIN(low) AS low,
                        LAST(last_price, "trade_time") AS close,
                        LAST(volume, "trade_time") - FIRST(volume, "trade_time") AS volume,
                        LAST(amount, "trade_time") - FIRST(amount, "trade_time") AS amount,
                        LAST(transaction_num, "trade_time") - FIRST(transaction_num, "trade_time") AS transaction_num,
                        AVG((bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5)*1.0 / 
                            NULLIF(ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5, 0)*1.0) AS avg_bid_ask_ratio,
                        CASE WHEN SUM(ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5) = 0 THEN 0
                            ELSE SUM(bid_volume1 + bid_volume2 + bid_volume3 + bid_volume4 + bid_volume5)*1.0 /
                                SUM(ask_volume1 + ask_volume2 + ask_volume3 + ask_volume4 + ask_volume5)*1.0
                        END AS total_bid_ask_ratio
                    FROM public.ods_a_stock_level1_data
                    WHERE ts_code = :stock
                    GROUP BY time_bucket('1 minutes', "trade_time"), ts_code
                    ORDER BY trade_time
                ) t1
            """)
            # 读取数据到DataFrame
            df = pd.read_sql(sql, conn, params={'stock': stock})
            if not df.empty:
                # 确保trade_time列是datetime类型，并设置正确的时区
                df['trade_time'] = pd.to_datetime(df['trade_time'])
                print("时区信息:", df['trade_time'].dt.tz)
                print("示例时间:", df['trade_time'].iloc[0])
                all_data.append(df)
            else:
                logger.warning(f"{stock} 没有数据")
        
        if all_data:
            # 合并所有数据
            final_df = pd.concat(all_data, ignore_index=True)
            # 保存为parquet文件，使用zstd压缩
            final_df.to_parquet(output_file, compression='zstd', index=False)
            logger.info("所有股票数据处理完成并保存为parquet文件")
        else:
            logger.warning("没有数据需要保存")

# ================================= 数据读取功能 =================================
def read_factor_data(file='factor01.parquet', stock_codes=None):
    """读取parquet文件"""
    df = pd.read_parquet(file)
    
    if stock_codes:
        df = df[df['ts_code'].isin(stock_codes)]
    
    return df

# 示例用法
if __name__ == '__main__':
    process_per_stock()
    df = read_factor_data()
    print(df.head())


