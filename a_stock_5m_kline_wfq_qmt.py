# -*- coding: utf-8 -*-
from common import *
from xtquant import xtdata
xtdata.enable_hello = False
data_dir = 'E:\\国金证券QMT交易端\\userdata_mini\\datadir'
logger = setup_logger()  # 初始化日志


# 定义初始变量
stock_list = xtdata.get_stock_list_in_sector('沪深A股')
start_time = '20000101'
end_time   = datetime.today().strftime('%Y%m%d')
period     = '5m'
table_name = 'a_stock_5m_kline_wfq_qmt'


# 先把加密数据下载到本地,再用get_local_data读取
def download_history_kline(stock_list, start_time, end_time, period):
    for stock_code in tqdm(stock_list, desc = '开始下载数据到本地'):
        # xtdata.download_history_data(stock_code, period = period, start_time = start_time, end_time = end_time) # 全量下载
        xtdata.download_history_data(stock_code, period = period, start_time = '', end_time = '', incrementally = True) # 增量下载

# download_history_kline(stock_list, start_time, end_time, period)
data_dict = xtdata.get_local_data(stock_list = stock_list, period = period, start_time = start_time, end_time = end_time, dividend_type = 'none')


def process_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """处理合并后的所有股票数据"""
    df['trade_time'] = pd.to_datetime(df['time'].apply(lambda x: datetime.fromtimestamp(x / 1000.0)))
    df = df.drop(columns=['time'])
    
    df = df.rename(columns={
        'settelementPrice': 'settelement_price',
        'openInterest': 'open_interest',
        'preClose': 'pre_close',
        'suspendFlag': 'suspend_flag'
    })
    
    numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount', 'settelement_price', 'open_interest', 'pre_close']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
    
    df['suspend_flag'] = df['suspend_flag'].astype(bool)
    
    return df[['trade_time', 'ts_code', 'open', 'high', 'low', 'close', 
              'volume', 'amount', 'settelement_price', 'open_interest',
              'pre_close', 'suspend_flag']]


def create_table_if_not_exists(engine) -> None:
    """创建数据表（如果不存在）"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS a_stock_5m_kline_wfq_qmt (
        trade_time TIMESTAMP NOT NULL,
        ts_code VARCHAR(20) NOT NULL,
        open NUMERIC(18, 4),
        high NUMERIC(18, 4),
        low NUMERIC(18, 4),
        close NUMERIC(18, 4),
        volume NUMERIC(18, 4),
        amount NUMERIC(18, 4),
        settelement_price NUMERIC(18, 4),
        open_interest NUMERIC(18, 4),
        pre_close NUMERIC(18, 4),
        suspend_flag BOOLEAN,
        PRIMARY KEY (trade_time, ts_code)
    );
    
    -- 创建索引以提升查询性能
    CREATE INDEX IF NOT EXISTS idx_stock_5m_kline_ts_code 
    ON a_stock_5m_kline_wfq_qmt(ts_code);
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))


def main():
    # 数据库配置
    config = load_config()
    engine = create_engine(get_pg_connection_string(config))
    
    # 确保表存在
    create_table_if_not_exists(engine)
    
    # 处理数据
    df_list = []
    for stock_code, stock_data in tqdm(data_dict.items(), desc = '开始处理本地加密数据'):
        stock_df = stock_data.copy()
        stock_df['ts_code'] = stock_code
        df_list.append(stock_df)
            
    # 合并所有数据
    df = pd.concat(df_list, axis=0).reset_index(drop=True)
    df = process_stock_data(df)
    df = df.sort_values(['ts_code', 'trade_time']).reset_index(drop=True)    

    # 更新数据
    insert_sql = f"""
    INSERT INTO {table_name} 
    (trade_time, ts_code, open, high, low, close, volume, amount,
        settelement_price, open_interest, pre_close, suspend_flag)
    SELECT 
        trade_time, ts_code, open, high, low, close, volume, amount,
        settelement_price, open_interest, pre_close, suspend_flag
    FROM temp_{table_name}_{int(time.time())}
    ON CONFLICT (trade_time, ts_code) DO NOTHING;
    """
    upsert_data(df, table_name, insert_sql, engine)
        

if __name__ == "__main__":
    main()
