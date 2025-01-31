# -*- coding: utf-8 -*-
from common import *
import baostock as bs

# 定义初始变量
index_code = '000300.SH'  # 沪深300指数
period     = 'd'
start_date = '20000101'
end_date   = datetime.today().strftime('%Y%m%d') # '20241231'
table_name = 'a_index_1day_kline_baostock'
tmp_table  = f"temp_{table_name}_{int(time.time())}"


def create_table(engine):
    """创建指数日K线表"""
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        trade_time DATE NOT NULL,
        ts_code VARCHAR(20) NOT NULL,
        open NUMERIC(12,4),
        high NUMERIC(12,4),
        low NUMERIC(12,4),
        close NUMERIC(12,4),
        volume NUMERIC(20,4),
        amount NUMERIC(20,4),
        PRIMARY KEY (ts_code, trade_time)
    );
    
    -- 创建索引以提升查询性能
    CREATE INDEX IF NOT EXISTS idx_index_1day_kline_ts_code 
    ON {table_name}(ts_code);
    
    CREATE INDEX IF NOT EXISTS idx_index_1day_kline_trade_time 
    ON {table_name}(trade_time);
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()  # 确保提交事务
        logger.info(f"表 {table_name} 创建成功")
    except Exception as e:
        logger.error(f"创建表失败: {str(e)}")
        raise

def get_index_data(ts_code, start_date, end_date):
    """从baostock获取指数日K线数据"""
    bs_code = convert_to_baostock_code(ts_code)
    bs_start_date = convert_date_format(start_date)
    bs_end_date = convert_date_format(end_date)
    
    # 查询指数日K线数据
    rs = bs.query_history_k_data_plus(bs_code, "date,open,high,low,close,volume,amount",
        start_date = bs_start_date, end_date = bs_end_date, frequency = period, adjustflag="3")
    
    # 转换为DataFrame
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    df = pd.DataFrame(data_list, columns=rs.fields)
        
    # 处理数据
    if not df.empty:
        df['ts_code'] = bs_code
        df.rename(columns={'date': 'trade_time'}, inplace=True)
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        df['trade_time'] = pd.to_datetime(df['trade_time'])
    # 确保列的顺序正确
    df = df[['trade_time', 'ts_code', 'open', 'high', 'low', 'close', 'volume', 'amount']]
    return df

def main():
    # 加载配置
    config = load_config()
    engine = create_engine(get_pg_connection_string(config))
    
    try:
        # 创建表 - 确保在任何操作之前创建表
        create_table(engine)
        
        # 登录并获取数据
        bs.login()
        df = get_index_data(index_code, start_date, end_date)
        
        if not df.empty:
            # 使用 upsert_data 保存到数据库
            insert_sql = f"""
                INSERT INTO {table_name} (trade_time, ts_code, open, high, low, close, volume, amount)
                SELECT trade_time, ts_code, open, high, low, close, volume, amount
                FROM {tmp_table} 
                ON CONFLICT (ts_code, trade_time) DO NOTHING;
            """    
            upsert_data(df, table_name, tmp_table, insert_sql, engine)
        else:
            logger.warning("没有获取到数据")
            
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        raise
    finally:
        # 确保无论如何都会登出
        bs.logout()

if __name__ == '__main__':
    main()
