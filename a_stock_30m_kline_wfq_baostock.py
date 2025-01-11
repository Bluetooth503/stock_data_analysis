import baostock as bs
import pandas as pd
import psycopg2
import configparser
from datetime import datetime
import os

def convert_to_baostock_code(ts_code):
    """将 tushare 格式的代码转换为 baostock 格式"""
    code, market = ts_code.split('.')
    if market == 'SZ':
        return f"sz.{code}"
    return f"sh.{code}"

def convert_to_tushare_code(baostock_code):
    """将 baostock 格式的代码转换为 tushare 格式"""
    market, code = baostock_code.split('.')
    market = market.upper()
    return f"{code}.{market}"

def read_config():
    """读取配置文件"""
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

def get_db_connection(config):
    """创建数据库连接"""
    return psycopg2.connect(
        host=config['postgresql']['host'],
        port=config['postgresql']['port'],
        database=config['postgresql']['database'],
        user=config['postgresql']['user'],
        password=config['postgresql']['password']
    )

def create_kline_table(conn):
    """如果表不存在，创建30分钟K线数据表"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS a_stock_30m_kline_wfq_baostock (
                ts_code VARCHAR(10),
                trade_date DATE,
                trade_time TIME,
                open DECIMAL(10,4),
                high DECIMAL(10,4),
                low DECIMAL(10,4),
                close DECIMAL(10,4),
                volume BIGINT,
                amount DECIMAL(20,4),
                adjustflag INTEGER,
                PRIMARY KEY (ts_code, trade_date, trade_time)
            )
        """)
    conn.commit()

def get_stock_codes(conn):
    """从数据库获取股票代码列表"""
    with conn.cursor() as cursor:
        cursor.execute("SELECT ts_code FROM a_stock_name")
        return [row[0] for row in cursor.fetchall()]

def download_30min_kline(stock_code, start_date, end_date):
    """下载指定股票的30分钟K线数据"""
    bs_code = convert_to_baostock_code(stock_code)
    
    rs = bs.query_history_k_data_plus(
        code=bs_code,
        fields="date,time,code,open,high,low,close,volume,amount,adjustflag",
        start_date=start_date,
        end_date=end_date,
        frequency="30",
        adjustflag="3"  # 不复权
    )
    
    if rs.error_code != '0':
        print(f"下载 {stock_code} 数据时出错: {rs.error_msg}")
        return None
    
    data_list = []
    while (rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        # 将代码转回 tushare 格式
        row[2] = convert_to_tushare_code(row[2])
        data_list.append(row)
    
    if not data_list:
        return None
        
    df = pd.DataFrame(data_list, columns=rs.fields)
    return df

def save_to_database(conn, df, ts_code):
    """保存数据到数据库"""
    if df is None or len(df) == 0:
        return
        
    # 转换时间格式
    def convert_time(time_str):
        """将baostock的时间字符串转换为PostgreSQL的time格式
        输入格式: '20190102100000000'
        输出格式: '10:00:00'
        """
        return f"{time_str[8:10]}:{time_str[10:12]}:{time_str[12:14]}"
        
    with conn.cursor() as cur:
        # 准备数据
        records = [
            (
                ts_code,
                row['date'],
                convert_time(row['time']),  # 转换时间格式
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume'],
                row['amount'],
                row['adjustflag']
            )
            for _, row in df.iterrows()
        ]
        
        # 使用批量插入，如果记录已存在则更新
        cur.executemany("""
            INSERT INTO a_stock_30m_kline_wfq_baostock 
            (ts_code, trade_date, trade_time, open, high, low, close, volume, amount, adjustflag)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ts_code, trade_date, trade_time) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                adjustflag = EXCLUDED.adjustflag
        """, records)
        
    conn.commit()

def main():
    # 读取配置文件
    config = read_config()
    
    # 登录 baostock
    bs.login()
    
    try:
        # 获取数据库连接
        conn = get_db_connection(config)
        
        # 创建数据表（如果不存在）
        create_kline_table(conn)
        
        # 获取股票代码列表
        stock_codes = get_stock_codes(conn)
        
        # 下载并保存每只股票的数据
        for stock_code in stock_codes:
            print(f"正在处理 {stock_code}")
            
            try:
                df = download_30min_kline(
                    stock_code,
                    start_date='2000-01-01',  # 你可以根据需要修改日期范围
                    end_date  ='2024-12-31',  # 你可以根据需要修改日期范围
                    # end_date=datetime.now().strftime('%Y-%m-%d')
                )
                
                if df is not None:
                    save_to_database(conn, df, stock_code)
                    print(f"成功保存 {stock_code} 的 {len(df)} 条记录")
                else:
                    print(f"{stock_code} 没有可用数据")
                    
            except Exception as e:
                print(f"处理 {stock_code} 时出错: {str(e)}")
                conn.rollback()
                
    finally:
        # 清理资源
        bs.logout()
        conn.close()

if __name__ == "__main__":
    main() 