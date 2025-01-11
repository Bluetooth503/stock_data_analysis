import baostock as bs
import pandas as pd
import psycopg2
import configparser
from datetime import datetime
import os
import logging
from typing import Optional, List
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./stock_data.log'),
        logging.StreamHandler()
    ]
)


def convert_to_baostock_code(ts_code: str) -> str:
    """将 tushare 格式的代码转换为 baostock 格式"""
    code, market = ts_code.split('.')
    if market == 'SZ':
        return f"sz.{code}"
    return f"sh.{code}"

def convert_to_tushare_code(baostock_code: str) -> str:
    """将 baostock 格式的代码转换为 tushare 格式"""
    market, code = baostock_code.split('.')
    market = market.upper()
    return f"{code}.{market}"

def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    if not os.path.exists(config_path):
        raise FileNotFoundError("配置文件 'config.ini' 不存在！")
    
    config.read(config_path, encoding='utf-8')
    return config


def get_db_connection(config: configparser.ConfigParser) -> psycopg2.extensions.connection:
    """创建数据库连接"""
    try:
        conn = psycopg2.connect(
            host=config['postgresql']['host'],
            port=config['postgresql']['port'],
            database=config['postgresql']['database'],
            user=config['postgresql']['user'],
            password=config['postgresql']['password']
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"数据库连接失败: {str(e)}")
        raise

def create_kline_table(conn: psycopg2.extensions.connection):
    """如果表不存在，创建30分钟K线数据表"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS a_stock_30m_kline_qfq_baostock (
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
                );
                
                -- 创建索引以提高查询性能
                CREATE INDEX IF NOT EXISTS idx_stock_30m_date 
                ON a_stock_30m_kline_qfq_baostock(trade_date);
                
                CREATE INDEX IF NOT EXISTS idx_stock_30m_code_date 
                ON a_stock_30m_kline_qfq_baostock(ts_code, trade_date);
            """)
        conn.commit()
        logging.info("成功创建数据表和索引")
    except psycopg2.Error as e:
        logging.error(f"创建表失败: {str(e)}")
        conn.rollback()
        raise

def get_stock_codes(conn: psycopg2.extensions.connection) -> List[str]:
    """从数据库获取股票代码列表"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT ts_code FROM a_stock_name")
            return [row[0] for row in cursor.fetchall()]
    except psycopg2.Error as e:
        logging.error(f"获取股票代码列表失败: {str(e)}")
        raise

def convert_time(time_str: str) -> str:
    """将baostock的时间字符串转换为PostgreSQL的time格式"""
    try:
        return f"{time_str[8:10]}:{time_str[10:12]}:{time_str[12:14]}"
    except IndexError as e:
        logging.error(f"时间格式转换失败，输入: {time_str}, 错误: {str(e)}")
        raise

def download_30min_kline(stock_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """下载指定股票的30分钟K线数据"""
    bs_code = convert_to_baostock_code(stock_code)
    
    rs = bs.query_history_k_data_plus(
        code=bs_code,
        fields="date,time,code,open,high,low,close,volume,amount,adjustflag",
        start_date=start_date,
        end_date=end_date,
        frequency="30",
        adjustflag="2"  # 前复权
    )
    
    if rs.error_code != '0':
        logging.error(f"下载 {stock_code} 数据时出错: {rs.error_msg}")
        return None
    
    data_list = []
    while (rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        row[2] = convert_to_tushare_code(row[2])
        data_list.append(row)
    
    if not data_list:
        logging.warning(f"{stock_code} 在指定时间段内没有数据")
        return None
        
    df = pd.DataFrame(data_list, columns=rs.fields)
    return df

def save_to_database(conn: psycopg2.extensions.connection, df: pd.DataFrame, ts_code: str):
    """保存数据到数据库"""
    if df is None or len(df) == 0:
        return
        
    try:
        with conn.cursor() as cur:
            # 准备数据
            records = [
                (
                    ts_code,
                    row['date'],
                    convert_time(row['time']),
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
            
            # 批量插入或更新
            cur.executemany("""
                INSERT INTO a_stock_30m_kline_qfq_baostock 
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
        logging.info(f"成功保存 {ts_code} 的 {len(df)} 条记录")
    except (psycopg2.Error, Exception) as e:
        logging.error(f"保存 {ts_code} 数据时出错: {str(e)}")
        conn.rollback()
        raise

def process_stock(conn: psycopg2.extensions.connection, stock_code: str, start_date: str, end_date: str):
    """处理单个股票的数据下载和保存"""
    try:
        df = download_30min_kline(stock_code, start_date, end_date)
        if df is not None:
            save_to_database(conn, df, stock_code)
        else:
            logging.warning(f"{stock_code} 没有可用数据")
    except Exception as e:
        logging.error(f"处理 {stock_code} 时出错: {str(e)}")
        raise

def main():
    conn = None
    try:
        # 加载配置
        config = load_config()
        
        # 登录 baostock
        bs.login()
        logging.info("成功登录 baostock")
        
        # 获取数据库连接
        conn = get_db_connection(config)
        logging.info("成功连接数据库")
        
        # 创建数据表
        create_kline_table(conn)
        
        # 获取股票代码列表
        stock_codes = get_stock_codes(conn)
        logging.info(f"共获取到 {len(stock_codes)} 只股票")
        
        # 设置时间范围
        start_date = '2000-01-01'
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        # 下载并保存每只股票的数据
        for i, stock_code in enumerate(stock_codes, 1):
            logging.info(f"正在处理第 {i}/{len(stock_codes)} 只股票: {stock_code}")
            
            try:
                process_stock(conn, stock_code, start_date, end_date)
            except Exception as e:
                logging.error(f"处理 {stock_code} 时出错: {str(e)}")
                continue
            
            # 添加延时，避免请求过于频繁
            time.sleep(0.1)
            
    except FileNotFoundError as e:
        logging.error(str(e))
    except KeyError as e:
        logging.error(f"配置文件错误: 缺少配置项 {str(e)}")
    except Exception as e:
        logging.error(f"程序执行出错: {str(e)}")
    finally:
        # 清理资源
        # bs.logout()
        logging.info("已登出 baostock")
        
        if conn:
            conn.close()
            logging.info("已关闭数据库连接")

if __name__ == "__main__":
    main()