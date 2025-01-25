# -*- coding: utf-8 -*-
import os
from sqlalchemy import create_engine, text
import psycopg2
from datetime import datetime, date, timedelta
import time
from loguru import logger
import warnings
warnings.filterwarnings("ignore") # 屏蔽jupyter的告警显示
import pandas as pd
pd.set_option('display.float_format',lambda x:'%.4f' % x)
import numpy as np
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False    # 用来正常显示负号
import configparser
from tqdm import tqdm
from typing import List, Dict
import sys




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

def get_pg_connection_string(config):
    """获取PostgreSQL连接字符串"""
    pg_config = config['postgresql']
    return f"postgresql://{pg_config['user']}:{pg_config['password']}@{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"

def get_mysql_connection_string(config):
    """获取MySQL连接字符串"""
    mysql_config = config['mysql']
    return f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"

def get_stock_data(fq_code, ts_code, start_date=None, end_date=None):
    """从PostgreSQL数据库获取股票数据，返回DataFrame"""
    config = load_config()
    engine = create_engine(get_pg_connection_string(config))
    query = f"""
        SELECT ts_code, trade_date, trade_time, open, high, low, close, volume, amount
        FROM a_stock_30m_kline_{fq_code}_baostock
        WHERE ts_code = '{ts_code}'
    """
    # 添加日期范围条件
    if start_date:
        query += f" AND trade_date >= '{start_date}'"
    if end_date:
        query += f" AND trade_date <= '{end_date}'"
    # 按日期和时间排序
    query += "ORDER BY trade_date, trade_time"
    df = pd.read_sql(query, engine)
    # 合并日期和时间
    df['datetime'] = pd.to_datetime(df['trade_date'].astype(str) + ' ' + df['trade_time'].astype(str))
    df.set_index('datetime', inplace=True)
    # 确保数据类型正确
    numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    # 删除任何包含 NaN 的行
    df = df.dropna()
    return df

def setup_logger(prefix: str = None) -> logger:
    """
    配置loguru日志处理
    Args:
        prefix: 日志文件前缀，默认使用调用者的文件名
    Returns:
        logger: 配置好的loguru日志记录器
    """
    if prefix is None:
        # 获取调用者的文件名（不含扩展名）作为前缀
        import inspect
        caller_frame = inspect.stack()[1]
        caller_file = os.path.basename(caller_frame.filename)
        prefix = os.path.splitext(caller_file)[0]
    
    # 获取调用者脚本所在目录
    caller_dir = os.path.dirname(os.path.abspath(inspect.stack()[1].filename))
    log_file = os.path.join(caller_dir, f'{prefix}.log')
    
    # 移除默认的sink
    logger.remove()
    
    # 添加控制台输出
    logger.add(
        sink=sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    
    # 添加文件输出
    logger.add(
        sink=log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="INFO",
        rotation="10 MB",  # 当文件达到10MB时轮转
        retention="1 week",  # 保留1周的日志
        encoding="utf-8",
        enqueue=True  # 线程安全
    )
    
    return logger


def upsert_data(df: pd.DataFrame, table_name: str, insert_sql: str, engine) -> None:
    """使用临时表进行批量更新"""
    temp_table = f"temp_{table_name}_{int(time.time())}"
    
    with engine.begin() as conn:
        print('开始导入数据到临时表')
        df.to_sql(temp_table, conn, if_exists='replace', index=False, method='multi', chunksize=10000)
        print('从临时表插入数据到目标表')
        conn.execute(text(insert_sql))
        print('drop临时表')
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))