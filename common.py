# -*- coding: utf-8 -*-
import os
from sqlalchemy import create_engine
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

