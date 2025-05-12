# -*- coding: utf-8 -*-
import os
import sys
import time
import inspect
import schedule
import subprocess
import configparser
import argparse
from datetime import datetime, date, timedelta, timezone
import psycopg2
from psycopg2 import pool
from typing import Dict, List, Optional, Callable, Set, Union, Tuple
from functools import wraps
from sqlalchemy import create_engine, text, DateTime
from loguru import logger
import warnings
warnings.filterwarnings("ignore") # 屏蔽jupyter的告警显示
import pandas as pd
import numpy as np
import math
from scipy import stats
from tqdm import tqdm
import talib
from wxpusher import WxPusher
import requests
from io import StringIO
import csv
from dataclasses import dataclass
import queue
import signal
import threading

def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    if not os.path.exists(config_path):
        raise FileNotFoundError("配置文件 'config.ini' 不存在！")
    config.read(config_path, encoding='utf-8')
    return config

def setup_logger(prefix=None):
    """设置日志记录器"""
    if prefix is None:
        caller_file = os.path.basename(inspect.stack()[1].filename)
        prefix = os.path.splitext(caller_file)[0]
    caller_dir = os.path.dirname(os.path.abspath(inspect.stack()[1].filename))
    logs_dir = os.path.join(caller_dir, 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, f'{prefix}.log')
    logger.remove()
    logger.add(
        sink=sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO")
    logger.add(
        sink=log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="INFO",
        rotation="10 MB",
        retention="30 days",
        encoding="utf-8",
        enqueue=True)
    return logger

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

def get_pg_connection_string(config):
    """获取PostgreSQL连接字符串"""
    pg_config = config['postgresql']
    return f"postgresql://{pg_config['user']}:{pg_config['password']}@{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
engine = create_engine(get_pg_connection_string(load_config()))

def save_to_database(df: pd.DataFrame, table_name: str, conflict_columns: list, data_type: str = None, engine = None, update_columns: list = None) -> bool:
    """使用COPY命令高效导入数据到PostgreSQL数据库
    Args:
        df: 要保存的数据框
        table_name: 目标表名
        conflict_columns: 用于处理冲突的列名列表
        data_type: 数据类型描述，用于日志记录
        engine: SQLAlchemy引擎实例
        dtype: 指定临时表字段类型的字典
        update_columns: 发生冲突时需要更新的列名列表
    """    
    def transform_column(col, df):
        if col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn', 'pre_close', 'pct_chg', 'pe_ttm', 'pb_mrq', 'ps_ttm', 'pcf_ncf_ttm']:
            return f'"{col}"::NUMERIC'
        elif col == 'trade_time':
            if df[col].dtype.kind in 'iu':  # i表示整数，u表示无符号整数
                return f'TO_TIMESTAMP("{col}"::BIGINT)'
            return f'"{col}"'
        elif col == 'trade_date':
            return f'"{col}"::DATE'
        else:
            return f'"{col}"'

    try:
        data_type = data_type or table_name
        engine = engine or create_engine(get_pg_connection_string(load_config()))
        with engine.begin() as conn:
            # 创建临时表并使用COPY命令导入数据
            temp_table = f"temp_{table_name.split('.')[-1]}_{int(time.time())}"
            df.head(0).to_sql(temp_table, conn, if_exists='replace', index=False)
            # 准备CSV数据并执行COPY
            with StringIO() as output:
                df.to_csv(output, sep='\t', header=False, index=False, quoting=csv.QUOTE_MINIMAL)
                output.seek(0)
                conn.connection.cursor().copy_from(output, temp_table, sep='\t', null='')
            # 构建并执行UPSERT语句
            conflict_cols = ', '.join(f'"{col}"' for col in conflict_columns)
            # 使用辅助函数处理列的转换
            select_columns = [transform_column(col, df) for col in df.columns]
            insert_columns = [f'"{col}"' for col in df.columns]
            insert_clause = ', '.join(insert_columns)
            select_clause = ', '.join(select_columns)
            if update_columns:
                set_clause = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in update_columns)
                sql = f"INSERT INTO {table_name} ({insert_clause}) SELECT {select_clause} FROM {temp_table} ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}"
            else:
                sql = f"INSERT INTO {table_name} ({insert_clause}) SELECT {select_clause} FROM {temp_table} ON CONFLICT ({conflict_cols}) DO NOTHING"
            conn.execute(text(sql))
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        logger.info(f"成功保存 {len(df)}条 {data_type} 数据到 {table_name} 表")
        return True
    except Exception as e:
        logger.error(f"保存{data_type}数据时出错: {str(e)}")
        return False

def convert_date_format(date_str: str) -> str:
    """将 YYYY-MM-DD 格式转换为 YYYYMMDD 格式"""
    return date_str.replace('-', '')

def format_time(time_str):
    """格式化时间字符串"""
    hour = time_str[8:10]
    minute = time_str[10:12]
    return f"{hour}:{minute}:00"

def send_notification_wecom(subject, content):
    """使用企业微信发送通知"""
    try:
        response = requests.post(
            load_config().get('wecom', 'webhook'),
            json={"msgtype": "markdown", "markdown": {"content": f"### {subject}\n{content}"}}
        )
        if response.status_code == 200 and response.json().get('errcode') == 0:
            return True
        logger.error(f"企业微信通知发送失败: {response.json().get('errmsg') if response.status_code == 200 else f'HTTP状态码 {response.status_code}'}")
    except Exception as e:
        logger.error(f"企业微信通知发送失败: {str(e)}")
    return False

def retry_on_failure(max_retries=3, delay=1):
    """重试装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    # 处理tick数据返回结果
                    if isinstance(result, dict):
                        return result
                    # 处理订单结果
                    if isinstance(result, tuple):
                        seq, success = result
                        if success:
                            return seq, True
                    # 处理列表结果
                    if isinstance(result, list):
                        return result
                    # 处理数值结果
                    if result is not None and result > 0:
                        return result
                    # 如果结果为空或无效，进行重试
                    if attempt < max_retries - 1:
                        print(f"执行{func.__name__}返回无效结果，{attempt + 1}/{max_retries}次，等待{delay}秒后重试...")
                        time.sleep(delay)
                    else:
                        print(f"执行{func.__name__}返回无效结果，已达到最大重试次数{max_retries}次")
                        return None
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        print(f"执行{func.__name__}出错: {str(e)}，{attempt + 1}/{max_retries}次，等待{delay}秒后重试...")
                        time.sleep(delay)
                    else:
                        print(f"执行{func.__name__}出错: {str(e)}，已达到最大重试次数{max_retries}次")
                        return None
            # 如果所有重试都失败，返回None
            return None
        return wrapper
    return decorator

# ================================= 获取最近n个交易日 =================================
def get_trade_dates(start_date, end_date):
    """获取交易日列表"""
    # 如果是YYYYMMDD格式，转换为YYYY-MM-DD格式
    if len(start_date) == 8 and start_date.isdigit():
        start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"

    # 构建SQL查询
    sql = text("""
        SELECT trade_date::text
        FROM a_stock_trade_cal
        WHERE trade_date BETWEEN :start_date AND :end_date
        AND is_open = '1'
        ORDER BY trade_date DESC
    """)
    # 执行查询
    with engine.connect() as conn:
        result = conn.execute(sql, {"start_date": start_date, "end_date": end_date})
        trade_dates = [row[0] for row in result]
    return trade_dates

# ================================= 判断是否为交易日 =================================
def is_trade_date(date_str):
    """判断是否为交易日"""
    # 如果是YYYYMMDD格式，转换为YYYY-MM-DD格式
    if len(date_str) == 8 and date_str.isdigit():
        date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    
    query = f"""
    SELECT COUNT(1) FROM a_stock_trade_cal 
    WHERE trade_date = '{date_str}' AND is_open = '1'
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).scalar()
    return result > 0
