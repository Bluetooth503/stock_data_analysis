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
from typing import Dict, List, Optional, Callable
from sqlalchemy import create_engine, text, DateTime
from loguru import logger
import warnings
warnings.filterwarnings("ignore") # 屏蔽jupyter的告警显示
import pandas as pd
import numpy as np
import math
from scipy import stats
from tqdm import tqdm
import pandas_ta as ta
import talib
from wxpusher import WxPusher
import requests
from io import StringIO
import csv
from dataclasses import dataclass
import queue
import signal
import threading
import tushare as ts

# ================================= 通用函数 =================================
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

# ================================= 读取配置文件 =================================
config = load_config()
token = config.get('tushare', 'token')
pro = ts.pro_api(token)

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
            # 构建SQL语句            
            insert_columns = [f'"{col}"' for col in df.columns]
            select_columns = [f'TO_TIMESTAMP("{col}"::BIGINT)' if col == 'trade_time' else f'"{col}"' for col in df.columns]
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
    """将 YYYYMMDD 格式转换为 YYYY-MM-DD 格式"""
    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

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

# ================================= 重试装饰器 =================================
def retry_on_failure(max_retries=3, delay=1):
    """重试装饰器 """
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    # 处理成功情况：元组(带success标志)、列表、正数
                    if (isinstance(result, tuple) and len(result) > 1 and result[1]) or \
                       isinstance(result, list) or \
                       (isinstance(result, (int, float)) and result > 0):
                        return result
                    
                    # 处理失败情况
                    if attempt < max_retries - 1:
                        logger.warning(f"执行{func.__name__}失败，重试 {attempt + 1}/{max_retries}")
                        time.sleep(delay)
                        continue
                    logger.error(f"执行{func.__name__}失败，达到最大重试次数")
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"执行{func.__name__}异常: {str(e)}，重试 {attempt + 1}/{max_retries}")
                        time.sleep(delay)
                        continue
                    logger.error(f"执行{func.__name__}异常: {str(e)}，达到最大重试次数")
            return -1, False
        return wrapper
    return decorator

# ================================= 获取最近n个交易日 =================================
def get_trade_dates(n=14, start_date=None, end_date=None):
    """获取交易日列表"""
    if not start_date or not end_date:
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=60)).strftime('%Y%m%d')
    
    calendar = pro.trade_cal(start_date=start_date, end_date=end_date)
    trade_dates = calendar[calendar['is_open'] == 1]['cal_date'].sort_values(ascending=False)
    return trade_dates[:n].tolist() if n else trade_dates.tolist()
