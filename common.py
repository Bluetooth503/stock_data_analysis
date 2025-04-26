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
import numpy as np
import math
from scipy import stats
import configparser
from tqdm import tqdm
from typing import Dict, List, Optional, Callable
import sys
import pandas_ta as ta
import talib
from wxpusher import WxPusher
import requests
from io import StringIO
import csv
import inspect

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

def get_30m_kline_data(fq_code, ts_code, start_date=None, end_date=None):
    """从PostgreSQL数据库获取股票数据，返回DataFrame"""
    config = load_config()
    engine = create_engine(get_pg_connection_string(config))
    query = f"""
        SELECT trade_time, ts_code, open, high, low, close, volume, amount
        FROM a_stock_30m_kline_{fq_code}_baostock
        WHERE ts_code = '{ts_code}'
    """
    # 添加日期范围条件
    if start_date:
        query += f" AND trade_time >= '{start_date}'"
    if end_date:
        query += f" AND trade_time <= '{end_date}'"
    # 按日期和时间排序
    query += "ORDER BY trade_time"
    df = pd.read_sql(query, engine)
    # 确保数据类型正确
    numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    # 删除任何包含 NaN 的行
    df = df.dropna()
    return df


def upsert_data(df: pd.DataFrame, table_name: str, temp_table: str, insert_sql: str, engine) -> None:
    """使用临时表进行批量更新"""
    with engine.begin() as conn:
        if engine.url.drivername.startswith('postgresql'):
            from io import StringIO
            import csv
            logger.info(f'开始导入数据到 {temp_table}')
            df.head(0).to_sql(temp_table, conn, if_exists='replace', index=False)
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False, quoting=csv.QUOTE_MINIMAL)
            output.seek(0)
            cur = conn.connection.cursor()
            cur.copy_from(output, temp_table, sep='\t', null='')
        else:
            chunk_size = 100000  # 每批处理的行数
            total_rows = len(df)
            for i in tqdm(range(0, total_rows, chunk_size), desc="导入进度"):
                chunk_df = df.iloc[i:i + chunk_size]
                chunk_df.to_sql(temp_table, conn, if_exists='append' if i > 0 else 'replace', index=False, method='multi')
        conn.execute(text(insert_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

def save_to_database(df: pd.DataFrame, table_name: str, conflict_columns: list, data_type: str = None, engine = None, update_columns: list = None) -> bool:
    """使用COPY命令导入数据"""
    try:
        if data_type is None:
            data_type = table_name   
        if engine is None:
            config = load_config()
            engine = create_engine(get_pg_connection_string(config))
        temp_table = f"temp_{table_name.split('.')[-1]}_{int(time.time())}"
        with engine.begin() as conn:
            df.head(0).to_sql(temp_table, conn, if_exists='replace', index=False)
            cur = conn.connection.cursor()
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False, quoting=csv.QUOTE_MINIMAL)
            output.seek(0)
            cur.copy_from(output, temp_table, sep='\t', null='')
            if update_columns:
                set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
                insert_sql = f"""
                    INSERT INTO {table_name}
                    SELECT * FROM {temp_table}
                    ON CONFLICT ({', '.join([f'"{col}"' for col in conflict_columns])})
                    DO UPDATE SET {set_clause}
                """
            else:
                insert_sql = f"""
                    INSERT INTO {table_name}
                    SELECT * FROM {temp_table}
                    ON CONFLICT ({', '.join([f'"{col}"' for col in conflict_columns])}) DO NOTHING
                """
            conn.execute(text(insert_sql))
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

def send_notification(subject, content):
    """发送微信通知"""
    try:
        config = load_config()
        token = config.get('wxpusher', 'token')
        uids = [config.get('wxpusher', 'uid')]
        WxPusher.send_message(
            content=f"{subject}\n\n{content}",
            uids=uids,
            token=token)
    except Exception as e:
        logger.error(f"微信通知发送失败: {str(e)}")

def send_notification_pushplus(subject, content):
    """使用PushPlus发送微信通知"""
    try:
        config = load_config()
        token = config.get('pushplus', 'token')
        url = "http://www.pushplus.plus/send"
        data = {
            "token": token,
            "title": subject,
            "content": content,
            "template": "html"
            }
        response = requests.post(url, json=data)
        if response.status_code == 200:
            result = response.json()
            if result.get('code') == 200:
                return True
            else:
                logger.error(f"微信通知发送失败: {result.get('msg')}")
                return False
        else:
            logger.error(f"微信通知发送失败，HTTP状态码: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"微信通知发送失败: {str(e)}")
        return False

def send_notification_wecom(subject, content):
    """使用企业微信发送通知"""
    try:
        config = load_config()
        webhook = config.get('wecom', 'webhook')
        message = {
            "msgtype": "markdown",
            "markdown": {"content": f"### {subject}\n{content}"}
        }
        response = requests.post(webhook, json=message)
        if response.status_code == 200:
            result = response.json()
            if result.get('errcode') == 0:
                return True
            else:
                logger.error(f"企业微信通知发送失败: {result.get('errmsg')}")
                return False
        else:
            logger.error(f"企业微信通知发送失败，HTTP状态码: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"企业微信通知发送失败: {str(e)}")
        return False

# ================================= 重试装饰器 =================================
def retry_on_failure(max_retries=3, delay=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    if isinstance(result, tuple):
                        seq, success = result
                        if success:
                            return seq, True
                    else:
                        if isinstance(result, list):
                            return result
                        if result > 0:
                            return result, True
                    if attempt < max_retries - 1:
                        logger.warning(f"尝试执行{func.__name__}失败，{attempt + 1}/{max_retries}次，等待{delay}秒后重试...")
                        time.sleep(delay)
                    else:
                        logger.error(f"尝试执行{func.__name__}失败，已达到最大重试次数{max_retries}次")
                        return -1, False
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"执行{func.__name__}出错: {str(e)}，{attempt + 1}/{max_retries}次，等待{delay}秒后重试...")
                        time.sleep(delay)
                    else:
                        logger.error(f"执行{func.__name__}出错: {str(e)}，已达到最大重试次数{max_retries}次")
                        return -1, False
            return -1, False
        return wrapper
    return decorator
