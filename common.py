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
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.width', 180)
import numpy as np
import math
from scipy import stats
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False    # 用来正常显示负号
import matplotlib
import configparser
from tqdm import tqdm
from typing import Dict, List, Optional, Callable
import sys
import talib
import pandas_ta as ta
from wxpusher import WxPusher
import requests
from io import StringIO
import csv


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


def upsert_data(df: pd.DataFrame, table_name: str, temp_table: str, insert_sql: str, engine) -> None:
    """
    使用临时表进行批量更新
    Args:
        df: 要导入的数据框
        table_name: 目标表名
        temp_table: 临时表名
        insert_sql: 插入SQL语句
        engine: SQLAlchemy引擎
    """
    with engine.begin() as conn:
        if engine.url.drivername.startswith('postgresql'):
            # 对于PostgreSQL，使用COPY命令进行快速导入
            from io import StringIO
            import csv
            
            logger.info(f'开始导入数据到 {temp_table}')
            # 创建临时表结构
            df.head(0).to_sql(temp_table, conn, if_exists='replace', index=False)
            
            # 将整个数据框直接转换为CSV格式
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False, quoting=csv.QUOTE_MINIMAL)
            output.seek(0)
            
            # 使用COPY命令一次性导入所有数据
            cur = conn.connection.cursor()
            cur.copy_from(output, temp_table, sep='\t', null='')
            
        else:
            # 对于其他数据库，使用分批导入
            chunk_size = 100000  # 每批处理的行数
            total_rows = len(df)
            
            for i in tqdm(range(0, total_rows, chunk_size), desc="导入进度"):
                chunk_df = df.iloc[i:i + chunk_size]
                chunk_df.to_sql(temp_table, conn, if_exists='append' if i > 0 else 'replace', index=False, method='multi')
        
        conn.execute(text(insert_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))


def save_to_database(df: pd.DataFrame, table_name: str, conflict_columns: list, data_type: str = None, engine = None, update_columns: list = None) -> bool:
    """
    将数据保存到PostgreSQL数据库，使用COPY命令进行快速导入
    Args:
        df: 要保存的数据框
        table_name: 目标表名
        conflict_columns: 用于处理冲突的列名列表
        data_type: 数据类型描述（用于日志），默认为None
        engine: SQLAlchemy引擎，如果为None则自动创建
        update_columns: 发生冲突时需要更新的列名列表，默认为None（执行DO NOTHING）
    Returns:
        bool: 是否保存成功
    """
    try:
        # 参数处理
        if data_type is None:
            data_type = table_name
            
        if engine is None:
            config = load_config()
            engine = create_engine(get_pg_connection_string(config))
        
        # 创建带时间戳的临时表名，避免并发冲突
        temp_table = f"temp_{table_name.split('.')[-1]}_{int(time.time())}"
        
        with engine.begin() as conn:
            # 创建临时表结构
            df.head(0).to_sql(temp_table, conn, if_exists='replace', index=False)
            
            # 使用COPY命令快速导入数据
            cur = conn.connection.cursor()
            
            # 将DataFrame转换为CSV格式
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False, quoting=csv.QUOTE_MINIMAL)
            output.seek(0)
            
            # 使用COPY命令一次性导入所有数据
            cur.copy_from(output, temp_table, sep='\t', null='')
            
            # 构建UPSERT操作的SQL语句
            if update_columns:
                # 构建UPDATE SET子句，为特殊字符的列名添加双引号
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
            
            # 清理临时表
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
    """
    格式化时间字符串，将baostock返回的时间格式转换为 HH:MM:00
    输入格式: YYYYMMDDHHMMSSMMM (如: 20250124100000000)
    输出格式: HH:MM:00
    """
    # 提取小时和分钟
    hour = time_str[8:10]
    minute = time_str[10:12]
    return f"{hour}:{minute}:00"

# heikin_ashi函数
def heikin_ashi(df):
    df.ta.ha(append=True)
    ha_ohlc = {"HA_open": "ha_open", "HA_high": "ha_high", "HA_low": "ha_low", "HA_close": "ha_close"}
    df.rename(columns=ha_ohlc, inplace=True)
    return df

def supertrend(df, length, multiplier):
    '''direction=1上涨，-1下跌'''
    supertrend_df = ta.supertrend(df['ha_high'], df['ha_low'], df['ha_close'], length, multiplier)
    df['supertrend'] = supertrend_df[f'SUPERT_{length}_{multiplier}.0']
    df['direction'] = supertrend_df[f'SUPERTd_{length}_{multiplier}.0']
    return df

# 隐藏策略思路
def ha_st(df, length, multiplier):
    try:
        ts_code = df['ts_code'].iloc[0] if 'ts_code' in df.columns else 'Unknown'
        df.ta.ha(append=True)
        ha_ohlc = {"HA_open": "ha_open", "HA_high": "ha_high", "HA_low": "ha_low", "HA_close": "ha_close"}
        df.rename(columns=ha_ohlc, inplace=True)
        supertrend_df = ta.supertrend(df['ha_high'], df['ha_low'], df['ha_close'], length, multiplier)
        
        if supertrend_df is None:
            logger.error(f"股票{ts_code} Supertrend计算失败: length={length}, multiplier={multiplier}")
            return None
            
        df['supertrend'] = supertrend_df[f'SUPERT_{length}_{multiplier}.0']
        df['direction'] = supertrend_df[f'SUPERTd_{length}_{multiplier}.0']
        df = df.round(3)
        return df
    except Exception as e:
        ts_code = df['ts_code'].iloc[0] if 'ts_code' in df.columns else 'Unknown'
        logger.error(f"股票{ts_code} ha_st计算错误: {str(e)}")
        return None

def send_notification(subject, content):
    """发送微信通知"""
    try:
        # 从配置文件获取token和uid
        config = load_config()
        token = config.get('wxpusher', 'token')
        uids = [config.get('wxpusher', 'uid')]
        
        # 发送消息
        WxPusher.send_message(
            content=f"{subject}\n\n{content}",
            uids=uids,
            token=token
        )
        # logger.info(f"微信通知发送成功: {subject}")
    except Exception as e:
        logger.error(f"微信通知发送失败: {str(e)}")


def send_notification_pushplus(subject, content):
    """使用PushPlus发送微信通知"""
    try:
        # 从配置文件获取token
        config = load_config()
        token = config.get('pushplus', 'token')
        url = "http://www.pushplus.plus/send"
        
        # 准备请求数据
        data = {
            "token": token,
            "title": subject,
            "content": content,
            "template": "html"  # 可选：html, json, markdown, cloudMonitor
        }
        
        # 发送请求
        response = requests.post(url, json=data)
        
        # 检查响应
        if response.status_code == 200:
            result = response.json()
            if result.get('code') == 200:
                # logger.info(f"微信通知发送成功: {subject}")
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
    """使用企业微信发送通知
    Args:
        subject: 通知标题
        content: 通知内容
    Returns:
        bool: 是否发送成功
    """
    try:
        # 从配置文件获取webhook地址
        config = load_config()
        webhook = config.get('wecom', 'webhook')
            
        # 准备消息内容
        message = {
            "msgtype": "markdown",
            "markdown": {
                "content": f"### {subject}\n{content}"
            }
        }
        
        # 发送请求
        response = requests.post(webhook, json=message)
        
        # 检查响应
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