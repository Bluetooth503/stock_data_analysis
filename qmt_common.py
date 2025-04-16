# -*- coding: utf-8 -*-
import os
import time
from datetime import datetime, timedelta
import schedule
import configparser
import pandas as pd
import numpy as np
import requests
from functools import wraps
from typing import Dict, List, Tuple
from tqdm import tqdm
import zmq
import zlib
from loguru import logger
import sys
import os
import inspect

# ================================= 配置加载 =================================
def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'), encoding='utf-8')
    return config

# ================================= 重试装饰器 =================================
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

# ================================= 通知 =================================
def send_wecom(subject, content, config=None):
    """使用企业微信发送通知"""
    try:
        if config is None:
            # 直接使用load_config获取webhook
            _config = load_config()
            webhook = _config.get('wecom', 'webhook')
        else:
            webhook = config.get_wecom_webhook()

        response = requests.post(webhook, json={
            "msgtype": "markdown",
            "markdown": {"content": f"### {subject}\n{content}"}
        })

        if not response.ok:
            print(f"通知发送失败 HTTP:{response.status_code}")
            return False

        result = response.json()
        if result.get('errcode') != 0:
            print(f"API错误: {result.get('errmsg')}")

        return result.get('errcode') == 0

    except Exception as e:
        print(f"通知异常: {str(e)}")
        return False

# ================================= Heikin-Ashi SuperTrend 计算 =================================
def ha_st_pine(df, length, multiplier):
    # ========== Heikin Ashi计算 ==========
    '''direction=1上涨，-1下跌'''
    df = df.copy()

    # 使用向量化操作计算HA价格
    ha_close = (df['open'] + df['high'] + df['low'] + df['close']) / 4

    # 使用向量化操作计算HA开盘价
    ha_open = pd.Series(index=df.index, dtype=float)
    ha_open.iloc[0] = (df['open'].iloc[0] + df['close'].iloc[0]) / 2

    # 使用向量化操作计算HA高低价
    ha_high = pd.Series(index=df.index, dtype=float)
    ha_low = pd.Series(index=df.index, dtype=float)

    # 使用cumsum和shift进行向量化计算
    for i in range(1, len(df)):
        ha_open.iloc[i] = (ha_open.iloc[i-1] + ha_close.iloc[i-1]) / 2

    # 向量化计算HA高低价
    ha_high = df[['high']].join(pd.DataFrame({
        'ha_open': ha_open,
        'ha_close': ha_close
    })).max(axis=1)

    ha_low = df[['low']].join(pd.DataFrame({
        'ha_open': ha_open,
        'ha_close': ha_close
    })).min(axis=1)

    # ========== ATR计算 ==========
    # 使用向量化操作计算TR
    tr1 = ha_high - ha_low
    tr2 = (ha_high - ha_close.shift(1)).abs()
    tr3 = (ha_low - ha_close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # 使用向量化操作计算RMA
    rma = pd.Series(index=df.index, dtype=float)
    alpha = 1.0 / length

    # 初始化RMA
    rma.iloc[length-1] = tr.iloc[:length].mean()

    # 使用向量化操作计算RMA
    for i in range(length, len(df)):
        rma.iloc[i] = alpha * tr.iloc[i] + (1 - alpha) * rma.iloc[i-1]

    # ========== SuperTrend计算 ==========
    src = (ha_high + ha_low) / 2
    upper_band = pd.Series(index=df.index, dtype=float)
    lower_band = pd.Series(index=df.index, dtype=float)
    super_trend = pd.Series(index=df.index, dtype=float)
    direction = pd.Series(0, index=df.index)

    # 初始化第一个有效值
    start_idx = length - 1
    upper_band.iloc[start_idx] = src.iloc[start_idx] + multiplier * rma.iloc[start_idx]
    lower_band.iloc[start_idx] = src.iloc[start_idx] - multiplier * rma.iloc[start_idx]
    super_trend.iloc[start_idx] = upper_band.iloc[start_idx]
    direction.iloc[start_idx] = 1

    # 使用向量化操作计算SuperTrend
    for i in range(start_idx+1, len(df)):
        current_upper = src.iloc[i] + multiplier * rma.iloc[i]
        current_lower = src.iloc[i] - multiplier * rma.iloc[i]

        lower_band.iloc[i] = current_lower if (current_lower > lower_band.iloc[i-1] or ha_close.iloc[i-1] < lower_band.iloc[i-1]) else lower_band.iloc[i-1]
        upper_band.iloc[i] = current_upper if (current_upper < upper_band.iloc[i-1] or ha_close.iloc[i-1] > upper_band.iloc[i-1]) else upper_band.iloc[i-1]

        if i == start_idx or pd.isna(rma.iloc[i-1]):
            direction.iloc[i] = -1
        elif super_trend.iloc[i-1] == upper_band.iloc[i-1]:
            direction.iloc[i] = 1 if ha_close.iloc[i] > upper_band.iloc[i] else -1
        else:
            direction.iloc[i] = -1 if ha_close.iloc[i] < lower_band.iloc[i] else 1

        super_trend.iloc[i] = lower_band.iloc[i] if direction.iloc[i] == 1 else upper_band.iloc[i]

    # 将计算结果添加到DataFrame中
    df['ha_open'] = ha_open
    df['ha_high'] = ha_high
    df['ha_low'] = ha_low
    df['ha_close'] = ha_close
    df['supertrend'] = super_trend
    df['direction'] = direction
    return df

# ================================= Heikin-Ashi SuperTrend 信号判断 =================================
def check_signal_change(df):
    if len(df) < 2:
        return None
    last_two = df.tail(2)
    if last_two['direction'].iloc[0] == -1 and last_two['direction'].iloc[1] == 1:
        return 'BUY'
    elif last_two['direction'].iloc[0] == 1 and last_two['direction'].iloc[1] == -1:
        return 'SELL'
    return None

# ================================= 生成ZeroMQ密钥对 =================================
def get_zmq_keys(config):
    """获取或生成ZMQ密钥对"""
    keys_dir = config.get('zmq', 'keys_dir')
    os.makedirs(keys_dir, exist_ok=True)

    public_path = os.path.join(keys_dir, 'server.public')
    secret_path = os.path.join(keys_dir, 'server.secret')

    # 如果密钥文件不存在，生成新的密钥对
    if not (os.path.exists(public_path) and os.path.exists(secret_path)):
        public_key, secret_key = zmq.curve_keypair()
        with open(public_path, 'wb') as f:
            f.write(public_key)
        with open(secret_path, 'wb') as f:
            f.write(secret_key)
        print(f"已生成新的ZMQ密钥对: {keys_dir}")
    else:
        with open(public_path, 'rb') as f:
            public_key = f.read()
        with open(secret_path, 'rb') as f:
            secret_key = f.read()

    return public_key, secret_key

# ================================= zlib压缩,解压缩 =================================
def compress_data(data: bytes) -> bytes:
    """使用zlib压缩二进制数据"""
    return zlib.compress(data)

def decompress_data(data: bytes) -> bytes:
    """使用zlib解压缩二进制数据"""
    return zlib.decompress(data)

# ================================= 记录日志 =================================
def setup_logger(prefix=None):
    """设置日志记录器"""
    if prefix is None:
        # 获取调用者的文件名作为前缀
        caller_file = os.path.basename(inspect.stack()[1].filename)
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
        level="DEBUG"  # 改为DEBUG级别以显示更多信息
    )

    # 添加文件输出
    logger.add(
        sink=log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",  # 改为DEBUG级别以记录更多信息
        rotation="1 day",
        retention="7 days",
        enqueue=True      # 启用异步写入，提高性能
    )

    return logger

