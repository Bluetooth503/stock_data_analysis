# -*- coding: utf-8 -*-
from common import *
import tushare as ts

# ================================= 读取配置文件 =================================
config = load_config()
token = config.get('tushare', 'token')
pro = ts.pro_api(token)

df = pro.daily_basic(ts_code='', trade_date='20250430')
print(df)
print(df.shape)