# -*- coding: utf-8 -*-
from common import *
import tushare as ts

# ================================= 读取配置文件 =================================
config = load_config()
token = config.get('tushare', 'token')
pro = ts.pro_api(token)
engine = create_engine(get_pg_connection_string(config))

# ================================= 配置日志 =================================
logger = setup_logger()

try:
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,cnspell,market,list_date,act_name,act_ent_type')
    with engine.begin() as connection:
        connection.execute(text('TRUNCATE TABLE a_stock_basic'))
    data.to_sql('a_stock_basic', engine, if_exists='append', index=False)
    logger.info('成功保存a_stock_basic数据')
except Exception as e:
    logger.error(f'保存a_stock_basic数据: {str(e)}')
    raise
