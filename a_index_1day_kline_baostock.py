# -*- coding: utf-8 -*-
import baostock as bs
import pandas as pd
from common import load_config, get_pg_connection_string, convert_to_tushare_code
from sqlalchemy import create_engine, text
from loguru import logger
import datetime

def create_table(engine):
    """创建指数日K线表"""
    sql = """
    CREATE TABLE IF NOT EXISTS a_index_1day_kline_baostock (
        ts_code VARCHAR(20) NOT NULL,
        trade_date DATE NOT NULL,
        open NUMERIC(12,4),
        high NUMERIC(12,4),
        low NUMERIC(12,4),
        close NUMERIC(12,4),
        volume NUMERIC(20,4),
        amount NUMERIC(20,4),
        PRIMARY KEY (ts_code, trade_date)
    );
    """
    with engine.connect() as conn:
        conn.execute(text(sql))

def get_index_data(code, start_date, end_date):
    """从baostock获取指数日K线数据"""
    # 登录baostock
    bs.login()
    
    # 查询指数日K线数据
    rs = bs.query_history_k_data_plus(code,
        "date,open,high,low,close,volume,amount",
        start_date=start_date, end_date=end_date,
        frequency="d", adjustflag="3")
    
    # 转换为DataFrame
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    df = pd.DataFrame(data_list, columns=rs.fields)
    
    # 登出baostock
    bs.logout()
    
    # 处理数据
    if not df.empty:
        df['ts_code'] = convert_to_tushare_code(code)
        df.rename(columns={'date': 'trade_date'}, inplace=True)
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        df['trade_date'] = pd.to_datetime(df['trade_date'])
    
    return df

def save_to_db(df, engine):
    """保存数据到数据库"""
    if not df.empty:
        df.to_sql('a_index_1day_kline_baostock', engine, 
                 if_exists='append', index=False)
        logger.info(f"成功插入{len(df)}条数据")
    else:
        logger.warning("没有获取到数据")

def main():
    # 加载配置
    config = load_config()
    engine = create_engine(get_pg_connection_string(config))
    
    # 创建表
    create_table(engine)
    
    # 获取数据
    index_code = 'sh.000300'  # 沪深300指数
    end_date = '2024-12-31'
    start_date = '2000-01-01'
    
    df = get_index_data(index_code, start_date, end_date)
    
    # 保存数据
    save_to_db(df, engine)

if __name__ == '__main__':
    main()
