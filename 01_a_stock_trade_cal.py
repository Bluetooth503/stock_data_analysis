# -*- coding: utf-8 -*-
from common import *
import tushare as ts

# ================================= 读取配置文件 =================================
config = load_config()
engine = create_engine(get_pg_connection_string(config))
logger = setup_logger()

# ================================= tushare配置 =================================
token = config.get('tushare', 'token')
pro = ts.pro_api(token)

# 获取当前年份的开始和结束日期
def get_current_year_trade_dates():
    current_year = datetime.now().year
    start_date = f'{current_year}0101'
    end_date = f'{current_year}1231'

    # 调用tushare的trade_cal接口获取交易日历
    df = pro.trade_cal(exchange='', start_date=start_date, end_date=end_date)
    df = df[['cal_date', 'is_open']]
    df['trade_date'] = pd.to_datetime(df['cal_date']).dt.strftime('%Y-%m-%d')
    df.drop(columns=['cal_date'], inplace=True)
    df['is_open'] = df['is_open'].astype(str)
    save_to_database(df, 'a_stock_trade_cal', conflict_columns=['trade_date'], data_type='交易日数据', engine=engine, update_columns=['is_open'])


get_current_year_trade_dates()