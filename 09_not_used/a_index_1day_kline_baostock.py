# -*- coding: utf-8 -*-
from common import *
import baostock as bs

# ================================= 读取配置文件 =================================
config = load_config()
engine = create_engine(get_pg_connection_string(config))

# ================================= 配置日志 =================================
logger = setup_logger()

# ================================= 定义初始变量 =================================
index_code = '000300.SH'  # 沪深300指数
period     = 'd'
start_date = '20000101'
end_date   = datetime.today().strftime('%Y%m%d')
table_name = 'a_index_1day_kline_baostock'

# ================================= 获取指数日K线数据 =================================
def get_index_data(ts_code, start_date, end_date):
    """从baostock获取指数日K线数据"""
    bs_code = convert_to_baostock_code(ts_code)
    bs_start_date = convert_date_format(start_date)
    bs_end_date = convert_date_format(end_date)
    
    # 查询指数日K线数据
    rs = bs.query_history_k_data_plus(bs_code, "date,open,high,low,close,volume,amount",
        start_date = bs_start_date, end_date = bs_end_date, frequency = period, adjustflag="3")
    
    # 转换为DataFrame
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    df = pd.DataFrame(data_list, columns=rs.fields)
        
    # 处理数据
    if not df.empty:
        df['ts_code'] = convert_to_tushare_code(bs_code)  # 转换回标准格式
        df.rename(columns={'date': 'trade_date'}, inplace=True)
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        # 将日期字符串转换为yyyMMdd格式
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
        # 确保数据类型正确
        df = df.astype({
            'trade_date': 'str',
            'ts_code': 'str',
            'open': 'float64',
            'high': 'float64',
            'low': 'float64',
            'close': 'float64',
            'volume': 'float64',
            'amount': 'float64'
        })
    # 确保列的顺序正确
    df = df[['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'volume', 'amount']]
    return df

def main(): 
    try:
        # 登录并获取数据
        bs.login()
        df = get_index_data(index_code, start_date, end_date)
        
        if not df.empty:
            # 使用 save_to_database 保存到数据库
            save_to_database(
                df=df,
                table_name=table_name,
                conflict_columns=['ts_code', 'trade_date'],
                data_type='指数日K线',
                engine=engine
            )
        else:
            logger.warning("没有获取到数据")
            
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        raise
    finally:
        # 确保无论如何都会登出
        bs.logout()

if __name__ == '__main__':
    main()
