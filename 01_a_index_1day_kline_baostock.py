# -*- coding: utf-8 -*-
from common import *
import baostock as bs
import argparse

# ================================= 配置日志 =================================
logger = setup_logger()
config = load_config()

# ================================= 重试配置 =================================
MAX_RETRIES = 12  # 最大重试次数（约6小时）
RETRY_INTERVAL = 1800  # 重试间隔（秒）

# ================================= 定义初始变量 =================================
index_code = '000300.SH'  # 沪深300指数
period     = 'd'
table_name = 'a_index_1day_kline_baostock'

# ================================= 解析命令行参数 =================================
def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='下载指数日K线数据')
    parser.add_argument('--start_date', help='开始日期 (YYYY-MM-DD)', type=str)
    parser.add_argument('--end_date', help='结束日期 (YYYY-MM-DD)', type=str)
    return parser.parse_args()

# ================================= 获取指数日K线数据 =================================
def get_index_data(ts_code, start_date, end_date):
    """从baostock获取指数日K线数据"""
    bs_code = convert_to_baostock_code(ts_code)
    
    # 查询指数日K线数据
    rs = bs.query_history_k_data_plus(bs_code, "date,open,high,low,close,volume,amount",
        start_date = start_date, end_date = end_date, frequency = period, adjustflag="3")
    
    # 转换为DataFrame
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    df = pd.DataFrame(data_list, columns=rs.fields)
    print(df)
        
    # 处理数据
    if not df.empty:
        df['ts_code'] = ts_code
        df.rename(columns={'date': 'trade_date'}, inplace=True)
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    # 确保列的顺序正确
    df = df[['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'volume', 'amount']]
    return df

def check_index_data_ready(check_date):
    """检查指定日期的指数数据是否就绪"""
    try:
        # 直接查询目标指数数据
        rs = bs.query_history_k_data_plus(
            index_code, 
            "date",
            start_date=check_date,
            end_date=check_date,
            frequency=period,
            adjustflag="3"
        )
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())

        if data_list:
            print(data_list)
            logger.info(f"{trade_date}数据已就绪")
            return True

    except Exception as e:
        logger.warning(f"数据检查异常: {str(e)}")
        return False


def wait_for_data(check_date):
    """等待数据生成"""
    logger.info(f"开始等待 {check_date} 数据生成...")
    for attempt in range(1, MAX_RETRIES + 1):
        if check_index_data_ready(check_date):
            logger.info("数据已就绪")
            return True
        
        logger.info(f"第{attempt}次尝试，{RETRY_INTERVAL//60}分钟后重试...")
        time.sleep(RETRY_INTERVAL)
        
    logger.error(f"等待超时，经过{MAX_RETRIES}次尝试仍未获取数据")
    return False


def main(): 
    # 解析命令行参数
    args = parse_arguments()
    bs.login()

    # 设置日期范围
    if args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        start_date = end_date = datetime.today().strftime('%Y-%m-%d')
        # 验证日期是否为交易日
        if not is_trade_date(end_date):
            logger.warning(f"{end_date} 不是交易日，程序退出")
            return

        # 当使用当天日期时触发等待流程
        if not wait_for_data(end_date):
            logger.error("重试耗尽，退出程序")
            return
    
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
            
if __name__ == '__main__':
    main()
