# -*- coding: utf-8 -*-
from common import *
import baostock as bs
import argparse

# ================================= 读取配置文件 =================================
config = load_config()
engine = create_engine(get_pg_connection_string(config))

# ================================= 配置日志 =================================
logger = setup_logger()

# ================================= 获取股票列表 =================================
def get_stock_list(engine):
    """从数据库获取股票代码列表"""
    query = "SELECT DISTINCT ts_code FROM a_stock_name"
    return pd.read_sql(query, engine)

# ================================= 参数解析 =================================
def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='下载A股5分钟K线数据')
    parser.add_argument('--start_date', help='开始日期 (YYYY-MM-DD)', type=str)
    parser.add_argument('--end_date', help='结束日期 (YYYY-MM-DD)', type=str)
    return parser.parse_args()

# ================================= 下载5分钟K线数据 =================================
def download_5min_kline(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """下载指定股票的5分钟K线数据"""
    try:
        # 查询数据
        rs = bs.query_history_k_data_plus(
            code=convert_to_baostock_code(ts_code),
            fields="date,time,code,open,high,low,close,volume,amount,adjustflag",
            start_date=start_date,
            end_date=end_date,
            frequency="5",
            adjustflag="3"  # 不复权
        )
        
        if rs.error_code != '0':
            logger.error(f"下载 {ts_code} 数据时出错: {rs.error_msg}")
            return pd.DataFrame()
            
        # 将结果转换为DataFrame
        data_list = []
        while (rs.error_code == '0') & rs.next():
            row = rs.get_row_data()
            data_list.append(row)
            
        if not data_list:
            return pd.DataFrame()
            
        # 创建DataFrame并进行数据处理
        df = pd.DataFrame(data_list, columns=rs.fields)
        df['time'] = df['time'].apply(format_time)
        df['trade_time'] = pd.to_datetime(df['date'] + ' ' + df['time'])
        df['code'] = df['code'].apply(convert_to_tushare_code)
        
        # 重命名并选择列
        result_df = df.rename(columns={'code': 'ts_code', 'adjustflag': 'adjust_flag'})
        result_df = result_df[['trade_time', 'ts_code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjust_flag']].copy()
        
        # 过滤零成交量数据
        result_df = result_df[pd.to_numeric(result_df['volume']) > 0]
        
        # 过滤异常数据
        result_df = result_df[
            (pd.to_numeric(result_df['open'])  > 0) &
            (pd.to_numeric(result_df['high'])  > 0) &
            (pd.to_numeric(result_df['low'])   > 0) &
            (pd.to_numeric(result_df['close']) > 0) &
            (pd.to_numeric(result_df['high'])  >= pd.to_numeric(result_df['low'])) &
            (pd.to_numeric(result_df['open'])  <= pd.to_numeric(result_df['high'])) &
            (pd.to_numeric(result_df['open'])  >= pd.to_numeric(result_df['low'])) &
            (pd.to_numeric(result_df['close']) <= pd.to_numeric(result_df['high'])) &
            (pd.to_numeric(result_df['close']) >= pd.to_numeric(result_df['low']))
        ]
        
        return result_df
        
    except Exception as e:
        logger.error(f"处理 {ts_code} 数据时发生错误: {str(e)}")
        return pd.DataFrame()

def download_and_store_data(engine, stocks, start_date, end_date, batch_size=10):
    """分批下载并存储数据"""
    logger.info(f"开始下载数据，时间范围: {start_date} 至 {end_date}")
    
    # 分批处理股票
    for i in range(0, len(stocks), batch_size):
        batch_stocks = stocks[i:i + batch_size]
        all_data = []  # 用于存储当前批次下载的数据
        
        for ts_code in tqdm(batch_stocks, desc=f'下载数据 (批次 {i // batch_size + 1})'):
            df = download_5min_kline(ts_code=ts_code, start_date=start_date, end_date=end_date)
            
            if not df.empty:
                all_data.append(df)
            else:
                logger.warning(f"{ts_code} 在 {start_date} 至 {end_date} 期间没有数据")
        
        # 合并当前批次数据并保存到数据库
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"批次 {i // batch_size + 1} 共下载 {len(final_df)} 条记录")
            # 使用 save_to_database 保存数据
            if save_to_database(
                df=final_df,
                table_name='a_stock_5m_kline_wfq_baostock',
                conflict_columns=['trade_time', 'ts_code'],
                data_type='5分钟K线',
                engine=engine,
                update_columns=['open', 'high', 'low', 'close', 'volume', 'amount', 'adjust_flag']
            ):
                logger.info(f"批次 {i // batch_size + 1} 数据保存完成")
            else:
                logger.error(f"批次 {i // batch_size + 1} 数据保存失败")

def wait_for_data_ready(trade_date):
    """等待当日数据就绪"""
    max_retries = 12  # 最大重试次数
    retry_interval = 1800  # 重试间隔（秒）
    retries = 0
    
    while retries < max_retries:
        try:
            # 尝试获取当日任意股票的数据
            stock_list = get_stock_list(engine)
            test_stock = stock_list['ts_code'].iloc[0]
            bs_code = convert_to_baostock_code(test_stock)
            rs = bs.query_history_k_data_plus(
                code=bs_code,
                fields="date,time",
                start_date=trade_date,
                end_date=trade_date,
                frequency="30",
                adjustflag="3"
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
                
            if data_list:
                logger.info(f"{trade_date}数据已就绪")
                return True
                
        except Exception as e:
            logger.warning(f"检查数据就绪状态时出错: {str(e)}")
        
        retries += 1
        logger.info(f"数据未就绪，{retry_interval}秒后重试 ({retries}/{max_retries})")
        time.sleep(retry_interval)
    
    logger.error(f"{trade_date}数据等待超时")
    return False

def main():
    """主函数"""
    args = parse_arguments()
    
    # 设置日期范围
    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    else:
        start_date = end_date = datetime.today().strftime('%Y-%m-%d')
        # 验证日期是否为交易日
        if not is_trade_date(end_date):
            logger.warning(f"{end_date} 不是交易日，程序退出")
            return

        # 当使用当天日期时触发等待流程
        bs.login()
        if not wait_for_data_ready(end_date):
            logger.error("重试耗尽，退出程序")
            return
    
    # 获取股票列表
    stock_list = get_stock_list(engine)
    stocks = stock_list['ts_code'].tolist()
    
    # 下载并存储数据
    download_and_store_data(engine, stocks, start_date, end_date)

if __name__ == "__main__":
    main()