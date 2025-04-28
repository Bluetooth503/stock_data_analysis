# -*- coding: utf-8 -*-
from common import *
import baostock as bs
import tushare as ts


# ================================= 读取配置文件 =================================
config = load_config()
engine = create_engine(get_pg_connection_string(config))
token = config.get('tushare', 'token')
pro = ts.pro_api(token)

# ================================= 配置日志 =================================
logger = setup_logger()

# ================================= 定义初始变量 =================================
# 获取所有上市股票列表
data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
stock_list = data[['ts_code']]
end_time   = datetime.today().strftime('%Y%m%d')
period     = '5m'
table_name = 'a_stock_5m_kline_wfq_baostock'

# ================================= 获取数据库中最新的记录时间 =================================
def get_latest_record_time(engine) -> str:
    """获取数据库中最新的记录时间"""
    query = f"""
        SELECT MAX(trade_time) 
        FROM {table_name}
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).scalar()
            return result.strftime('%Y%m%d') if result else '20190101' # 5分钟K线数据从2019年开始
    except:
        return '20190101' # 5分钟K线数据从2019年开始

# ================================= 下载指定股票的5分钟K线数据 =================================
def download_5min_kline(ts_code: str, start_date: str, end_date: str, max_retries: int = 3) -> pd.DataFrame:
    """下载指定股票的5分钟K线数据"""
    # code和日期格式转换
    bs_code = convert_to_baostock_code(ts_code)
    bs_start_date = convert_date_format(start_date)
    bs_end_date = convert_date_format(end_date)
    
    for retry in range(max_retries):
        try:
            rs = bs.query_history_k_data_plus(
                code = bs_code,
                fields = "date,time,code,open,high,low,close,volume,amount,adjustflag",
                start_date = bs_start_date,
                end_date = bs_end_date,
                frequency = "5",
                adjustflag = "3"  # 不复权
            )
            
            if rs.error_code != '0':
                logger.error(f"下载 {ts_code} 数据时出错 (尝试 {retry + 1}/{max_retries}): {rs.error_msg}")
                if retry < max_retries - 1:
                    time.sleep(1)  # 等待1秒后重试
                    continue
                return pd.DataFrame()
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
                
            if not data_list:
                return pd.DataFrame()
                
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 转换股票代码格式和时间格式
            df['code'] = df['code'].apply(convert_to_tushare_code)
            df['time'] = df['time'].apply(format_time)
            df['trade_time'] = pd.to_datetime(df['date'] + ' ' + df['time'], format='%Y-%m-%d %H:%M:%S')
            
            # 重命名列以匹配数据库表结构
            df = df.rename(columns={'code': 'ts_code', 'adjustflag': 'adjust_flag'})
            
            # 选择需要的列并转换数据类型
            result_df = df[['trade_time', 'ts_code', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adjust_flag']].copy()
            
            # 转换数据类型
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
            result_df[numeric_columns] = result_df[numeric_columns].apply(pd.to_numeric)
            result_df['adjust_flag'] = result_df['adjust_flag'].astype(int)
                
            return result_df
            
        except Exception as e:
            logger.error(f"下载 {ts_code} 数据时发生异常 (尝试 {retry + 1}/{max_retries}): {str(e)}")
            if retry < max_retries - 1:
                time.sleep(1)  # 等待1秒后重试
                continue
            return pd.DataFrame()

def get_trade_dates_between(start_date, end_date):
    """获取两个日期之间的所有交易日列表"""
    calendar = pro.trade_cal(start_date=start_date, end_date=end_date)
    trade_dates = calendar[calendar['is_open'] == 1]['cal_date'].sort_values().tolist()
    return trade_dates

def process_date_stocks(date, stocks):
    """处理指定日期的所有股票数据"""
    results = []
    
    # 创建进度条
    for ts_code in tqdm(stocks, desc=f"下载{date}数据"):
        try:
            df = download_5min_kline(ts_code=ts_code, start_date=date, end_date=date)
            if not df.empty:
                results.append(df)
        except Exception as e:
            logger.error(f"处理股票 {ts_code} 时发生错误: {str(e)}")
    
    return results

def save_to_database_batch(df_list, engine):
    """批量保存数据到数据库"""
    if not df_list:
        return
    
    try:
        final_df = pd.concat(df_list, ignore_index=True)
        if save_to_database(
            df=final_df,
            table_name=table_name,
            conflict_columns=['trade_time', 'ts_code'],
            data_type='5分钟K线',
            engine=engine
        ):
            logger.info(f"成功保存 {len(final_df)} 条记录到数据库")
        else:
            logger.error(f"保存 {len(final_df)} 条记录到数据库失败")
    except Exception as e:
        logger.error(f"保存数据到数据库时发生错误: {str(e)}")

def main():
    """主函数"""
    bs.login()
    
    try:
        # 获取最新记录时间
        latest_time = (pd.to_datetime(get_latest_record_time(engine)) + pd.Timedelta(days=1)).strftime('%Y%m%d')

        # 获取股票列表
        stocks = stock_list['ts_code'].tolist()
        
        # 获取交易日列表
        trade_dates = get_trade_dates_between(latest_time, end_time)
        logger.info(f"需要处理的交易日数量: {len(trade_dates)}")
        
        # 按日期循环
        for current_date in trade_dates:
            # 处理当前日期的所有股票
            results = process_date_stocks(current_date, stocks)
            
            # 保存结果到数据库
            if results:
                save_to_database_batch(results, engine)
    
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")

if __name__ == "__main__":
    main()