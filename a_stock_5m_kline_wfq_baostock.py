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
stock_list = pd.read_csv('沪深A股_stock_list.csv', header=None, names=['ts_code'])
end_time   = datetime.today().strftime('%Y%m%d')
period     = '5m'
table_name = 'a_stock_5m_kline_wfq_baostock'


def get_latest_record_time(engine) -> str:
    """获取数据库中最新的记录时间，如果表不存在或为空则返回19900101"""
    query = f"""
        SELECT MAX(trade_time) 
        FROM {table_name}
        WHERE trade_time IS NOT NULL
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).scalar()
            return result.strftime('%Y%m%d') if result else '19900101'
    except:
        return '19900101'

def download_5min_kline(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """下载指定股票的5分钟K线数据"""
    # code和日期格式转换
    bs_code = convert_to_baostock_code(ts_code)
    bs_start_date = convert_date_format(start_date)
    bs_end_date = convert_date_format(end_date)
    
    rs = bs.query_history_k_data_plus(
        code = bs_code,
        fields = "date,time,code,open,high,low,close,volume,amount,adjustflag",
        start_date = bs_start_date,
        end_date = bs_end_date,
        frequency = "5",
        adjustflag = "3"  # 不复权
    )
    
    if rs.error_code != '0':
        logger.error(f"下载 {ts_code} 数据时出错: {rs.error_msg}")
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

# ================================= 交易日相关 =================================
def get_trade_dates_between(start_date, end_date):
    """获取两个日期之间的所有交易日列表，按降序排列（从新到旧）"""
    calendar = pro.trade_cal(start_date=start_date, end_date=end_date)
    trade_dates = calendar[calendar['is_open'] == 1]['cal_date'].sort_values(ascending=False).tolist()
    return trade_dates

def download_and_store_data(engine, stocks, start_date, end_date, batch_size=1000):
    """分批下载并存储数据
    Args:
        engine: 数据库引擎
        stocks: 股票代码列表
        start_date: 开始日期
        end_date: 结束日期
        batch_size: 每批处理的股票数量
    """
    # 获取交易日列表
    trade_dates = get_trade_dates_between(start_date, end_date)
    logger.info(f"需要处理的交易日数量: {len(trade_dates)}")
    
    # 按交易日循环
    for current_date in trade_dates:
        logger.info(f"开始处理交易日: {current_date}")
        
        # 对每个交易日，分批处理股票
        for i in range(0, len(stocks), batch_size):
            batch_stocks = stocks[i:i + batch_size]
            all_data = []  # 用于存储当前批次下载的数据
            
            for ts_code in tqdm(batch_stocks, desc=f'下载数据 (日期: {current_date}, 批次 {i // batch_size + 1})'):
                df = download_5min_kline(ts_code=ts_code, start_date=current_date, end_date=current_date)
                
                if not df.empty:
                    all_data.append(df)
                else:
                    logger.warning(f"{ts_code} 在 {current_date} 没有数据")
            
            # 合并当前批次数据并保存到数据库
            if all_data:
                final_df = pd.concat(all_data, ignore_index=True)
                logger.info(f"日期 {current_date} 批次 {i // batch_size + 1} 共下载 {len(final_df)} 条记录")
                
                # 使用 save_to_database 保存数据
                if save_to_database(
                    df=final_df,
                    table_name=table_name,
                    conflict_columns=['trade_time', 'ts_code'],
                    data_type='5分钟K线',
                    engine=engine
                ):
                    logger.info(f"日期 {current_date} 批次 {i // batch_size + 1} 数据保存完成")
                else:
                    logger.error(f"日期 {current_date} 批次 {i // batch_size + 1} 数据保存失败")

def main():
    """主函数"""
    # 登录 baostock
    bs.login()
    
    # 获取最新记录时间
    # latest_time = get_latest_record_time(engine)
    # logger.info(f"数据库最新记录时间: {latest_time}")
    latest_time = '20000101'
    end_time = '20250319'

    # 获取股票列表
    stocks = stock_list['ts_code'].tolist()
    
    # 下载并存储数据
    download_and_store_data(engine, stocks, latest_time, end_time)
    

if __name__ == "__main__":
    main()