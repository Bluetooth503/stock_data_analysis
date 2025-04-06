# -*- coding: utf-8 -*-
from qmt_common import *
import tushare as ts
from xtquant import xtdata
xtdata.enable_hello = False

# ================================= 读取配置文件 =================================
config = load_config()
token = config.get('tushare', 'token')
pro = ts.pro_api(token)

# ================================= 获取最近5个交易日 =================================
def get_trade_dates():
    """获取最近5个交易日"""    
    # 获取最近30天的交易日历
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')    
    # 获取交易日历
    calendar = pro.trade_cal(start_date=start_date, end_date=end_date)
    # 筛选交易日并排序
    trade_dates = calendar[calendar['is_open'] == 1]['cal_date'].sort_values(ascending=False)
    # 获取最近5个交易日
    return trade_dates[:5].tolist()

def get_all_stocks():
    """获取所有股票代码"""
    stocks = xtdata.get_stock_list_in_sector('沪深A股')
    return stocks[:5]  # 只返回前10个股票代码

def on_progress(data):
	print(data)

def download_tick_data(start_date, end_date):
    """下载tick数据并保存为本地文件"""
    # 获取所有股票代码
    stocks = get_all_stocks()
    print(f"获取到 {len(stocks)} 只股票")
    
    print(f"获取数据日期范围: {start_date} 到 {end_date}")
    
    # 直接下载tick数据，不分批次
    try:
        print(f"正在下载所有股票的数据...")
        xtdata.download_history_data2(
            stock_list=stocks, 
            period='tick', 
            start_time=start_date, 
            end_time=end_date,
            # callback=on_progress,
            incrementally=True
        )
        print("所有股票数据下载完成")
                
    except Exception as e:
        print(f"下载数据时出错: {str(e)}")
        print(f"错误类型: {type(e)}")
        import traceback
        print(traceback.format_exc())

# ================================= 读取数据并进行统计 =================================
def read_and_process_data(start_date, end_date):
    """从本地读取已下载的数据并进行统计"""
    # 使用get_market_data_ex从本地读取数据
    all_data = []
    stocks = get_all_stocks()
    
    for stock in stocks:
        try:
            tick_data = xtdata.get_market_data_ex(
                stock_list=[stock],
                period='tick',
                start_time=start_date,  # 使用外层传入的开始时间
                end_time=end_date        # 使用外层传入的结束时间
            )
            
            if tick_data is not None:
                # 处理每个股票的数据
                for stock_code, data in tick_data.items():
                    if isinstance(data, pd.DataFrame) and not data.empty:  # 确保数据是DataFrame且不为空
                        # 将数据转换为DataFrame
                        df = data.copy()  # 直接使用获取到的DataFrame
                        df['stock_code'] = stock_code
                        all_data.append(df)
        
        except Exception as e:
            print(f"读取 {stock} 数据时出错: {str(e)}")
    
    if all_data:
        # 合并所有数据
        final_df = pd.concat(all_data, ignore_index=True)
        
        # 保存原始tick数据
        final_df.to_parquet('qmt_historical_tick_data.parquet', index=False)
        
        # 直接进行统计计算
        try:
            # 使用正确的时间字段
            time_field = 'time'
            if time_field in final_df.columns:
                # 尝试不同的转换方式
                try:
                    # 尝试毫秒级时间戳，并指定时区为 UTC
                    if final_df[time_field].max() > 1000000000000:  # 如果是毫秒时间戳
                        final_df['datetime_utc'] = pd.to_datetime(final_df[time_field], unit='ms', utc=True)
                    else:  # 如果是秒时间戳
                        final_df['datetime_utc'] = pd.to_datetime(final_df[time_field], unit='s', utc=True)
                    
                    # 再将 UTC 时间转换为北京时间 (Asia/Shanghai, 即 UTC+8)
                    final_df['datetime'] = final_df['datetime_utc'].dt.tz_convert('Asia/Shanghai')
                    
                except Exception as e:
                    print(f"转换时间戳时出错: {str(e)}")
                    return
                
                # 提取日期和5分钟时间
                final_df['date'] = final_df['datetime'].dt.date
                final_df['time_key'] = final_df['datetime'].dt.floor('5min').dt.strftime('%H%M%S')
                
                # 过滤掉非交易时间
                # start_am = pd.Timestamp('09:30').time()
                # end_am = pd.Timestamp('11:30').time()
                # start_pm = pd.Timestamp('13:00').time()
                # end_pm = pd.Timestamp('15:00').time()

                # trading_mask = (
                #     ((final_df['datetime'].dt.time >= start_am) &
                #      (final_df['datetime'].dt.time <= end_am)) |
                #     ((final_df['datetime'].dt.time >= start_pm) &
                #      (final_df['datetime'].dt.time <= end_pm))
                # )
                # final_df = final_df[trading_mask]
                # if len(final_df) == 0:
                #     print("警告: 过滤后数据为空!")
                #     return
            else:
                print(f"警告: '{time_field}' 字段不存在于数据中。")
                return
            
            # 1. 先按股票、日期、5分钟时间分组求和
            agg_dict = {}
            if 'volume' in final_df.columns:
                agg_dict['volume'] = 'sum'
            if 'transactionNum' in final_df.columns:
                 agg_dict['transactionNum'] = 'sum'

            if not agg_dict:
                 print("错误：无法找到 'volume' 或 'transactionNum' 列进行聚合。")
                 return

            daily_stats = final_df.groupby(['stock_code', 'date', 'time_key']).agg(agg_dict).reset_index()

            # 2. 再按股票和5分钟时间分组求平均
            agg_dict_mean = {}
            if 'volume' in daily_stats.columns:
                 agg_dict_mean['volume'] = 'mean'
            if 'transactionNum' in daily_stats.columns:
                 agg_dict_mean['transactionNum'] = 'mean'

            if not agg_dict_mean:
                 print("错误：无法找到 'volume' 或 'transactionNum' 列进行平均值计算。")
                 return

            stats = daily_stats.groupby(['stock_code', 'time_key']).agg(agg_dict_mean).reset_index()

            # 重命名列
            rename_map = {'volume': 'avg_volume', 'transactionNum': 'avg_trans'}
            stats.rename(columns=rename_map, inplace=True)
            
            # 检查是否有数据要保存
            if len(stats) == 0:
                print("警告: 没有统计数据可以保存!")
                return
                
            # 保存统计结果
            stats.to_parquet('qmt_tick_statistics.parquet', index=False)
            
        except Exception as e:
            print(f"计算统计数据时出错: {str(e)}")
            import traceback
            print(traceback.format_exc()) # 打印详细错误堆栈
    else:
        print("没有下载到任何数据")

if __name__ == "__main__":
    print("开始获取交易日期...")
    trade_dates = get_trade_dates()
    start_date = trade_dates[-1]  # 最早的一个交易日
    end_date = trade_dates[0]      # 最近的一个交易日
    
    print("开始下载tick数据...")
    download_tick_data(start_date, end_date)
    
    print("开始读取和处理数据...")
    read_and_process_data(start_date, end_date)
    
    print("处理完成！") 