# -*- coding: utf-8 -*-
from qmt_common import *
import tushare as ts
from xtquant import xtdata
xtdata.enable_hello = False
import concurrent.futures


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

# ================================= 获取所有股票代码 =================================
def get_all_stocks():
    """获取所有股票代码"""
    stocks = xtdata.get_stock_list_in_sector('沪深A股')
    return stocks

# ================================= 下载tick数据 =================================
def on_progress(data):
    print(f"已完成:{data['finished']}/{data['total']} - {data['message']}")

def download_tick_data(start_date: str, end_date: str) -> None:
    """下载tick数据并保存为本地文件"""
    stocks = get_all_stocks()
    print(f"获取到 {len(stocks)} 只股票")
    print(f"获取数据日期范围: {start_date} 到 {end_date}")
    
    try:
        xtdata.download_history_data2(
            stock_list=stocks, 
            period='tick', 
            start_time=start_date,
            end_time=end_date,
            incrementally=True,
            callback=on_progress
        )
        print("所有股票数据下载完成")
                
    except Exception as e:
        print(f"下载tick数据时出错: {str(e)}")


# ================================= 下载Level 1数据 =================================
def create_directory(directory: str) -> None:
    """创建文件夹"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def download_batch_stock_data(stocks: list, date: str) -> None:
    """下载一批股票的Level 1数据"""
    try:
        tick_data = xtdata.get_market_data_ex(
            stock_list=stocks,
            period='tick',
            start_time=date,
            end_time=date  # 下载指定日期的数据
        )

        for stock in stocks:
            if tick_data is not None and stock in tick_data:
                df = tick_data[stock]
                df['ts_code'] = stock
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # 生成文件名
                    file_name = f'qmt_tick_level1_data/{date}/{date}_{stock}.parquet'
                    df.to_parquet(file_name, index=False, compression='snappy')
                    print(f"保存 {file_name} 成功")
    except Exception as e:
        print(f"下载 {stocks} 的数据时出错: {str(e)}")

def download_and_save_level1_data(start_date: str, end_date: str) -> None:
    """下载并保存Level 1数据"""
    stocks = get_all_stocks()
    create_directory('qmt_tick_level1_data')  # 创建文件夹

    # 生成日期范围
    date_range = pd.date_range(start=start_date, end=end_date).strftime('%Y%m%d').tolist()

    # 获取所有交易日
    trade_dates = get_trade_dates()

    for date in tqdm(date_range, desc="下载Level 1数据进度"):
        if date not in trade_dates:  # 检查是否为交易日
            print(f"跳过非交易日: {date}")
            continue

        create_directory(f'qmt_tick_level1_data/{date}')  # 创建日期文件夹

        # 使用线程池并行下载数据
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 将股票分成批次
            batch_size = 500  # 每批下载的股票数量
            for i in range(0, len(stocks), batch_size):
                stock_batch = stocks[i:i + batch_size]
                executor.submit(download_batch_stock_data, stock_batch, date)


# ================================= 读取Level 1数据并进行统计 =================================
def read_single_stock_data(stock: str, date: str) -> pd.DataFrame:
    """读取单个股票的Level 1数据"""
    file_name = f'qmt_tick_level1_data/{date}/{date}_{stock}.parquet'
    if os.path.exists(file_name):
        return pd.read_parquet(file_name)
    return None

def read_and_process_level1_data(start_date: str, end_date: str, max_workers: int = 32) -> None:
    """读取Level 1数据并进行统计"""
    all_data = []

    # 获取所有股票代码
    stocks = get_all_stocks()

    # 生成日期范围
    date_range = pd.date_range(start=start_date, end=end_date).strftime('%Y%m%d').tolist()

    # 获取所有交易日
    trade_dates = get_trade_dates()

    for date in date_range:
        if date not in trade_dates:  # 检查是否为交易日
            print(f"跳过非交易日: {date}")
            continue

        # 使用线程池并行读取数据
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(read_single_stock_data, stock, date): stock for stock in stocks}
            for future in concurrent.futures.as_completed(futures):
                stock = futures[future]
                try:
                    temp_data = future.result()
                    if temp_data is not None:
                        all_data.append(temp_data)
                except Exception as e:
                    print(f"读取 {stock} 的数据时出错: {str(e)}")

    if all_data:
        all_data = pd.concat(all_data, ignore_index=True)

        # 进行统计计算
        try:
            time_field = 'time'
            if time_field in all_data.columns:
                # 尝试不同的时间戳转换
                if all_data[time_field].max() > 1000000000000:  # 毫秒时间戳
                    all_data['datetime_utc'] = pd.to_datetime(all_data[time_field], unit='ms', utc=True)
                else:  # 秒时间戳
                    all_data['datetime_utc'] = pd.to_datetime(all_data[time_field], unit='s', utc=True)
                
                # 转换为北京时间
                all_data['datetime'] = all_data['datetime_utc'].dt.tz_convert('Asia/Shanghai')
                
                # 提取日期和5分钟时间
                all_data['date'] = all_data['datetime'].dt.date
                all_data['time_key'] = all_data['datetime'].dt.floor('5min').dt.strftime('%H%M%S')
            else:
                print(f"警告: '{time_field}' 字段不存在于数据中。")
                return
            
            # 1. 先按股票、日期、5分钟时间分组求和
            agg_dict = {}
            if 'volume' in all_data.columns:
                agg_dict['volume'] = 'sum'
            if 'transactionNum' in all_data.columns:
                agg_dict['transactionNum'] = 'sum'

            if not agg_dict:
                print("错误：无法找到 'volume' 或 'transactionNum' 列进行聚合。")
                return

            daily_stats = all_data.groupby(['ts_code', 'date', 'time_key']).agg(agg_dict).reset_index()

            # 2. 再按股票和5分钟时间分组求平均
            agg_dict_mean = {}
            if 'volume' in daily_stats.columns:
                agg_dict_mean['volume'] = 'mean'
            if 'transactionNum' in daily_stats.columns:
                agg_dict_mean['transactionNum'] = 'mean'

            if not agg_dict_mean:
                print("错误：无法找到 'volume' 或 'transactionNum' 列进行平均值计算。")
                return

            stats = daily_stats.groupby(['ts_code', 'time_key']).agg(agg_dict_mean).reset_index()

            # 重命名列
            rename_map = {'volume': 'avg_volume', 'transactionNum': 'avg_trans'}
            stats.rename(columns=rename_map, inplace=True)
            
            # 检查是否有数据要保存
            if len(stats) == 0:
                print("警告: 没有统计数据可以保存!")
                return
                
            # 保存统计结果
            stats.to_parquet('qmt_past_5_days_statistics.parquet', index=False, compression='snappy')
        
        except Exception as e:
            print(f"计算统计数据时出错: {str(e)}")
            import traceback
            print(traceback.format_exc())  # 打印详细错误堆栈
    else:
        print("没有读取到任何parquet数据")


# ================================= 主函数 =================================
if __name__ == "__main__":
    print("开始获取交易日期...")
    trade_dates = get_trade_dates()
    start_date = trade_dates[-1]   # 最早的一个交易日
    end_date = trade_dates[0]      # 最近的一个交易日
    
    print("开始下载tick数据...")
    download_tick_data(end_date, end_date)
        
    print("开始下载Level 1数据...")
    download_and_save_level1_data(end_date, end_date)
    
    print("开始读取和处理Level 1数据...")
    read_and_process_level1_data(start_date, end_date)
    
    print("处理完成！") 