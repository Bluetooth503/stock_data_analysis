import tushare as ts
import pandas as pd
from sqlalchemy import create_engine
import configparser
import datetime
import time
import os
from retrying import retry
from datetime import datetime, timedelta

def load_config():
    """加载配置文件"""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    if not os.path.exists(config_path):
        raise FileNotFoundError("配置文件 'config.ini' 不存在！")
    
    config.read(config_path, encoding='utf-8')
    return config

# 加载配置
config = load_config()

# Tushare token配置
ts.set_token(config['tushare']['token'])
pro = ts.pro_api()

# MySQL配置部分替换为PostgreSQL配置
postgresql_config = config['postgresql']
engine = create_engine(
    f"postgresql://{postgresql_config['user']}:{postgresql_config['password']}@"
    f"{postgresql_config['host']}:{postgresql_config['port']}/{postgresql_config['database']}"
)

def get_date_ranges(start_date, end_date, period_days=90):
    """将时间范围分割成若干个小区间"""
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')
    
    date_ranges = []
    current = start
    
    while current < end:
        range_end = min(current + timedelta(days=period_days), end)
        date_ranges.append((
            current.strftime('%Y%m%d'),
            range_end.strftime('%Y%m%d')
        ))
        current = range_end + timedelta(days=1)
    
    return date_ranges

def retry_if_api_limit_error(exception):
    """判断是否需要重试"""
    error_msg = str(exception)
    return isinstance(exception, Exception) and (
        "每分钟最多访问该接口" in error_msg or
        "每天最多访问该接口" in error_msg
    )

@retry(retry_on_exception=retry_if_api_limit_error,
       wait_fixed=60000,  # 等待60秒
       stop_max_attempt_number=3)  # 最多重试3次
def get_30m_data(ts_code, start_date, end_date):
    """获取单个股票的30分钟K线数据"""
    try:
        df = ts.pro_bar(ts_code=ts_code, 
                       start_date=start_date,
                       end_date=end_date,
                       freq='30min',
                       asset='E',
                       adj='qfq')
        
        if df is not None and not df.empty:
            df['trade_time'] = pd.to_datetime(df['trade_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            return df
        return None
    except Exception as e:
        print(f"获取股票 {ts_code} 的30分钟数据失败: {str(e)}")
        if "每天最多访问该接口" in str(e):
            print("达到每日限制，程序将退出")
            raise SystemExit("达到每日API限制")
        raise

def get_stock_list():
    """获取股票列表"""
    try:
        # 获取所有股票列表
        df = pro.stock_basic(exchange='', list_status='L', 
                            fields='ts_code,symbol,name,area,industry,list_date')
        
        # 存入数据库，如果表存在则替换
        # PostgreSQL中需要小写表名
        df.to_sql('a_stock_name', engine, if_exists='replace', index=False, schema='public')
        print("股票列表数据已成功存入数据库")
        return df['ts_code'].tolist()
    except Exception as e:
        print(f"获取股票列表失败: {str(e)}")
        return []

def process_single_stock(ts_code, date_ranges):
    """处理单个股票的所有时间段数据"""
    all_stock_data = pd.DataFrame()
    
    for start_date, end_date in date_ranges:
        try:
            df = get_30m_data(ts_code, start_date, end_date)
            if df is not None and not df.empty:
                all_stock_data = pd.concat([all_stock_data, df], ignore_index=True)
            
            # 每次请求后暂停30秒
            time.sleep(30)
        except SystemExit:
            raise
        except Exception as e:
            print(f"处理股票 {ts_code} 时间段 {start_date}-{end_date} 时发生错误: {str(e)}")
            continue
    
    return all_stock_data

def main():
    """主函数"""
    try:
        # 获取股票列表
        stock_list = get_stock_list()
        if not stock_list:
            return
        
        # 设置时间范围
        start_date = '20000101'
        end_date = '20241231'
        date_ranges = get_date_ranges(start_date, end_date)
        
        # 创建空的DataFrame用于存储所有数据
        all_data = pd.DataFrame()
        
        # 获取每个股票的30分钟数据
        total_stocks = len(stock_list)
        for i, ts_code in enumerate(stock_list, 1):
            print(f"正在处理 {ts_code}，进度 {i}/{total_stocks}")
            
            try:
                stock_data = process_single_stock(ts_code, date_ranges)
                if not stock_data.empty:
                    all_data = pd.concat([all_data, stock_data], ignore_index=True)
            except SystemExit as e:
                print(str(e))
                break
            except Exception as e:
                print(f"处理股票 {ts_code} 时发生错误: {str(e)}")
                continue
        
        if not all_data.empty:
            all_data.to_sql('a_stock_30m_kline_qfq', engine, 
                           if_exists='replace', 
                           index=False, 
                           schema='public')
            print("所有30分钟K线数据已成功存入数据库")
        else:
            print("没有获取到任何数据")
            
    except Exception as e:
        print(f"程序执行失败: {str(e)}")

if __name__ == "__main__":
    main() 