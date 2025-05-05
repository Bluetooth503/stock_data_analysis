# -*- coding: utf-8 -*-
from common import *
import tushare as ts
import argparse

# ================================= 定义初始变量 =================================
wait_seconds = 600  # 等待时间
max_retries = 100   # 最大重试次数

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='下载A股每日基本面数据')
    parser.add_argument('--start_date', help='开始日期 (YYYY-MM-DD)', type=str)
    parser.add_argument('--end_date', help='结束日期 (YYYY-MM-DD)', type=str)
    return parser.parse_args()

# ================================= 读取配置文件 =================================
config = load_config()
token = config.get('tushare', 'token')
pro = ts.pro_api(token)
engine = create_engine(get_pg_connection_string(config))

# ================================= 配置日志 =================================
logger = setup_logger()

# ================================= 通用数据获取函数 =================================
def get_data_with_retry_tushare(
    data_type: str,
    date_params: Union[str, Tuple[str, str]],
    max_retries: int,
    wait_seconds: int,
    api_func: Callable,
    process_func: Optional[Callable] = None,
    **kwargs
) -> Optional[pd.DataFrame]:
    """
    通用数据获取函数，支持单日和日期范围的数据获取
    
    Args:
        data_type: 数据类型描述
        date_params: 日期参数，可以是以下两种形式之一：
            - 单个日期字符串 (YYYYMMDD)
            - 日期范围元组 (start_date, end_date)
        max_retries: 最大重试次数
        wait_seconds: 重试等待时间
        api_func: API调用函数
        process_func: 数据处理函数（可选）
        **kwargs: 传递给api_func的额外参数
    
    Returns:
        Optional[pd.DataFrame]: 处理后的数据框或None
    """
    def get_single_date_data(date: str, ts_code: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        """
        获取单日或日期范围数据
        
        Args:
            date: 查询日期（单日模式）或起始日期（范围模式）
            ts_code: 股票代码（范围模式必填）
            end_date: 结束日期（范围模式）
        """
        # 参数验证
        if end_date and not ts_code:
            raise ValueError("日期范围查询必须提供ts_code参数")
            
        # 构建请求参数
        params = {'trade_date': date} if not end_date else {
            'ts_code': ts_code,
            'start_date': date,
            'end_date': end_date
        }
        try:
            df = api_func(**params, **kwargs)
            if df is not None and not df.empty and process_func:
                df = process_func(df)
            return df if df is not None and not df.empty else None
        except Exception as e:
            logger.error(f"API请求失败: {str(e)}")
            return None

    for retry in range(max_retries):
        try:
            if isinstance(date_params, str):
                # 单日数据获取
                df = get_single_date_data(date_params)
                if df is not None and not df.empty:
                    logger.info(f"成功获取{data_type}单日数据，记录数：{len(df)}")
                    return df
                else:
                    logger.warning(f"未获取到{data_type}单日数据")
                    return None
            elif isinstance(date_params, tuple):
                # 日期范围数据获取
                start_date, end_date = date_params
                # 从数据库获取股票列表
                sql = text("""
                    SELECT DISTINCT ts_code
                    FROM a_stock_basic
                """)
                with engine.connect() as conn:
                    stock_list = pd.read_sql(sql, conn)
                
                all_data = []
                for _, row in tqdm(stock_list.iterrows(), desc="下载股票基本面数据"):
                    try:
                        df = get_single_date_data(start_date, ts_code=row['ts_code'], end_date=end_date)
                        if df is not None and not df.empty:
                            all_data.append(df)
                    except Exception as e:
                        logger.error(f"获取股票{row['ts_code']}的数据失败: {str(e)}")
                        time.sleep(wait_seconds)
                        continue
                
                if all_data:
                    return pd.concat(all_data, ignore_index=True).sort_values('trade_date', ascending=True)

            retry += 1
            logger.warning(f"第{retry}次尝试未获取到{data_type}数据，等待{wait_seconds}秒后重试...")
            time.sleep(wait_seconds)
            
        except Exception as e:
            retry += 1
            logger.error(f"获取{data_type}数据失败: {str(e)}")
            time.sleep(wait_seconds)
    
    logger.error(f"无法获取{data_type}数据")
    return None

# ================================= 个股daily_basic数据 =================================
def _get_circ_mv_range(circ_mv):
    """根据流通市值计算区间标签（内部函数）"""
    circ_mv = circ_mv / 10000  # circ_mv单位万元,转换为亿元
    if circ_mv <= 20:
        return '0-20亿'
    elif circ_mv <= 50:
        return '20-50亿'
    elif circ_mv <= 100:
        return '50-100亿'
    elif circ_mv <= 500:
        return '100-500亿'
    elif circ_mv <= 1000:
        return '500-1000亿'
    elif circ_mv <= 10000:
        return '1000-10000亿'
    else:
        return '10000亿以上'

def process_basic_data(df):
    """处理每日基本面数据"""
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
    df['circ_mv_range'] = df['circ_mv'].apply(_get_circ_mv_range)
    numeric_columns = ['close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio', 
                     'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm',
                     'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def get_daily_basic_with_retry(date_range=None, max_retries=100, wait_seconds=600):
    """获取每日基本面数据
    
    Args:
        date_range: 元组(start_date, end_date)，格式为(YYYYMMDD, YYYYMMDD)。如果为None，则获取当天数据
        max_retries: 最大重试次数
        wait_seconds: 重试等待时间
    
    Returns:
        pd.DataFrame: 处理后的数据框或None
    """
    if date_range is None:
        today = datetime.now().strftime('%Y%m%d')
        date_params = today
    else:
        date_params = date_range
    
    return get_data_with_retry_tushare(
        data_type="每日基本面",
        date_params=date_params,
        max_retries=max_retries,
        wait_seconds=wait_seconds,
        api_func=pro.daily_basic,
        process_func=process_basic_data
    )


# ================================= 每日执行任务 =================================
def process_date_range(start_date=None, end_date=None):
    """处理日期范围数据下载
    
    Args:
        start_date: 开始日期，格式YYYY-MM-DD
        end_date: 结束日期，格式YYYY-MM-DD
    """
    # 如果没有指定日期范围，则处理当天数据
    if not start_date and not end_date:
        today = datetime.now().strftime('%Y%m%d')
        if not is_trade_date(today):
            logger.info(f"{today} 不是交易日，跳过执行")
            return
        logger.info(f"开始获取当天({today})的每日基本面数据")
        basic_df = get_daily_basic_with_retry()
    else:
        # 确保日期格式统一
        start_date = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y%m%d') if start_date else None
        end_date = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y%m%d') if end_date else None
        
        logger.info(f"开始获取日期范围({start_date}至{end_date})的每日基本面数据")
        basic_df = get_daily_basic_with_retry(date_range=(start_date, end_date))
    
    if basic_df is None:
        logger.error("无法获取每日基本面数据，请检查数据源")
        return
    
    if not save_to_database(
        df=basic_df,
        table_name='a_stock_daily_basic',
        conflict_columns=['ts_code', 'trade_date'],
        # update_columns=['close','turnover_rate','turnover_rate_f','volume_ratio','pe','pe_ttm','pb','ps','ps_ttm','dv_ratio','dv_ttm','total_share','float_share','free_share','total_mv','circ_mv','circ_mv_range'],
        data_type='每日基本面',
        engine=engine
    ):
        logger.error("保存数据失败")


def main():
    """主函数"""
    args = parse_arguments()
    process_date_range(args.start_date, args.end_date)

if __name__ == '__main__':
    main()