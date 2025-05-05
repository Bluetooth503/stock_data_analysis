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

# ================================= 个股资金流向数据 =================================
def get_moneyflow_with_retry(date_range=None, max_retries=100, wait_seconds=600):
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
        data_type="个股资金流向数据",
        date_params=date_params,
        max_retries=max_retries,
        wait_seconds=wait_seconds,
        api_func=pro.moneyflow,
        process_func=None
    )




    # 获取资金流向数据
    logger.info(f"开始获取最近{n_days}天的资金流向数据...")
    moneyflow_df = get_moneyflow_with_retry(today, max_retries, wait_seconds)
    if moneyflow_df is None:
        logger.error(f"无法获取完整的资金流向数据，请检查数据源")
        return
    
    if not save_to_database(
        df=moneyflow_df, 
        table_name='a_stock_moneyflow', 
        conflict_columns=['ts_code', 'trade_date'],
        # update_columns=['buy_sm_vol','buy_sm_amount','sell_sm_vol','sell_sm_amount','buy_md_vol','buy_md_amount','sell_md_vol','sell_md_amount','buy_lg_vol','buy_lg_amount','sell_lg_vol','sell_lg_amount','buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol','net_mf_amount'],
        data_type='资金流向',
        engine=engine
    ):
        return