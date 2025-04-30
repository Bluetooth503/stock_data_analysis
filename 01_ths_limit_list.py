# -*- coding: utf-8 -*-
from common import *
import tushare as ts
import argparse

# ================================= 读取配置文件 =================================
config = load_config()
engine = create_engine(get_pg_connection_string(config))
logger = setup_logger()

# ================================= tushare配置 =================================
token = config.get('tushare', 'token')
pro = ts.pro_api(token)

def is_trade_date(date_str):
    """判断是否为交易日
    Args:
        date_str: 日期字符串，格式为YYYYMMDD
    Returns:
        bool: 是否为交易日
    """
    # 转换日期格式为数据库格式（YYYY-MM-DD）
    date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    # 查询数据库判断是否为交易日
    sql = text("""
        SELECT is_open
        FROM a_stock_trade_cal
        WHERE trade_date = :date_str
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {"date_str": date_str}).scalar()
    return result == '1' if result is not None else False

def get_ths_limit_data(trade_date, limit_type):
    """获取同花顺涨跌停数据"""
    try:
        df = pro.limit_list_ths(
            trade_date=trade_date.strftime('%Y%m%d'),
            limit_type=limit_type
        )
        if df is not None and not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            df['lu_limit_order'] = df['lu_limit_order'].fillna(0)
            df['open_num'] = df['open_num'].fillna(0)
            df['limit_order'] = df['limit_order'].fillna(0)
            df['limit_amount'] = df['limit_amount'].fillna(0)
            df['limit_up_suc_rate'] = df['limit_up_suc_rate'].fillna(0)
            df['turnover'] = df['turnover'].fillna(0)
            return df
    except Exception as e:
        logger.error(f"获取{limit_type}数据失败: {str(e)}")
    return None

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='下载同花顺涨跌停数据')
    parser.add_argument('--start_date', help='开始日期 (YYYY-MM-DD)', type=str)
    parser.add_argument('--end_date', help='结束日期 (YYYY-MM-DD)', type=str)
    args = parser.parse_args()

    # 设置日期范围
    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    else:
        start_date = end_date = datetime.now()

    # 生成日期范围
    date_range = pd.date_range(start=start_date, end=end_date)

    # 涨跌停类型列表
    limit_types = ['涨停池', '连扳池', '冲刺涨停', '炸板池', '跌停池']

    # 遍历日期范围
    for current_date in date_range:
        date_str = current_date.strftime('%Y%m%d')
        
        # 检查是否为交易日
        if not is_trade_date(date_str):
            logger.warning(f"{date_str}不是交易日，跳过")
            continue

        logger.info(f"开始处理{date_str}的数据")
        
        # 获取数据并保存
        for limit_type in limit_types:
            df = get_ths_limit_data(current_date, limit_type)
            if df is not None and not df.empty:
                table_name = 'ths_limit_list'
                save_to_database(
                    df, 
                    table_name, 
                    conflict_columns=['ts_code', 'trade_date', 'limit_type'],
                    data_type=f'同花顺{limit_type}数据',
                    engine=engine
                )
                logger.info(f"成功保存{date_str}的{limit_type}数据，共{len(df)}条记录")


if __name__ == "__main__":
    main()

