# -*- coding: utf-8 -*-
from common import *
from xtquant import xtdata
xtdata.enable_hello = False


# ================================= 读取配置文件 =================================
logger = setup_logger()
config = load_config()
engine = create_engine(get_pg_connection_string(config))

# ================================= 获取所有股票代码 =================================
def get_all_stocks():
    """获取所有股票代码"""
    stocks = xtdata.get_stock_list_in_sector('沪深A股')
    stocks.sort()
    return stocks

# ================================= 下载并保存Level 1数据 =================================
def download_batch_stock_data(stocks: list, date: str) -> None:
    """下载一批股票的Level 1数据并保存到本地"""
    def on_progress(data):
        if data['finished'] % 100 == 0:
            logger.info(f"已完成:{data['finished']}/{data['total']} - {data['message']}")

    # 先下载数据到本地
    xtdata.download_history_data2(
        stock_list=stocks,
        period='tick',
        start_time=date,
        end_time=date,
        incrementally=True,
        callback=on_progress)
    time.sleep(5)

    # 从本地读取数据
    tick_data = xtdata.get_market_data_ex(
        stock_list=stocks,
        period='tick',
        start_time=date,
        end_time=date)

    if tick_data is not None:
        for stock in stocks:
            df = tick_data[stock]
            if isinstance(df, pd.DataFrame) and not df.empty:
                df['ts_code'] = stock
                # 过滤volume>0的数据
                df = df[df['volume'] > 0]
                if not df.empty:
                    # 按时间排序，确保增量计算的准确性
                    df = df.sort_values('time')
                    # 处理买卖5档数据
                    bid_prices = df['bidPrice'].apply(lambda x: x if isinstance(x, list) else []).tolist()
                    bid_volumes = df['bidVol'].apply(lambda x: x if isinstance(x, list) else []).tolist()
                    ask_prices = df['askPrice'].apply(lambda x: x if isinstance(x, list) else []).tolist()
                    ask_volumes = df['askVol'].apply(lambda x: x if isinstance(x, list) else []).tolist()

                    # 创建买卖5档的列
                    for i in range(5):
                        df[f'bid_price{i+1}'] = [prices[i] if i < len(prices) else 0.0 for prices in bid_prices]
                        df[f'bid_volume{i+1}'] = [vols[i] if i < len(vols) else 0 for vols in bid_volumes]
                        df[f'ask_price{i+1}'] = [prices[i] if i < len(prices) else 0.0 for prices in ask_prices]
                        df[f'ask_volume{i+1}'] = [vols[i] if i < len(vols) else 0 for vols in ask_volumes]

                    # 将累计值转换为每个时间段的增量值
                    for col in ['volume', 'amount', 'transactionNum']:
                        # 创建一个临时列保存原始值
                        df[f'orig_{col}'] = df[col].copy()
                        # 计算差值（当前值减去前一个值）
                        df[col] = df[col].diff()
                        # 第一行的差值是NaN，用原始值替换
                        df.loc[df.index[0], col] = df.loc[df.index[0], f'orig_{col}']
                        # 删除临时列
                        df = df.drop(columns=[f'orig_{col}'])

                    columns = ['time', 'ts_code', 'lastPrice', 'open', 'high', 'low', 'lastClose', 'volume', 'amount', 'transactionNum']
                    columns.extend([f'bid_price{i+1}' for i in range(5)])
                    columns.extend([f'bid_volume{i+1}' for i in range(5)])
                    columns.extend([f'ask_price{i+1}' for i in range(5)])
                    columns.extend([f'ask_volume{i+1}' for i in range(5)])

                    df = df[columns].copy()
                    rename_dict = {
                        'time': 'trade_time',
                        'lastPrice': 'last_price',
                        'lastClose': 'pre_close',
                        'transactionNum': 'transaction_num'}
                    df = df.rename(columns=rename_dict)

                    # 保存为parquet文件
                    base_path = r'D:\a_stock_data\0'
                    stock_dir = os.path.join(base_path, stock)
                    os.makedirs(stock_dir, exist_ok=True)

                    # 构建文件路径
                    file_path = os.path.join(stock_dir, f'{date}.parquet')
                    df.to_parquet(file_path, compression='zstd', index=False)
    else:
        logger.info("没有获取到数据!")

def download_and_save_level1_data(start_date: str, end_date: str) -> dict:
    """下载并保存Level 1数据"""
    # 获取所有股票代码
    stocks = get_all_stocks()
    total_stocks = len(stocks)
    logger.info(f"获取到 {total_stocks} 只股票")

    # 生成有效交易日期范围
    trade_dates = get_trade_dates(start_date=start_date, end_date=end_date)
    date_range = [d.replace('-', '') for d in trade_dates]
    total_dates = len(date_range)
    logger.info(f"需要下载的日期列表是:{date_range}")

    stats = {
        'total_stocks': total_stocks,
        'processed_dates': date_range,
        'total_dates': total_dates,
        'start_time': datetime.now()
    }

    for date in date_range:
        logger.info(f"开始处理日期: {date}")
        # 将股票分成批次并顺序处理
        batch_size = 100  # 每批下载的股票数量
        total_batches = (total_stocks + batch_size - 1) // batch_size
        
        for i in range(0, total_stocks, batch_size):
            batch_num = i // batch_size + 1
            stock_batch = stocks[i:i + batch_size]
            logger.info(f"处理第 {batch_num}/{total_batches} 批股票数据 ({i+1} - {min(i+batch_size, total_stocks)})")
            download_batch_stock_data(stock_batch, date)

    stats['end_time'] = datetime.now()
    stats['total_time'] = stats['end_time'] - stats['start_time']
    
    return stats


# ================================= 参数解析 =================================
def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='股票Level1数据下载工具')
    parser.add_argument('--start_date', help='开始日期 (YYYY-MM-DD)', type=str)
    parser.add_argument('--end_date', help='结束日期 (YYYY-MM-DD)', type=str)
    return parser.parse_args()

# ================================= 修改主函数逻辑 =================================
if __name__ == "__main__":
    args = parse_arguments()

    if args.start_date and args.end_date:
        # 手动模式处理指定日期范围
        logger.info(f"手动模式启动 日期范围: {args.start_date} 至 {args.end_date}")
        stats = download_and_save_level1_data(args.start_date.replace('-', ''), args.end_date.replace('-', ''))
    else:
        # 自动模式处理当天数据
        logger.info("自动模式启动 处理当天数据")
        current_date = datetime.now().strftime('%Y-%m-%d')
        trade_dates = get_trade_dates(current_date, current_date)

        if current_date in trade_dates:
            stats = download_and_save_level1_data(current_date.replace('-', ''), current_date.replace('-', ''))
        else:
            logger.warning(f"当前日期 {current_date} 不是交易日，跳过下载")
            sys.exit(0)

    # 输出统计信息
    logger.info("=== 处理完成，统计信息如下 ===")
    logger.info(f"处理日期: {', '.join(stats['processed_dates'])}")
    logger.info(f"总股票数: {stats['total_stocks']}")
    logger.info(f"总日期数: {stats['total_dates']}")
    logger.info(f"总耗时: {stats['total_time']}")
    logger.info("=== 统计信息结束 ===")