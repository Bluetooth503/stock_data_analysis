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
    return stocks

# ================================= 下载并保存Level 1数据 =================================
def download_batch_stock_data(stocks: list, date: str) -> None:
    """下载一批股票的Level 1数据并保存到数据库"""
    logger.info(f"开始下载{len(stocks)}只股票的数据，日期：{date}")
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
    time.sleep(10)

    # 从本地读取数据
    tick_data = xtdata.get_market_data_ex(
        stock_list=stocks,
        period='tick',
        start_time=date,
        end_time=date)

    all_data = []
    for stock in stocks:
        if tick_data is not None and stock in tick_data:
            df = tick_data[stock]
            if isinstance(df, pd.DataFrame) and not df.empty:
                df['ts_code'] = stock
                df['trade_time'] = df['time'] // 1000  # 使用整除运算符确保结果为整数
                # 过滤volume>0的数据
                df = df[df['volume'] > 0]
                if not df.empty:
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
                    
                    all_data.append(df)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        # 选择需要的列并重命名
        columns = ['trade_time', 'ts_code', 'lastPrice', 'open', 'high', 'low', 'lastClose', 'volume', 'amount', 'transactionNum']
        columns.extend([f'bid_price{i+1}' for i in range(5)])
        columns.extend([f'bid_volume{i+1}' for i in range(5)])
        columns.extend([f'ask_price{i+1}' for i in range(5)])
        columns.extend([f'ask_volume{i+1}' for i in range(5)])
        
        final_df = final_df[columns].copy()
        rename_dict = {
            'lastPrice': 'last_price',
            'lastClose': 'pre_close',
            'transactionNum': 'transaction_num'}
        final_df = final_df.rename(columns=rename_dict)            
        # 使用save_to_database函数保存到数据库，避免重复数据
        conflict_columns = ['ts_code', 'trade_time']
        save_to_database(final_df, 'ods_a_stock_level1_data', conflict_columns, 'Level 1 Data', engine)
        
        # 保存为parquet文件
        base_path = r'D:\a_stock_data\0'
        # 按股票代码分组保存
        for ts_code, group_df in final_df.groupby('ts_code'):
            # 创建股票对应的目录
            stock_dir = os.path.join(base_path, ts_code)
            os.makedirs(stock_dir, exist_ok=True)
            
            # 构建文件路径
            file_path = os.path.join(stock_dir, f'{date}.parquet')
            
            # 保存为parquet文件，使用zstd压缩
            group_df.to_parquet(file_path, compression='zstd', index=False)
            logger.info(f"已保存 {ts_code} 的parquet文件到 {file_path}")


def download_and_save_level1_data(start_date: str, end_date: str) -> None:
    """下载并保存Level 1数据到数据库"""
    # 获取所有股票代码    
    stocks = get_all_stocks()
    logger.info(f"获取到 {len(stocks)} 只股票")

    # 生成有效交易日期范围
    trade_dates = get_trade_dates(start_date=start_date, end_date=end_date)
    date_range = [d.replace('-', '') for d in trade_dates]
    logger.info(f"需要下载的日期列表是:{date_range}")

    for date in date_range:
        # 将股票分成批次并顺序处理
        batch_size = 500  # 每批下载的股票数量
        for i in range(0, len(stocks), batch_size):
            stock_batch = stocks[i:i + batch_size]
            logger.info(f"处理第 {i//batch_size + 1} 批股票数据 ({i+1} - {min(i+batch_size, len(stocks))})")  # 添加批次信息
            download_batch_stock_data(stock_batch, date)


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
        download_and_save_level1_data(args.start_date.replace('-', ''), args.end_date.replace('-', ''))
    else:
        # 自动模式处理当天数据
        logger.info("自动模式启动 处理当天数据")
        trade_dates = get_trade_dates()
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        if current_date in trade_dates:
            download_and_save_level1_data(current_date.replace('-', ''), current_date.replace('-', ''))
        else:
            logger.warning(f"当前日期 {current_date} 不是交易日，跳过下载")
    logger.info("处理完成！")