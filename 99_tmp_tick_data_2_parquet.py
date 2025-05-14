# -*- coding: utf-8 -*-
from common import *
import zipfile
import glob
import re
# 注意：pyarrow库是可选的，pandas会自动选择可用的引擎

# 配置路径
src_path = r"D:\BaiduNetdiskDownload\tick数据"
dest_path = r"D:\a_stock_data\0"

# 设置日志
logger = setup_logger()

def extract_stock_code_from_filename(filename):
    """从文件名中提取股票代码"""
    match = re.search(r'(sh|sz)(\d+)', os.path.basename(filename))
    if match:
        market, code = match.groups()
        if market == 'sh':
            return f"{code}.SH"
        elif market == 'sz':
            return f"{code}.SZ"
    return None

def convert_csv_to_parquet(csv_path, dest_dir, date_str):
    """将CSV文件转换为parquet格式"""
    try:
        # 从文件名中提取股票代码
        ts_code = extract_stock_code_from_filename(csv_path)
        if not ts_code:
            logger.warning(f"无法从文件名 {os.path.basename(csv_path)} 中提取股票代码")
            return False

        # 创建目标目录
        stock_dir = os.path.join(dest_dir, ts_code)
        os.makedirs(stock_dir, exist_ok=True)

        # 读取CSV文件
        df = pd.read_csv(csv_path, encoding='gbk')

        # 重命名列
        column_mapping = {
            '市场代码': 'market',
            '证券代码': 'code',
            '时间': 'trade_time',
            '最新价': 'last_price',
            '成交笔数': 'transaction_num',
            '成交额': 'amount',
            '成交量': 'volume',
            '方向': 'direction',
            '买一价': 'bid_price1',
            '买二价': 'bid_price2',
            '买三价': 'bid_price3',
            '买四价': 'bid_price4',
            '买五价': 'bid_price5',
            '卖一价': 'ask_price1',
            '卖二价': 'ask_price2',
            '卖三价': 'ask_price3',
            '卖四价': 'ask_price4',
            '卖五价': 'ask_price5',
            '买一量': 'bid_volume1',
            '买二量': 'bid_volume2',
            '买三量': 'bid_volume3',
            '买四量': 'bid_volume4',
            '买五量': 'bid_volume5',
            '卖一量': 'ask_volume1',
            '卖二量': 'ask_volume2',
            '卖三量': 'ask_volume3',
            '卖四量': 'ask_volume4',
            '卖五量': 'ask_volume5'
        }
        df.rename(columns=column_mapping, inplace=True)

        # 添加ts_code列
        df['ts_code'] = ts_code
        df['trade_time'] = pd.to_datetime(df['trade_time']).dt.tz_localize('Asia/Shanghai').dt.tz_convert('UTC').view('int64') // 10**6
        df = df.sort_values('trade_time')


        # 添加缺失的列并设置为NULL
        required_columns = [
            'trade_time', 'ts_code', 'last_price', 'open', 'high', 'low', 'pre_close',
            'volume', 'amount', 'transaction_num',
            'bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2',
            'bid_price3', 'bid_volume3', 'bid_price4', 'bid_volume4',
            'bid_price5', 'bid_volume5', 'ask_price1', 'ask_volume1',
            'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3',
            'ask_price4', 'ask_volume4', 'ask_price5', 'ask_volume5'
        ]

        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        # 只保留需要的列
        df = df[required_columns]

        # 转换数据类型
        numeric_columns = [
            'last_price', 'open', 'high', 'low', 'pre_close',
            'volume', 'amount', 'transaction_num',
            'bid_price1', 'bid_volume1', 'bid_price2', 'bid_volume2',
            'bid_price3', 'bid_volume3', 'bid_price4', 'bid_volume4',
            'bid_price5', 'bid_volume5', 'ask_price1', 'ask_volume1',
            'ask_price2', 'ask_volume2', 'ask_price3', 'ask_volume3',
            'ask_price4', 'ask_volume4', 'ask_price5', 'ask_volume5'
        ]

        for col in numeric_columns:
            if col in ['volume', 'transaction_num', 'bid_volume1', 'bid_volume2', 'bid_volume3',
                      'bid_volume4', 'bid_volume5', 'ask_volume1', 'ask_volume2', 'ask_volume3',
                      'ask_volume4', 'ask_volume5']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 保存为parquet文件
        parquet_path = os.path.join(stock_dir, f"{date_str}.parquet")
        df.to_parquet(parquet_path, compression='zstd', index=False)

        return True
    except Exception as e:
        logger.error(f"转换 {os.path.basename(csv_path)} 失败: {str(e)}")
        return False

def process_zip_file(zip_path, dest_dir):
    """处理单个zip文件"""
    try:
        # 从zip文件名中提取年月
        zip_basename = os.path.basename(zip_path)
        year_month = zip_basename.split('.')[0]  # 例如: 202501

        # 创建临时目录用于解压文件
        temp_dir = os.path.join(os.path.dirname(zip_path), f"temp_{year_month}")
        os.makedirs(temp_dir, exist_ok=True)

        # 解压zip文件
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        # 查找所有日期目录
        date_dirs = glob.glob(os.path.join(temp_dir, year_month, '*'))

        total_files = 0
        processed_files = 0

        # 处理每个日期目录
        for date_dir in date_dirs:
            date_str = os.path.basename(date_dir)  # 例如: 20250102

            # 查找所有CSV文件
            csv_files = glob.glob(os.path.join(date_dir, '*.csv'))
            total_files += len(csv_files)

            # 使用tqdm显示进度
            for csv_file in tqdm(csv_files, desc=f"处理 {date_str} 的文件"):
                if convert_csv_to_parquet(csv_file, dest_dir, date_str):
                    processed_files += 1

        # 清理临时目录
        import shutil
        shutil.rmtree(temp_dir)

        logger.info(f"完成处理 {zip_path}: 共处理 {processed_files}/{total_files} 个文件")
        return processed_files
    except Exception as e:
        logger.error(f"处理 {zip_path} 失败: {str(e)}")
        return 0

def process_year_month(src_path, dest_path, year, month=None):
    """处理特定年份和月份的数据"""
    year_dir = os.path.join(src_path, str(year))
    if not os.path.isdir(year_dir):
        logger.error(f"年份目录不存在: {year_dir}")
        return 0

    total_processed = 0

    # 如果指定了月份，只处理该月份的zip文件
    if month:
        month_str = f"{month:02d}" if isinstance(month, int) else month
        zip_pattern = f"{year}{month_str}.zip"
        zip_files = glob.glob(os.path.join(year_dir, zip_pattern))

        if not zip_files:
            logger.error(f"未找到指定月份的zip文件: {os.path.join(year_dir, zip_pattern)}")
            return 0

        # 使用tqdm显示进度
        for zip_file in tqdm(zip_files, desc=f"处理 {year}年{month_str}月 的zip文件"):
            processed = process_zip_file(zip_file, dest_path)
            total_processed += processed
    else:
        # 处理该年份下的所有zip文件
        zip_files = glob.glob(os.path.join(year_dir, '*.zip'))

        if not zip_files:
            logger.error(f"未找到任何zip文件: {year_dir}")
            return 0

        # 使用tqdm显示进度
        for zip_file in tqdm(zip_files, desc=f"处理 {year}年 的zip文件"):
            processed = process_zip_file(zip_file, dest_path)
            total_processed += processed

    month_str = f"{month}月" if month else ""
    logger.info(f"{year}年{month_str}数据处理完成: 共处理 {total_processed} 个文件")
    return total_processed

def process_all_data(src_path, dest_path):
    """处理所有数据"""
    # 查找所有年份目录
    year_dirs = glob.glob(os.path.join(src_path, '*'))

    total_processed = 0

    # 处理每个年份目录
    for year_dir in year_dirs:
        if not os.path.isdir(year_dir):
            continue

        year = os.path.basename(year_dir)
        logger.info(f"处理 {year} 年的数据")

        # 处理该年份的所有数据
        processed = process_year_month(src_path, dest_path, year)
        total_processed += processed

    logger.info(f"所有数据处理完成: 共处理 {total_processed} 个文件")
    return total_processed

if __name__ == "__main__":
    import argparse

    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='将tick数据从CSV转换为parquet格式')
    parser.add_argument('--src', type=str, default=src_path, help='源数据路径')
    parser.add_argument('--dest', type=str, default=dest_path, help='目标数据路径')
    parser.add_argument('--year', type=str, help='指定要处理的年份，例如：2025')
    parser.add_argument('--month', type=str, help='指定要处理的月份，例如：01（需要同时指定年份）')

    # 解析命令行参数
    args = parser.parse_args()

    # 更新路径
    src_path = args.src
    dest_path = args.dest

    # 显示处理开始信息
    logger.info("开始处理tick数据转换为parquet格式")
    logger.info(f"源路径: {src_path}")
    logger.info(f"目标路径: {dest_path}")

    # 确保目标路径存在
    os.makedirs(dest_path, exist_ok=True)

    # 根据参数决定处理方式
    if args.year:
        if args.month:
            logger.info(f"处理 {args.year}年{args.month}月 的数据")
            total_processed = process_year_month(src_path, dest_path, args.year, args.month)
        else:
            logger.info(f"处理 {args.year}年 的所有数据")
            total_processed = process_year_month(src_path, dest_path, args.year)
    else:
        logger.info("处理所有年份的数据")
        total_processed = process_all_data(src_path, dest_path)

    # 显示处理完成信息
    logger.info(f"处理完成! 共处理 {total_processed} 个文件")
    logger.info(f"数据已保存到 {dest_path}")

    # 如果没有处理任何文件，可能是路径有问题
    if total_processed == 0:
        logger.warning("未处理任何文件，请检查源路径是否正确")
        logger.warning(f"源路径: {src_path}")
        logger.warning("目录结构应为: src_path/yyyy/yyyymm.zip")
