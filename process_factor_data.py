import pandas as pd
import h5py
import numpy as np
import logging

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_factor_data(input_file='factor01.h5', output_file='factor_with_returns.h5'):
    """从HDF5文件读取因子数据，计算未来收益率，并保存结果
    
    Args:
        input_file (str): 输入的HDF5文件路径
        output_file (str): 输出的HDF5文件路径
    """
    try:
        # 读取HDF5文件
        with h5py.File(input_file, 'r') as hf:
            stocks_group = hf['stocks']
            
            # 创建输出HDF5文件
            with h5py.File(output_file, 'w') as out_hf:
                root_group = out_hf.create_group('stocks')
                
                # 遍历每个股票的数据
                for stock_code in stocks_group.keys():
                    logger.info(f"处理股票: {stock_code}")
                    
                    # 读取股票数据
                    stock_group = stocks_group[stock_code]
                    
                    # 将数据转换为DataFrame
                    df = pd.DataFrame({
                        'trade_time': [str(t) for t in stock_group['trade_time'][:]], # 转换时间戳为字符串
                        'ts_code': [stock_code] * len(stock_group['trade_time']),
                        'avg_bid_ask_ratio': stock_group['avg_bid_ask_ratio'][:],
                        'total_bid_ask_ratio': stock_group['total_bid_ask_ratio'][:],
                        'close': stock_group['close'][:]
                    })
                    
                    # 计算未来收益率
                    df['close_5min'] = df['close'].shift(-5)
                    df['close_15min'] = df['close'].shift(-15)
                    df['close_30min'] = df['close'].shift(-30)
                    
                    df['return_5min'] = (df['close_5min'] - df['close']) / df['close']
                    df['return_15min'] = (df['close_15min'] - df['close']) / df['close']
                    df['return_30min'] = (df['close_30min'] - df['close']) / df['close']
                    
                    # 创建股票组并保存数据
                    stock_out_group = root_group.create_group(stock_code)
                    
                    # 保存所有列
                    for col in df.columns:
                        if col == 'trade_time':
                            # 使用特殊数据类型存储时间戳字符串
                            dt = h5py.special_dtype(vlen=str)
                            stock_out_group.create_dataset(
                                col,
                                data=df[col].values,
                                dtype=dt,
                                compression='lzf'
                            )
                        else:
                            # 其他数值类型数据
                            stock_out_group.create_dataset(
                                col,
                                data=df[col].values,
                                compression='lzf'
                            )
                    
                    logger.info(f"完成处理股票: {stock_code}")
                
                logger.info("所有股票数据处理完成")
                
    except Exception as e:
        logger.error(f"处理数据时发生错误: {str(e)}")
        raise

if __name__ == '__main__':
    process_factor_data()