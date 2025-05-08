import os
import pandas as pd
path = r"D:\a_stock_level1_data\factor01"
os.chdir(path)

# 读取 HDF5 文件中特定组的数据
df_stocks = pd.read_hdf('factor01.h5', key='stocks/data')
print(df_stocks.head())