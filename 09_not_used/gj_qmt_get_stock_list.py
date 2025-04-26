import pandas as pd
from xtquant import xtdata
xtdata.enable_hello = False

# stock_list = xtdata.get_stock_list_in_sector('沪深A股')
# series = pd.Series(stock_list)
# series.to_csv('沪深A股_stock_list.csv',encoding='utf-8',index=False,header=False)

stock_list = xtdata.get_stock_list_in_sector('沪深300')
series = pd.Series(stock_list)
series.to_csv('沪深300_stock_list_1.csv',encoding='utf-8',index=False,header=False)