# -*- coding: utf-8 -*-
from common import *
from xtquant import xtdata
xtdata.enable_hello = False

index_name = ['上证50','沪深300','中证500','中证1000']
# 循环获取各指数成分股并合并
df = pd.concat([
    pd.DataFrame({
        'index_name': name,
        'ts_code': xtdata.get_stock_list_in_sector(name)
    }) 
    for name in index_name
], ignore_index=True)

df.to_sql('a_stock_qmt_sector', con=engine, if_exists='replace', index=False)