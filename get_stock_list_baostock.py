# -*- coding: utf-8 -*-
from common import *
import baostock as bs

# 登陆系统
lg = bs.login()

# 获取上证50成分股
rs = bs.query_sz50_stocks()
sz50_stocks = []
while (rs.error_code == '0') & rs.next():
    sz50_stocks.append(rs.get_row_data())
result = pd.DataFrame(sz50_stocks, columns=rs.fields)
result['code'] = result['code'].apply(convert_to_tushare_code)
result['code'].to_csv("上证50_stock_list.csv",encoding='utf-8',index=False,header=False)

# 获取沪深300成分股
rs = bs.query_hs300_stocks()
hs300_stocks = []
while (rs.error_code == '0') & rs.next():
    hs300_stocks.append(rs.get_row_data())
result = pd.DataFrame(hs300_stocks, columns=rs.fields)
result['code'] = result['code'].apply(convert_to_tushare_code)
result['code'].to_csv("沪深300_stock_list.csv",encoding='utf-8',index=False,header=False)

# 获取中证500成分股
rs = bs.query_zz500_stocks()
zz500_stocks = []
while (rs.error_code == '0') & rs.next():
    zz500_stocks.append(rs.get_row_data())
result = pd.DataFrame(zz500_stocks, columns=rs.fields)
result['code'] = result['code'].apply(convert_to_tushare_code)
result['code'].to_csv("中证500_stock_list.csv",encoding='utf-8',index=False,header=False)


# 登出系统
bs.logout()