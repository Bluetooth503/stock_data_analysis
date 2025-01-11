import pandas as pd
from xtquant import xtdata
xtdata.enable_hello = False

sectors = xtdata.get_sector_list()
series = pd.Series(sectors)
series.to_csv('市场板块信息.csv',encoding='utf-8',index=False,header=False)