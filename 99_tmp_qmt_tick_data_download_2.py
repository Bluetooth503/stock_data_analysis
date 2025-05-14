# -*- coding: utf-8 -*-
from common import *
from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount
from xtquant import xtconstant

config = load_config()
path = config.get('qmt', 'path')
session_id = 123456
xt_trader = XtQuantTrader(path, session_id)
acc = StockAccount(config['trader']['account'])
xt_trader.start()
xt_trader.connect()
time.sleep(1)
account_status = xt_trader.query_account_status()
print(f"账户状态: {account_status}")

account_info = xt_trader.query_account_infos()
print(f"账户信息: {account_info}")


def on_progress(data):
    print(f"已完成:{data['finished']}/{data['total']} - {data['message']}")

start_date = '20240101'
end_date  = '20250513'
stocks = ['001309.SZ','600809.SH','300779.SZ','600839.SH','003006.SZ','605011.SH','003031.SZ','603290.SH']
xtdata.download_history_data2(stocks, period='5m', start_time='20240101', end_time='')


for stock in tqdm(stocks):
    xtdata.download_history_data(stock_code=stock,period='5m',start_time=start_date,end_time='')
    time.sleep(3)
    # tick_data = xtdata.get_market_data_ex(stock_list=[stock],period='tick',start_time=date,end_time=date)
    # if tick_data is not None and stock in tick_data:
    #     df = tick_data[stock]
    #     df['ts_code'] = stock
    #     if isinstance(df, pd.DataFrame) and not df.empty:
    #         # 生成文件名
    #         file_name = f'{date}_{stock}.parquet'
    #         df.to_parquet(file_name, index=False, compression='snappy')
    #         print(f"保存 {file_name} 成功")