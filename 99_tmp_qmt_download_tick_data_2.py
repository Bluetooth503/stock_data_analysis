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

def get_all_stocks():
    """获取所有股票代码"""
    # stocks = xtdata.get_stock_list_in_sector('沪深A股')
    stocks = xtdata.get_stock_list_in_sector('上证50')
    return stocks

def on_progress(data):
    # print(data)
    if data['finished'] % 500 == 0:
        logger.info(f"已完成:{data['finished']}/{data['total']} - {data['message']}")

stocks = get_all_stocks()
xtdata.download_history_data2(
    stock_list=stocks,
    period='tick',
    start_time='20250508',
    end_time='20250508',
    incrementally=True,
    callback=on_progress
)