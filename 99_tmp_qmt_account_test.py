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

code = '601865.SH'
def get_stock_tick(code):
    """获取股票tick数据"""
    tick_data = xtdata.get_full_tick([code])
    if isinstance(tick_data, dict) and code in tick_data:
        return tick_data
    return None


tick = get_stock_tick(code)
tick_data = tick[code]
last_price = tick_data["lastPrice"]  # 最新价
bid_price = tick_data["bidPrice"][0]  # 买一价
ask_price = tick_data["askPrice"][0]  # 卖一价
print(f"最新价: {last_price}")