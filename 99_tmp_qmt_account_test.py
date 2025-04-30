# -*- coding: utf-8 -*-
from common import *
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
