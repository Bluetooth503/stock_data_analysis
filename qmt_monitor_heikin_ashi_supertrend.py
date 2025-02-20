# -*- coding: utf-8 -*-
from common import *
from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount
from multiprocessing import Pool
from sqlalchemy import text
import schedule
from xtquant import xtconstant
xtdata.enable_hello = False


# ================================= QMT初始化 =================================
def init_trader():
    path = r'E:\国金证券QMT交易端\userdata_mini'
    session_id = int(time.time())
    xt_trader = XtQuantTrader(path, session_id)
    acc = StockAccount(config['trader']['account'])
    xt_trader.start()
    xt_trader.connect()
    time.sleep(3)
    account_status = xt_trader.query_account_status()
    print(f"账户状态: {account_status}")
    return xt_trader, acc

# ================================= 读取配置文件 =================================
config = load_config()
engine = create_engine(get_pg_connection_string(config), pool_size=10, max_overflow=20)

# ================================= 获取监控标的和持仓标的 =================================
def get_top_stocks(positions):
    #### 获取持仓标的代码 ####
    pos_dict = {pos.stock_code: pos.volume for pos in positions}
    position_codes = list(pos_dict.keys())
    
    #### 构建SQL查询 ####
    query = text("""
    SELECT DISTINCT ts_code, period, multiplier, sharpe, sortino, win_rate, profit_factor
    FROM (
        SELECT ts_code, period, multiplier, sharpe, sortino, win_rate, profit_factor
        FROM heikin_ashi_supertrend_metrics
        WHERE ts_code NOT LIKE '688%' AND ts_code NOT LIKE '30%' 
        AND (profit_factor >= 1.3 AND win_rate >= 47 AND sharpe >= 1.43)
        UNION
        SELECT ts_code, period, multiplier, sharpe, sortino, win_rate, profit_factor
        FROM heikin_ashi_supertrend_metrics
        WHERE ts_code = ANY(:position_codes)
    ) combined_data
    """)

    #### 执行SQL查询 ####
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"position_codes": position_codes if position_codes else [None]})
    print(f"监控标的数量: {len(df)}")
    return df

# ================================= 获取监控标的和持仓标的 =================================
def check_signal_change(df):
    if len(df) < 2:
        return None
    last_two = df.tail(2)
    if last_two['direction'].iloc[0] == -1 and last_two['direction'].iloc[1] == 1:
        return 'BUY'
    elif last_two['direction'].iloc[0] == 1 and last_two['direction'].iloc[1] == -1:
        return 'SELL'
    return None

# ================================= 检查是否已经发送过通知 =================================
def check_if_notified(trade_time, ts_code):
    with engine.connect() as conn:
        query = "SELECT EXISTS (SELECT 1 FROM signal_notifications WHERE trade_time = :trade_time AND ts_code = :ts_code)"
        result = conn.execute(text(query), {"trade_time": trade_time, "ts_code": ts_code}).scalar()
        return bool(result)

# ================================= 添加新通知到数据库 =================================
def add_notification_record(trade_time, ts_code, signal_type, price):
    with engine.connect() as conn:
        with conn.begin():
            query = """
            INSERT INTO signal_notifications (trade_time, ts_code, signal_type, price) 
            VALUES (:trade_time, :ts_code, :signal_type, :price) 
            ON CONFLICT (trade_time, ts_code) DO NOTHING
            """
            conn.execute(text(query), {"trade_time": trade_time, "ts_code": ts_code, "signal_type": signal_type, "price": price})

# ================================= 并行计算指标函数 =================================
def calculate_signals(args):
    code, stock_data, stock_params = args
    
    #### 计算信号 ####
    stock_data['ts_code'] = code
    stock_data['trade_time'] = pd.to_datetime(stock_data['time'].apply(lambda x: datetime.fromtimestamp(x / 1000.0)))
    stock_data = ha_st(stock_data, stock_params['period'], stock_params['multiplier'])
    if '0930' <= datetime.now().strftime('%H%M') <= '1500':
        stock_data = stock_data.iloc[:-1]
    
    #### 检查信号 ####
    signal = check_signal_change(stock_data)
    if signal:
        return {
            'code': code,
            'signal': signal,
            'trade_time': stock_data['trade_time'].iloc[-1],
            'current_price': stock_data['close'].iloc[-1],
            'stock_data': stock_data.tail(3),
            'params': stock_params
        }
    return None

# ================================= 修改运行函数 =================================
def run_market_analysis():
    print(f"开始监控市场数据... 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    #### 获取合成周期数据 ####
    df = xtdata.get_market_data_ex([], code_list, period='30m', start_time='20240101')
    
    #### 准备并行计算参数 ####
    calc_args = []
    for code in code_list:
        if code in df:
            stock_params = top_stocks[top_stocks['ts_code'] == code].iloc[0].to_dict()
            calc_args.append((code, df[code], stock_params))
    
    #### 并行计算信号 ####
    with Pool(processes=10) as pool:
        signal_results = pool.map(calculate_signals, calc_args)
    
    #### 过滤出有效信号 ####
    valid_signals = [r for r in signal_results if r is not None]
    
    #### 获取最新持仓 ####
    positions = xt_trader.query_stock_positions(acc)
    positions_dict = [{'stock_code': pos.stock_code,'volume': pos.volume} for pos in positions]
    
    #### 串行处理交易操作 ####
    for result in valid_signals:
        code = result['code']
        signal = result['signal']
        trade_time = result['trade_time']
        current_price = result['current_price']
        stock_params = result['params']
                
        ### 提前定义发送消息的subject和content ###
        subject = f"{code} - {signal} - {current_price}"
        content = f"""
        信号类型: {signal}
        股票代码: {code}
        信号时间: {trade_time}
        当前价格: {current_price}
        sharpe: {stock_params['sharpe']}
        胜率: {stock_params['win_rate']}
        盈亏比: {stock_params['profit_factor']}
        """
        
        ### 检查是否已经通知过该组合 ###
        if not check_if_notified(trade_time, code):
            if signal == "BUY":
                order_volume = 100
                seq = xt_trader.order_stock(acc, code, xtconstant.STOCK_BUY, order_volume, xtconstant.FIX_PRICE, current_price, 'strategy:ha_st', '买入')
                if seq > 0:
                    send_notification(subject, content)
                    add_notification_record(trade_time, code, signal, current_price)
                    print(f"买入委托成功 - 股票: {code}, 价格: {current_price}, 数量: {order_volume}")
                else:
                    print(f"买入委托失败 - 股票: {code}, 错误码: {seq}, 价格: {current_price}, 数量: {order_volume}")
            else:
                for pos in positions_dict:
                    if pos['stock_code'] == code and pos['volume'] > 0:
                        seq = xt_trader.order_stock(acc, code, xtconstant.STOCK_SELL, pos['volume'], xtconstant.LATEST_PRICE, -1, 'strategy:ha_st', '卖出')
                        if seq > 0:
                            send_notification(subject, content)
                            add_notification_record(trade_time, code, signal, current_price)
                            print(f"卖出委托成功 - 股票: {code}, 数量: {pos['volume']}")
                        else:
                            print(f"卖出委托失败 - 股票: {code}, 错误码: {seq}")
                        break

if __name__ == "__main__":
    #### 在主进程中初始化交易接口 ####
    xt_trader, acc = init_trader()
    
    #### 获取股票列表 ####
    positions = xt_trader.query_stock_positions(acc)
    top_stocks = get_top_stocks(positions)
    code_list = top_stocks['ts_code'].tolist()
    
    #### 下载基础周期数据 ####
    for code in code_list:
        xtdata.download_history_data(code, period='5m', incrementally=True)
        xtdata.subscribe_quote(code, '5m')
        print(f"订阅{code}的5分钟数据成功")

    #### 设置定时任务 ####
    for hour in range(9, 15):   # 9点到15点
        for minute in [0, 30]:  # 每个小时的00分和30分
            schedule_time = f"{hour:02d}:{minute:02d}:01"
            schedule.every().day.at(schedule_time).do(run_market_analysis)
    
    #### 运行定时任务 ####
    while True:
        now = datetime.now()
        now_time = now.strftime('%H%M%S')
        if '090000' <= now_time <= '150500':
            schedule.run_pending()        
        time.sleep(1)

    xtdata.run()
