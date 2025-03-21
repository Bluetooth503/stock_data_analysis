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
    logger.info(f"账户状态: {account_status}")
    return xt_trader, acc

# ================================= 读取配置文件 =================================
config = load_config()
engine = create_engine(get_pg_connection_string(config), pool_size=10, max_overflow=20)

# ================================= 获取监控标的和持仓标的 =================================
def get_top_stocks(positions):
    #### 构建SQL查询 ####
    query = text("""
    SELECT DISTINCT ts_code, period, multiplier, sharpe, sortino, win_rate, profit_factor
    FROM heikin_ashi_supertrend_metrics
    WHERE ts_code NOT LIKE '688%' AND (profit_factor >= 1.3 AND win_rate >= 47 AND sharpe >= 1.5)
    AND ts_code NOT IN ('603392.SH','605358.SH','605011.SH','001270.SZ','601059.SH')
    ORDER BY ts_code DESC
    """)

    #### 执行SQL查询 ####
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    logger.info(f"监控标的数量: {len(df)}")
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

# # ================================= 检查是否已经发送过通知 =================================
# def check_if_notified(trade_time, ts_code):
#     with engine.connect() as conn:
#         query = "SELECT EXISTS (SELECT 1 FROM signal_notifications WHERE trade_time = :trade_time AND ts_code = :ts_code)"
#         result = conn.execute(text(query), {"trade_time": trade_time, "ts_code": ts_code}).scalar()
#         return bool(result)

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

# ================================= 重试装饰器 =================================
def retry_on_failure(max_retries=3, delay=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    if isinstance(result, tuple):
                        seq, success = result
                        if success:
                            return seq, True
                    else:
                        if result > 0:  # 对于直接返回seq的情况
                            return result, True
                    if attempt < max_retries - 1:
                        logger.warning(f"尝试执行{func.__name__}失败，{attempt + 1}/{max_retries}次，等待{delay}秒后重试...")
                        time.sleep(delay)
                    else:
                        logger.error(f"尝试执行{func.__name__}失败，已达到最大重试次数{max_retries}次")
                        return -1, False
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"执行{func.__name__}出错: {str(e)}，{attempt + 1}/{max_retries}次，等待{delay}秒后重试...")
                        time.sleep(delay)
                    else:
                        logger.error(f"执行{func.__name__}出错: {str(e)}，已达到最大重试次数{max_retries}次")
                        return -1, False
            return -1, False
        return wrapper
    return decorator

# ================================= 交易函数 =================================
@retry_on_failure(max_retries=3, delay=1)
def place_buy_order(acc, code, volume, price):
    seq = xt_trader.order_stock(acc, code, xtconstant.STOCK_BUY, volume, xtconstant.FIX_PRICE, price, 'strategy:ha_st', '买入')
    return seq

@retry_on_failure(max_retries=3, delay=1)
def place_sell_order(acc, code, volume, price):
    seq = xt_trader.order_stock(acc, code, xtconstant.STOCK_SELL, volume, xtconstant.FIX_PRICE, price, 'strategy:ha_st', '卖出')
    return seq

# ================================= 修改运行函数 =================================
def run_market_analysis():
    logger.info(f"开始监控市场数据... 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    #### 获取合成周期数据 ####
    df = xtdata.get_market_data_ex([], code_list, period='30m', start_time='20240101')
    
    #### 准备并行计算参数 ####
    calc_args = []
    for code in code_list:
        if code in df:
            stock_params = top_stocks[top_stocks['ts_code'] == code].iloc[0].to_dict()
            calc_args.append((code, df[code], stock_params))
    
    #### 并行计算信号 ####
    with Pool(processes=30) as pool:
        signal_results = pool.map(calculate_signals, calc_args)
    
    #### 过滤出有效信号 ####
    valid_signals = [r for r in signal_results if r is not None]
    
    #### 获取最新持仓 ####
    positions = xt_trader.query_stock_positions(acc)
    positions_dict = {pos.stock_code: pos.volume for pos in positions}
    
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
        
        
        if signal == "BUY":
            money_threshold = 10000  # 买入资金阈值，单位：元
            order_volume = max(100, int(money_threshold / current_price) // 100 * 100)
            seq, success = place_buy_order(acc, code, order_volume, current_price)
            if success:
                send_notification(subject, content)
                add_notification_record(trade_time, code, signal, current_price)
                logger.info(f"买入委托成功 - 股票: {code}, 价格: {current_price}, 数量: {order_volume}, 金额: {round(current_price * order_volume, 2)}元")
            else:
                logger.error(f"买入委托失败(重试3次) - 股票: {code}, 错误码: {seq}, 价格: {current_price}, 数量: {order_volume}")
        else:
            if code in positions_dict and positions_dict[code] > 0:
                try:
                    tick = xtdata.get_full_tick([code])
                    if code in tick and tick[code]:
                        sell_price = round((tick[code]["bidPrice"][0] if tick[code]["bidPrice"][0] != 0 else tick[code]["lastPrice"]) * 0.995, 2)
                        seq, success = place_sell_order(acc, code, positions_dict[code], sell_price)
                        if success:
                            send_notification(subject, content)
                            add_notification_record(trade_time, code, signal, current_price)
                            logger.info(f"卖出委托成功 - 股票: {code}, 数量: {positions_dict[code]}")
                        else:
                            logger.error(f"卖出委托失败(重试3次) - 股票: {code}, 错误码: {seq}")
                    else:
                        logger.warning(f"无法获取股票{code}的tick数据")
                except Exception as e:
                    logger.error(f"处理股票{code}的tick数据时出错: {str(e)}")


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
        logger.info(f"订阅{code}的5分钟数据成功")

    #### 设置定时任务 ####
    for hour in range(9, 16):   # 9点到15点
        for minute in [0, 30]:  # 每个小时的00分和30分
            schedule_time = f"{hour:02d}:{minute:02d}:05"
            schedule.every().day.at(schedule_time).do(run_market_analysis)
    
    #### 运行定时任务 ####
    while True:
        now = datetime.now()
        now_time = now.strftime('%H%M%S')
        if '090000' <= now_time <= '150500':
            schedule.run_pending()        
        time.sleep(1)

    xtdata.run()