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


# ================================= 读取配置文件 =================================
config = load_config()
engine = create_engine(get_pg_connection_string(config), pool_size=10, max_overflow=20)

# ================================= QMT初始化 =================================
path = r'E:\国金证券QMT交易端\userdata_mini'
session_id = int(time.time())
xt_trader = XtQuantTrader(path, session_id)
acc = StockAccount(config['trader']['account'])
xt_trader.start()
connect_result = xt_trader.connect()
subscribe_result = xt_trader.subscribe(acc)
positions = xt_trader.query_stock_positions(acc)

# ================================= 获取监控标的和持仓标的 =================================
def get_top_stocks(positions):
    # 获取监控标
    query = text("""
    SELECT distinct ts_code, period, multiplier, sharpe, sortino, win_rate, profit_factor
    FROM heikin_ashi_supertrend_metrics
    WHERE ts_code NOT LIKE '688%' AND ts_code NOT LIKE '30%' AND (sharpe >= 1.5 OR sortino >= 3)
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    # 获取持仓标的
    pos_dict = {pos.stock_code: pos.volume for pos in positions}
    position_codes = list(pos_dict.keys())
    
    if position_codes:
        position_query = text("""
        SELECT ts_code, period, multiplier, sharpe, sortino, win_rate, profit_factor
        FROM heikin_ashi_supertrend_metrics
        WHERE ts_code = ANY(:codes)
        """)
        with engine.connect() as conn:
            position_df = pd.read_sql(position_query, conn, params={"codes": position_codes})
        df = pd.concat([df, position_df[~position_df['ts_code'].isin(df['ts_code'])]], ignore_index=True)    
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
def add_notification_record(trade_time, ts_code, signal_type):
    with engine.connect() as conn:
        with conn.begin():
            query = """
            INSERT INTO signal_notifications (trade_time, ts_code, signal_type) 
            VALUES (:trade_time, :ts_code, :signal_type) 
            ON CONFLICT (trade_time, ts_code) DO NOTHING
            """
            conn.execute(text(query), {"trade_time": trade_time, "ts_code": ts_code, "signal_type": signal_type})

# ================================= 处理单个股票的数据 =================================
def process_stock_data(args):
    code, stock_data, stock_params, positions_dict = args

    #### 计算信号 ####
    stock_data['trade_time'] = pd.to_datetime(stock_data['time'].apply(lambda x: datetime.fromtimestamp(x / 1000.0)))
    stock_data = ha_st(stock_data, stock_params['period'], stock_params['multiplier'])
    stock_data = stock_data.round(3)
    stock_data['ts_code'] = code
    if '0930' <= datetime.now().strftime('%H%M') <= '1500':
        stock_data = stock_data.iloc[:-1]  # xtdata.subscribe_quote是秒级的,会产生未完结30mK线数据
    print(stock_data[['ts_code','trade_time','direction']].tail(3).to_string(index=False))

    #### 检查信号 ####
    signal = check_signal_change(stock_data)
    if signal:
        trade_time = stock_data['trade_time'].iloc[-1]
        current_price = stock_data['close'].iloc[-1]

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
                order_volume = 100  # 1手
                seq = xt_trader.order_stock(acc, code, xtconstant.STOCK_BUY, order_volume, xtconstant.FIX_PRICE, current_price, 'strategy:ha_st', '买入')
                if seq > 0:
                    print(f"买入委托成功 - 股票: {code}, 价格: {current_price}, 数量: {order_volume}")
                else:
                    print(f"买入委托失败 - 股票: {code}, 错误码: {seq}")
            else:  # SELL
                # 使用positions_dict查找持仓
                for pos in positions_dict:
                    if pos['stock_code'] == code and pos['volume'] > 0:
                        seq = xt_trader.order_stock(acc, code, xtconstant.STOCK_SELL, pos['volume'], xtconstant.LATEST_PRICE, -1, 'strategy:ha_st', '卖出')
                        if seq > 0:
                            print(f"卖出委托成功 - 股票: {code}, 数量: {pos['volume']}")
                        else:
                            print(f"卖出委托失败 - 股票: {code}, 错误码: {seq}")
                        break
                    else:
                        print(f"无持仓可卖出 - 股票: {code}")

            ## 发送通知 ##
            send_notification(subject, content)
            add_notification_record(trade_time, code, signal)
            return (code, signal, subject, content)

# ================================= 并行处理 =================================
def run_market_analysis():
    print("开始监控市场数据...")    

    #### 获取合成周期数据 ####
    df = xtdata.get_market_data_ex([], code_list, period='30m', start_time='20240101')
    
    #### 准备并行处理的参数 ####
    positions_dict = [{'stock_code': pos.stock_code,'volume': pos.volume} for pos in positions]
    process_args = []
    for code in code_list:
        if code in df:
            stock_params = top_stocks[top_stocks['ts_code'] == code].iloc[0].to_dict()
            process_args.append((code, df[code], stock_params, positions_dict))
    pool.map(process_stock_data, process_args)
    

if __name__ == "__main__":
    #### 获取股票列表 ####
    top_stocks = get_top_stocks(positions)
    code_list = top_stocks['ts_code'].tolist()
    
    #### 下载基础周期数据 ####
    for code in code_list:
        xtdata.download_history_data(code, period='5m', incrementally=True)
        xtdata.subscribe_quote(code, '5m')

    #### 创建进程池 ####
    pool = Pool(processes=10)

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

    pool.close()
    pool.join()
    xtdata.run()
