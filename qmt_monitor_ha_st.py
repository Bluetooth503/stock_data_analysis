# -*- coding: utf-8 -*-
from contextlib import contextmanager
from common import *
from multiprocessing import Pool
import schedule
from xtquant import xtconstant
from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount
xtdata.enable_hello = False


class QMTTrader:
    """封装QMT交易相关功能"""
    def __init__(self, config):
        self.config = config
        self.engine = create_engine(get_pg_connection_string(config), pool_size=10, max_overflow=20)
        self.xt_trader = None
        self.acc = None
        
    def init_trader(self):
        """初始化QMT交易接口"""
        path = r'E:\国金证券QMT交易端\userdata_mini'
        session_id = int(time.time())
        self.xt_trader = XtQuantTrader(path, session_id)
        self.acc = StockAccount(self.config['trader']['account'])
        self.xt_trader.start()
        self.xt_trader.connect()
        time.sleep(3)
        account_status = self.xt_trader.query_account_status()
        logger.info(f"账户状态: {account_status}")
        return self.xt_trader, self.acc
    
    @contextmanager
    def db_session(self):
        """数据库连接上下文管理"""
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    @retry_on_failure(max_retries=3, delay=1)
    def get_positions(self):
        """查询持仓信息"""
        positions = self.xt_trader.query_stock_positions(self.acc)
        return [pos for pos in positions if pos.volume > 0] if positions else []

    @retry_on_failure(max_retries=3, delay=1)
    def place_order(self, code, signal_type, volume, price):
        """统一订单方法"""
        trade_type = xtconstant.STOCK_BUY if signal_type == 'BUY' else xtconstant.STOCK_SELL
        result = self.xt_trader.order_stock(
            self.acc, code, trade_type, volume,
            xtconstant.FIX_PRICE, price, 
            'strategy:ha_st', signal_type
        )
        return result, result is not None  # 返回订单编号和是否成功

    def _send_notification(self, subject, content):
        """统一发送通知"""
        send_notification_wecom(subject, content)
    
    def _log_order(self, code, signal_type, price, volume=0, success=True):
        """统一记录订单日志"""
        log_msg = f"{'买入' if signal_type == 'BUY' else '卖出'}委托{'成功' if success else '失败'} - "
        log_msg += f"股票: {code}, 价格: {price}"
        if volume > 0:
            log_msg += f", 数量: {volume}, 金额: {round(price * volume, 2)}元"
        logger.info(log_msg) if success else logger.error(log_msg)

# ================================= 初始化配置 =================================
config = load_config()
trader = QMTTrader(config)

# ================================= 计算指标 =================================
def ha_st(df, length, multiplier):
    try:
        ts_code = df['ts_code'].iloc[0] if 'ts_code' in df.columns else 'Unknown'
        df.ta.ha(append=True)
        ha_ohlc = {"HA_open": "ha_open", "HA_high": "ha_high", "HA_low": "ha_low", "HA_close": "ha_close"}
        df.rename(columns=ha_ohlc, inplace=True)
        supertrend_df = ta.supertrend(df['ha_high'], df['ha_low'], df['ha_close'], length, multiplier) 
        df['supertrend'] = supertrend_df[f'SUPERT_{length}_{multiplier}.0']
        df['direction'] = supertrend_df[f'SUPERTd_{length}_{multiplier}.0']
        df = df.round(3)
        return df
    except Exception as e:
        ts_code = df['ts_code'].iloc[0] if 'ts_code' in df.columns else 'Unknown'
        logger.error(f"股票{ts_code} ha_st计算错误: {str(e)}")
        return None
    
# ================================= 获取监控标的 =================================
def get_top_stocks(positions):
    #### 构建SQL查询 ####
    query = text("""
    SELECT DISTINCT ts_code, period, multiplier, sharpe, sortino, win_rate, profit_factor
    FROM heikin_ashi_supertrend_metrics
    WHERE ts_code NOT LIKE '688%' AND (profit_factor >= 1.3 AND win_rate >= 47 AND sharpe >= 1.5)
    AND ts_code NOT IN ('603392.SH','605358.SH','605011.SH','001270.SZ','601059.SH')
    ORDER BY ts_code DESC
    """)

    #### 使用封装后的数据库会话 ####
    with trader.db_session() as conn:
        df = pd.read_sql(query, conn)
    logger.info(f"监控标的数量: {len(df)}")
    return df

# ================================= 信号判断 =================================
def check_signal_change(df):
    if len(df) < 2:
        return None
    last_two = df.tail(2)
    if last_two['direction'].iloc[0] == -1 and last_two['direction'].iloc[1] == 1:
        return 'BUY'
    elif last_two['direction'].iloc[0] == 1 and last_two['direction'].iloc[1] == -1:
        return 'SELL'
    return None

# ================================= 添加新通知到数据库 =================================
def add_notification_record(trade_time, ts_code, signal_type, price):
    with trader.db_session() as conn:
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
    positions = trader.get_positions()
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
        
        # 根据信号类型进行交易
        if signal == "BUY":
            money_threshold = 10000  # 买入资金阈值，单位：元
            order_volume = max(100, int(money_threshold / current_price) // 100 * 100)
            seq, success = trader.place_order(code, "BUY", order_volume, current_price)
            if success:
                send_notification_wecom(subject, content)
                add_notification_record(trade_time, code, signal, current_price)
                logger.info(f"买入委托成功 - 股票: {code}, 价格: {current_price}, 数量: {order_volume}, 金额: {round(current_price * order_volume, 2)}元")
            else:
                logger.error(f"买入委托失败(重试3次) - 股票: {code}, 错误码: {seq}, 价格: {current_price}, 数量: {order_volume}")
        else:
            if code in positions_dict and positions_dict[code] > 0:
                tick = xtdata.get_full_tick([code])
                if code in tick and tick[code]:
                    sell_price = round((tick[code]["bidPrice"][0] if tick[code]["bidPrice"][0] != 0 else tick[code]["lastPrice"]) * 0.995, 2)
                    seq, success = trader.place_order(code, "SELL", positions_dict[code], sell_price)
                    if success:
                        send_notification_wecom(subject, content)
                        add_notification_record(trade_time, code, signal, current_price)
                        logger.info(f"卖出委托成功 - 股票: {code}, 数量: {positions_dict[code]}")
                    else:
                        logger.error(f"卖出委托失败(重试3次) - 股票: {code}, 错误码: {seq}")
                else:
                    logger.warning(f"无法获取股票{code}的tick数据")


if __name__ == "__main__":
    #### 初始化交易接口 ####
    trader.init_trader()
    
    #### 获取持仓并生成监控列表 ####
    positions = trader.get_positions()
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