# -*- coding: utf-8 -*-
from common import *
from multiprocessing import Pool
from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount
from xtquant import xtconstant
xtdata.enable_hello = False

# ================================= 配置日志 =================================
logger = setup_logger()

# ================================= Heikin-Ashi SuperTrend 计算 =================================
def ha_st_pine(df, length, multiplier):
    '''Heikin Ashi计算:direction=1上涨,-1下跌.'''
    df = df.copy()
    # 使用向量化操作计算HA价格
    ha_close = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    # 使用向量化操作计算HA开盘价
    ha_open = pd.Series(index=df.index, dtype=float)
    ha_open.iloc[0] = (df['open'].iloc[0] + df['close'].iloc[0]) / 2
    # 使用向量化操作计算HA高低价
    ha_high = pd.Series(index=df.index, dtype=float)
    ha_low = pd.Series(index=df.index, dtype=float)
    # 使用cumsum和shift进行向量化计算
    for i in range(1, len(df)):
        ha_open.iloc[i] = (ha_open.iloc[i-1] + ha_close.iloc[i-1]) / 2
    # 向量化计算HA高低价
    ha_high = df[['high']].join(pd.DataFrame({
        'ha_open': ha_open,
        'ha_close': ha_close
    })).max(axis=1)
    ha_low = df[['low']].join(pd.DataFrame({
        'ha_open': ha_open,
        'ha_close': ha_close
    })).min(axis=1)
    # 使用向量化操作计算TR
    tr1 = ha_high - ha_low
    tr2 = (ha_high - ha_close.shift(1)).abs()
    tr3 = (ha_low - ha_close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    # 使用向量化操作计算RMA
    rma = pd.Series(index=df.index, dtype=float)
    alpha = 1.0 / length
    # 初始化RMA
    rma.iloc[length-1] = tr.iloc[:length].mean()
    # 使用向量化操作计算RMA
    for i in range(length, len(df)):
        rma.iloc[i] = alpha * tr.iloc[i] + (1 - alpha) * rma.iloc[i-1]
    # SuperTrend计算
    src = (ha_high + ha_low) / 2
    upper_band = pd.Series(index=df.index, dtype=float)
    lower_band = pd.Series(index=df.index, dtype=float)
    super_trend = pd.Series(index=df.index, dtype=float)
    direction = pd.Series(0, index=df.index)
    # 初始化第一个有效值
    start_idx = length - 1
    upper_band.iloc[start_idx] = src.iloc[start_idx] + multiplier * rma.iloc[start_idx]
    lower_band.iloc[start_idx] = src.iloc[start_idx] - multiplier * rma.iloc[start_idx]
    super_trend.iloc[start_idx] = upper_band.iloc[start_idx]
    direction.iloc[start_idx] = 1
    # 使用向量化操作计算SuperTrend
    for i in range(start_idx+1, len(df)):
        current_upper = src.iloc[i] + multiplier * rma.iloc[i]
        current_lower = src.iloc[i] - multiplier * rma.iloc[i]
        lower_band.iloc[i] = current_lower if (current_lower > lower_band.iloc[i-1] or ha_close.iloc[i-1] < lower_band.iloc[i-1]) else lower_band.iloc[i-1]
        upper_band.iloc[i] = current_upper if (current_upper < upper_band.iloc[i-1] or ha_close.iloc[i-1] > upper_band.iloc[i-1]) else upper_band.iloc[i-1]
        if i == start_idx or pd.isna(rma.iloc[i-1]):
            direction.iloc[i] = -1
        elif super_trend.iloc[i-1] == upper_band.iloc[i-1]:
            direction.iloc[i] = 1 if ha_close.iloc[i] > upper_band.iloc[i] else -1
        else:
            direction.iloc[i] = -1 if ha_close.iloc[i] < lower_band.iloc[i] else 1
        super_trend.iloc[i] = lower_band.iloc[i] if direction.iloc[i] == 1 else upper_band.iloc[i]
    # 将计算结果添加到DataFrame中
    df['ha_open'] = ha_open
    df['ha_high'] = ha_high
    df['ha_low'] = ha_low
    df['ha_close'] = ha_close
    df['supertrend'] = super_trend
    df['direction'] = direction
    return df

# ================================= Heikin-Ashi SuperTrend 信号判断 =================================
def check_signal_change(df):
    """Heikin-Ashi SuperTrend 信号判断"""
    if len(df) < 2:
        return None
    last_two = df.tail(2)
    if last_two['direction'].iloc[0] == -1 and last_two['direction'].iloc[1] == 1:
        return 'BUY'
    elif last_two['direction'].iloc[0] == 1 and last_two['direction'].iloc[1] == -1:
        return 'SELL'
    return None

# ================================= 配置管理 =================================
class Config:
    """配置管理类"""
    def __init__(self):
        # 如果没有传入配置，则使用全局配置
        self.config = load_config()
        self.trading_config = {
            'qmt_path': self.config.get('qmt', 'path'),
            'buy_threshold': 20000,  # 买入资金阈值
            'buy_price_ratio': 1.005,  # 买入价格比例
            'sell_price_ratio': 0.995,  # 卖出价格比例
            'min_volume': 100,  # 最小交易数量
            'retry_times': 3,  # 重试次数
            'retry_delay': 1,  # 重试延迟(秒)
        }

# ================================= 交易类 =================================
class QMTTrader:
    """QMT交易类"""
    def __init__(self):
        self.config = Config()
        self.xt_trader = None
        self.acc = None
        self.subscribed_codes = {}

    def _log_order(self, code, signal_type, price, volume=0, success=True):
        """记录订单日志"""
        action = '买入' if signal_type == 'BUY' else '卖出'
        status = '成功' if success else '失败'
        # 构建日志信息
        log_msg = f"{action}委托{status} - 股票: {code} - 价格: {price}"
        # 添加数量和金额信息
        if volume > 0:
            log_msg += f" - 数量: {volume} - 金额: {round(price * volume, 2)}元"
        # 根据成功状态选择日志级别
        (logger.info if success else logger.error)(log_msg)

    def init_trader(self):
        """初始化QMT交易接口"""
        path = self.config.trading_config['qmt_path']
        session_id = int(time.time())
        self.xt_trader = XtQuantTrader(path, session_id)
        self.acc = StockAccount(self.config.config['trader']['account'])
        self.xt_trader.start()
        self.xt_trader.connect()
        time.sleep(3)
        account_status = self.xt_trader.query_account_status()
        logger.info(f"账户状态: {account_status}")
        return self.xt_trader, self.acc

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
        return result, result is not None

    @retry_on_failure(max_retries=3, delay=2)
    def get_stock_tick(self, code):
        """获取股票tick数据"""
        tick_data = xtdata.get_full_tick([code])
        if isinstance(tick_data, dict) and code in tick_data:
            return tick_data
        return None

    def subscribe_stocks(self, code_list):
        """订阅股票行情"""
        for code in code_list:
            # 订阅K线数据
            kline_seq = xtdata.subscribe_quote(code, '30m')
            if kline_seq > 0:  # 订阅成功
                self.subscribed_codes[f"{code}_kline"] = kline_seq
                logger.info(f"订阅 {code} K线数据成功，订阅号: {kline_seq}")
            else:
                logger.error(f"订阅 {code} K线数据失败")
        return True

    def unsubscribe_stocks(self):
        """使用订阅号取消所有股票订阅"""
        for code_type, seq in list(self.subscribed_codes.items()):
            xtdata.unsubscribe_quote(seq)
            logger.info(f"取消订阅 {code_type}(订阅号:{seq}) 成功")
            del self.subscribed_codes[code_type]
        # 以防万一，清空订阅字典
        self.subscribed_codes.clear()
        return True

    def process_signal(self, code, signal, trade_time, current_price, stock_params, positions_dict):
        """处理交易信号"""
        # 1. 获取最新行情
        tick = self.get_stock_tick(code)
        if not (tick and code in tick and tick[code]):
            error_msg = f"无法获取股票{code}的tick数据"
            logger.error(error_msg)
            send_notification_wecom("获取Tick数据失败", error_msg)
            return
        tick_data = tick[code]
        last_price = tick_data["lastPrice"]  # 最新价
        bid_price = tick_data["bidPrice"][0]  # 买一价
        ask_price = tick_data["askPrice"][0]  # 卖一价

        # 2. 准备通知内容
        subject = f"{stock_params['name']} - {signal} - {current_price}"
        content = f"""
        信号类型: {signal}
        股票信息: {stock_params['name']}({code})({stock_params['circ_mv_range']})
        信号时间: {trade_time}
        当前价格: {current_price}
        最新价: {last_price}
        买一价: {bid_price}
        卖一价: {ask_price}
        sharpe: {stock_params['sharpe']}
        胜率: {stock_params['win_rate']}
        盈亏比: {stock_params['profit_factor']}
        """

        # 3. 检查持仓状态
        has_position = code in positions_dict and positions_dict[code] > 0

        # 4. 处理买卖信号
        if signal == "BUY":
            # 如果已持有该股票，跳过买入
            if has_position:
                logger.info(f"跳过买入信号 - 已持有股票{code}")
                return
            # 买入价格计算逻辑：优先使用卖一价，如果卖一价为0则使用最新价*买入价格比例
            if ask_price > 0:
                price = round(ask_price, 2)  # 直接使用卖一价
            else:
                price = round(last_price * self.config.trading_config['buy_price_ratio'], 2)  # 使用最新价乘以比例
            # 确保买入数量为100的整数倍
            volume = max(
                self.config.trading_config['min_volume'],
                int(self.config.trading_config['buy_threshold'] / price) // 100 * 100
            )
        else:  # SELL
            # 如果没有持仓，跳过卖出
            if not has_position:
                return
            # 卖出价格计算逻辑：优先使用买一价，如果买一价为0则使用最新价*卖出价格比例
            if bid_price > 0:
                price = round(bid_price, 2)  # 直接使用买一价
            else:
                price = round(last_price * self.config.trading_config['sell_price_ratio'], 2)  # 使用最新价乘以比例
            # 卖出全部持仓
            volume = positions_dict[code]

        # 5. 执行交易并发送通知
        seq, success = self.place_order(code, signal, volume, price)
        if success:
            content += f"\n交易详情:\n委托价格: {price}\n委托数量: {volume}\n交易金额: {round(price * volume, 2)}"
            send_notification_wecom(subject, content)
        self._log_order(code, signal, price, volume, success)

# ================================= 市场分析 =================================
def get_top_stocks():
    """从CSV文件获取监控标的"""
    try:
        # 读取CSV文件
        df = pd.read_csv('qmt_monitor_stocks_calmar.csv', encoding='utf-8')
        # 确保必要的列存在
        required_columns = ['ts_code', 'period', 'multiplier', 'sharpe', 'sortino', 'win_rate', 'profit_factor', 'name', 'circ_mv_range']
        if not all(col in df.columns for col in required_columns):
            raise ValueError("CSV文件缺少必要的列")
        # 按股票代码降序排序
        df = df.sort_values('ts_code', ascending=False)
        logger.info(f"监控标的数量: {len(df)}")
        return df
    except FileNotFoundError:
        logger.error("错误: 未找到qmt_monitor_stocks_calmar.csv文件")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"读取监控标的时发生错误: {str(e)}")
        return pd.DataFrame()

def calculate_signals(args):
    """计算交易信号"""
    code, stock_data, stock_params = args
    current_time = datetime.now().strftime('%H%M')
    stock_data['ts_code'] = code
    stock_data['trade_time'] = pd.to_datetime(stock_data['time'].apply(lambda x: datetime.fromtimestamp(x / 1000.0)))
    # 开盘和收盘前特殊时段使用未完成K线
    if current_time in ['0935', '1455']:
        stock_data = ha_st_pine(stock_data, stock_params['period'], stock_params['multiplier'])
    else:
        stock_data = ha_st_pine(stock_data, stock_params['period'], stock_params['multiplier'])
        # 其他时间去掉最后一根未完成的K线
        stock_data = stock_data.iloc[:-1]
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

def run_market_analysis():
    """运行市场分析"""
    logger.info(f"开始监控市场数据... 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    # 获取当前持仓信息
    positions = trader.get_positions()
    positions_dict = {pos.stock_code: pos.volume for pos in positions}
    logger.info(f"当前持仓数量: {len(positions)}, 持仓股票: {list(positions_dict.keys())}")
    # 获取合成周期数据
    df = xtdata.get_market_data_ex([], code_list, period='30m', start_time='20240101', end_time='')
    # 准备并行计算参数
    calc_args = []
    # 为每个股票获取计算参数
    for code in code_list:
        if code in df:
            stock_params = top_stocks[top_stocks['ts_code'] == code].iloc[0].to_dict()
            stock_data = df[code].copy()
            calc_args.append((code, stock_data, stock_params))
    # 并行计算信号
    with Pool(processes=30) as pool:
        signal_results = pool.map(calculate_signals, calc_args)
    # 过滤出有效信号
    valid_signals = [r for r in signal_results if r is not None]
    # 处理交易信号
    for result in valid_signals:
        trader.process_signal(
            result['code'],
            result['signal'],
            result['trade_time'],
            result['current_price'],
            result['params'],
            positions_dict  # 传递持仓字典
        )

def check_positions():
    """检查持仓状态，对下跌趋势未卖出的股票发出提醒"""
    # 获取当前持仓
    positions = trader.get_positions()
    if not positions:
        return
    # 获取持仓股票的最新数据
    position_codes = [pos.stock_code for pos in positions if pos.volume > 0]
    df = xtdata.get_market_data_ex([], position_codes, period='30m', start_time='20240101', end_time='')
    # 检查每个持仓股票的状态
    warning_stocks = []
    for pos in positions:
        code = pos.stock_code
        if code not in df:
            continue
        stock_params = top_stocks[top_stocks['ts_code'] == code].iloc[0].to_dict()
        stock_data = df[code].copy()
        # stock_data.to_csv(f"{code}_debug.csv")
        stock_data['trade_time'] = pd.to_datetime(stock_data['time'].apply(lambda x: datetime.fromtimestamp(x / 1000.0)))
        stock_data = ha_st_pine(stock_data, stock_params['period'], stock_params['multiplier'])
        # 如果最新方向为下跌
        if stock_data['direction'].iloc[-1] == -1:
            warning_stocks.append(f"{stock_params['name']}({code})")

    # 只在发现问题时发送通知
    if warning_stocks:
        subject = "持仓股票异常提醒"
        content = f"股票处于下跌趋势但是没有卖出：{warning_stocks}"
        logger.info(content)
        send_notification_wecom(subject, content)

def setup_schedule():
    """设置定时任务"""
    trading_hours = [
        (9, 35),  # 开盘特殊时段
        (10, 0), (10, 30), (11, 0), (11, 30),  # 上午交易时段
        (13, 0), (13, 30), (14, 0), (14, 30),  # 下午交易时段
        (14, 55)  # 收盘前特殊时段
    ]

    for hour, minute in trading_hours:
        # 设置市场分析任务
        schedule_time = f"{hour:02d}:{minute:02d}:10"
        schedule.every().day.at(schedule_time).do(run_market_analysis)

        schedule_time_sell_check = f"{hour:02d}:{minute:02d}:55"
        schedule.every().day.at(schedule_time_sell_check).do(check_positions)

    # 额外的15分和45分持仓检查
    for hour in range(9, 15):
        for minute in [15, 45]:
            if (hour == 9 and minute < 35) or (11 < hour < 13) or (hour == 14 and minute > 45):
                continue
            schedule_time = f"{hour:02d}:{minute:02d}:55"
            schedule.every().day.at(schedule_time).do(check_positions)

# ================================= 主程序 =================================
if __name__ == "__main__":
    trader = None
    try:
        # 校验交易日
        current_date = datetime.now().strftime('%Y%m%d')
        trade_dates = get_trade_dates()
        
        if current_date not in trade_dates:
            self.logger.info(f"当前日期 {current_date} 非交易日，跳过执行")
            sys.exit(0)

        # 设置停止时间
        stop_time = datetime.strptime('16:00:00', '%H:%M:%S').time()

        # 初始化交易接口
        trader = QMTTrader()
        trader.init_trader()
        # 获取持仓并生成监控列表
        positions = trader.get_positions()
        top_stocks = get_top_stocks()
        code_list = top_stocks['ts_code'].tolist()
        # 每次执行时下载基础周期数据
        xtdata.download_history_data2(code_list, period='30m', start_time='20240101', end_time='', incrementally=True)
        # 订阅行情数据
        trader.subscribe_stocks(code_list)
        # 设置定时任务
        setup_schedule()
        # 运行定时任务
        while True:
            try:
                # 检查是否到达停止时间
                current_time = datetime.now().time()
                if current_time >= stop_time:
                    logger.info("到达停止时间，程序退出")
                    if trader:
                        trader.unsubscribe_stocks()
                    sys.exit(0)

                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                error_msg = f"程序运行出错: {str(e)}"
                logger.error(error_msg)
                send_notification_wecom("程序运行错误", error_msg)
                time.sleep(5)
                continue
    except KeyboardInterrupt:
        logger.info("\n检测到退出信号，正在清理...")
        if trader:
            trader.unsubscribe_stocks()
        logger.info("程序已安全退出")
    except Exception as e:
        error_msg = f"程序初始化失败: {str(e)}"
        logger.error(error_msg)
        send_notification_wecom("程序初始化错误", error_msg)
        if trader:
            trader.unsubscribe_stocks()
        raise
