# -*- coding: utf-8 -*-
import os
import time
from datetime import datetime
import schedule
import configparser
import pandas as pd
import requests
from multiprocessing import Pool
from xtquant import xtdata
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount
from xtquant import xtconstant
from functools import wraps
xtdata.enable_hello = False


# ================================= 配置管理 =================================
class Config:
    """配置管理类"""
    def __init__(self):
        self.config = self._load_config()
        self.trading_config = self._get_trading_config()
        
    def _load_config(self):
        """加载配置文件"""
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'config.ini'), encoding='utf-8')
        return config
    
    def _get_trading_config(self):
        """获取交易相关配置"""
        return {
            'qmt_path': r'C:\国金证券QMT交易端\userdata_mini',
            'buy_threshold': 10000,  # 买入资金阈值
            'sell_price_ratio': 0.995,  # 卖出价格比例
            'min_volume': 100,  # 最小交易数量
            'retry_times': 3,  # 重试次数
            'retry_delay': 1,  # 重试延迟(秒)
        }
    
    def get_wecom_webhook(self):
        """获取企业微信webhook"""
        return self.config.get('wecom', 'webhook')
    
    def get_account(self):
        """获取交易账户"""
        return self.config['trader']['account']

# ================================= 工具函数 =================================
def retry_on_failure(max_retries=3, delay=1):
    """重试装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    # 处理tick数据返回结果
                    if isinstance(result, dict) and args[0][0] in result and result[args[0][0]]:
                        return result
                    # 处理其他返回结果
                    if isinstance(result, tuple):
                        seq, success = result
                        if success:
                            return seq, True
                    else:
                        if isinstance(result, list):
                            return result
                        if result > 0:
                            return result, True
                    if attempt < max_retries - 1:
                        print(f"尝试执行{func.__name__}失败，{attempt + 1}/{max_retries}次，等待{delay}秒后重试...")
                        time.sleep(delay)
                    else:
                        print(f"尝试执行{func.__name__}失败，已达到最大重试次数{max_retries}次")
                        return -1, False
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"执行{func.__name__}出错: {str(e)}，{attempt + 1}/{max_retries}次，等待{delay}秒后重试...")
                        time.sleep(delay)
                    else:
                        print(f"执行{func.__name__}出错: {str(e)}，已达到最大重试次数{max_retries}次")
                        return -1, False
            return -1, False
        return wrapper
    return decorator

def send_notification_wecom(subject, content):
    """使用企业微信发送通知"""
    try:
        webhook = config.get_wecom_webhook()
        response = requests.post(webhook, json={
            "msgtype": "markdown",
            "markdown": {"content": f"### {subject}\n{content}"}
        })
        
        if not response.ok:
            print(f"通知发送失败 HTTP:{response.status_code}")
            return False
            
        result = response.json()
        if result.get('errcode') != 0:
            print(f"API错误: {result.get('errmsg')}")
            
        return result.get('errcode') == 0
        
    except Exception as e:
        print(f"通知异常: {str(e)}")
        return False

def log_order(code, signal_type, price, volume=0, success=True):
    """记录订单日志"""
    log_msg = f"{'买入' if signal_type == 'BUY' else '卖出'}委托{'成功' if success else '失败'} - "
    log_msg += f"股票: {code}, 价格: {price}"
    if volume > 0:
        log_msg += f", 数量: {volume}, 金额: {round(price * volume, 2)}元"
    print(log_msg)

# ================================= 指标计算 =================================
def ha_st_pine(df, length, multiplier):
    # ========== Heikin Ashi计算 ==========
    '''direction=1上涨，-1下跌'''
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
    
    # ========== ATR计算 ==========
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
    
    # ========== SuperTrend计算 ==========
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

def check_signal_change(df):
    if len(df) < 2:
        return None
    last_two = df.tail(2)
    if last_two['direction'].iloc[0] == -1 and last_two['direction'].iloc[1] == 1:
        return 'BUY'
    elif last_two['direction'].iloc[0] == 1 and last_two['direction'].iloc[1] == -1:
        return 'SELL'
    return None

# ================================= 交易类 =================================
class QMTTrader:
    """QMT交易类"""
    def __init__(self, config):
        self.config = config
        self.xt_trader = None
        self.acc = None
        self.trading_config = config.trading_config
        self.processed_klines_file = "processed_klines.txt"
        self.subscribed_codes = {}  # 改为字典，存储 code: seq 的映射
        
    def _load_processed_klines(self):
        """从文件加载已处理的K线记录"""
        try:
            if os.path.exists(self.processed_klines_file):
                with open(self.processed_klines_file, "r") as f:
                    return set(line.strip() for line in f)
            return set()
        except Exception as e:
            print(f"加载已处理K线记录出错: {str(e)}")
            return set()
            
    def _save_processed_kline(self, kline_key):
        """保存处理过的K线记录到文件"""
        try:
            with open(self.processed_klines_file, "a") as f:
                f.write(f"{kline_key}\n")
        except Exception as e:
            print(f"保存K线记录出错: {str(e)}")
            
    def init_trader(self):
        """初始化QMT交易接口"""
        path = self.trading_config['qmt_path']
        session_id = int(time.time())
        self.xt_trader = XtQuantTrader(path, session_id)
        self.acc = StockAccount(self.config.get_account())
        self.xt_trader.start()
        self.xt_trader.connect()
        time.sleep(3)
        account_status = self.xt_trader.query_account_status()
        print(f"账户状态: {account_status}")
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

    @retry_on_failure(max_retries=3, delay=1)
    def get_stock_tick(self, code):
        """获取股票tick数据"""
        tick_data = xtdata.get_full_tick([code])
        if isinstance(tick_data, dict) and code in tick_data:
            return tick_data
        return None

    def subscribe_stocks(self, code_list):
        """订阅股票行情"""
        try:
            for code in code_list:
                # 订阅并保存返回的订阅号
                seq = xtdata.subscribe_quote(code, '5m')
                if seq > 0:  # 订阅成功
                    self.subscribed_codes[code] = seq
                    print(f"订阅 {code} 5分钟数据成功，订阅号: {seq}")
                else:
                    print(f"订阅 {code} 失败")
            return True
        except Exception as e:
            print(f"订阅股票行情失败: {e}")
            return False

    def unsubscribe_stocks(self):
        """取消所有股票订阅"""
        try:
            # 逐个取消订阅
            for code, seq in list(self.subscribed_codes.items()):
                try:
                    # 使用订阅号取消订阅
                    xtdata.unsubscribe_quote(seq)
                    print(f"取消订阅 {code}(订阅号:{seq}) 成功")
                    del self.subscribed_codes[code]
                except Exception as e:
                    print(f"取消订阅 {code}(订阅号:{seq}) 失败: {e}")
            
            # 以防万一，清空订阅字典
            self.subscribed_codes.clear()
            return True
        except Exception as e:
            print(f"取消订阅过程中出错: {e}")
            return False

    def process_signal(self, code, signal, trade_time, current_price, stock_params):
        """处理交易信号"""
        # 生成K线唯一标识
        kline_key = f"{code}_{trade_time.strftime('%Y%m%d%H%M')}"
        
        # 检查是否已经处理过这根K线
        processed_klines = self._load_processed_klines()
        if kline_key in processed_klines:
            print(f"跳过重复K线信号 - 股票:{code}, 信号:{signal}, K线时间:{trade_time}")
            return
            
        # 记录新处理的K线
        self._save_processed_kline(kline_key)
        
        subject = f"{stock_params['name']} - {signal} - {current_price}"
        content = f"""
        信号类型: {signal}
        股票信息: {stock_params['name']}({code})({stock_params['circ_mv_range']})
        信号时间: {trade_time}
        当前价格: {current_price}
        sharpe: {stock_params['sharpe']}
        胜率: {stock_params['win_rate']}
        盈亏比: {stock_params['profit_factor']}
        """
        
        if signal == "BUY":
            money_threshold = self.trading_config['buy_threshold']
            order_volume = max(self.trading_config['min_volume'], int(money_threshold / current_price) // 100 * 100)
            seq, success = self.place_order(code, "BUY", order_volume, current_price)
            if success:
                send_notification_wecom(subject, content)
                log_order(code, "BUY", current_price, order_volume, True)
            else:
                log_order(code, "BUY", current_price, order_volume, False)
        else:
            positions = self.get_positions()
            positions_dict = {pos.stock_code: pos.volume for pos in positions}
            if code in positions_dict and positions_dict[code] > 0:
                tick = self.get_stock_tick(code)
                if tick and code in tick and tick[code]:
                    sell_price = round((tick[code]["bidPrice"][0] if tick[code]["bidPrice"][0] != 0 
                                      else tick[code]["lastPrice"]) * self.trading_config['sell_price_ratio'], 2)
                    seq, success = self.place_order(code, "SELL", positions_dict[code], sell_price)
                    if success:
                        send_notification_wecom(subject, content)
                        log_order(code, "SELL", sell_price, positions_dict[code], True)
                    else:
                        log_order(code, "SELL", sell_price, positions_dict[code], False)
                else:
                    error_msg = f"无法获取股票{code}的tick数据"
                    print(error_msg)
                    send_notification_wecom("获取Tick数据失败", error_msg)

# ================================= 市场分析 =================================
def get_top_stocks():
    """从CSV文件获取监控标的"""
    try:
        # 读取CSV文件
        df = pd.read_csv('monitor_stocks.csv', encoding='utf-8')
        
        # 确保必要的列存在
        required_columns = ['ts_code', 'period', 'multiplier', 'sharpe', 'sortino', 'win_rate', 'profit_factor', 'name', 'circ_mv_range']
        if not all(col in df.columns for col in required_columns):
            raise ValueError("CSV文件缺少必要的列")
        
        # 按股票代码降序排序
        df = df.sort_values('ts_code', ascending=False)
        
        print(f"监控标的数量: {len(df)}")
        return df
        
    except FileNotFoundError:
        print("错误: 未找到monitor_stocks.csv文件")
        return pd.DataFrame()
    except Exception as e:
        print(f"读取监控标的时发生错误: {str(e)}")
        return pd.DataFrame()

def calculate_signals(args):
    """计算交易信号"""
    code, stock_data, stock_params = args
    
    stock_data['ts_code'] = code
    stock_data['trade_time'] = pd.to_datetime(stock_data['time'].apply(lambda x: datetime.fromtimestamp(x / 1000.0)))
    stock_data = ha_st_pine(stock_data, stock_params['period'], stock_params['multiplier'])
    if '0930' <= datetime.now().strftime('%H%M') <= '1500':
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
    # 每天第一次运行时清空记录
    if datetime.now().strftime('%H%M') == '0900':
        trader.clear_processed_klines()
    
    print(f"\n开始监控市场数据... 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 获取合成周期数据
    df = xtdata.get_market_data_ex([], code_list, period='30m', start_time='20240101')
    
    # 准备并行计算参数
    calc_args = []
    buy_status_stocks = []
    sell_status_stocks = []
    
    # 计算每个股票的状态并打印
    print("\n当前股票状态:")
    for code in code_list:
        if code in df:
            stock_params = top_stocks[top_stocks['ts_code'] == code].iloc[0].to_dict()
            stock_data = df[code].copy()
            # 计算指标
            stock_data = ha_st_pine(stock_data, stock_params['period'], stock_params['multiplier'])
            # 获取最新状态并记录
            if stock_data['direction'].iloc[-1] == 1:
                buy_status_stocks.append(f"{stock_params['name']}({code})")
            else:
                sell_status_stocks.append(f"{stock_params['name']}({code})")
            
            calc_args.append((code, stock_data, stock_params))
    
    print(f"需要持仓的股票({len(buy_status_stocks)}只)：{', '.join(buy_status_stocks)}")
    # print(f"卖出状态的股票({len(sell_status_stocks)}只)：{', '.join(sell_status_stocks)}")
    print("-" * 80)
    
    # 并行计算信号
    with Pool(processes=2) as pool:
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
            result['params']
        )

# ================================= 主程序 =================================
if __name__ == "__main__":
    trader = None
    try:
        # 初始化配置
        config = Config()
        
        # 初始化交易接口
        trader = QMTTrader(config)
        trader.init_trader()
        
        # 获取持仓并生成监控列表
        positions = trader.get_positions()
        top_stocks = get_top_stocks()
        code_list = top_stocks['ts_code'].tolist()
            
        # 订阅行情数据
        trader.subscribe_stocks(code_list)

        # 设置定时任务
        for hour in range(9, 16):   # 09:30 ~ 11:30
            for minute in [0, 30]:
                schedule_time = f"{hour:02d}:{minute:02d}:05"
                schedule.every().day.at(schedule_time).do(run_market_analysis)

        # 运行定时任务
        while True:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                error_msg = f"程序运行出错: {str(e)}"
                print(error_msg)
                send_notification_wecom("程序运行错误", error_msg)
                time.sleep(5)
                continue

    except KeyboardInterrupt:
        print("\n检测到退出信号，正在清理...")
        if trader:
            trader.unsubscribe_stocks()
        print("程序已安全退出")
    except Exception as e:
        error_msg = f"程序初始化失败: {str(e)}"
        print(error_msg)
        send_notification_wecom("程序初始化错误", error_msg)
        if trader:
            trader.unsubscribe_stocks()
        raise
