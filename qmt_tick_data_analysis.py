from collections import defaultdict, deque
import time
import numpy as np
import pandas as pd
from datetime import datetime
import heapq
from typing import Dict, List, Tuple
from xtquant import xtdata
import swifter
import os
import traceback

xtdata.enable_hello = False


# 添加常量配置
FACTOR_WEIGHTS = {
    'pct_change': 0.3,
    'volume_ratio': 0.25, 
    'bid_ask_power': 0.2,
    'new_high_freq': 0.15,
    'transaction_growth': 0.1
}

BUFFER_SIZE = 1000  # 缓冲区大小
DELAY_THRESHOLD = 4  # 延迟处理阈值（秒）
MISSING_DATA_THRESHOLD = 6  # 数据缺失判定阈值（秒）

class TickBuffer:
    """用于处理tick数据的缓冲区，确保时间顺序"""
    def __init__(self, buffer_size: int = 1000):
        self.buffer = []  # 优先队列
        self.buffer_size = buffer_size
        self.last_processed_time = 0
    
    def add_tick(self, timestamp: int, stock_code: str, tick_data: dict):
        """添加tick数据到缓冲区"""
        # 确保时间戳是整数
        if isinstance(timestamp, int) and timestamp > 1000000000000:  # 如果是毫秒时间戳
            timestamp = timestamp // 1000  # 转换为秒
            
        # 使用元组作为堆的元素，第一个元素是时间戳用于排序
        entry = (timestamp, len(self.buffer), stock_code, tick_data)  # 添加计数器作为第二排序键
        heapq.heappush(self.buffer, entry)
        
        # 如果缓冲区过大，处理最早的数据
        while len(self.buffer) > self.buffer_size:
            self.process_earliest_tick()
    
    def process_earliest_tick(self) -> Tuple[int, str, dict]:
        """处理最早的tick数据"""
        if not self.buffer:
            return None
        timestamp, _, stock_code, tick_data = heapq.heappop(self.buffer)
        self.last_processed_time = timestamp
        return timestamp, stock_code, tick_data
    
    def get_ready_ticks(self, current_time: int, delay_threshold: int = 2) -> List[Tuple[int, str, dict]]:
        """获取已经可以安全处理的tick数据"""
        ready_ticks = []
        while self.buffer and self.buffer[0][0] <= current_time - delay_threshold:
            tick = self.process_earliest_tick()
            if tick:  # 确保返回的数据不是None
                ready_ticks.append(tick)
        return ready_ticks

class StockScorer:
    # 行情数据相关的常量
    TICK_INTERVAL = 3  # level1行情数据的正常间隔时间（秒）
    MISSING_THRESHOLD = 15  # 数据缺失判定阈值（秒，考虑到不活跃股票的情况）
    QUALITY_CHECK_INTERVAL = 60  # 数据质量检查间隔（秒）
    
    def __init__(self):
        # 修改 tick_buffer 的初始化方式
        self.tick_buffer = TickBuffer(BUFFER_SIZE)  # 使用 TickBuffer 类而不是 defaultdict
        
        # 使用 numpy 数组替代 defaultdict 存储历史数据
        self.price_history = {}  # 改为普通dict
        self.volume_history = {}
        
        # 初始化时预分配数组
        def init_arrays(stock_code):
            self.price_history[stock_code] = np.zeros(30, dtype=np.float32)  # 使用float32减少内存
            self.volume_history[stock_code] = np.zeros(30, dtype=np.float32)
        
        # 存储1分钟K线数据
        self.min1_data = defaultdict(list)
        # 存储股票得分
        self.stock_scores = {}
        # 上次更新得分的时间
        self.last_score_time = 0
        
        # 新增属性
        self.last_close = {}  # 存储昨收价
        self.historical_volume = self._load_historical_volume()  # 加载历史成交量数据
        self.historical_high = defaultdict(float)  # 存储历史最高价
        self.new_high_count = defaultdict(int)  # 统计创新高次数
        
        # 性能优化：使用numpy数组存储历史数据
        self.current_idx = defaultdict(int)  # 循环数组的当前索引
        
        # 新增属性
        self.last_update_time = defaultdict(int)  # 记录每个股票最后更新时间
        self.missing_data_count = defaultdict(int)  # 记录数据缺失次数
        self.subscribed_codes = set()  # 记录已订阅的股票代码
        self.subscription_seqs = {}    # 新增：记录订阅号 {stock_code: seq}
        
    def _load_historical_volume(self) -> Dict[str, Dict[str, float]]:
        """加载历史5日成交量均值数据"""
        historical_data = defaultdict(dict)
        try:
            # 读取历史数据文件
            df = pd.read_parquet('qmt_historical_volume_5min.parquet')
            for _, row in df.iterrows():
                stock_code = row['stock_code']
                time_key = row['time_key']
                avg_volume = float(row['avg_volume'])
                historical_data[stock_code][time_key] = avg_volume
            print(f"成功加载历史成交量数据,共{len(historical_data)}只股票")
            return historical_data
        except Exception as e:
            print(f"加载历史成交量数据失败: {e}")
            return historical_data

    def _get_time_key(self, timestamp: int) -> str:
        """根据时间戳获取5分钟时间键"""
        dt = datetime.fromtimestamp(timestamp)
        # 向下取整到最近的5分钟
        minute = (dt.minute // 5) * 5
        return f"{dt.hour:02d}{minute:02d}00"

    def process_tick(self, datas: Dict[str, dict]):
        """添加更robust的错误处理"""
        try:
            current_time = int(time.time())
            
            # 数据验证
            if not isinstance(datas, dict):
                raise ValueError("Invalid data format")
                
            # 批量处理前的数据验证
            valid_ticks = {}
            for code, tick in datas.items():
                if self._validate_tick_data(tick):
                    # 确保时间戳是秒级的
                    if tick['time'] > 1000000000000:  # 如果是毫秒时间戳
                        tick['time'] = tick['time'] // 1000  # 转换为秒
                    valid_ticks[code] = tick
            
            # 处理有效数据
            for stock_code, tick in valid_ticks.items():
                self._process_single_tick(current_time, stock_code, tick)
            
            # 检查数据完整性
            self._check_data_integrity(current_time)
            
            # 每5分钟计算一次得分
            if self._should_calculate_score(current_time):
                self._calculate_scores()
                
        except Exception as e:
            print(f"Error processing tick data: {e}")
            traceback.print_exc()
        
        return True
        
    def _validate_tick_data(self, tick: dict) -> bool:
        """验证tick数据的完整性"""
        required_fields = {'time', 'lastPrice', 'volume', 'askPrice', 'bidPrice'}
        return all(field in tick for field in required_fields)

    def _process_single_tick(self, timestamp: int, stock_code: str, tick: dict):
        """处理单个tick数据"""
        try:
            # 更新最后处理时间
            self.last_update_time[stock_code] = timestamp
            
            # 保存昨收价
            if 'lastClose' in tick:
                self.last_close[stock_code] = tick['lastClose']
            
            # 处理5档行情数据
            ask_prices = tick['askPrice']
            bid_prices = tick['bidPrice']
            ask_vols = tick['askVol']
            bid_vols = tick['bidVol']
            
            # 计算有效的委托量（排除0值）
            total_ask_vol = sum(vol for vol in ask_vols if vol > 0)
            total_bid_vol = sum(vol for vol in bid_vols if vol > 0)
            
            # 将tick数据加入缓存
            self.tick_buffer.add_tick(timestamp, stock_code, {
                'time': timestamp,
                'price': tick['lastPrice'],
                'volume': tick['volume'],
                'pvolume': tick.get('pvolume', 0),
                'amount': tick.get('amount', 0),
                'bid_vol': total_bid_vol,
                'ask_vol': total_ask_vol,
                'weighted_ask_price': self._calculate_weighted_price(ask_prices, ask_vols),
                'weighted_bid_price': self._calculate_weighted_price(bid_prices, bid_vols),
                'bid_ask_spread': ask_prices[0] - bid_prices[0] if ask_prices[0] > 0 and bid_prices[0] > 0 else 0,
                'transaction_num': tick.get('transactionNum', 0),
                'open': tick.get('open', tick['lastPrice']),
                'high': tick.get('high', tick['lastPrice']),
                'low': tick.get('low', tick['lastPrice']),
                'last_close': tick.get('lastClose', tick['lastPrice'])
            })
            
            # 检查是否创新高
            if tick['lastPrice'] > self.historical_high[stock_code]:
                self.historical_high[stock_code] = tick['lastPrice']
                self.new_high_count[stock_code] += 1
            
            # 每分钟聚合一次
            if self._should_aggregate_1min(timestamp):
                self._aggregate_1min_data(stock_code)
            
        except Exception as e:
            print(f"处理 tick 数据出错 ({stock_code}): {e}")
            traceback.print_exc()

    def _should_aggregate_1min(self, current_time):
        """判断是否应该进行1分钟聚合"""
        dt = datetime.fromtimestamp(current_time)
        # 放宽条件，允许在每分钟的前55秒内触发
        return dt.second < 55

    def _should_calculate_score(self, current_time):
        """判断是否应该计算得分"""
        # 获取当前时间
        current_dt = datetime.fromtimestamp(current_time)
        current_hour = current_dt.hour
        current_minute = current_dt.minute
        current_second = current_dt.second
        
        # 更精确的交易时间判断
        is_trading_time = (
            (current_hour == 9 and current_minute >= 30) or  # 9:30-10:00
            (current_hour > 9 and current_hour < 11) or      # 10:00-11:00
            (current_hour == 11 and current_minute <= 30) or # 11:00-11:30
            (current_hour >= 13 and current_hour < 15)       # 13:00-15:00
        )
        
        if not is_trading_time:
            return False
        
        # 每5分钟计算一次
        if current_minute % 5 == 0:
            # 放宽条件，允许在每分钟的前15秒内触发
            if current_second < 15:
                # 确保同一个5分钟内不会重复计算
                if current_time - self.last_score_time >= 240:  # 至少间隔4分钟
                    self.last_score_time = current_time
                    return True
        return False
    
    def _aggregate_1min_data(self, stock_code: str):
        """优化的1分钟K线聚合"""
        current_time = int(time.time())
        dt = datetime.fromtimestamp(current_time)
        
        # 获取上一分钟的时间范围
        start_time = current_time - 60
        end_time = current_time
        
        # 获取这个时间范围内的所有tick数据
        ticks = self.tick_buffer.get_ready_ticks(current_time)
        if not ticks:
            return
            
        # 过滤出当前股票的 ticks，并且时间在上一分钟内
        stock_ticks = [t[2] for t in ticks if t[1] == stock_code and start_time <= t[0] < end_time]
        if not stock_ticks:
            return
            
        # 使用numpy进行快速计算
        prices = np.array([t['price'] for t in stock_ticks])
        volumes = np.array([t['volume'] for t in stock_ticks])
        pvolumes = np.array([t['pvolume'] for t in stock_ticks])  # 原始成交量
        amounts = np.array([t['amount'] for t in stock_ticks])
        bid_vols = np.array([t['bid_vol'] for t in stock_ticks])
        ask_vols = np.array([t['ask_vol'] for t in stock_ticks])
        transaction_nums = np.array([t['transaction_num'] for t in stock_ticks])
        
        min1_bar = {
            'time': current_time,  # 添加time字段
            'open': stock_ticks[0]['open'],  # 使用第一个tick的开盘价
            'high': np.max([t['high'] for t in stock_ticks]),  # 使用period内的最高价
            'low': np.min([t['low'] for t in stock_ticks]),    # 使用period内的最低价
            'close': prices[-1],
            'volume': np.sum(volumes),
            'pvolume': np.sum(pvolumes),  # 原始成交量合计
            'amount': np.sum(amounts),
            'bid_vol': np.sum(bid_vols),
            'ask_vol': np.sum(ask_vols),
            'transaction_num': np.sum(transaction_nums),  # 成交笔数合计
            'tick_count': len(stock_ticks),
            'missing_data': self.missing_data_count[stock_code],
            'last_close': stock_ticks[0]['last_close']  # 保存前收盘价
        }
        
        # 只保留最近5分钟的数据
        self.min1_data[stock_code].append(min1_bar)
        self.min1_data[stock_code] = self.min1_data[stock_code][-5:]  # 只保留最近5根K线
        self.missing_data_count[stock_code] = 0  # 重置缺失计数

    def _calculate_scores(self):
        """使用swifter加速向量化操作"""
        try:
            current_time = datetime.now().strftime('%H:%M:%S')
            
            # 创建一个包含所有股票数据的大DataFrame
            all_data = []
            for stock_code, data in self.min1_data.items():
                if len(data) > 0:  # 只要有数据就计算
                    df = pd.DataFrame(data)
                    df['stock_code'] = stock_code
                    all_data.append(df)  # 使用所有数据
            
            if not all_data:
                print(f"\n=== 5分钟分析结果 ({current_time}) ===")
                print("暂无足够数据计算得分")
                return
                
            # 合并所有股票数据
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 按股票分组进行计算
            grouped = combined_df.groupby('stock_code')
            
            # 1. 批量计算涨跌幅得分
            latest_prices = grouped['close'].last()
            pct_changes = pd.Series({
                code: (price - self.last_close.get(code, price)) / self.last_close.get(code, price) * 100
                for code, price in latest_prices.items()
            })
            pct_scores = pct_changes * FACTOR_WEIGHTS['pct_change']
            
            # 2. 批量计算成交量比率得分
            volume_sums = grouped['volume'].sum()
            volume_means = grouped['volume'].mean()
            
            # 获取历史成交量均值
            historical_volume_ratios = pd.Series(index=grouped.groups.keys(), dtype=float)
            for code in grouped.groups.keys():
                time_key = self._get_time_key(int(time.time()))
                hist_volume = self.historical_volume.get(code, {}).get(time_key, volume_means[code])
                if hist_volume > 0:
                    historical_volume_ratios[code] = volume_sums[code] / hist_volume
                else:
                    historical_volume_ratios[code] = volume_sums[code] / volume_means[code]
            
            volume_scores = np.minimum(historical_volume_ratios, 3) * FACTOR_WEIGHTS['volume_ratio']
            
            # 3. 批量计算买卖力量对比得分
            bid_sums = grouped['bid_vol'].sum()
            ask_sums = grouped['ask_vol'].sum()
            power_ratios = bid_sums / (bid_sums + ask_sums)
            power_ratios = power_ratios.fillna(0.5)
            power_scores = power_ratios * FACTOR_WEIGHTS['bid_ask_power']
            
            # 4. 批量计算创新高得分
            new_high_scores = pd.Series({
                code: min(self.new_high_count[code] / 5, 1) * FACTOR_WEIGHTS['new_high_freq']
                for code in grouped.groups.keys()
            })
            
            # 5. 计算交易活跃度得分
            transaction_scores = pd.Series(index=grouped.groups.keys(), dtype=float)
            for code, group in grouped:
                transactions = group['transaction_num'].values
                if len(transactions) > 0 and np.any(transactions > 0):  # 只要有非零交易就计算
                    # 计算当前5分钟的平均成交笔数
                    current_avg_trans = np.mean(transactions)
                    
                    # 获取历史同期的平均成交笔数
                    time_key = self._get_time_key(int(time.time()))
                    hist_trans = self.historical_volume.get(code, {}).get(f"{time_key}_trans", current_avg_trans)
                    
                    # 计算交易活跃度比率
                    if hist_trans > 0:
                        trans_ratio = current_avg_trans / hist_trans
                        # 计算变化趋势
                        changes = np.diff(transactions)
                        valid_indices = transactions[:-1] > 0
                        if np.any(valid_indices):
                            changes = changes[valid_indices] / transactions[:-1][valid_indices]
                            avg_change = np.mean(changes)
                            direction = 1 if transactions[-1] > transactions[0] else -1
                            # 综合考虑历史比较和变化趋势
                            transaction_scores[code] = min(abs(avg_change) * direction * trans_ratio, 1) * FACTOR_WEIGHTS['transaction_growth']
                        else:
                            transaction_scores[code] = min(trans_ratio, 1) * FACTOR_WEIGHTS['transaction_growth']
                    else:
                        # 如果没有历史数据,使用当前平均值的相对变化
                        changes = np.diff(transactions)
                        valid_indices = transactions[:-1] > 0
                        if np.any(valid_indices):
                            changes = changes[valid_indices] / transactions[:-1][valid_indices]
                            avg_change = np.mean(changes)
                            direction = 1 if transactions[-1] > transactions[0] else -1
                            transaction_scores[code] = min(abs(avg_change) * direction, 1) * FACTOR_WEIGHTS['transaction_growth']
                        else:
                            transaction_scores[code] = 0
                else:
                    transaction_scores[code] = 0
            
            # 计算总分
            total_scores = pct_scores + volume_scores + power_scores + new_high_scores + transaction_scores
            
            # 更新股票得分
            self.stock_scores = total_scores.to_dict()
            
            # 输出5分钟分析结果
            print(f"\n=== 5分钟分析结果 ({current_time}) ===")
            print("股票代码  涨跌幅  成交量比  买卖力量  创新高  交易活跃  总分")
            print("-" * 50)
            sorted_scores = sorted(self.stock_scores.items(), key=lambda x: x[1], reverse=True)
            for code, score in sorted_scores[:10]:  # 显示前10只股票
                if pd.isna(score):
                    continue
                pct = pct_changes.get(code, 0)
                vol = volume_scores.get(code, 0) / FACTOR_WEIGHTS['volume_ratio']
                power = power_scores.get(code, 0) / FACTOR_WEIGHTS['bid_ask_power']
                high = new_high_scores.get(code, 0) / FACTOR_WEIGHTS['new_high_freq']
                trans = transaction_scores.get(code, 0) / FACTOR_WEIGHTS['transaction_growth']
                print(f"{code:<8} {pct:>6.2f}% {vol:>8.2f} {power:>8.2f} {high:>6.2f} {trans:>8.2f} {score:>6.2f}")
            print("-" * 50)
                
        except Exception as e:
            print(f"计算得分出错: {e}")
            traceback.print_exc()

    def _check_data_integrity(self, current_time: int):
        """检查数据完整性"""
        for stock_code in self.last_update_time.keys():
            time_gap = current_time - self.last_update_time[stock_code]
            
            # 如果超过正常间隔的3倍没有收到数据，记录缺失
            if time_gap > self.MISSING_THRESHOLD:
                # 计算实际缺失的次数（减去正常的一次间隔）
                missing_count = (time_gap - self.TICK_INTERVAL) // self.TICK_INTERVAL
                self.missing_data_count[stock_code] += missing_count
                
                # 填充缺失数据
                self._fill_missing_data(stock_code, current_time)
            else:
                # 如果时间间隔正常，减少缺失计数
                self.missing_data_count[stock_code] = max(0, self.missing_data_count[stock_code] - 1)

    def _fill_missing_data(self, stock_code: str, current_time: int):
        """填充缺失数据"""
        if not self.min1_data[stock_code]:
            return
            
        # 使用最后一个有效数据填充
        last_valid_data = self.min1_data[stock_code][-1]
        filled_data = {
            'time': current_time,
            'lastPrice': last_valid_data['close'],
            'volume': 0,  # 缺失期间成交量设为0
            'amount': 0,
            'bid_vol': last_valid_data['bid_vol'],
            'ask_vol': last_valid_data['ask_vol'],
            'askPrice': [last_valid_data['close'], 0, 0, 0, 0],
            'bidPrice': [last_valid_data['close'], 0, 0, 0, 0],
            'askVol': [0, 0, 0, 0, 0],
            'bidVol': [0, 0, 0, 0, 0],
            'open': last_valid_data['close'],
            'high': last_valid_data['close'],
            'low': last_valid_data['close'],
            'lastClose': last_valid_data['last_close'],
            'pvolume': 0,
            'transactionNum': 0
        }
        
        self._process_single_tick(current_time, stock_code, filled_data)

    def monitor_data_quality(self):
        """监控数据质量"""
        current_time = int(time.time())
        
        # 获取上一分钟的时间范围
        start_time = current_time - 60
        end_time = current_time
        
        # 获取这个时间范围内的所有tick数据
        ticks = self.tick_buffer.get_ready_ticks(current_time)
        if not ticks:
            return
            
        # 按股票代码分组统计
        stock_ticks = {}
        for t in ticks:
            if start_time <= t[0] < end_time:
                if t[1] not in stock_ticks:
                    stock_ticks[t[1]] = []
                stock_ticks[t[1]].append(t[2])
        
        # 创建字典的副本进行遍历
        min1_data_copy = dict(self.min1_data)
        for stock_code in min1_data_copy:
            # 计算每分钟应该收到的数据次数
            expected_ticks = self.QUALITY_CHECK_INTERVAL // self.TICK_INTERVAL
            
            # 获取实际收到的数据次数
            actual_ticks = len(stock_ticks.get(stock_code, []))
            
            # 计算实际缺失率
            missing_rate = max(0, (expected_ticks - actual_ticks) / expected_ticks)
            
            # 检查是否长时间未更新
            last_update = self.last_update_time.get(stock_code, 0)
            time_since_last_update = current_time - last_update
            if time_since_last_update > self.MISSING_THRESHOLD:
                pass  # 移除警告日志

    def cleanup_stale_data(self):
        """定期清理过期数据"""
        current_time = int(time.time())
        stale_threshold = current_time - 300  # 5分钟
        
        # 创建字典的副本进行遍历
        last_update_time_copy = dict(self.last_update_time)
        
        # 批量清理过期数据
        stale_codes = [code for code, last_time in last_update_time_copy.items() 
                      if last_time < stale_threshold]
        
        for code in stale_codes:
            # 一次性清理所有相关数据
            self.price_history.pop(code, None)
            self.volume_history.pop(code, None)
            self.min1_data.pop(code, None)
            self.last_update_time.pop(code, None)
            self.missing_data_count.pop(code, None)

    def _calculate_weighted_price(self, prices: List[float], volumes: List[float]) -> float:
        """计算加权平均价格"""
        total_volume = sum(vol for vol, price in zip(volumes, prices) if vol > 0 and price > 0)
        if total_volume == 0:
            return 0
        
        weighted_sum = sum(price * vol for price, vol in zip(prices, volumes) if vol > 0 and price > 0)
        return weighted_sum / total_volume

    def subscribe_stocks(self, code_list: List[str]):
        """订阅股票行情"""
        try:
            # 确保股票代码格式正确
            valid_codes = []
            for code in code_list:
                # 检查股票代码格式
                if '.' not in code:
                    print(f"警告: 股票代码 {code} 格式可能不正确")
                valid_codes.append(code)
            
            if not valid_codes:
                print("没有有效的股票代码可订阅")
                return None
            
            print(f"准备订阅股票: {valid_codes}")
            
            # 订阅行情，获取订阅号
            seq = xtdata.subscribe_whole_quote(valid_codes, callback=self.on_tick_data)
            
            # 更新已订阅的股票列表和订阅号
            for code in valid_codes:
                self.subscribed_codes.add(code)
                self.subscription_seqs[code] = seq
                # 初始化该股票的数据结构
                if code not in self.price_history:
                    self.price_history[code] = np.zeros(30, dtype=np.float32)
                if code not in self.volume_history:
                    self.volume_history[code] = np.zeros(30, dtype=np.float32)
                
            print(f"成功订阅 {len(valid_codes)} 只股票的行情，订阅号: {seq}")
            return seq
        except Exception as e:
            print(f"订阅行情失败: {e}")
            traceback.print_exc()
            return None
            
    def on_tick_data(self, datas: Dict[str, dict]):
        """处理tick数据回调"""
        try:
            # 如果收到的是单个股票代码
            if isinstance(datas, str):
                return
            
            # 如果收到的是字典格式
            if isinstance(datas, dict):
                # 继续处理数据
                if not self.process_tick(datas):
                    print(f"处理 tick 数据失败，数据: {datas}")
            else:
                print(f"未知的数据格式: {type(datas)}")
            
        except Exception as e:
            print(f"处理tick数据失败: {e}")
            traceback.print_exc()

    def unsubscribe_stocks(self, code_list: List[str] = None):
        """取消订阅股票行情"""
        if code_list is None:
            code_list = list(self.subscribed_codes)
            
        try:
            # 获取需要取消订阅的seq列表
            seqs_to_unsubscribe = set()
            for code in code_list:
                if code in self.subscription_seqs:
                    seqs_to_unsubscribe.add(self.subscription_seqs[code])
                    
            # 取消订阅
            for seq in seqs_to_unsubscribe:
                xtdata.unsubscribe_quote(seq)
                
            # 更新已订阅的股票列表和订阅号
            for code in code_list:
                self.subscribed_codes.discard(code)
                self.subscription_seqs.pop(code, None)
                
            print(f"成功取消订阅 {len(code_list)} 只股票的行情")
        except Exception as e:
            print(f"取消订阅行情失败: {e}")

    def run(self):
        """维持运行状态，持续处理订阅数据"""
        try:
            print("开始运行，等待接收行情数据...")
            last_heartbeat = time.time()
            
            while True:
                current_time = time.time()
                
                # 每30秒打印一次心跳
                if current_time - last_heartbeat >= 30:
                    print(f"程序正在运行... {datetime.now().strftime('%H:%M:%S')}")
                    last_heartbeat = current_time
                    
                    # 检查连接状态
                    if not xtdata.connect():
                        print("警告：与行情服务器的连接已断开，尝试重新连接...")
                        continue
                
                # 每隔一段时间检查数据质量和清理过期数据
                self.monitor_data_quality()
                self.cleanup_stale_data()
                time.sleep(1)  # 缩短检查间隔为1秒
                
        except KeyboardInterrupt:
            print("\n检测到退出信号，正在清理...")
            self.unsubscribe_stocks()
            print("程序已安全退出")
        except Exception as e:
            print(f"运行异常: {e}")
            self.unsubscribe_stocks()
            raise

if __name__ == "__main__":
    try:
        # 创建 StockScorer 实例
        scorer = StockScorer()
        
        # 订阅更多的示例股票代码
        stock_codes = [
            '000001.SZ',  # 平安银行
            '600000.SH',  # 浦发银行
            '600036.SH',  # 招商银行
            '601318.SH',  # 中国平安
            "301536.SZ",
            "603893.SH",
            "603290.SH",
            "301297.SZ",
            "601865.SH",
            "001309.SZ",
            "300857.SZ",
            "003031.SZ",
            "301165.SZ",
            "300394.SZ",
            "601136.SH",
            "002583.SZ",
            "002920.SZ"
        ]
        
        print("开始订阅股票...")
        scorer.subscribe_stocks(stock_codes)
        
        print("开始运行主程序...")
        scorer.run()
        
    except Exception as e:
        print(f"程序运行出错: {e}")
        traceback.print_exc()

