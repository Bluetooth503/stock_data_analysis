# -*- coding: utf-8 -*-
from qmt_common import *
from proto.qmt_level1_data_pb2 import StockQuote, StockQuoteBatch
import threading
from xtquant import xtdata
xtdata.enable_hello = False


class QMTDataPublisher:
    def __init__(self):
        self.config = load_config()
        self.logger = setup_logger()

        # 初始化ZMQ
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)

        # 获取密钥并配置加密
        public_key, secret_key = get_zmq_keys(self.config)
        self.socket.curve_server = True
        self.socket.curve_secretkey = secret_key
        self.socket.curve_publickey = public_key

        # 优化套接字参数
        self.socket.setsockopt(zmq.SNDHWM, 100000)  # 高水位标记
        self.socket.setsockopt(zmq.LINGER, 1000)    # 关闭时等待消息发送完成
        self.socket.setsockopt(zmq.TCP_KEEPALIVE, 1)  # 启用TCP保活机制
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 60)  # 60秒无活动发送保活包
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 30)  # 30秒间隔

        # 绑定端口
        port = self.config.get('zmq', 'port')
        self.socket.bind(f"tcp://*:{port}")
        self.logger.info(f"ZMQ发布器已初始化并绑定到端口 {port}")

        # 订阅号
        self.subscription_seq = None

        # 添加互斥锁防止并发问题
        self.socket_lock = threading.Lock()

        # 添加统计信息
        self.stats = {
            'total_messages': 0,
            'total_heartbeats': 0,
            'last_message_time': None,
            'last_heartbeat_time': None,
            'start_time': datetime.now(),
            'last_stats_time': datetime.now(),
            'period_messages': 0,
            'period_heartbeats': 0,
            'errors': 0,
            'send_retries': 0
        }

        # 心跳设置
        self.heartbeat_interval = 5
        self.running = True

        # 统计周期配置（分钟）
        self.stats_interval = 5  # 可选值：5, 10, 30
        if self.stats_interval not in [5, 10, 30]:
            raise ValueError("统计周期只能是 5, 10, 30 分钟")

        # 初始化上次日志时间为当前统计周期的开始时间
        now = datetime.now()
        self.last_log_time = self._get_period_start_time(now)

    def _get_period_start_time(self, dt=None):
        """获取给定时间所在的自然时间周期的开始时间"""
        if dt is None:
            dt = datetime.now()

        hour = dt.hour
        minute = dt.minute

        if self.stats_interval == 5:
            # 向下取整到最近的5分钟
            minute = (minute // 5) * 5
        elif self.stats_interval == 10:
            # 向下取整到最近的10分钟
            minute = (minute // 10) * 10
        elif self.stats_interval == 30:
            # 向下取整到最近的30分钟
            minute = (minute // 30) * 30

        return dt.replace(hour=hour, minute=minute, second=0, microsecond=0)

    def _get_next_period_time(self, current_time):
        """获取下一个自然时间周期的开始时间"""
        current_period = self._get_period_start_time(current_time)
        next_period = current_period + timedelta(minutes=self.stats_interval)
        
        # 如果跨天，确保从第二天的00:00开始
        if next_period.day != current_period.day:
            next_period = next_period.replace(hour=0, minute=0)
            
        return next_period

    def log_stats(self, force=False):
        """按自然时间周期记录运行统计信息"""
        now = datetime.now()
        current_period = self._get_period_start_time(now)
        last_period = self._get_period_start_time(self.last_log_time)

        # 检查是否到达新的自然时间周期或强制打印
        if current_period > last_period or force:
            # 计算这个统计周期内的时长（秒）
            period_seconds = (now - self.stats['last_stats_time']).total_seconds()
            if period_seconds <= 0:
                return False

            # 计算该周期内的平均速率
            msg_rate = self.stats['period_messages'] / period_seconds
            heartbeat_rate = self.stats['period_heartbeats'] / period_seconds

            # 打印统计信息
            self.logger.info(
                f"行情数: {self.stats['period_messages']}, "
                f"平均行情率: {msg_rate:.2f}/秒, "
                f"心跳数: {self.stats['period_heartbeats']}, "
                f"心跳率: {heartbeat_rate:.2f}/秒, "
                f"错误数: {self.stats['errors']}, "
                f"重试数: {self.stats['send_retries']}, "
                f"心跳间隔: {self.heartbeat_interval}s"
            )

            # 重置统计数据
            self._reset_period_stats(now)
            return True
        return False

    def _reset_period_stats(self, current_time):
        """重置周期统计数据"""
        self.stats.update({
            'period_messages': 0,
            'period_heartbeats': 0,
            'errors': 0,
            'send_retries': 0,
            'last_stats_time': current_time
        })
        self.last_log_time = current_time

    def send_message(self, topic, data, retry_count=3):
        """安全发送消息的封装函数"""
        attempt = 0
        while attempt < retry_count and self.running:
            try:
                with self.socket_lock:
                    # PUSH模式下直接发送数据
                    self.socket.send(data)
                return True
            except zmq.Again:
                attempt += 1
                self.stats['send_retries'] += 1
                if attempt < retry_count:
                    time.sleep(0.01)  # 短暂等待后重试
                else:
                    self.logger.warning(f"发送消息失败，缓冲区已满，放弃发送: {topic}")
                    return False
            except Exception as e:
                self.logger.error(f"发送消息时发生错误: {str(e)}")
                self.stats['errors'] += 1
                return False
        return False

    def _safe_get_value(self, arr, idx, default=0):
        """安全获取买卖档位数据的辅助方法"""
        try:
            val = arr[idx]
            return val if val is not None else default
        except (IndexError, TypeError):
            return default

    def on_tick_data(self, datas):
        """处理并发送行情数据"""
        try:
            if not datas or not self.running:  # 空数据或程序停止运行时跳过
                return

            batch = StockQuoteBatch()
            batch.batch_timestamp = int(time.time() * 1000)
            batch.publisher_id = "QMT_PUBLISHER_001"
            batch.message_type = "MARKET_DATA"

            for code, tick in datas.items():
                quote = StockQuote()

                quote.ts_code = code
                quote.timestamp = tick['time']
                quote.last_price = tick['lastPrice']
                quote.open_price = tick['open']
                quote.high_price = tick['high']
                quote.low_price = tick['low']
                quote.pre_close = tick['lastClose']
                quote.amount = tick['amount']
                quote.volume = tick['volume']
                quote.pvolume = tick['pvolume']
                quote.stock_status = tick['stockStatus']
                quote.transaction_num = tick['transactionNum']

                # 简化，仅填充买卖5档
                for side in ('bid', 'ask'):
                    prices = tick.get(f'{side}Price', [])
                    vols   = tick.get(f'{side}Vol', [])
                    for i in range(1, 6):
                        setattr(quote, f'{side}_price{i}',  self._safe_get_value(prices, i-1))
                        setattr(quote, f'{side}_volume{i}', self._safe_get_value(vols,   i-1))

                batch.quotes.append(quote)

            # 序列化并压缩
            serialized_data = batch.SerializeToString()
            if not serialized_data:
                self.logger.warning("序列化数据为空，跳过发送")
                return

            compressed_data = compress_data(serialized_data)
            if not compressed_data:
                self.logger.warning("压缩数据为空，跳过发送")
                return

            # 发送行情数据
            if self.send_message(None, compressed_data):
                # 更新统计信息
                self.stats['total_messages'] += 1
                self.stats['period_messages'] += 1
                self.stats['last_message_time'] = datetime.now()

        except Exception as e:
            self.logger.error(f"处理tick数据失败: {str(e)}")
            self.logger.exception(e)
            self.stats['errors'] += 1

    def unsubscribe_stocks(self):
        """取消所有股票订阅"""
        try:
            if self.subscription_seq is not None:
                xtdata.unsubscribe_quote(self.subscription_seq)
                self.logger.info(f"已取消订阅 (订阅号: {self.subscription_seq})")
                self.subscription_seq = None
        except Exception as e:
            self.logger.error(f"取消订阅失败: {str(e)}")
            self.logger.exception(e)

    def send_heartbeat(self):
        """发送心跳消息"""
        try:
            # 创建心跳消息
            batch = StockQuoteBatch()
            batch.batch_timestamp = int(time.time() * 1000)
            batch.publisher_id = "QMT_PUBLISHER_001"
            batch.message_type = "HEARTBEAT"

            # 序列化并压缩
            serialized_data = batch.SerializeToString()
            compressed_data = compress_data(serialized_data)

            # 发送心跳消息
            if self.send_message(None, compressed_data):
                self.stats['total_heartbeats'] += 1
                self.stats['period_heartbeats'] += 1
                self.stats['last_heartbeat_time'] = datetime.now()
                self.logger.debug("心跳消息已发送")  # 调试级别日志

        except Exception as e:
            self.logger.error(f"准备心跳消息失败: {str(e)}")
            self.logger.exception(e)
            self.stats['errors'] += 1

    def stats_thread_func(self):
        """统计日志线程函数，专门负责定时打印统计信息"""
        self.logger.info(f"统计日志线程已启动 (自然{self.stats_interval}分钟打印)")
        
        while self.running:
            try:
                now = datetime.now()
                next_period = self._get_next_period_time(now)
                seconds_to_wait = (next_period - now).total_seconds()
                
                # 确保等待时间为正数
                if seconds_to_wait <= 0:
                    seconds_to_wait = self.stats_interval * 60
                
                # 检查是否需要打印统计信息
                self.log_stats()
                
                # 分段等待，每秒检查一次是否需要退出
                for _ in range(min(int(seconds_to_wait), 60)):
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                self.logger.error(f"统计日志线程出错: {str(e)}")
                self.logger.exception(e)
                time.sleep(5)  # 出错后等待5秒再继续

    def heartbeat_thread_func(self):
        """心跳线程函数"""
        self.logger.debug("心跳线程已启动")
        last_heartbeat_time = time.time()
        while self.running:
            try:
                current_time = time.time()
                # 检查是否需要发送心跳
                if current_time - last_heartbeat_time >= self.heartbeat_interval:
                    self.send_heartbeat()
                    last_heartbeat_time = current_time
                time.sleep(0.5)  # 降低CPU使用率
            except Exception as e:
                self.logger.error(f"心跳线程出错: {str(e)}")
                self.logger.exception(e)
                time.sleep(1)  # 出错后等待1秒再继续

    def run(self):
        try:
            # 订阅股票行情
            stock_list = xtdata.get_stock_list_in_sector('沪深A股')
            self.subscription_seq = xtdata.subscribe_whole_quote(stock_list, callback=self.on_tick_data)

            self.logger.info(f"已订阅 {len(stock_list)} 只股票, 订阅号: {self.subscription_seq}")
            self.logger.info("数据发布服务已启动，等待接收行情数据...")

            # 创建并启动心跳线程
            heartbeat_thread = threading.Thread(target=self.heartbeat_thread_func)
            heartbeat_thread.daemon = True
            heartbeat_thread.start()

            # 创建并启动统计日志线程
            stats_thread = threading.Thread(target=self.stats_thread_func)
            stats_thread.daemon = True
            stats_thread.start()

            while self.running:
                time.sleep(1)  # 减少CPU使用率

        except KeyboardInterrupt:
            self.logger.info("\n检测到退出信号，正在清理...")
            self.running = False  # 停止所有线程
            time.sleep(1)  # 等待线程安全退出
            self.unsubscribe_stocks()
            self.logger.info("程序已安全退出")
        except Exception as e:
            self.logger.error(f"运行出错: {str(e)}")
            self.logger.exception(e)
            self.running = False
            self.unsubscribe_stocks()
            raise
        finally:
            # 记录最终统计信息
            self.log_stats()
            # 确保在任何情况下都清理ZMQ资源
            self.socket.close()
            self.context.term()
            self.logger.info("已清理所有资源")

if __name__ == "__main__":
    publisher = QMTDataPublisher()
    publisher.run()
