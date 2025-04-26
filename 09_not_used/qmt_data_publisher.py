
# -*- coding: utf-8 -*-
from common_qmt import *
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
        self.socket = self.context.socket(zmq.PUB)
        
        # 获取密钥并配置加密
        public_key, secret_key = get_zmq_keys(self.config)
        self.socket.curve_server = True
        self.socket.curve_secretkey = secret_key
        self.socket.curve_publickey = public_key
        
        # 优化套接字参数
        self.socket.setsockopt(zmq.SNDHWM, 100000)  # 提高高水位标记
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
        
        # 消息ID计数器
        self.message_counter = 0

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
        self.heartbeat_interval = 10  # 休市期间可能需要更频繁的心跳
        
        # 标记程序是否应该继续运行
        self.running = True
        
        # 市场状态 - 用于调整行为
        self.market_open = False
        self.update_market_status()
        
    def update_market_status(self):
        """更新市场开闭市状态"""
        now = datetime.now()
        weekday = now.weekday()
        
        # 判断是否为工作日(0-4表示周一至周五)
        is_weekday = weekday < 5
        
        # 判断当前时间是否在交易时段
        current_time = now.time()
        morning_start = datetime.strptime("09:15:00", "%H:%M:%S").time()
        morning_end = datetime.strptime("11:30:00", "%H:%M:%S").time()
        afternoon_start = datetime.strptime("13:00:00", "%H:%M:%S").time()
        afternoon_end = datetime.strptime("15:00:00", "%H:%M:%S").time()
        
        in_morning_session = morning_start <= current_time <= morning_end
        in_afternoon_session = afternoon_start <= current_time <= afternoon_end
        
        # 更新市场状态
        old_status = self.market_open
        self.market_open = is_weekday and (in_morning_session or in_afternoon_session)
        
        # 如果状态发生变化，记录日志
        if old_status != self.market_open:
            self.logger.info(f"市场状态变更: {'开市' if self.market_open else '休市'}")
        
        # 根据市场状态调整心跳间隔
        if not self.market_open:
            # 休市期间，更频繁发送心跳
            self.heartbeat_interval = 5
        else:
            # 开市期间，正常心跳间隔
            self.heartbeat_interval = 10

    def log_stats(self):
        """按自然10分钟记录运行统计信息"""
        now = datetime.now()
        
        # 计算当前10分钟周期的开始时间
        current_period = now.replace(
            minute=(now.minute // 10) * 10,
            second=0,
            microsecond=0
        )
        
        # 计算上一个统计周期的开始时间
        last_period = self.stats['last_stats_time'].replace(
            minute=(self.stats['last_stats_time'].minute // 10) * 10,
            second=0,
            microsecond=0
        )
        
        # 如果还在同一个10分钟周期内，不打印日志
        if current_period == last_period:
            return
        
        # 计算这个10分钟周期内的时长（秒）
        period_seconds = (now - self.stats['last_stats_time']).total_seconds()
        
        # 计算该周期内的平均速率
        msg_rate = self.stats['period_messages'] / period_seconds if period_seconds > 0 else 0
        heartbeat_rate = self.stats['period_heartbeats'] / period_seconds if period_seconds > 0 else 0
        
        # 打印统计信息
        self.logger.info(
            f"10分钟统计 - "
            f"行情数: {self.stats['period_messages']}, "
            f"平均行情率: {msg_rate:.2f}/秒, "
            f"心跳数: {self.stats['period_heartbeats']}, "
            f"心跳率: {heartbeat_rate:.2f}/秒, "
            f"错误数: {self.stats['errors']}, "
            f"重试数: {self.stats['send_retries']}, "
            f"市场状态: {'开市' if self.market_open else '休市'}"
        )
        
        # 重置周期计数器
        self.stats['period_messages'] = 0
        self.stats['period_heartbeats'] = 0
        self.stats['errors'] = 0
        self.stats['send_retries'] = 0
        self.stats['last_stats_time'] = now

    def get_message_id(self):
        """生成唯一消息ID"""
        self.message_counter += 1
        return self.message_counter

    def send_message(self, topic, data, retry_count=3):
        """安全发送消息的封装函数"""
        attempt = 0
        while attempt < retry_count and self.running:
            try:
                with self.socket_lock:
                    # 构造带有消息ID的帧结构
                    msg_id = self.get_message_id()
                    id_frame = f"{msg_id}".encode()
                    
                    # 按照严格定义的格式发送
                    self.socket.send(topic, zmq.SNDMORE)
                    self.socket.send(id_frame, zmq.SNDMORE)
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

    def on_tick_data(self, datas):
        """处理并发送行情数据"""
        try:
            if not datas or not self.running:  # 空数据或程序停止运行时跳过
                return
                
            # 更新市场状态
            self.update_market_status()
                
            batch = StockQuoteBatch()
            batch.batch_timestamp = int(time.time() * 1000)
            batch.publisher_id = "QMT_PUBLISHER_001"

            for code, tick in datas.items():
                quote = StockQuote()

                # 安全获取买卖档位数据的辅助函数
                def safe_get_value(arr, idx, default=0):
                    try:
                        val = arr[idx]
                        return val if val is not None else default
                    except (IndexError, TypeError):
                        return default

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

                # 买一到买十档
                bid_prices = tick.get('bidPrice', [])
                bid_vols = tick.get('bidVol', [])
                quote.bid_price1 = safe_get_value(bid_prices, 0)
                quote.bid_volume1 = safe_get_value(bid_vols, 0)
                quote.bid_price2 = safe_get_value(bid_prices, 1)
                quote.bid_volume2 = safe_get_value(bid_vols, 1)
                quote.bid_price3 = safe_get_value(bid_prices, 2)
                quote.bid_volume3 = safe_get_value(bid_vols, 2)
                quote.bid_price4 = safe_get_value(bid_prices, 3)
                quote.bid_volume4 = safe_get_value(bid_vols, 3)
                quote.bid_price5 = safe_get_value(bid_prices, 4)
                quote.bid_volume5 = safe_get_value(bid_vols, 4)
                quote.bid_price6 = safe_get_value(bid_prices, 5)
                quote.bid_volume6 = safe_get_value(bid_vols, 5)
                quote.bid_price7 = safe_get_value(bid_prices, 6)
                quote.bid_volume7 = safe_get_value(bid_vols, 6)
                quote.bid_price8 = safe_get_value(bid_prices, 7)
                quote.bid_volume8 = safe_get_value(bid_vols, 7)
                quote.bid_price9 = safe_get_value(bid_prices, 8)
                quote.bid_volume9 = safe_get_value(bid_vols, 8)
                quote.bid_price10 = safe_get_value(bid_prices, 9)
                quote.bid_volume10 = safe_get_value(bid_vols, 9)

                # 卖一到卖十档
                ask_prices = tick.get('askPrice', [])
                ask_vols = tick.get('askVol', [])
                quote.ask_price1 = safe_get_value(ask_prices, 0)
                quote.ask_volume1 = safe_get_value(ask_vols, 0)
                quote.ask_price2 = safe_get_value(ask_prices, 1)
                quote.ask_volume2 = safe_get_value(ask_vols, 1)
                quote.ask_price3 = safe_get_value(ask_prices, 2)
                quote.ask_volume3 = safe_get_value(ask_vols, 2)
                quote.ask_price4 = safe_get_value(ask_prices, 3)
                quote.ask_volume4 = safe_get_value(ask_vols, 3)
                quote.ask_price5 = safe_get_value(ask_prices, 4)
                quote.ask_volume5 = safe_get_value(ask_vols, 4)
                quote.ask_price6 = safe_get_value(ask_prices, 5)
                quote.ask_volume6 = safe_get_value(ask_vols, 5)
                quote.ask_price7 = safe_get_value(ask_prices, 6)
                quote.ask_volume7 = safe_get_value(ask_vols, 6)
                quote.ask_price8 = safe_get_value(ask_prices, 7)
                quote.ask_volume8 = safe_get_value(ask_vols, 7)
                quote.ask_price9 = safe_get_value(ask_prices, 8)
                quote.ask_volume9 = safe_get_value(ask_vols, 8)
                quote.ask_price10 = safe_get_value(ask_prices, 9)
                quote.ask_volume10 = safe_get_value(ask_vols, 9)

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
            if self.send_message(b"MARKET_DATA", compressed_data):
                # 更新统计信息
                self.stats['total_messages'] += 1
                self.stats['period_messages'] += 1
                self.stats['last_message_time'] = datetime.now()
                
            # 检查是否需要打印统计信息
            self.log_stats()

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
            # 心跳消息不包含任何行情数据

            # 序列化并压缩
            serialized_data = batch.SerializeToString()
            compressed_data = compress_data(serialized_data)

            # 发送心跳消息
            if self.send_message(b"HEARTBEAT", compressed_data):
                # 更新统计信息
                self.stats['total_heartbeats'] += 1
                self.stats['period_heartbeats'] += 1
                self.stats['last_heartbeat_time'] = datetime.now()
            
            # 检查是否需要打印统计信息
            self.log_stats()
            
            # 更新市场状态
            self.update_market_status()

        except Exception as e:
            self.logger.error(f"准备心跳消息失败: {str(e)}")
            self.logger.exception(e)
            self.stats['errors'] += 1

    def heartbeat_thread_func(self):
        """心跳线程函数"""
        last_heartbeat_time = time.time()
        while self.running:
            current_time = time.time()
            # 检查是否需要发送心跳
            if current_time - last_heartbeat_time >= self.heartbeat_interval:
                self.send_heartbeat()
                last_heartbeat_time = current_time
            time.sleep(0.5)  # 降低CPU使用率

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
            
            # 主线程记录统计信息
            last_hour = datetime.now().hour
            
            while self.running:
                current_time = datetime.now()
                current_hour = current_time.hour

                # 当小时数变化时打印统计信息
                if current_hour != last_hour:
                    self.log_stats()
                    last_hour = current_hour

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
