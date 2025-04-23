
# -*- coding: utf-8 -*-
from qmt_common import *
from proto.qmt_level1_data_pb2 import StockQuote, StockQuoteBatch
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

        # 绑定端口
        port = self.config.get('zmq', 'port')
        self.socket.bind(f"tcp://*:{port}")
        self.logger.info(f"ZMQ publisher已初始化并绑定到端口 {port}")

        # 订阅号
        self.subscription_seq = None

        # 添加统计信息
        self.stats = {
            'total_messages': 0,
            'total_heartbeats': 0,
            'last_message_time': None,
            'last_heartbeat_time': None,
            'start_time': datetime.now()
        }

        # 心跳设置
        self.heartbeat_interval = 10  # 心跳间隔（秒）

    def log_stats(self):
        """记录运行统计信息"""
        now = datetime.now()
        runtime = now - self.stats['start_time']
        msg_rate = self.stats['total_messages'] / runtime.total_seconds() if runtime.total_seconds() > 0 else 0
        heartbeat_rate = self.stats['total_heartbeats'] / runtime.total_seconds() if runtime.total_seconds() > 0 else 0

        self.logger.info(
            f"运行统计 - 总消息数: {self.stats['total_messages']}, "
            f"消息率: {msg_rate:.2f}/秒, "
            f"心跳数: {self.stats['total_heartbeats']}, "
            f"心跳率: {heartbeat_rate:.2f}/秒, "
            f"运行时长: {runtime}"
        )

    def on_tick_data(self, datas):
        try:
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
            compressed_data = compress_data(serialized_data)

            # 发送数据
            self.socket.send_multipart([b"MARKET_DATA", compressed_data])

            # 更新统计信息
            self.stats['total_messages'] += 1
            self.stats['last_message_time'] = datetime.now()

        except Exception as e:
            self.logger.error(f"处理tick数据失败: {str(e)}")
            self.logger.exception(e)  # 记录完整的异常堆栈

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
            # 心跳消息不包含任何行情数据，quotes列表为空

            # 序列化并压缩
            serialized_data = batch.SerializeToString()
            compressed_data = compress_data(serialized_data)

            # 发送心跳消息，使用HEARTBEAT主题
            self.socket.send_multipart([b"HEARTBEAT", compressed_data])

            # 更新统计信息
            self.stats['total_heartbeats'] += 1
            self.stats['last_heartbeat_time'] = datetime.now()

        except Exception as e:
            self.logger.error(f"发送心跳消息失败: {str(e)}")
            self.logger.exception(e)

    def run(self):
        try:
            # 订阅股票行情
            stock_list = xtdata.get_stock_list_in_sector('沪深A股')
            self.subscription_seq = xtdata.subscribe_whole_quote(stock_list, callback=self.on_tick_data)

            self.logger.info(f"已订阅 {len(stock_list)} 只股票, 订阅号: {self.subscription_seq}")
            self.logger.info("数据发布服务已启动，等待接收行情数据...")

            # 在每个自然小时的整点记录心跳和统计信息
            last_hour = datetime.now().hour
            last_heartbeat_time = time.time()

            while True:
                current_time = datetime.now()
                current_hour = current_time.hour
                current_timestamp = time.time()

                # 当小时数变化时打印统计信息
                if current_hour != last_hour:
                    self.log_stats()
                    last_hour = current_hour

                # 检查是否需要发送心跳
                if current_timestamp - last_heartbeat_time >= self.heartbeat_interval:
                    self.send_heartbeat()
                    last_heartbeat_time = current_timestamp

                time.sleep(0.5)  # 减少CPU使用率，同时保持足够的响应性

        except KeyboardInterrupt:
            self.logger.info("\n检测到退出信号，正在清理...")
            self.unsubscribe_stocks()
            self.logger.info("程序已安全退出")
        except Exception as e:
            self.logger.error(f"运行出错: {str(e)}")
            self.logger.exception(e)
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
