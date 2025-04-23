from qmt_common import *
import zmq
from proto.qmt_level1_data_pb2 import StockQuoteBatch
from concurrent.futures import ThreadPoolExecutor
from psycopg2.extras import execute_batch
from psycopg2 import pool
from datetime import timezone
import threading

class QMTDataSubscriber:
    def __init__(self):
        self.config = load_config()
        self.logger = setup_logger()

        # 初始化ZMQ
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)

        # 设置ZMQ超时参数
        self.socket.setsockopt(zmq.RCVTIMEO, 5000)  # 接收超时5秒
        self.socket.setsockopt(zmq.LINGER, 0)      # 关闭时立即断开
        self.socket.setsockopt(zmq.RECONNECT_IVL, 1000)  # 重连间隔1秒
        self.socket.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)  # 最大重连间隔5秒

        # 配置客户端加密
        server_public_key = self._load_server_public_key()
        client_public, client_secret = zmq.curve_keypair()  # 客户端临时密钥对

        self.socket.curve_serverkey = server_public_key
        self.socket.curve_publickey = client_public
        self.socket.curve_secretkey = client_secret

        # 连接到发布者
        ip = self.config.get('zmq', 'ip')
        port = self.config.get('zmq', 'port')
        self.socket.connect(f"tcp://{ip}:{port}")
        self.socket.subscribe(b"MARKET_DATA")
        self.socket.subscribe(b"HEARTBEAT")  # 订阅心跳消息
        self.logger.info(f"已连接到发布者IP {ip}，端口 {port}")

        # 数据库连接
        self.db_pool = pool.ThreadedConnectionPool(
            minconn=int(self.config.get('postgresql', 'min_connections', fallback='20')),
            maxconn=int(self.config.get('postgresql', 'max_connections', fallback='100')),
            database=self.config.get('postgresql', 'database'),
            host=self.config.get('postgresql', 'host'),
            port=self.config.get('postgresql', 'port'),
            user=self.config.get('postgresql', 'user'),
            password=self.config.get('postgresql', 'password')
        )

        # 使用CPU核心数的2-3倍作为线程池大小，因为大部分操作是I/O密集型
        cpu_count = os.cpu_count() or 32  # 如果无法获取，默认使用32
        self.executor = ThreadPoolExecutor(max_workers=cpu_count * 2)

        # 添加统计信息
        self.stats = {
            'total_quotes': 0,
            'total_heartbeats': 0,
            'start_time': datetime.now(),
            'last_stats_time': datetime.now(),  # 上次统计时间，用于每10分钟统计
            'last_message_time': time.time()    # 上次接收任何消息的时间
        }

        # 连接监控设置
        self.connection_timeout = 30  # 连接超时时间（秒）
        self.connection_monitor_thread = None
        self.running = True

    def _load_server_public_key(self):
        """加载服务器公钥"""
        try:
            keys_dir = self.config.get('zmq', 'keys_dir')
            public_path = os.path.join(keys_dir, 'server.public')
            with open(public_path, 'rb') as f:
                return f.read()
        except Exception as e:
            self.logger.error(f"加载服务器公钥失败: {e}")
            raise

    def log_stats(self):
        """记录运行统计信息"""
        now = datetime.now()
        # 计算自上次统计以来的时间间隔
        time_since_last_stats = (now - self.stats['last_stats_time']).total_seconds()
        # 计算这个10分钟周期内的平均速率
        quote_rate = self.stats['total_quotes'] / time_since_last_stats if time_since_last_stats > 0 else 0
        heartbeat_rate = self.stats['total_heartbeats'] / time_since_last_stats if time_since_last_stats > 0 else 0

        self.logger.info(
            f"10分钟统计 - 行情数: {self.stats['total_quotes']}, "
            f"平均行情率: {quote_rate:.2f}/秒, "
            f"心跳数: {self.stats['total_heartbeats']}, "
            f"心跳率: {heartbeat_rate:.2f}/秒"
        )

        # 重置计数器
        self.stats['total_quotes'] = 0
        self.stats['total_heartbeats'] = 0
        self.stats['last_stats_time'] = now

    def save_to_timescaledb(self, quotes):
        """将行情数据保存到TimescaleDB数据库"""
        if not quotes:
            return

        conn = None
        try:
            # 获取数据库连接
            conn = self.db_pool.getconn()

            cur = conn.cursor()

            # 准备批量插入数据
            data = []
            for quote in quotes:
                # 将时间戳转换为datetime对象
                timestamp = datetime.fromtimestamp(quote.timestamp / 1000.0, tz=timezone.utc)

                # 准备数据
                data.append((
                    quote.ts_code,
                    timestamp,
                    quote.last_price,
                    quote.open_price,
                    quote.high_price,
                    quote.low_price,
                    quote.pre_close,
                    quote.volume,
                    quote.amount,
                    quote.pvolume,
                    quote.transaction_num,
                    quote.stock_status,
                    quote.bid_price1,
                    quote.bid_volume1,
                    quote.bid_price2,
                    quote.bid_volume2,
                    quote.bid_price3,
                    quote.bid_volume3,
                    quote.bid_price4,
                    quote.bid_volume4,
                    quote.bid_price5,
                    quote.bid_volume5,
                    quote.bid_price6,
                    quote.bid_volume6,
                    quote.bid_price7,
                    quote.bid_volume7,
                    quote.bid_price8,
                    quote.bid_volume8,
                    quote.bid_price9,
                    quote.bid_volume9,
                    quote.bid_price10,
                    quote.bid_volume10,
                    quote.ask_price1,
                    quote.ask_volume1,
                    quote.ask_price2,
                    quote.ask_volume2,
                    quote.ask_price3,
                    quote.ask_volume3,
                    quote.ask_price4,
                    quote.ask_volume4,
                    quote.ask_price5,
                    quote.ask_volume5,
                    quote.ask_price6,
                    quote.ask_volume6,
                    quote.ask_price7,
                    quote.ask_volume7,
                    quote.ask_price8,
                    quote.ask_volume8,
                    quote.ask_price9,
                    quote.ask_volume9,
                    quote.ask_price10,
                    quote.ask_volume10
                ))

            # 执行批量插入
            execute_batch(cur, """
                INSERT INTO a_stock_level1_data (
                    ts_code, timestamp, last_price, open_price, high_price, low_price, pre_close,
                    volume, amount, pvolume, transaction_num, stock_status,
                    bid_price1, bid_volume1, bid_price2, bid_volume2, bid_price3, bid_volume3,
                    bid_price4, bid_volume4, bid_price5, bid_volume5, bid_price6, bid_volume6,
                    bid_price7, bid_volume7, bid_price8, bid_volume8, bid_price9, bid_volume9,
                    bid_price10, bid_volume10, ask_price1, ask_volume1, ask_price2, ask_volume2,
                    ask_price3, ask_volume3, ask_price4, ask_volume4, ask_price5, ask_volume5,
                    ask_price6, ask_volume6, ask_price7, ask_volume7, ask_price8, ask_volume8,
                    ask_price9, ask_volume9, ask_price10, ask_volume10
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, data)

            conn.commit()

        except Exception as e:
            self.logger.error(f"保存数据失败: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)


    def reconnect(self):
        """重新连接到ZMQ服务器"""
        max_attempts = 3  # 最大重试次数
        retry_delay = 1   # 重试间隔（秒）

        for attempt in range(max_attempts):
            try:
                self.logger.info(f"开始第 {attempt + 1} 次重连尝试...")

                # 关闭现有连接
                self.socket.close()
                time.sleep(0.1)  # 短暂等待确保socket完全关闭

                # 创建新的socket
                self.socket = self.context.socket(zmq.SUB)

                # 重新设置超时参数
                self.socket.setsockopt(zmq.RCVTIMEO, 5000)
                self.socket.setsockopt(zmq.LINGER, 0)
                self.socket.setsockopt(zmq.RECONNECT_IVL, 1000)
                self.socket.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)

                # 重新配置加密
                server_public_key = self._load_server_public_key()
                client_public, client_secret = zmq.curve_keypair()
                self.socket.curve_serverkey = server_public_key
                self.socket.curve_publickey = client_public
                self.socket.curve_secretkey = client_secret

                # 重新连接
                ip = self.config.get('zmq', 'ip')
                port = self.config.get('zmq', 'port')
                self.socket.connect(f"tcp://{ip}:{port}")
                self.socket.subscribe(b"MARKET_DATA")
                self.socket.subscribe(b"HEARTBEAT")  # 同时订阅心跳消息

                # 测试连接
                try:
                    parts = self.socket.recv_multipart(flags=zmq.NOBLOCK)
                    # 检查接收到的消息格式
                    if len(parts) >= 1:
                        self.logger.info(f"重连成功并收到数据: {parts[0]}")
                        if len(parts) > 2:
                            self.logger.warning(f"接收到超过2个部分的消息，共{len(parts)}个部分")
                    else:
                        self.logger.info("重连成功并收到空消息")
                    # 更新最后消息接收时间
                    self.stats['last_message_time'] = time.time()
                    return True
                except zmq.error.Again:
                    self.logger.info("重连成功，等待数据...")
                    return True

            except Exception as e:
                self.logger.error(f"第 {attempt + 1} 次重连失败: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(retry_delay)
                else:
                    self.logger.error("达到最大重试次数，重连失败")
                    return False

        return False

    def monitor_connection(self):
        """监控连接状态的线程函数"""
        self.logger.info("启动连接监控线程")

        while self.running:
            current_time = time.time()
            last_message_time = self.stats['last_message_time']
            time_since_last_message = current_time - last_message_time

            # 如果超过设定的超时时间没有收到任何消息，尝试重连
            if time_since_last_message > self.connection_timeout:
                self.logger.warning(f"已有 {time_since_last_message:.1f} 秒未收到任何消息，尝试重新连接...")
                success = self.reconnect()
                if success:
                    self.logger.info("重连成功")
                else:
                    self.logger.error("重连失败，将在下一个检查周期再次尝试")

            # 每5秒检查一次连接状态
            time.sleep(5)

    def run(self):
        self.logger.info(f"启动市场数据接收服务... ZMQ: {self.config.get('zmq', 'ip')}:{self.config.get('zmq', 'port')}")

        # 启动连接监控线程
        self.connection_monitor_thread = threading.Thread(target=self.monitor_connection, daemon=True)
        self.connection_monitor_thread.start()
        self.logger.info("连接监控线程已启动")

        try:
            while True:
                try:
                    # 非阻塞接收数据
                    parts = self.socket.recv_multipart(flags=zmq.NOBLOCK)

                    # 更新最后消息接收时间
                    self.stats['last_message_time'] = time.time()

                    # 检查接收到的消息格式
                    if len(parts) < 2:
                        self.logger.warning(f"接收到格式不正确的消息: {parts}")
                        continue

                    # 提取主题和消息内容
                    topic = parts[0]
                    message = parts[1]

                    # 如果收到超过2个部分的消息，记录日志但继续处理
                    if len(parts) > 2:
                        # 打印基本信息
                        self.logger.warning(f"接收到超过2个部分的消息，共{len(parts)}个部分")

                        # 打印每个部分的信息
                        for i, part in enumerate(parts):
                            try:
                                # 尝试将bytes解码为字符串
                                text = part.decode('utf-8', errors='replace')
                                self.logger.warning(f"第{i+1}部分内容(文本): {text}")
                            except:
                                # 如果解码失败，打印十六进制
                                self.logger.warning(f"第{i+1}部分内容(hex): {part.hex()[:100]}...")

                            # 打印长度信息
                            self.logger.warning(f"第{i+1}部分大小: {len(part)} bytes")

                    # 解压缩和反序列化
                    decompressed_data = decompress_data(message)
                    batch = StockQuoteBatch()
                    batch.ParseFromString(decompressed_data)

                    # 根据消息类型处理
                    if topic == b"MARKET_DATA" and batch.quotes:
                        # 异步保存数据
                        self.executor.submit(self.save_to_timescaledb, batch.quotes)

                        # 更新统计信息
                        self.stats['total_quotes'] += len(batch.quotes)

                    elif topic == b"HEARTBEAT":
                        # 处理心跳消息
                        self.stats['total_heartbeats'] += 1

                    # 每自然10分钟记录一次统计信息
                    now = datetime.now()
                    current_minute = now.minute

                    # 检查是否到达自然10分钟的边界（00, 10, 20, 30, 40, 50分）
                    if current_minute % 10 == 0 and now.minute != self.stats['last_stats_time'].minute:
                        self.log_stats()

                except zmq.error.Again:
                    time.sleep(0.001)
                    continue
                except Exception as e:
                    self.logger.error(f"处理数据失败: {e}")
                    time.sleep(0.001)

        except KeyboardInterrupt:
            self.logger.info("\n检测到退出信号，正在清理...")
        finally:
            # 停止连接监控线程
            self.running = False
            if self.connection_monitor_thread and self.connection_monitor_thread.is_alive():
                self.connection_monitor_thread.join(timeout=1.0)

            self.socket.close()
            self.context.term()
            self.logger.info("已清理所有资源")

if __name__ == "__main__":
    # 创建订阅器实例并运行
    subscriber = QMTDataSubscriber()
    subscriber.run()
