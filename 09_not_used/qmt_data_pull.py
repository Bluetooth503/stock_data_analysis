from common_qmt import *
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
        self.socket = self.context.socket(zmq.PULL)

        # 设置ZMQ参数
        self.socket.setsockopt(zmq.RCVTIMEO, 5000)  # 接收超时5秒
        self.socket.setsockopt(zmq.RCVHWM, 100000)  # 接收高水位标记
        self.socket.setsockopt(zmq.LINGER, 1000)    # 关闭时等待消息接收完成
        self.socket.setsockopt(zmq.TCP_KEEPALIVE, 1)  # 启用TCP保活机制
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 30)  # 30秒无活动发送保活包
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 10)  # 10秒间隔
        self.socket.setsockopt(zmq.RCVBUF, 1024 * 1024 * 32)  # 设置32MB的接收缓冲区
        self.socket.setsockopt(zmq.RECONNECT_IVL, 1000)  # 重连间隔1秒
        self.socket.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)  # 最大重连间隔5秒

        # 配置客户端加密
        server_public_key = self._load_server_public_key()
        client_public, client_secret = zmq.curve_keypair()  # 客户端临时密钥对

        self.socket.curve_serverkey = server_public_key
        self.socket.curve_publickey = client_public
        self.socket.curve_secretkey = client_secret

        # 连接到公网服务器
        ip = self.config.get('zmq', 'ip')
        port = self.config.get('zmq', 'port')
        self.socket.connect(f"tcp://{ip}:{port}")
        self.logger.info(f"已连接到服务器 {ip}:{port}")

        # 数据库连接
        self.db_pool = pool.ThreadedConnectionPool(
            minconn=50,     # 最小连接数增加到50
            maxconn=200,    # 最大连接数增加到200
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

        # 连接监控设置
        self.connection_timeout = 15  # 连接超时时间（秒）
        self.connection_monitor_thread = None
        self.stats_check_thread = None
        self.running = True

        # 市场状态
        self.market_open = False
        self.update_market_status()

        # 统计周期配置（分钟）
        self.stats_interval = 5  # 可选值：5, 10, 30
        if self.stats_interval not in [5, 10, 30]:
            raise ValueError("统计周期只能是 5, 10, 30 分钟")

        # 初始化上次日志时间为当前统计周期的开始时间
        now = datetime.now()
        self.last_log_time = self._get_period_start_time(now)

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

    def reconnect(self):
        """重新连接到ZMQ服务器"""
        try:
            # 关闭旧连接
            self.socket.close()

            # 创建新连接
            self.socket = self.context.socket(zmq.PULL)

            # 设置ZMQ参数
            self.socket.setsockopt(zmq.RCVTIMEO, 5000)  # 接收超时5秒
            self.socket.setsockopt(zmq.RCVHWM, 100000)  # 接收高水位标记
            self.socket.setsockopt(zmq.LINGER, 1000)    # 关闭时等待消息接收完成
            self.socket.setsockopt(zmq.TCP_KEEPALIVE, 1)  # 启用TCP保活机制
            self.socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 30)  # 30秒无活动发送保活包
            self.socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 10)  # 10秒间隔
            self.socket.setsockopt(zmq.RCVBUF, 1024 * 1024 * 32)  # 设置32MB的接收缓冲区
            self.socket.setsockopt(zmq.RECONNECT_IVL, 1000)  # 重连间隔1秒
            self.socket.setsockopt(zmq.RECONNECT_IVL_MAX, 5000)  # 最大重连间隔5秒

            # 配置客户端加密
            server_public_key = self._load_server_public_key()
            client_public, client_secret = zmq.curve_keypair()  # 客户端临时密钥对

            self.socket.curve_serverkey = server_public_key
            self.socket.curve_publickey = client_public
            self.socket.curve_secretkey = client_secret

            # 连接到公网服务器
            ip = self.config.get('zmq', 'ip')
            port = self.config.get('zmq', 'port')
            self.socket.connect(f"tcp://{ip}:{port}")
            self.logger.info(f"已重新连接到服务器 {ip}:{port}")

        except Exception as e:
            self.logger.error(f"重新连接失败: {e}")
            self.stats['errors'] += 1

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
                f"重试数: {self.stats['send_retries']}"
            )

            # 重置统计数据
            self._reset_period_stats(now)
            return True
        return False

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
                timestamp = datetime.fromtimestamp(quote.timestamp / 1000.0, tz=timezone(timedelta(hours=8)))

                # 获取当前时间作为etl_time
                etl_time = datetime.now(tz=timezone(timedelta(hours=8)))

                # 准备数据
                data.append((
                    quote.ts_code, timestamp, quote.last_price, quote.open_price, quote.high_price, quote.low_price, quote.pre_close,
                    quote.volume, quote.amount, quote.pvolume, quote.transaction_num, quote.stock_status,
                    quote.bid_price1, quote.bid_volume1, quote.bid_price2, quote.bid_volume2, quote.bid_price3, quote.bid_volume3, quote.bid_price4, quote.bid_volume4, quote.bid_price5, quote.bid_volume5,
                    quote.ask_price1, quote.ask_volume1, quote.ask_price2, quote.ask_volume2, quote.ask_price3, quote.ask_volume3, quote.ask_price4, quote.ask_volume4, quote.ask_price5, quote.ask_volume5,
                    etl_time
                ))

            # 执行批量插入
            execute_batch(cur, """
                INSERT INTO a_stock_level1_data (
                    ts_code, timestamp, last_price, open_price, high_price, low_price, pre_close,
                    volume, amount, pvolume, transaction_num, stock_status,
                    bid_price1, bid_volume1, bid_price2, bid_volume2, bid_price3, bid_volume3, bid_price4, bid_volume4, bid_price5, bid_volume5,
                    ask_price1, ask_volume1, ask_price2, ask_volume2, ask_price3, ask_volume3, ask_price4, ask_volume4, ask_price5, ask_volume5,
                    etl_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, data)

            conn.commit()

        except Exception as e:
            self.logger.error(f"保存数据失败: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)

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

        # 根据市场状态调整连接超时时间
        if not self.market_open:
            self.connection_timeout = 30  # 休市期间延长超时时间
        else:
            self.connection_timeout = 15  # 开市期间缩短超时时间

    def check_stats(self):
        """定期检查并打印统计信息的线程函数"""
        self.logger.info("启动统计信息检查线程")

        while self.running:
            # 每秒检查一次是否需要打印统计信息
            self.log_stats()
            time.sleep(1)

    def monitor_connection(self):
        """监控连接状态的线程函数"""
        self.logger.info("启动连接监控线程")

        while self.running:
            # 更新市场状态
            self.update_market_status()

            current_time = time.time()
            last_message_time = self.stats['last_message_time']

            if last_message_time is None:
                time.sleep(1)
                continue

            time_since_last_message = current_time - last_message_time

            # 根据市场状态和超时时间决定是否重连
            if time_since_last_message > self.connection_timeout:
                self.logger.warning(
                    f"已有 {time_since_last_message:.1f} 秒未收到任何消息，"
                    f"当前市场状态: {'开市' if self.market_open else '休市'}，"
                    f"尝试重新连接..."
                )
                self.reconnect()

            # 每秒检查一次连接状态
            time.sleep(1)

    def run(self):
        self.logger.info(f"启动市场数据接收服务... ZMQ: {self.config.get('zmq', 'ip')}:{self.config.get('zmq', 'port')}")

        # 启动连接监控线程
        self.connection_monitor_thread = threading.Thread(target=self.monitor_connection, daemon=True)
        self.connection_monitor_thread.start()
        self.logger.info("连接监控线程已启动")

        # 启动统计信息检查线程
        self.stats_check_thread = threading.Thread(target=self.check_stats, daemon=True)
        self.stats_check_thread.start()
        self.logger.info("统计信息检查线程已启动")

        try:
            while True:
                try:
                    # 非阻塞接收数据
                    message = self.socket.recv(flags=zmq.NOBLOCK)

                    # 更新最后消息接收时间
                    self.stats['last_message_time'] = time.time()

                    # 解压缩和反序列化
                    decompressed_data = decompress_data(message)
                    batch = StockQuoteBatch()
                    batch.ParseFromString(decompressed_data)

                    # 根据消息类型处理
                    if batch.message_type == "MARKET_DATA":
                        if batch.quotes:
                            # 异步保存数据
                            self.executor.submit(self.save_to_timescaledb, batch.quotes)
                            # 更新统计信息
                            self.stats['total_messages'] += len(batch.quotes)
                            self.stats['period_messages'] += len(batch.quotes)

                    elif batch.message_type == "HEARTBEAT":
                        # 更新心跳统计
                        self.stats['total_heartbeats'] += 1
                        self.stats['period_heartbeats'] += 1
                        self.stats['last_heartbeat_time'] = time.time()

                    # 统计信息由专门的线程处理，这里不需要额外调用

                except zmq.error.Again:
                    time.sleep(0.001)
                    continue
                except Exception as e:
                    self.logger.error(f"处理数据失败: {e}")
                    time.sleep(0.001)

        except KeyboardInterrupt:
            self.logger.info("\n检测到退出信号，正在清理...")
        finally:
            # 停止所有线程
            self.running = False

            # 停止连接监控线程
            if self.connection_monitor_thread and self.connection_monitor_thread.is_alive():
                self.connection_monitor_thread.join(timeout=1.0)

            # 停止统计信息检查线程
            if self.stats_check_thread and self.stats_check_thread.is_alive():
                self.stats_check_thread.join(timeout=1.0)

            self.socket.close()
            self.context.term()
            self.logger.info("已清理所有资源")





if __name__ == "__main__":
    # 创建订阅器实例并运行
    subscriber = QMTDataSubscriber()
    subscriber.run()
