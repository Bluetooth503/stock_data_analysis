from qmt_common import *
import zmq
from proto.qmt_level1_data_pb2 import StockQuoteBatch
from concurrent.futures import ThreadPoolExecutor
from psycopg2.extras import execute_batch
from psycopg2 import pool
from datetime import timezone

class QMTDataSubscriber:
    def __init__(self):
        self.config = load_config()
        self.logger = setup_logger()

        # 初始化ZMQ
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)

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
            'start_time': datetime.now(),
            'last_stats_time': datetime.now()  # 上次统计时间，用于每小时统计
        }

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
        runtime = now - self.stats['start_time']
        quote_rate = self.stats['total_quotes'] / runtime.total_seconds() if runtime.total_seconds() > 0 else 0

        self.logger.info(
            f"运行统计 - 总行情数: {self.stats['total_quotes']}, "
            f"平均行情率: {quote_rate:.2f}/秒"
        )

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
                ON CONFLICT (ts_code, timestamp) DO NOTHING
            """, data)

            conn.commit()

            # 数据已成功保存到数据库

        except Exception as e:
            self.logger.error(f"保存数据失败: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def run(self):
        self.logger.info(f"启动市场数据接收服务... ZMQ: {self.config.get('zmq', 'ip')}:{self.config.get('zmq', 'port')}")

        try:
            while True:
                try:
                    # 非阻塞接收数据
                    [_, message] = self.socket.recv_multipart(flags=zmq.NOBLOCK)

                    # 解压缩和反序列化
                    decompressed_data = decompress_data(message)
                    batch = StockQuoteBatch()
                    batch.ParseFromString(decompressed_data)

                    # 如果有数据就处理
                    if batch.quotes:
                        # 异步保存数据
                        self.executor.submit(self.save_to_timescaledb, batch.quotes)

                        # 更新统计信息
                        self.stats['total_quotes'] += len(batch.quotes)

                        # 每自然小时（整点）记录一次统计信息
                        now = datetime.now()
                        current_hour = now.hour
                        last_hour = self.stats['last_stats_time'].hour

                        # 如果小时发生了变化，或者是第一次运行，则打印统计信息
                        if current_hour != last_hour:
                            self.log_stats()
                            self.stats['last_stats_time'] = now

                except zmq.error.Again:
                    # 没有数据时短暂等待以避免CPU占用过高
                    time.sleep(0.001)
                    continue
                except Exception as e:
                    self.logger.error(f"处理数据失败: {e}")
                    # 发生错误时短暂等待以避免CPU占用过高
                    time.sleep(0.001)

        except KeyboardInterrupt:
            self.logger.info("\n检测到退出信号，正在清理...")
        finally:
            self.socket.close()
            self.context.term()
            self.logger.info("已清理所有资源")

if __name__ == "__main__":
    # 创建订阅器实例并运行
    subscriber = QMTDataSubscriber()
    subscriber.run()
