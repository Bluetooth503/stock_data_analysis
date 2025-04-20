from qmt_common import *
import zmq
from proto.qmt_level1_data_pb2 import StockQuoteBatch
from concurrent.futures import ThreadPoolExecutor
from psycopg2.extras import execute_batch
from psycopg2 import pool
import tushare as ts

class QMTDataSubscriber:
    def __init__(self):
        self.config = load_config()
        # 设置日志
        self.logger = setup_logger()

        # 初始化tushare
        token = self.config.get('tushare', 'token')
        self.pro = ts.pro_api(token)

        # 超时设置
        self.trading_timeout = 10000  # 交易时间超时：10秒
        self.non_trading_timeout = 60000  # 非交易时间超时：60秒
        self.last_timeout_check = datetime.now()
        self.timeout_check_interval = 300  # 每5分钟检查一次交易状态

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
        self.logger.info(f"已连接到发布者端口 {port}")

        # 数据库连接池 - 从postgresql_pub配置节读取
        # 增加连接池大小以适应32核心服务器
        self.db_pool = pool.ThreadedConnectionPool(
            minconn=10,
            maxconn=50,  # 增加最大连接数
            database=self.config.get('postgresql_pub', 'database'),
            host=self.config.get('postgresql_pub', 'host'),
            port=self.config.get('postgresql_pub', 'port'),
            user=self.config.get('postgresql_pub', 'user'),
            password=self.config.get('postgresql_pub', 'password')
        )
        self.logger.info("数据库连接池已初始化 (minconn=10, maxconn=50)")

        # 线程池 - 优化为适应32核心服务器
        # 使用CPU核心数的2-3倍作为线程池大小，因为大部分操作是I/O密集型
        cpu_count = os.cpu_count() or 32  # 如果无法获取，默认使用32
        self.executor = ThreadPoolExecutor(max_workers=cpu_count * 2)

        # 添加统计信息
        self.stats = {
            'total_messages': 0,
            'total_quotes': 0,
            'last_message_time': None,
            'start_time': datetime.now(),
            'processing_times': [],  # 处理时间列表，用于计算平均处理时间
            'db_save_times': [],    # 数据库保存时间列表
            'batch_sizes': []        # 批次大小列表
        }

        # 性能监控参数
        self.max_stats_items = 100   # 最多保存多少条统计数据
        self.last_batch_time = None  # 上一批数据的接收时间

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
        msg_rate = self.stats['total_messages'] / runtime.total_seconds() if runtime.total_seconds() > 0 else 0
        quote_rate = self.stats['total_quotes'] / runtime.total_seconds() if runtime.total_seconds() > 0 else 0

        # 计算平均处理时间和数据库保存时间
        avg_processing_time = sum(self.stats['processing_times']) / len(self.stats['processing_times']) if self.stats['processing_times'] else 0
        avg_db_save_time = sum(self.stats['db_save_times']) / len(self.stats['db_save_times']) if self.stats['db_save_times'] else 0
        avg_batch_size = sum(self.stats['batch_sizes']) / len(self.stats['batch_sizes']) if self.stats['batch_sizes'] else 0

        # 计算最大值
        max_processing_time = max(self.stats['processing_times']) if self.stats['processing_times'] else 0
        max_db_save_time = max(self.stats['db_save_times']) if self.stats['db_save_times'] else 0
        max_batch_size = max(self.stats['batch_sizes']) if self.stats['batch_sizes'] else 0

        self.logger.info(
            f"运行统计 - 总消息数: {self.stats['total_messages']}, "
            f"总行情数: {self.stats['total_quotes']}, "
            f"消息率: {msg_rate:.2f}/秒, "
            f"行情率: {quote_rate:.2f}/秒"
        )

        self.logger.info(
            f"性能统计 - 平均批次: {avg_batch_size:.1f}条, "
            f"平均处理时间: {avg_processing_time*1000:.2f}ms, "
            f"平均入库时间: {avg_db_save_time*1000:.2f}ms, "
            f"最大处理时间: {max_processing_time*1000:.2f}ms, "
            f"最大入库时间: {max_db_save_time*1000:.2f}ms"
        )

    def save_to_timescaledb(self, quotes, batch_count=None):
        """将行情数据保存到TimescaleDB数据库"""
        if not quotes or len(quotes) == 0:
            return

        # 记录开始时间
        start_time = time.time()

        # 只在每100批次数据时记录一次，减少日志量
        log_batch = batch_count is not None and batch_count % 100 == 0

        # 记录批次大小
        self.stats['batch_sizes'].append(len(quotes))
        if len(self.stats['batch_sizes']) > self.max_stats_items:
            self.stats['batch_sizes'] = self.stats['batch_sizes'][-self.max_stats_items:]

        if log_batch:
            self.logger.info(f"保存第{batch_count}批，{len(quotes)}条行情数据")

        conn = None
        try:
            conn = self.db_pool.getconn()
            cur = conn.cursor()

            # 优化：设置游标的批处理大小
            cur.itersize = 2000  # 增加游标的批处理大小

            # 准备批量插入数据
            data = []
            for quote in quotes:
                try:
                    # 直接将时间戳视为毫秒级并转换为秒
                    timestamp_value = quote.timestamp / 1000

                    # 构建数据记录
                    record = (
                        quote.ts_code,
                        datetime.fromtimestamp(timestamp_value),
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
                        quote.bid_price1, quote.bid_volume1,
                        quote.bid_price2, quote.bid_volume2,
                        quote.bid_price3, quote.bid_volume3,
                        quote.bid_price4, quote.bid_volume4,
                        quote.bid_price5, quote.bid_volume5,
                        quote.bid_price6, quote.bid_volume6,
                        quote.bid_price7, quote.bid_volume7,
                        quote.bid_price8, quote.bid_volume8,
                        quote.bid_price9, quote.bid_volume9,
                        quote.bid_price10, quote.bid_volume10,
                        quote.ask_price1, quote.ask_volume1,
                        quote.ask_price2, quote.ask_volume2,
                        quote.ask_price3, quote.ask_volume3,
                        quote.ask_price4, quote.ask_volume4,
                        quote.ask_price5, quote.ask_volume5,
                        quote.ask_price6, quote.ask_volume6,
                        quote.ask_price7, quote.ask_volume7,
                        quote.ask_price8, quote.ask_volume8,
                        quote.ask_price9, quote.ask_volume9,
                        quote.ask_price10, quote.ask_volume10
                    )
                    data.append(record)
                except Exception as e:
                    # 只记录错误，继续处理下一条
                    self.logger.error(f"处理行情数据时出错: {e}")
                    continue

            if not data:
                return

            # 使用execute_batch进行批量插入，优化批处理参数
            execute_batch(cur, """
                INSERT INTO a_stock_level1_data (
                    ts_code, timestamp, last_price, open_price,
                    high_price, low_price, pre_close, volume,
                    amount, pvolume, transaction_num, stock_status,
                    bid_price1, bid_volume1, bid_price2, bid_volume2,
                    bid_price3, bid_volume3, bid_price4, bid_volume4,
                    bid_price5, bid_volume5, bid_price6, bid_volume6,
                    bid_price7, bid_volume7, bid_price8, bid_volume8,
                    bid_price9, bid_volume9, bid_price10, bid_volume10,
                    ask_price1, ask_volume1, ask_price2, ask_volume2,
                    ask_price3, ask_volume3, ask_price4, ask_volume4,
                    ask_price5, ask_volume5, ask_price6, ask_volume6,
                    ask_price7, ask_volume7, ask_price8, ask_volume8,
                    ask_price9, ask_volume9, ask_price10, ask_volume10
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
            """, data, page_size=1000)  # 增大page_size提高批处理效率

            conn.commit()

            # 记录数据库保存时间
            end_time = time.time()
            save_time = end_time - start_time
            self.stats['db_save_times'].append(save_time)
            if len(self.stats['db_save_times']) > self.max_stats_items:
                self.stats['db_save_times'] = self.stats['db_save_times'][-self.max_stats_items:]

            if log_batch:
                self.logger.info(f"成功保存 {len(data)} 条数据到数据库 (耗时: {save_time*1000:.2f}ms)")

        except Exception as e:
            self.logger.error(f"保存数据失败: {e}")
        finally:
            # 将连接返回到连接池
            if conn:
                self.db_pool.putconn(conn)

    def is_trade_date(self, date_str):
        """判断是否为交易日"""
        try:
            calendar = self.pro.trade_cal(start_date=date_str, end_date=date_str)
            if calendar.empty:
                return False
            return calendar.iloc[0]['is_open'] == 1
        except Exception as e:
            self.logger.error(f"判断交易日出错: {e}")
            return True  # 出错时默认为交易日，确保数据接收

    def check_trading_day(self):
        """检查当前是否为交易日，并返回下一个交易日的日期"""
        current_time = datetime.now()
        date_str = current_time.strftime('%Y%m%d')

        # 判断当前是否为交易日
        is_trade_date = self.is_trade_date(date_str)

        # 如果不是交易日，查找下一个交易日
        if not is_trade_date:
            next_trade_date = None
            check_date = current_time
            # 最多向后查找30天
            for _ in range(30):
                check_date = check_date + timedelta(days=1)
                check_date_str = check_date.strftime('%Y%m%d')
                if self.is_trade_date(check_date_str):
                    next_trade_date = check_date
                    break

            # 计算到下一个交易日的时间差
            if next_trade_date:
                # 设置为下一个交易日的早上8:30（开盘前准备）
                next_trade_date = next_trade_date.replace(hour=8, minute=30, second=0, microsecond=0)
                time_diff = next_trade_date - current_time
                seconds_to_next = max(0, time_diff.total_seconds())
                return False, next_trade_date, seconds_to_next
            else:
                # 如果找不到下一个交易日，默认等待24小时后再检查
                return False, current_time + timedelta(days=1), 86400

        return True, None, 0

    def is_trading_time(self, current_time):
        """判断是否为交易时间
        交易时间: 9:30-11:30, 13:00-15:00
        """
        hour = current_time.hour
        minute = current_time.minute

        # 上午交易时段: 9:30-11:30
        if (hour == 9 and minute >= 30) or (hour == 10) or (hour == 11 and minute <= 30):
            return True
        # 下午交易时段: 13:00-15:00
        elif (hour >= 13 and hour < 15):
            return True
        return False

    def update_timeout_setting(self):
        """根据当前是否为交易日和交易时间更新超时设置"""
        current_time = datetime.now()

        # 检查是否需要更新超时设置（每5分钟检查一次）
        time_diff = (current_time - self.last_timeout_check).total_seconds()
        if time_diff < self.timeout_check_interval:
            return

        self.last_timeout_check = current_time
        date_str = current_time.strftime('%Y%m%d')

        # 判断是否为交易日
        is_trade_date = self.is_trade_date(date_str)

        # 判断是否为交易时间
        is_trade_time = False
        if is_trade_date:
            is_trade_time = self.is_trading_time(current_time)

        # 根据交易状态设置不同的超时时间
        if is_trade_date and is_trade_time:
            if self.socket.getsockopt(zmq.RCVTIMEO) != self.trading_timeout:
                self.socket.setsockopt(zmq.RCVTIMEO, self.trading_timeout)
                self.logger.info(f"当前为交易时段，设置超时时间为 {self.trading_timeout/1000} 秒")
        else:
            if self.socket.getsockopt(zmq.RCVTIMEO) != self.non_trading_timeout:
                self.socket.setsockopt(zmq.RCVTIMEO, self.non_trading_timeout)
                self.logger.info(f"当前为非交易时段，设置超时时间为 {self.non_trading_timeout/1000} 秒")

    def run(self):
        self.logger.info(f"启动市场数据接收服务... ZMQ: {self.config.get('zmq', 'ip')}:{self.config.get('zmq', 'port')}, DB: {self.config.get('postgresql_pub', 'host')}:{self.config.get('postgresql_pub', 'port')}")

        try:
            while True:
                # 检查当前是否为交易日
                is_trading_day, next_trading_day, seconds_to_wait = self.check_trading_day()

                if not is_trading_day:
                    # 如果不是交易日，记录日志并等待到下一个交易日
                    if next_trading_day:
                        self.logger.info(f"当前不是交易日，将在 {next_trading_day.strftime('%Y-%m-%d %H:%M:%S')} 重新检查 (约 {seconds_to_wait/3600:.1f} 小时)")
                    else:
                        self.logger.info(f"当前不是交易日，将在 {seconds_to_wait/3600:.1f} 小时后重新检查")

                    # 关闭之前的连接（如果有）
                    try:
                        self.socket.close()
                        self.context.term()
                    except:
                        pass

                    # 等待到下一个交易日
                    time.sleep(seconds_to_wait)

                    # 重新初始化ZMQ连接
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

                    continue

                # 是交易日，开始接收数据
                self.logger.info("今天是交易日，开始接收市场数据...")

                # 设置初始超时
                current_time = datetime.now()
                is_trade_time = self.is_trading_time(current_time)

                if is_trade_time:
                    self.socket.setsockopt(zmq.RCVTIMEO, self.trading_timeout)
                    self.logger.info(f"当前为交易时段，设置超时时间为 {self.trading_timeout/1000} 秒")
                else:
                    self.socket.setsockopt(zmq.RCVTIMEO, self.non_trading_timeout)
                    self.logger.info(f"当前为非交易时段，设置超时时间为 {self.non_trading_timeout/1000} 秒")

                # 交易日数据接收循环
                try:
                    # 重置统计信息
                    self.stats['total_messages'] = 0
                    self.stats['total_quotes'] = 0
                    self.stats['start_time'] = datetime.now()
                    self.stats['processing_times'] = []
                    self.stats['db_save_times'] = []
                    self.stats['batch_sizes'] = []

                    # 设置一个定时器，每天凌晨0:05检查是否为交易日
                    next_day_check = datetime.now().replace(hour=0, minute=5, second=0, microsecond=0)
                    if next_day_check <= datetime.now():
                        next_day_check += timedelta(days=1)
                    seconds_to_midnight = (next_day_check - datetime.now()).total_seconds()

                    # 交易日内的数据接收循环
                    end_time = time.time() + seconds_to_midnight
                    while time.time() < end_time:
                        # 记录开始处理时间
                        process_start_time = time.time()

                        # 记录上一批数据到当前批数据的时间间隔
                        current_time = datetime.now()
                        if self.last_batch_time is not None:
                            batch_interval = (current_time - self.last_batch_time).total_seconds()
                            if batch_interval > 5:  # 如果间隔超过5秒，可能有延迟
                                self.logger.warning(f"批次间隔过长: {batch_interval:.2f}秒")
                        self.last_batch_time = current_time

                        # 更新超时设置
                        self.update_timeout_setting()

                        # 接收数据，处理超时
                        try:
                            [_, message] = self.socket.recv_multipart()
                        except zmq.error.Again:
                            # 获取当前超时设置
                            current_timeout = self.socket.getsockopt(zmq.RCVTIMEO)

                            # 判断当前是否为交易时间
                            current_time = datetime.now()
                            is_trade_time = self.is_trading_time(current_time)

                            # 只在交易时间或每小时的第一次超时时记录警告
                            if is_trade_time:
                                self.logger.warning("接收数据超时，继续等待...")
                            elif current_time.minute < 5:  # 每小时的前5分钟记录一次
                                self.logger.info(f"非交易时段接收数据超时 (当前超时设置: {current_timeout/1000}秒)")

                            continue

                        # 解压缩和反序列化
                        decompressed_data = decompress_data(message)
                        batch = StockQuoteBatch()
                        batch.ParseFromString(decompressed_data)

                        # 记录批次信息
                        self.logger.info(f"接收到批次数据: 行情数量={len(batch.quotes)}")

                        # 如果有行情数据，只记录第一条的基本信息
                        if batch.quotes:
                            quote = batch.quotes[0]
                            try:
                                timestamp_str = datetime.fromtimestamp(quote.timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                                self.logger.info(f"行情数据: 代码={quote.ts_code}, 时间={timestamp_str}, 价格={quote.last_price}")
                            except Exception as e:
                                self.logger.error(f"处理时间戳出错: {e}, 原始时间戳值: {quote.timestamp}")

                        # 更新统计信息
                        self.stats['total_messages'] += 1
                        self.stats['total_quotes'] += len(batch.quotes)
                        self.stats['last_message_time'] = current_time

                        # 记录处理时间
                        process_end_time = time.time()
                        process_time = process_end_time - process_start_time
                        self.stats['processing_times'].append(process_time)
                        if len(self.stats['processing_times']) > self.max_stats_items:
                            self.stats['processing_times'] = self.stats['processing_times'][-self.max_stats_items:]

                        # 每100条消息记录一次统计信息，减少日志量
                        if self.stats['total_messages'] % 100 == 0:
                            self.log_stats()

                        # 异步保存数据
                        self.executor.submit(self.save_to_timescaledb, batch.quotes, self.stats['total_messages'])

                    # 一天结束，记录统计信息
                    self.logger.info("当天数据接收完成，等待下一次检查...")
                    self.log_stats()

                except Exception as e:
                    self.logger.error(f"处理数据失败: {e}")
                    # 减少等待时间，加快恢复
                    time.sleep(0.5)

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
