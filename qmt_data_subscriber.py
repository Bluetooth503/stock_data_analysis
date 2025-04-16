import zmq
import time
from datetime import datetime
import os
import argparse
from google.protobuf import timestamp_pb2
from proto.qmt_level1_data_pb2 import StockQuote, StockQuoteBatch
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import pool
import ssl
from qmt_common import *

class QMTDataSubscriber:
    def __init__(self, filter_ts_code=None):
        # 设置过滤的股票代码
        self.filter_ts_code = filter_ts_code
        if self.filter_ts_code:
            print(f"只打印股票代码 {self.filter_ts_code} 的日志")
        self.config = load_config()
        # 设置日志
        self.logger = setup_logger('qmt_subscriber')

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
        self.db_pool = pool.SimpleConnectionPool(
            minconn=5,
            maxconn=20,
            database=self.config.get('postgresql_pub', 'database'),
            host=self.config.get('postgresql_pub', 'host'),
            port=self.config.get('postgresql_pub', 'port'),
            user=self.config.get('postgresql_pub', 'user'),
            password=self.config.get('postgresql_pub', 'password')
        )
        self.logger.info("数据库连接池已初始化")

        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=4)

        # 添加统计信息
        self.stats = {
            'total_messages': 0,
            'total_quotes': 0,
            'last_message_time': None,
            'start_time': datetime.now()
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
        msg_rate = self.stats['total_messages'] / runtime.total_seconds() if runtime.total_seconds() > 0 else 0
        quote_rate = self.stats['total_quotes'] / runtime.total_seconds() if runtime.total_seconds() > 0 else 0

        self.logger.info(
            f"运行统计 - 总消息数: {self.stats['total_messages']}, "
            f"总行情数: {self.stats['total_quotes']}, "
            f"消息率: {msg_rate:.2f}/秒, "
            f"行情率: {quote_rate:.2f}/秒, "
            f"运行时长: {runtime}"
        )

    def save_to_timescaledb(self, quotes):
        if not quotes:
            self.logger.warning("没有数据需要保存")
            return

        self.logger.info(f"开始保存 {len(quotes)} 条行情数据到数据库")

        try:
            conn = self.db_pool.getconn()
            self.logger.debug("成功获取数据库连接")
        except Exception as e:
            self.logger.error(f"获取数据库连接失败: {e}")
            self.logger.exception(e)
            return

        try:
            cur = conn.cursor()

            # 准备批量插入数据
            self.logger.debug("开始准备数据记录")
            data = []
            for i, quote in enumerate(quotes):
                try:
                    # 检查必要字段
                    if not hasattr(quote, 'ts_code') or not quote.ts_code:
                        self.logger.warning(f"第 {i} 条数据缺少ts_code字段，跳过")
                        continue

                    if not hasattr(quote, 'timestamp') or not quote.timestamp:
                        self.logger.warning(f"第 {i} 条数据缺少timestamp字段，跳过")
                        continue

                    # 构建数据记录
                    # 处理时间戳，如果是毫秒级别的时间戳，需要除以1000
                    timestamp_value = quote.timestamp
                    if timestamp_value > 10000000000:  # 大于2286年的UNIX时间戳，应该是毫秒
                        timestamp_value = timestamp_value / 1000  # 转换为秒

                    record = (
                        quote.ts_code,
                        datetime.fromtimestamp(timestamp_value),  # 转换为datetime对象
                        quote.last_price,
                        quote.open_price,
                        quote.high_price,
                        quote.low_price,
                        quote.pre_close,
                        quote.volume,
                        quote.amount,
                        quote.pvolume,              # 原始成交量
                        quote.transaction_num,      # 成交笔数
                        quote.stock_status,         # 股票状态
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
                    )
                    data.append(record)
                except Exception as e:
                    self.logger.error(f"处理第 {i} 条数据时出错: {e}")
                    continue

            if not data:
                self.logger.warning("没有有效数据需要保存")
                return

            self.logger.info(f"已准备 {len(data)} 条有效数据记录用于插入")

            # 使用execute_batch进行批量插入到a_stock_level1_data超表
            self.logger.debug("开始执行批量插入")
            execute_batch(cur, """
                INSERT INTO a_stock_level1_data (
                    ts_code, timestamp, last_price, open_price,
                    high_price, low_price, pre_close, volume,
                    amount, pvolume, transaction_num, stock_status,
                    bid_price1, bid_volume1,
                    bid_price2, bid_volume2,
                    bid_price3, bid_volume3,
                    bid_price4, bid_volume4,
                    bid_price5, bid_volume5,
                    bid_price6, bid_volume6,
                    bid_price7, bid_volume7,
                    bid_price8, bid_volume8,
                    bid_price9, bid_volume9,
                    bid_price10, bid_volume10,
                    ask_price1, ask_volume1,
                    ask_price2, ask_volume2,
                    ask_price3, ask_volume3,
                    ask_price4, ask_volume4,
                    ask_price5, ask_volume5,
                    ask_price6, ask_volume6,
                    ask_price7, ask_volume7,
                    ask_price8, ask_volume8,
                    ask_price9, ask_volume9,
                    ask_price10, ask_volume10
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
            """, data)

            self.logger.debug("执行数据库提交")
            conn.commit()
            self.logger.info(f"成功保存 {len(data)} 条数据到数据库")

        except Exception as e:
            self.logger.error(f"保存数据失败: {e}")
            self.logger.exception(e)
            try:
                conn.rollback()
                self.logger.info("数据库事务已回滚")
            except Exception as rollback_error:
                self.logger.error(f"回滚事务失败: {rollback_error}")
        finally:
            try:
                self.db_pool.putconn(conn)
                self.logger.debug("数据库连接已返回连接池")
            except Exception as e:
                self.logger.error(f"返回数据库连接到连接池失败: {e}")

    def run(self):
        self.logger.info("开始接收市场数据...")
        last_stats_time = time.time()
        self.logger.info(f"ZMQ连接信息: {self.config.get('zmq', 'ip')}:{self.config.get('zmq', 'port')}")
        self.logger.info(f"数据库连接信息: {self.config.get('postgresql_pub', 'host')}:{self.config.get('postgresql_pub', 'port')}")

        # 测试数据库连接
        try:
            conn = self.db_pool.getconn()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM a_stock_level1_data")
            count = cur.fetchone()[0]
            self.logger.info(f"数据库连接测试成功，当前表中有 {count} 条记录")
            self.db_pool.putconn(conn)
        except Exception as e:
            self.logger.error(f"数据库连接测试失败: {e}")
            self.logger.exception(e)

        # 设置超时，避免无限等待
        self.socket.setsockopt(zmq.RCVTIMEO, 10000)  # 10秒超时

        try:
            while True:
                try:
                    self.logger.debug("等待接收数据...")
                    try:
                        [topic, message] = self.socket.recv_multipart()
                        self.logger.info(f"接收到数据，主题: {topic.decode('utf-8') if isinstance(topic, bytes) else topic}")
                    except zmq.error.Again:
                        self.logger.warning("接收数据超时，继续等待...")
                        continue

                    # 解压缩和反序列化
                    self.logger.debug(f"接收到原始数据大小: {len(message)} 字节")
                    decompressed_data = decompress_data(message)
                    self.logger.debug(f"解压后数据大小: {len(decompressed_data)} 字节")

                    batch = StockQuoteBatch()
                    batch.ParseFromString(decompressed_data)

                    # 记录批次信息
                    self.logger.info(f"接收到批次数据: 批次时间戳={batch.batch_timestamp if hasattr(batch, 'batch_timestamp') else 'N/A'}, "
                                    f"发布者ID={batch.publisher_id if hasattr(batch, 'publisher_id') else 'N/A'}, "
                                    f"行情数量={len(batch.quotes)}")

                    # 如果有行情数据，记录行情的详细信息
                    if batch.quotes:
                        # 查找匹配过滤条件的行情
                        filtered_quotes = []
                        if self.filter_ts_code:
                            filtered_quotes = [q for q in batch.quotes if q.ts_code == self.filter_ts_code]
                        else:
                            # 如果没有设置过滤，只取第一条
                            filtered_quotes = [batch.quotes[0]]

                        # 如果有匹配的行情，记录详细信息
                        if filtered_quotes:
                            for quote in filtered_quotes:
                                try:
                                    # 尝试处理时间戳，如果是毫秒级别的时间戳，需要除以1000
                                    timestamp_value = quote.timestamp
                                    # 检查时间戳长度，如果超过13位，可能是毫秒级别
                                    if timestamp_value > 10000000000:  # 大于2286年的UNIX时间戳，应该是毫秒
                                        timestamp_value = timestamp_value / 1000  # 转换为秒

                                    timestamp_str = datetime.fromtimestamp(timestamp_value).strftime('%Y-%m-%d %H:%M:%S')

                                    # 记录更详细的信息，包括成交笔数和买卖档位
                                    self.logger.info(f"行情数据: 代码={quote.ts_code}, 时间={timestamp_str}, "
                                                    f"原始时间戳={quote.timestamp}, 价格={quote.last_price}, 成交量={quote.volume}")

                                    # 记录成交笔数和类型，以及是否为0
                                    self.logger.info(f"成交笔数: {quote.transaction_num}, 类型: {type(quote.transaction_num)}, 是否为0: {quote.transaction_num == 0}")

                                    # 记录买卖档位信息
                                    self.logger.info(f"买一: 价格={quote.bid_price1}, 量={quote.bid_volume1}")
                                    self.logger.info(f"买二: 价格={quote.bid_price2}, 量={quote.bid_volume2}")
                                    self.logger.info(f"买三: 价格={quote.bid_price3}, 量={quote.bid_volume3}")
                                    self.logger.info(f"卖一: 价格={quote.ask_price1}, 量={quote.ask_volume1}")
                                    self.logger.info(f"卖二: 价格={quote.ask_price2}, 量={quote.ask_volume2}")
                                    self.logger.info(f"卖三: 价格={quote.ask_price3}, 量={quote.ask_volume3}")

                                    # 输出完整的对象属性
                                    self.logger.info(f"对象属性: {dir(quote)}")
                                except Exception as e:
                                    self.logger.error(f"处理时间戳出错: {e}, 原始时间戳值: {quote.timestamp}")
                                    self.logger.info(f"行情数据: 代码={quote.ts_code}, 时间=(无法解析), "
                                                    f"价格={quote.last_price}, 成交量={quote.volume}")

                        # 如果没有匹配的行情，只记录简单信息
                        elif not self.filter_ts_code:
                            first_quote = batch.quotes[0]
                            self.logger.info(f"第一条行情: 代码={first_quote.ts_code}, 价格={first_quote.last_price}")

                    # 更新统计信息
                    self.stats['total_messages'] += 1
                    self.stats['total_quotes'] += len(batch.quotes)
                    self.stats['last_message_time'] = datetime.now()

                    # 每10条消息记录一次统计信息
                    if self.stats['total_messages'] % 10 == 0:
                        self.log_stats()

                    # 异步保存数据
                    self.logger.info(f"提交保存 {len(batch.quotes)} 条行情数据到数据库")
                    self.executor.submit(self.save_to_timescaledb, batch.quotes)

                except Exception as e:
                    self.logger.error(f"处理数据失败: {e}")
                    self.logger.exception(e)
                    time.sleep(1)

        except KeyboardInterrupt:
            self.logger.info("\n检测到退出信号，正在清理...")
        finally:
            self.socket.close()
            self.context.term()
            self.logger.info("已清理所有资源")

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='QMT数据订阅器')
    parser.add_argument('--ts_code', type=str, help='要过滤的股票代码，例如：000001.SZ')
    args = parser.parse_args()

    # 创建订阅器实例并运行
    subscriber = QMTDataSubscriber(filter_ts_code=args.ts_code)
    subscriber.run()
