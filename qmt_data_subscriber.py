import zmq
import time
from google.protobuf import timestamp_pb2
from common.compression import decompress_data
from proto.stock_data_pb2 import StockQuote, StockQuoteBatch
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2.extras import execute_batch
import ssl
from qmt_common import load_config, get_zmq_keys

class QMTDataSubscriber:
    def __init__(self):
        self.config = load_config()
        
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
        port = self.config.get('zmq', 'port')
        self.socket.connect(f"tcp://localhost:{port}")
        self.socket.subscribe(b"MARKET_DATA")
        
        # 数据库连接池
        self.db_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=5,
            maxconn=20,
            database="market_data",
            host="localhost",
            port="5432",
            user="your_user",
            password="your_password"
        )
        
        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=4)
    
    def _load_server_public_key(self):
        """加载服务器公钥"""
        keys_dir = self.config.get('zmq', 'keys_dir')
        public_path = os.path.join(keys_dir, 'server.public')
        with open(public_path, 'rb') as f:
            return f.read()

    def save_to_timescaledb(self, quotes):
        conn = self.db_pool.getconn()
        try:
            cur = conn.cursor()
            
            # 准备批量插入数据
            data = [(
                quote.ts_code,
                quote.timestamp,
                quote.last_price,
                quote.open_price,
                quote.high_price,
                quote.low_price,
                quote.pre_close,
                quote.volume,
                quote.amount,
                # 买一档到买十档
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
                # 卖一档到卖十档
                quote.ask_price1, quote.ask_volume1,
                quote.ask_price2, quote.ask_volume2,
                quote.ask_price3, quote.ask_volume3,
                quote.ask_price4, quote.ask_volume4,
                quote.ask_price5, quote.ask_volume5,
                quote.ask_price6, quote.ask_volume6,
                quote.ask_price7, quote.ask_volume7,
                quote.ask_price8, quote.ask_volume8,
                quote.ask_price9, quote.ask_volume9,
                quote.ask_price10, quote.ask_volume10,
                # 额外字段
                quote.transaction_num,
                quote.stock_status,
                quote.pvolume
            ) for quote in quotes]
            
            # 使用execute_batch进行批量插入
            execute_batch(cur, """
                INSERT INTO market_data_tick (
                    ts_code, timestamp, last_price, open_price,
                    high_price, low_price, pre_close, volume,
                    amount, 
                    bid_price1, bid_volume1, bid_price2, bid_volume2,
                    bid_price3, bid_volume3, bid_price4, bid_volume4,
                    bid_price5, bid_volume5, bid_price6, bid_volume6,
                    bid_price7, bid_volume7, bid_price8, bid_volume8,
                    bid_price9, bid_volume9, bid_price10, bid_volume10,
                    ask_price1, ask_volume1, ask_price2, ask_volume2,
                    ask_price3, ask_volume3, ask_price4, ask_volume4,
                    ask_price5, ask_volume5, ask_price6, ask_volume6,
                    ask_price7, ask_volume7, ask_price8, ask_volume8,
                    ask_price9, ask_volume9, ask_price10, ask_volume10,
                    transaction_num, stock_status, pvolume
                ) VALUES (
                    %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
            """, data)
            
            conn.commit()
            
        except Exception as e:
            print(f"保存数据失败: {e}")
        finally:
            self.db_pool.putconn(conn)

    def run(self):
        print("开始接收市场数据...")
        
        while True:
            try:
                [topic, message] = self.socket.recv_multipart()
                
                # 解压缩和反序列化
                decompressed_data = decompress_data(message)
                batch = StockQuoteBatch()
                batch.ParseFromString(decompressed_data)
                
                # 异步保存数据
                self.executor.submit(self.save_to_timescaledb, batch.quotes)
                
            except Exception as e:
                print(f"处理数据失败: {e}")
                time.sleep(1)

if __name__ == "__main__":
    subscriber = QMTDataSubscriber()
    subscriber.run()
