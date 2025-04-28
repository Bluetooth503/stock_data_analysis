from common import *
from xtquant import xtdata
xtdata.enable_hello = False

@dataclass
class StockQuote:
    """股票行情数据类"""
    ts_code: str = ""
    timestamp: int = 0
    last_price: float = 0.0
    open_price: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    pre_close: float = 0.0
    volume: int = 0
    amount: float = 0.0
    pvolume: int = 0
    transaction_num: int = 0
    stock_status: int = 0
    bid_price1: float = 0.0
    bid_volume1: int = 0
    bid_price2: float = 0.0
    bid_volume2: int = 0
    bid_price3: float = 0.0
    bid_volume3: int = 0
    bid_price4: float = 0.0
    bid_volume4: int = 0
    bid_price5: float = 0.0
    bid_volume5: int = 0
    ask_price1: float = 0.0
    ask_volume1: int = 0
    ask_price2: float = 0.0
    ask_volume2: int = 0
    ask_price3: float = 0.0
    ask_volume3: int = 0
    ask_price4: float = 0.0
    ask_volume4: int = 0
    ask_price5: float = 0.0
    ask_volume5: int = 0

class Level1DataProcessor:
    """Level1行情数据处理器"""
    def __init__(self):
        self.stop_time = datetime.strptime('16:00:00', '%H:%M:%S').time()
        self.tz = timezone(timedelta(hours=8))
        self.config = load_config()
        self.logger = setup_logger()
        self.running = True
        self.subscription_seq = None
        self.data_queue = queue.Queue(maxsize=20000)
        self.batch_size = 1000  # 每1000条保存一次
        self.save_interval = 1  # 每10秒保存一次
        self.last_status_time = time.time()
        self.status_interval = 600  # 每10分钟输出一次状态
        self.total_received = 0
        self.total_saved = 0
        
        self.db_pool = pool.ThreadedConnectionPool(
            minconn=50,
            maxconn=200,
            database=self.config.get('postgresql', 'database'),
            host=self.config.get('postgresql', 'host'),
            port=self.config.get('postgresql', 'port'),
            user=self.config.get('postgresql', 'user'),
            password=self.config.get('postgresql', 'password')
        )
        
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        self.logger.info(f"接收到信号 {signum}，准备停止程序")
        self.stop()

    def save_to_timescaledb(self, quotes):
        if not quotes:
            return

        conn = None
        try:
            conn = self.db_pool.getconn()
            cur = conn.cursor()
            
            # 准备COPY命令
            copy_sql = "COPY a_stock_level1_data (ts_code, timestamp, last_price, open_price, high_price, low_price, pre_close, "
            copy_sql += "volume, amount, pvolume, transaction_num, stock_status, "
            copy_sql += "bid_price1, bid_volume1, bid_price2, bid_volume2, bid_price3, bid_volume3, bid_price4, bid_volume4, bid_price5, bid_volume5, "
            copy_sql += "ask_price1, ask_volume1, ask_price2, ask_volume2, ask_price3, ask_volume3, ask_price4, ask_volume4, ask_price5, ask_volume5, etl_time) FROM STDIN WITH CSV"
            
            # 创建StringIO对象用于写入CSV数据
            from io import StringIO
            csv_data = StringIO()
            
            # 写入数据
            for quote in quotes:
                if quote.volume > 0:
                    # 将毫秒级时间戳转换为datetime对象
                    timestamp = datetime.fromtimestamp(quote.timestamp / 1000, tz=timezone(timedelta(hours=8))).replace(microsecond=(quote.timestamp % 1000) * 1000)
                    etl_time = datetime.now(tz=timezone(timedelta(hours=8)))
                    
                    row = f"{quote.ts_code},{timestamp},{quote.last_price},{quote.open_price},{quote.high_price},"
                    row += f"{quote.low_price},{quote.pre_close},{quote.volume},{quote.amount},{quote.pvolume},"
                    row += f"{quote.transaction_num},{quote.stock_status},"
                    row += f"{quote.bid_price1},{quote.bid_volume1},{quote.bid_price2},{quote.bid_volume2},{quote.bid_price3},{quote.bid_volume3},{quote.bid_price4},{quote.bid_volume4},{quote.bid_price5},{quote.bid_volume5},"
                    row += f"{quote.ask_price1},{quote.ask_volume1},{quote.ask_price2},{quote.ask_volume2},{quote.ask_price3},{quote.ask_volume3},{quote.ask_price4},{quote.ask_volume4},{quote.ask_price5},{quote.ask_volume5},{etl_time}\n"
                    csv_data.write(row)
            
            # 将指针移到开始位置
            csv_data.seek(0)
            
            # 执行COPY命令
            cur.copy_expert(copy_sql, csv_data)
            conn.commit()
                        
            # 定期输出状态统计
            current_time = time.time()
            if current_time - self.last_status_time >= self.status_interval:
                self.logger.info(f"运行状态 - 累计接收: {self.total_received}条, 累计保存: {self.total_saved}条, 队列大小: {self.data_queue.qsize()}")
                self.last_status_time = current_time

        except Exception as e:
            self.logger.error(f"数据库操作失败: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)
            if 'csv_data' in locals():
                csv_data.close()

    def on_tick_data(self, datas):
        if not datas or not self.running:
            return

        try:
            valid_count = 0
            for code, tick in datas.items():
                # 过滤零成交量数据
                if tick.get('volume', 0) <= 0:
                    continue
                valid_count += 1
                quote = StockQuote(
                    ts_code=code,
                    timestamp=tick['time'],
                    last_price=tick['lastPrice'],
                    open_price=tick['open'],
                    high_price=tick['high'],
                    low_price=tick['low'],
                    pre_close=tick['lastClose'],
                    amount=tick['amount'],
                    volume=tick['volume'],
                    pvolume=tick.get('pvolume', 0),
                    stock_status=tick.get('stockStatus', 0),
                    transaction_num=tick.get('transactionNum', 0)
                )

                bid_prices = tick.get('bidPrice', [])
                bid_vols = tick.get('bidVol', [])
                ask_prices = tick.get('askPrice', [])
                ask_vols = tick.get('askVol', [])
                
                for i in range(5):
                    setattr(quote, f'bid_price{i+1}', bid_prices[i] if i < len(bid_prices) else 0.0)
                    setattr(quote, f'bid_volume{i+1}', bid_vols[i] if i < len(bid_vols) else 0)
                    setattr(quote, f'ask_price{i+1}', ask_prices[i] if i < len(ask_prices) else 0.0)
                    setattr(quote, f'ask_volume{i+1}', ask_vols[i] if i < len(ask_vols) else 0)

                try:
                    self.data_queue.put(quote, block=False)
                except queue.Full:
                    self.logger.warning("数据队列已满，丢弃部分数据")

            if valid_count > 0:
                self.total_received += valid_count
                
            # 定期输出接收状态
            current_time = time.time()
            if current_time - self.last_status_time >= self.status_interval:
                self.logger.info(f"数据接收状态 - 本批次有效数据: {valid_count}条, 累计接收: {self.total_received}条, 队列大小: {self.data_queue.qsize()}")
                self.last_status_time = current_time

        except Exception as e:
            self.logger.error(f"处理行情数据失败: {e}")

    def _process_data_worker(self):
        batch = []
        last_save_time = time.time()

        while self.running or not self.data_queue.empty():
            try:
                try:
                    quote = self.data_queue.get(timeout=1.0)
                    batch.append(quote)
                    self.data_queue.task_done()
                except queue.Empty:
                    pass

                current_time = time.time()
                if (len(batch) >= self.batch_size or
                    current_time - last_save_time >= self.save_interval) and batch:
                    self.save_to_timescaledb(batch)
                    batch = []
                    last_save_time = current_time

            except Exception as e:
                self.logger.error(f"数据处理异常: {e}")
                time.sleep(1)

        if batch:
            self.save_to_timescaledb(batch)

    def unsubscribe_stocks(self):
        if self.subscription_seq is not None:
            try:
                xtdata.unsubscribe_quote(self.subscription_seq)
                self.subscription_seq = None
            except Exception as e:
                self.logger.error(f"取消订阅失败: {e}")

    def stop(self):
        if not self.running:
            return
            
        self.logger.info("开始停止程序...")
        self.running = False
        self.unsubscribe_stocks()
        self.logger.info("等待数据处理线程完成...")
        # 等待数据队列处理完成
        if hasattr(self, 'data_queue'):
            self.data_queue.join()
        
        # 关闭数据库连接池
        if hasattr(self, 'db_pool') and self.db_pool is not None:
            try:
                self.logger.info("关闭数据库连接池...")
                self.db_pool.closeall()
                self.db_pool = None
            except Exception as e:
                self.logger.error(f"关闭数据库连接池时发生错误: {e}")
        
        self.logger.info("程序已完全停止")

    def run(self):
        try:
            # 校验交易日
            current_date = datetime.now().strftime('%Y%m%d')
            trade_dates = get_trade_dates()
            
            if current_date not in trade_dates:
                self.logger.info(f"当前日期 {current_date} 非交易日，跳过执行")
                return
            
            process_thread = threading.Thread(target=self._process_data_worker)
            process_thread.daemon = True
            process_thread.start()

            stock_list = xtdata.get_stock_list_in_sector('沪深A股')
            self.logger.info(f"订阅 {len(stock_list)} 只股票")
            self.subscription_seq = xtdata.subscribe_whole_quote(stock_list, callback=self.on_tick_data)

            while self.running:
                current_time = datetime.now().time()
                if current_time >= self.stop_time:
                    self.logger.info(f"已到达收盘时间 {self.stop_time}，程序将自动停止")
                    break
                time.sleep(1)

            process_thread.join(timeout=10)

        except Exception as e:
            self.logger.error(f"运行异常: {e}")
            self.stop()

if __name__ == "__main__":
    processor = Level1DataProcessor()
    try:
        processor.run()
    except Exception as e:
        print(f"程序异常: {e}")
    finally:
        processor.stop()
