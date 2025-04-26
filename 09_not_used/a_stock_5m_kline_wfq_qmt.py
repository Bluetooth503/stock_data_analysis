# -*- coding: utf-8 -*-
"""
A股5分钟K线数据下载与处理模块 (前复权QMT版本)

该模块负责从QMT接口下载A股5分钟K线数据，并将数据处理后存入PostgreSQL数据库。
主要功能包括：
1. 并行下载股票历史数据
2. 数据清洗和标准化
3. 批量写入数据库
"""

from common import *
from xtquant import xtdata
from multiprocessing import Pool


def init_worker():
    """初始化工作进程"""
    xtdata.enable_hello = False

class StockDataConfig:
    """配置类，集中管理所有配置参数"""
    def __init__(self):
        self.stock_list = xtdata.get_stock_list_in_sector('沪深A股')
        self.start_time = '20000101'
        self.end_time = datetime.today().strftime('%Y%m%d')
        self.period = '5m'
        self.table_name = 'a_stock_5m_kline_wfq_qmt'
        self.path = r'E:\国金证券QMT交易端\userdata_mini'
        self.n_processes = 10
        self.chunk_size = 50
        self.batch_size = 100

class DataDownloader:
    """负责数据下载的类"""
    def __init__(self, config: StockDataConfig, logger):
        self.config = config
        self.logger = logger
        xtdata.enable_hello = False

    @staticmethod
    def _download_single_stock(args) -> bool:
        """下载单个股票的数据"""
        stock_code, period, start_time, end_time = args
        try:
            xtdata.download_history_data(stock_code, period=period, start_time=start_time, end_time=end_time)
            return True
        except Exception as e:
            return False

    def download_batch(self) -> int:
        """并行下载多个股票的数据"""
        total_stocks = len(self.config.stock_list)
        total_success = 0
        
        download_args = [
            (code, self.config.period, self.config.start_time, self.config.end_time) 
            for code in self.config.stock_list
        ]
        
        for i in range(0, total_stocks, self.config.chunk_size * self.config.n_processes):
            chunk_end = min(i + self.config.chunk_size * self.config.n_processes, total_stocks)
            current_chunk = download_args[i:chunk_end]
            
            with Pool(self.config.n_processes, initializer=init_worker) as pool:
                results = list(tqdm(
                    pool.imap(self._download_single_stock, current_chunk),
                    total=len(current_chunk),
                    desc=f'下载进度 ({i+1}-{chunk_end}/{total_stocks})'
                ))
            
            success_count = sum(results)
            total_success += success_count
            self.logger.info(f"当前批次完成: {success_count}/{len(current_chunk)} 成功")
        
        self.logger.info(f"全部下载完成: {total_success}/{total_stocks} 只股票成功下载")
        return total_success

class DataProcessor:
    """负责数据处理的类"""
    @staticmethod
    def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """处理单个DataFrame的数据"""
        df['trade_time'] = pd.to_datetime(df['time'].apply(lambda x: datetime.fromtimestamp(x / 1000.0)))
        df = df.drop(columns=['time'])
        df = df.rename(columns={
            'settelementPrice': 'settelement_price',
            'openInterest': 'open_interest',
            'preClose': 'pre_close',
            'suspendFlag': 'suspend_flag'
        })
        
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount', 
                         'settelement_price', 'open_interest', 'pre_close']
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
        df['suspend_flag'] = df['suspend_flag'].astype(bool)
        
        return df[['trade_time', 'ts_code', 'open', 'high', 'low', 'close', 
                  'volume', 'amount', 'settelement_price', 'open_interest',
                  'pre_close', 'suspend_flag']]

    @staticmethod
    def process_batch(stock_batch: List[str], data_dict: Dict) -> Optional[pd.DataFrame]:
        """处理一批股票数据"""
        df_list = []
        for stock_code in stock_batch:
            if stock_code in data_dict:
                stock_df = data_dict[stock_code].copy()
                stock_df['ts_code'] = stock_code
                df_list.append(stock_df)
        
        if not df_list:
            return None
            
        df = pd.concat(df_list, axis=0).reset_index(drop=True)
        return DataProcessor.process_dataframe(df)

class DatabaseManager:
    """负责数据库操作的类"""
    def __init__(self, engine, config: StockDataConfig, logger):
        self.engine = engine
        self.config = config
        self.logger = logger

    def create_table(self) -> None:
        """创建数据表及索引"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS a_stock_5m_kline_wfq_qmt (
            trade_time TIMESTAMP NOT NULL,
            ts_code VARCHAR(20) NOT NULL,
            open NUMERIC(18, 4),
            high NUMERIC(18, 4),
            low NUMERIC(18, 4),
            close NUMERIC(18, 4),
            volume NUMERIC(18, 4),
            amount NUMERIC(18, 4),
            settelement_price NUMERIC(18, 4),
            open_interest NUMERIC(18, 4),
            pre_close NUMERIC(18, 4),
            suspend_flag BOOLEAN,
            PRIMARY KEY (trade_time, ts_code)
        );
        
        CREATE INDEX IF NOT EXISTS idx_stock_5m_kline_ts_code 
        ON a_stock_5m_kline_wfq_qmt(ts_code);
        """
        with self.engine.begin() as conn:
            conn.execute(text(create_table_sql))

    def batch_write(self, data_dict: Dict) -> None:
        """批量写入数据到数据库"""
        stock_codes = list(data_dict.keys())
        total_batches = (len(stock_codes) + self.config.batch_size - 1) // self.config.batch_size
        
        self.logger.info(f"开始批量写入数据库，共{len(stock_codes)}只股票，分{total_batches}批处理")
        
        for i in tqdm(range(0, len(stock_codes), self.config.batch_size), 
                     desc='数据库写入进度', total=total_batches):
            batch = stock_codes[i:i + self.config.batch_size]
            df_batch = DataProcessor.process_batch(batch, data_dict)
            
            if df_batch is not None:
                df_batch = df_batch.sort_values(['ts_code', 'trade_time']).reset_index(drop=True)
                tmp_table = f"temp_{self.config.table_name}_{int(time.time())}"
                insert_sql = f"""
                    INSERT INTO {self.config.table_name}
                    SELECT * FROM {tmp_table}
                    ON CONFLICT (trade_time, ts_code) DO NOTHING
                """
                upsert_data(df_batch, self.config.table_name, tmp_table, insert_sql, self.engine)

class StockDataPipeline:
    """主要数据处理流水线"""
    def __init__(self):
        self.config = StockDataConfig()
        self.logger = setup_logger()
        self.engine = create_engine(
            get_pg_connection_string(load_config()), 
            pool_size=10, 
            max_overflow=20
        )
        self.downloader = DataDownloader(self.config, self.logger)
        self.db_manager = DatabaseManager(self.engine, self.config, self.logger)

    def run(self):
        """运行完整的数据处理流水线"""
        try:
            # 1. 下载数据
            self.logger.info("开始下载股票数据...")
            success_count = self.downloader.download_batch()
            
            if success_count == 0:
                self.logger.error("没有成功下载任何股票数据，程序终止")
                return
            
            # 2. 加载本地数据
            self.logger.info("从本地加载数据...")
            data_dict = xtdata.get_local_data(
                stock_list=self.config.stock_list,
                period=self.config.period,
                start_time=self.config.start_time,
                end_time=self.config.end_time,
                dividend_type='none'
            )
            
            if not data_dict:
                self.logger.error("没有找到本地数据，程序终止")
                return
                
            # 3. 创建数据表
            self.db_manager.create_table()
            
            # 4. 处理并写入数据库
            self.db_manager.batch_write(data_dict)
            
            self.logger.info("数据处理流水线执行完成")
            
        except Exception as e:
            self.logger.error(f"程序执行出错: {str(e)}")
            raise

def main():
    pipeline = StockDataPipeline()
    pipeline.run()

if __name__ == "__main__":
    main()
