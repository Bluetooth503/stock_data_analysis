# -*- coding: utf-8 -*-
from common import *
import tushare as ts
from datetime import datetime


class KlineQualityChecker:
    def __init__(self, start_date, end_date):
        self.logger = setup_logger()
        self.start_date = start_date
        self.end_date = end_date
        self.config = load_config()
        self.engine = create_engine(get_pg_connection_string(self.config))
        self.pro = ts.pro_api(self.config.get('tushare', 'token'))
        
    def format_date(self, date_str):
        """将YYYYMMDD格式转换为YYYY-MM-DD HH:MM:SS格式"""
        return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d %H:%M:%S')
        
    def get_trading_days(self):
        """获取交易日历"""
        calendar = self.pro.trade_cal(start_date=self.start_date, end_date=self.end_date)
        trading_days = calendar[calendar['is_open'] == 1]['cal_date'].tolist()
        return trading_days
    
    def check_trading_day_completeness(self):
        """检查交易日数据完整性"""
        self.logger.info("开始检查交易日数据完整性...")
        
        # 获取所有交易日
        trading_days = self.get_trading_days()
        
        # 查询数据库中的交易日
        query = """
            SELECT DISTINCT DATE(trade_time)::date as trade_date
            FROM a_stock_5m_kline_wfq_baostock
            WHERE trade_time >= %(start_date)s AND trade_time <= %(end_date)s
            ORDER BY trade_date
        """
        df = pd.read_sql(query, self.engine, params={
            'start_date': self.format_date(self.start_date),
            'end_date': self.format_date(self.end_date)
        })
        # 确保trade_date列是datetime类型
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        db_days = df['trade_date'].dt.strftime('%Y%m%d').tolist()
        
        # 找出缺失的交易日
        missing_days = set(trading_days) - set(db_days)
        if missing_days:
            self.logger.warning(f"发现缺失的交易日: {sorted(missing_days)}")
        else:
            self.logger.info("交易日数据完整")
            
        return missing_days
    
    def check_stock_completeness(self):
        """检查每只股票的数据完整性"""
        self.logger.info("开始检查股票数据完整性...")
        
        # 获取所有交易日
        trading_days = self.get_trading_days()
        
        # 获取所有股票代码
        query = """
            SELECT DISTINCT ts_code
            FROM a_stock_5m_kline_wfq_baostock
        """
        stocks = pd.read_sql(query, self.engine)['ts_code'].tolist()
        
        # 检查每只股票的数据完整性
        incomplete_stocks = {}
        for stock in tqdm(stocks, desc="检查股票数据完整性"):
            query = """
                SELECT DISTINCT DATE(trade_time)::date as trade_date
                FROM a_stock_5m_kline_wfq_baostock
                WHERE ts_code = %(ts_code)s AND trade_time >= %(start_date)s AND trade_time <= %(end_date)s
            """
            df = pd.read_sql(query, self.engine, params={
                'ts_code': stock,
                'start_date': self.format_date(self.start_date),
                'end_date': self.format_date(self.end_date)
            })
            # 确保trade_date列是datetime类型
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            stock_days = df['trade_date'].dt.strftime('%Y%m%d').tolist()
            
            missing_days = set(trading_days) - set(stock_days)
            if missing_days:
                incomplete_stocks[stock] = sorted(missing_days)
                
        if incomplete_stocks:
            self.logger.warning(f"发现 {len(incomplete_stocks)} 只股票存在数据缺失")
            for stock, days in list(incomplete_stocks.items())[:5]:  # 只显示前5只股票
                self.logger.warning(f"股票 {stock} 缺失交易日: {days[:5]}...")
        else:
            self.logger.info("所有股票数据完整")
            
        return incomplete_stocks
    
    def check_data_anomalies(self):
        """检查数据异常值"""
        self.logger.info("开始检查数据异常值...")
        
        anomalies = []
        
        # 检查价格异常
        query = """
            SELECT ts_code, trade_time, open, high, low, close, volume, amount
            FROM a_stock_5m_kline_wfq_baostock
            WHERE trade_time >= %(start_date)s AND trade_time <= %(end_date)s
            AND (
                open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 OR
                high < low OR
                open > high OR open < low OR
                close > high OR close < low
            )
        """
        price_anomalies = pd.read_sql(query, self.engine, params={
            'start_date': self.format_date(self.start_date),
            'end_date': self.format_date(self.end_date)
        })
        if not price_anomalies.empty:
            self.logger.warning(f"发现 {len(price_anomalies)} 条价格异常数据")
            anomalies.append(price_anomalies)
            
        # 检查成交量异常
        query = """
            SELECT ts_code, trade_time, volume, amount
            FROM a_stock_5m_kline_wfq_baostock
            WHERE trade_time >= %(start_date)s AND trade_time <= %(end_date)s
            AND (volume <= 0 OR amount <= 0)
        """
        volume_anomalies = pd.read_sql(query, self.engine, params={
            'start_date': self.format_date(self.start_date),
            'end_date': self.format_date(self.end_date)
        })
        if not volume_anomalies.empty:
            self.logger.warning(f"发现 {len(volume_anomalies)} 条成交量异常数据")
            anomalies.append(volume_anomalies)
            
        return anomalies
    
    def check_data_consistency(self):
        """检查数据一致性"""
        self.logger.info("开始检查数据一致性...")
        
        # 检查每个交易日的5分钟K线数量
        query = """
            SELECT DATE(trade_time)::date as trade_date, ts_code, COUNT(*) as kline_count
            FROM a_stock_5m_kline_wfq_baostock
            WHERE trade_time >= %(start_date)s AND trade_time <= %(end_date)s
            GROUP BY DATE(trade_time), ts_code
            HAVING COUNT(*) != 48
        """
        inconsistent_data = pd.read_sql(query, self.engine, params={
            'start_date': self.format_date(self.start_date),
            'end_date': self.format_date(self.end_date)
        })
        # 确保trade_date列是datetime类型
        inconsistent_data['trade_date'] = pd.to_datetime(inconsistent_data['trade_date'])
        
        if not inconsistent_data.empty:
            self.logger.warning(f"发现 {len(inconsistent_data)} 条数据不一致记录")
            self.logger.warning("每个交易日应该有48根5分钟K线")
        else:
            self.logger.info("数据一致性检查通过")
            
        return inconsistent_data
    
    def run_all_checks(self):
        """运行所有检查"""
        self.logger.info("开始数据质量检查...")
        
        results = {
            'missing_trading_days': self.check_trading_day_completeness(),
            'incomplete_stocks': self.check_stock_completeness(),
            'data_anomalies': self.check_data_anomalies(),
            'inconsistent_data': self.check_data_consistency()
        }
        
        self.logger.info("数据质量检查完成")
        return results

if __name__ == "__main__":
    # 设置检查的时间范围
    start_date = "20190101"  # 可以根据需要修改
    end_date = "20250321"
    
    checker = KlineQualityChecker(start_date, end_date)
    results = checker.run_all_checks()
