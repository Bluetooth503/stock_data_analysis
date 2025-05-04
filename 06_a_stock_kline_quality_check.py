# -*- coding: utf-8 -*-
from common import *

class KlineQualityChecker:
    def __init__(self, table_name, kline_interval, start_date, end_date):
        """
        初始化K线质量检查器
        Args:
            table_name (str): 要检查的K线数据表名
            kline_interval (str): K线时间粒度，如'5m'、'30m'、'1h'等
            start_date (str): 开始日期，格式：YYYY-MM-DD
            end_date (str): 结束日期，格式：YYYY-MM-DD
        """
        self.logger = setup_logger()
        self.table_name = table_name
        self.kline_interval = kline_interval
        self.start_date = start_date
        self.end_date = end_date
        self.config = load_config()
        self.engine = create_engine(get_pg_connection_string(self.config))

        # 创建输出目录
        self.output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data_quality_reports')
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # 当前日期时间字符串，用于文件名
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 根据K线时间粒度设置每日应有的K线数量
        self.kline_count_map = {
            '5m': 48,   # 每天48根5分钟K线
            '15m': 16,  # 每天16根15分钟K线
            '30m': 8,   # 每天8根30分钟K线
            '1h': 4,    # 每天4根1小时K线
        }
        self.expected_kline_count = self.kline_count_map.get(kline_interval)
        if not self.expected_kline_count:
            raise ValueError(f"不支持的K线时间粒度: {kline_interval}，支持的时间粒度: {list(self.kline_count_map.keys())}")

    def format_date(self, date_str):
        """确保日期格式为YYYY-MM-DD"""
        if '-' not in date_str:
            return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
        return date_str

    def save_to_csv(self, df, check_type, description=None):
        """将异常记录保存到CSV文件
        Args:
            df: 包含异常记录的DataFrame
            check_type: 检查类型，用于文件名
            description: 异常描述，如果提供，将添加到DataFrame中
        """
        if df is None or df.empty:
            return

        # 如果提供了描述，添加到DataFrame中
        if description is not None:
            df['issue_description'] = description

        # 生成文件名
        filename = f"{check_type}_{self.timestamp}.csv"
        filepath = os.path.join(self.output_dir, filename)

        # 保存到CSV
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        self.logger.info(f"已将{len(df)}条{check_type}异常记录保存到: {filepath}")
        return filepath

    def get_trading_days(self):
        """从a_stock_trade_cal表获取交易日历"""
        query = """
            SELECT TO_CHAR(trade_date, 'YYYY-MM-DD') as trade_date
            FROM a_stock_trade_cal
            WHERE trade_date BETWEEN %(start_date)s AND %(end_date)s
            AND is_open = '1'
            ORDER BY trade_date
        """
        df = pd.read_sql(query, self.engine, params={
            'start_date': self.format_date(self.start_date),
            'end_date': self.format_date(self.end_date)
        })
        return df['trade_date'].tolist()

    def check_trading_day_completeness(self):
        """检查交易日数据完整性"""
        self.logger.info("开始检查交易日数据完整性...")

        # 获取所有交易日
        trading_days = self.get_trading_days()

        # 查询数据库中的交易日
        query = f"""
            SELECT DISTINCT DATE(trade_time)::date as trade_date
            FROM {self.table_name}
            WHERE trade_time >= %(start_date)s AND trade_time <= %(end_date)s
            ORDER BY trade_date
        """
        df = pd.read_sql(query, self.engine, params={
            'start_date': self.format_date(self.start_date),
            'end_date': self.format_date(self.end_date) + ' 23:59:59'  # 确保包含当天的所有数据
        })

        # 确保trade_date列是datetime类型
        df['trade_date'] = pd.to_datetime(df['trade_date'])

        # 将数据库中的日期转换为与交易日历相同的格式 (YYYY-MM-DD)
        db_days = df['trade_date'].dt.strftime('%Y-%m-%d').tolist()

        # 调试信息
        self.logger.info(f"数据库中的交易日数量: {len(db_days)}")
        self.logger.info(f"交易日历中的交易日数量: {len(trading_days)}")

        # 找出缺失的交易日
        missing_days = [day for day in trading_days if day not in db_days]

        if missing_days:
            self.logger.warning(f"发现缺失的交易日: {missing_days}")

            # 创建DataFrame并保存到CSV
            missing_df = pd.DataFrame({
                'missing_date': missing_days,
                'formatted_date': [self.format_date(day) for day in missing_days]
            })
            self.save_to_csv(missing_df, 'missing_trading_days', '缺失的交易日')
        else:
            self.logger.info("交易日数据完整")

        return missing_days

    def check_stock_completeness(self):
        """检查每只股票的数据完整性"""
        self.logger.info("开始检查股票数据完整性...")

        # 获取所有交易日
        trading_days = self.get_trading_days()

        # 获取所有股票代码
        query = f"""
            SELECT DISTINCT ts_code
            FROM {self.table_name}
        """
        stocks = pd.read_sql(query, self.engine)['ts_code'].tolist()

        # 检查每只股票的数据完整性
        incomplete_stocks = {}
        for stock in tqdm(stocks, desc="检查股票数据完整性"):
            query = f"""
                SELECT DISTINCT DATE(trade_time)::date as trade_date
                FROM {self.table_name}
                WHERE ts_code = %(ts_code)s AND trade_time >= %(start_date)s AND trade_time <= %(end_date)s
            """
            df = pd.read_sql(query, self.engine, params={
                'ts_code': stock,
                'start_date': self.format_date(self.start_date),
                'end_date': self.format_date(self.end_date) + ' 23:59:59'  # 确保包含当天的所有数据
            })
            # 确保trade_date列是datetime类型
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            stock_days = df['trade_date'].dt.strftime('%Y-%m-%d').tolist()

            missing_days = set(trading_days) - set(stock_days)
            if missing_days:
                incomplete_stocks[stock] = sorted(missing_days)

        if incomplete_stocks:
            self.logger.warning(f"发现 {len(incomplete_stocks)} 只股票存在数据缺失")
            for stock, days in list(incomplete_stocks.items())[:5]:  # 只显示前5只股票
                self.logger.warning(f"股票 {stock} 缺失交易日: {days[:5]}...")

            # 创建DataFrame并保存到CSV
            records = []
            for stock, days in incomplete_stocks.items():
                for day in days:
                    records.append({
                        'ts_code': stock,
                        'missing_date': day,
                        'formatted_date': self.format_date(day)
                    })

            if records:
                incomplete_df = pd.DataFrame(records)
                self.save_to_csv(incomplete_df, 'incomplete_stocks', '股票数据缺失')
        else:
            self.logger.info("所有股票数据完整")

        return incomplete_stocks

    def check_data_anomalies(self):
        """检查数据异常值"""
        self.logger.info("开始检查数据异常值...")

        anomalies = []

        # 检查价格异常
        query = f"""
            SELECT ts_code, trade_time, open, high, low, close, volume, amount
            FROM {self.table_name}
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
            'end_date': self.format_date(self.end_date) + ' 23:59:59'  # 确保包含当天的所有数据
        })
        if not price_anomalies.empty:
            self.logger.warning(f"发现 {len(price_anomalies)} 条价格异常数据")
            anomalies.append(price_anomalies)
            self.save_to_csv(price_anomalies, 'price_anomalies', '价格异常数据')

        # 检查成交量异常
        query = f"""
            SELECT ts_code, trade_time, volume, amount
            FROM {self.table_name}
            WHERE trade_time >= %(start_date)s AND trade_time <= %(end_date)s
            AND (volume <= 0 OR amount <= 0)
        """
        volume_anomalies = pd.read_sql(query, self.engine, params={
            'start_date': self.format_date(self.start_date),
            'end_date': self.format_date(self.end_date) + ' 23:59:59'  # 确保包含当天的所有数据
        })
        if not volume_anomalies.empty:
            self.logger.warning(f"发现 {len(volume_anomalies)} 条成交量异常数据")
            anomalies.append(volume_anomalies)
            self.save_to_csv(volume_anomalies, 'volume_anomalies', '成交量异常数据')

        return anomalies

    def check_data_consistency(self):
        """检查数据一致性"""
        self.logger.info("开始检查数据一致性...")

        # 检查每个交易日的K线数量
        query = f"""
            SELECT DATE(trade_time)::date as trade_date, ts_code, COUNT(*) as kline_count
            FROM {self.table_name}
            WHERE trade_time >= %(start_date)s AND trade_time <= %(end_date)s
            GROUP BY DATE(trade_time), ts_code
            HAVING COUNT(*) != {self.expected_kline_count}
        """
        inconsistent_data = pd.read_sql(query, self.engine, params={
            'start_date': self.format_date(self.start_date),
            'end_date': self.format_date(self.end_date) + ' 23:59:59'  # 确保包含当天的所有数据
        })
        # 确保trade_date列是datetime类型
        inconsistent_data['trade_date'] = pd.to_datetime(inconsistent_data['trade_date'])

        if not inconsistent_data.empty:
            self.logger.warning(f"发现 {len(inconsistent_data)} 条数据不一致记录")
            self.logger.warning(f"每个交易日应该有{self.expected_kline_count}根{self.kline_interval}K线")

            # 保存到CSV
            inconsistent_data['formatted_date'] = inconsistent_data['trade_date'].dt.strftime('%Y-%m-%d')
            self.save_to_csv(inconsistent_data, 'inconsistent_data', 
                           f'每个交易日应该有{self.expected_kline_count}根{self.kline_interval}K线，这些记录不符合要求')
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
            'inconsistent_data': self.check_data_consistency(),
        }

        # 生成汇总报告
        self.generate_summary_report(results)

        self.logger.info("数据质量检查完成")
        return results

    def generate_summary_report(self, results):
        """生成汇总报告
        Args:
            results: 各项检查的结果
        """
        missing_days = results['missing_trading_days']
        incomplete_stocks = results['incomplete_stocks']
        data_anomalies = results['data_anomalies']
        inconsistent_data = results['inconsistent_data']

        # 创建汇总报告文件
        report_filename = f"summary_report_{self.timestamp}.txt"
        report_path = os.path.join(self.output_dir, report_filename)

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(f"A股{self.kline_interval}K线数据质量检查报告\n")
            f.write(f"检查表名: {self.table_name}\n")
            f.write(f"检查时间范围: {self.start_date} 至 {self.end_date}\n")
            f.write(f"\n{'-'*50}\n\n")

            # 缺失交易日报告
            f.write(f"1. 缺失交易日检查\n")
            if missing_days:
                f.write(f"   发现 {len(missing_days)} 个缺失的交易日\n")
                for day in missing_days[:10]:  # 只显示前10个
                    f.write(f"   - {day}\n")
                if len(missing_days) > 10:
                    f.write(f"   ... 等共 {len(missing_days)} 个缺失交易日\n")
            else:
                f.write(f"   交易日数据完整\n")
            f.write(f"\n{'-'*50}\n\n")

            # 股票数据缺失报告
            f.write(f"2. 股票数据完整性检查\n")
            if incomplete_stocks:
                f.write(f"   发现 {len(incomplete_stocks)} 只股票存在数据缺失\n")
                for i, (stock, days) in enumerate(list(incomplete_stocks.items())[:5]):  # 只显示前5只
                    f.write(f"   - 股票 {stock} 缺失 {len(days)} 个交易日\n")
                    for day in days[:3]:  # 每只股票只显示前3个缺失日
                        f.write(f"     * {day}\n")
                    if len(days) > 3:
                        f.write(f"     * ... 等共 {len(days)} 个缺失交易日\n")
                if len(incomplete_stocks) > 5:
                    f.write(f"   ... 等共 {len(incomplete_stocks)} 只股票有数据缺失\n")
            else:
                f.write(f"   所有股票数据完整\n")
            f.write(f"\n{'-'*50}\n\n")

            # 数据异常报告
            f.write(f"3. 数据异常值检查\n")
            if data_anomalies and len(data_anomalies) > 0:
                total_anomalies = sum(len(df) for df in data_anomalies)
                f.write(f"   发现共 {total_anomalies} 条异常数据\n")
                # 如果有价格异常
                if len(data_anomalies) > 0 and not data_anomalies[0].empty:
                    f.write(f"   - 价格异常: {len(data_anomalies[0])} 条\n")
                # 如果有成交量异常
                if len(data_anomalies) > 1 and not data_anomalies[1].empty:
                    f.write(f"   - 成交量异常: {len(data_anomalies[1])} 条\n")
            else:
                f.write(f"   未发现数据异常\n")
            f.write(f"\n{'-'*50}\n\n")

            # 数据一致性报告
            f.write(f"4. 数据一致性检查\n")
            if not isinstance(inconsistent_data, pd.DataFrame) or not inconsistent_data.empty:
                if isinstance(inconsistent_data, pd.DataFrame):
                    f.write(f"   发现 {len(inconsistent_data)} 条数据不一致记录\n")
                    f.write(f"   每个交易日应该有{self.expected_kline_count}根{self.kline_interval}K线\n")
                    # 显示前5条不一致数据
                    for i in range(min(5, len(inconsistent_data))):
                        row = inconsistent_data.iloc[i]
                        f.write(f"   - 股票 {row['ts_code']} 在 {row['formatted_date']} 只有 {row['kline_count']} 根K线\n")
                    if len(inconsistent_data) > 5:
                        f.write(f"   ... 等共 {len(inconsistent_data)} 条不一致记录\n")
            else:
                f.write(f"   数据一致性检查通过\n")

        self.logger.info(f"汇总报告已生成: {report_path}")
        return report_path

if __name__ == "__main__":
    # 设置检查的时间范围和参数
    start_date = "2019-01-01"  # 可以根据需要修改
    end_date = "2025-04-30"
    table_name = "a_stock_30m_kline_wfq_baostock"  # 要检查的表名
    kline_interval = "30m"  # K线时间粒度

    checker = KlineQualityChecker(table_name, kline_interval, start_date, end_date)
    results = checker.run_all_checks()
