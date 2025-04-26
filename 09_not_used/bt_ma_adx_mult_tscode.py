# -*- coding: utf-8 -*-
from common import *
import backtrader as bt
import quantstats_lumi as qs
import multiprocessing as mp
from functools import partial
from deap import base, creator, tools, algorithms
import random
import datetime

#################################
# 参数设置
#################################
# 回测时间范围
START_DATE = '2000-01-01'
END_DATE   = '2024-12-31'

# 参数优化范围
FAST_MA_RANGE = np.arange(5, 35, 5)      # [5, 10, 15, 20, 25, 30]
SLOW_MA_RANGE = np.arange(10, 70, 10)    # [10, 20, 30, 40, 50, 60]
ADX_PERIOD_RANGE = np.arange(10, 35, 5)  # [10, 15, 20, 25, 30]
ADX_THRESH_RANGE = np.arange(20, 45, 5)  # [20, 25, 30, 35, 40]

# 遗传算法参数
POPULATION_SIZE = 50    # 种群大小
N_GENERATIONS = 20      # 迭代代数
P_CROSSOVER = 0.8      # 交叉概率
P_MUTATION = 0.2       # 变异概率
TOURNAMENT_SIZE = 3    # 锦标赛选择大小

# 并行处理参数
MAX_PROCESSES = max(1, mp.cpu_count() - 1)  # 保留一个CPU核心

# 止损止盈参数
STOP_LOSS_PCT = 0.05   # 5%止损
TAKE_PROFIT_PCT = 0.20 # 20%止盈

#################################

# 创建数据库连接
config = load_config()
engine = create_engine(get_pg_connection_string(config))

# 创建遗传算法的适应度类和个体类
creator.create("FitnessMax", base.Fitness, weights=(1.0,))  # 最大化适应度
creator.create("Individual", list, fitness=creator.FitnessMax)

def calculate_indicators(df, fast_ma, slow_ma, adx_period):
    """计算技术指标"""
    try:
        # 确保数据类型正确
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        
        # 计算双均线
        df[f'ma_fast'] = df['close'].rolling(window=fast_ma).mean()
        df[f'ma_slow'] = df['close'].rolling(window=slow_ma).mean()
        
        # 计算ADX指标
        if adx_period < 1:
            raise ValueError(f"无效的ADX周期: {adx_period}")
            
        # 使用ta-lib计算ADX指标
        adx = df.ta.adx(high='high', low='low', close='close', length=adx_period)
        
        # 检查ADX计算结果
        if adx is None:
            raise ValueError(f"ADX计算失败，返回为None")
            
        # 获取ADX列名
        adx_cols = [col for col in adx.columns if 'ADX' in col.upper()]
        dmp_cols = [col for col in adx.columns if 'DMP' in col.upper() or 'DI+' in col.upper()]
        dmn_cols = [col for col in adx.columns if 'DMN' in col.upper() or 'DI-' in col.upper()]
        
        if not (adx_cols and dmp_cols and dmn_cols):
            raise ValueError(f"未找到ADX相关列: ADX={adx_cols}, DMP={dmp_cols}, DMN={dmn_cols}")
        
        # 赋值给对应的列
        df['ADX'] = adx[adx_cols[0]]
        df['DI_plus'] = adx[dmp_cols[0]]
        df['DI_minus'] = adx[dmn_cols[0]]
        
        # 删除包含NaN的行
        df.dropna(subset=['ADX', 'DI_plus', 'DI_minus', 'ma_fast', 'ma_slow'], inplace=True)
        
        # 检查是否有足够的有效数据
        min_required = max(fast_ma, slow_ma, adx_period) * 2
        if len(df) < min_required:
            raise ValueError(f"有效数据不足: {len(df)} 行，需要至少 {min_required} 行")
            
        # 重置索引以确保数据连续
        df = df.reset_index()
        
        return df
        
    except Exception as e:
        print(f"[错误] 计算技术指标: {str(e)}")
        raise

class MAData(bt.feeds.PandasData):
    """自定义数据源"""
    lines = ('ma_fast', 'ma_slow', 'ADX', 'DI_plus', 'DI_minus',)
    params = (
        ('datetime', None),
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'volume'),
        ('ma_fast', 'ma_fast'),
        ('ma_slow', 'ma_slow'),
        ('ADX', 'ADX'),
        ('DI_plus', 'DI_plus'),
        ('DI_minus', 'DI_minus'),
        ('openinterest', None),
    )

class MAAdxStrategy(bt.Strategy):
    """双均线+ADX策略"""
    params = (
        ('fast_ma', 10),
        ('slow_ma', 20),
        ('adx_period', 14),
        ('adx_threshold', 25),
        ('stop_loss_pct', STOP_LOSS_PCT),
        ('take_profit_pct', TAKE_PROFIT_PCT),
    )

    def __init__(self):
        self.ma_fast = self.data.ma_fast
        self.ma_slow = self.data.ma_slow
        self.adx = self.data.ADX
        self.di_plus = self.data.DI_plus
        self.di_minus = self.data.DI_minus
        
        self.order = None
        self.buy_price = None
        self.stop_loss = None
        self.take_profit = None

    def next(self):
        if self.order:
            return

        if not self.position:
            # 买入条件：金叉 + ADX确认 + DI+大于DI-
            if (self.ma_fast[0] > self.ma_slow[0] and 
                self.adx[0] > self.p.adx_threshold and 
                self.di_plus[0] > self.di_minus[0]):
                
                size = int((self.broker.get_cash() * 0.95) / self.data.close[0])
                self.order = self.buy(size=size)
                self.buy_price = self.data.close[0]
                self.stop_loss = self.buy_price * (1 - self.p.stop_loss_pct)
                self.take_profit = self.buy_price * (1 + self.p.take_profit_pct)
        
        else:
            # 止损检查
            if self.data.close[0] <= self.stop_loss:
                self.order = self.close()
                return
            
            # 止盈检查
            if self.data.close[0] >= self.take_profit:
                self.order = self.close()
                return
            
            # 卖出条件：死叉 + ADX确认 + DI-大于DI+
            if (self.ma_fast[0] < self.ma_slow[0] and
                self.adx[0] > self.p.adx_threshold and 
                self.di_minus[0] > self.di_plus[0]):
                
                self.order = self.close()

def evaluate_strategy(individual, df):
    """评估策略的适应度函数"""
    fast_ma, slow_ma, adx_period, adx_threshold = individual
    
    # 检查参数有效性
    if fast_ma >= slow_ma or adx_period < 1:
        print(f"[警告] 无效参数: fast_ma={fast_ma}, slow_ma={slow_ma}, adx_period={adx_period}")
        return (-np.inf,)
        
    try:
        # 计算技术指标
        df_copy = df.copy()
        df_copy = calculate_indicators(df_copy, fast_ma, slow_ma, adx_period)
        
        # 运行回测
        cerebro = bt.Cerebro()
        data = MAData(dataname=df_copy)
        cerebro.adddata(data)
        cerebro.broker.setcash(100000)
        cerebro.broker.setcommission(commission=0.0003)
        cerebro.addstrategy(MAAdxStrategy,
                           fast_ma=fast_ma,
                           slow_ma=slow_ma,
                           adx_period=adx_period,
                           adx_threshold=adx_threshold)
        cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn', timeframe=bt.TimeFrame.Days)
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
        
        results = cerebro.run()
        strat = results[0]
        
        # 检查是否有交易
        trade_analysis = strat.analyzers.trades.get_analysis()
        total_trades = trade_analysis.get('total', {}).get('total', 0)
        if not total_trades:
            return (-np.inf,)
        
        # 获取收益序列
        returns_dict = strat.analyzers.timereturn.get_analysis()
        if not returns_dict:
            return (-np.inf,)
            
        # 将收益字典转换为Series
        returns = pd.Series()
        for k, v in returns_dict.items():
            try:
                # 如果k是datetime对象，直接使用
                if isinstance(k, datetime.datetime):
                    dt = k
                # 如果k是日期字符串，转换为datetime
                elif isinstance(k, str):
                    dt = pd.to_datetime(k)
                # 如果k是时间戳（整数或浮点数），转换为datetime
                elif isinstance(k, (int, float)):
                    dt = pd.Timestamp(k, unit='s')  # 假设时间戳是秒
                else:
                    print(f"[警告] 跳过未知类型的时间戳: {type(k)}")
                    continue
                    
                returns[dt] = float(v)  # 确保收益率是浮点数
            except Exception as e:
                print(f"[警告] 处理时间戳时出错 {k}: {str(e)}")
                continue
        
        # 检查收益序列是否为空
        if len(returns) == 0:
            print("[警告] 收益序列为空")
            return (-np.inf,)
        
        # 确保索引是日期时间类型
        if not isinstance(returns.index, pd.DatetimeIndex):
            print("[警告] 收益序列索引不是日期时间类型，尝试转换...")
            returns.index = pd.to_datetime(returns.index)
        
        # 将收益聚合为日度收益
        daily_returns = returns.groupby(returns.index.date).apply(lambda x: (1 + x).prod() - 1)
        daily_returns.index = pd.to_datetime(daily_returns.index)
        
        # 检查是否有足够的交易日
        if len(daily_returns) < 30:  # 至少需要30个交易日
            print(f"[警告] 交易日数不足: {len(daily_returns)} < 30")
            return (-np.inf,)
        
        # 计算夏普比率
        sharpe = qs.stats.sharpe(daily_returns, rf=0.0, periods=252, annualize=True)
        
        # 如果夏普比率无效，返回最差分数
        if np.isnan(sharpe) or np.isinf(sharpe):
            print("[警告] 无效的夏普比率")
            return (-np.inf,)
            
        return (sharpe,)
        
    except Exception as e:
        print(f"[错误] 评估策略: {str(e)}")
        return (-np.inf,)

def mutate_params(individual):
    """自定义变异操作"""
    # 随机选择一个参数进行变异
    idx = random.randint(0, len(individual) - 1)
    if idx == 0:  # fast_ma
        individual[idx] = int(random.choice(FAST_MA_RANGE))
    elif idx == 1:  # slow_ma
        individual[idx] = int(random.choice([x for x in SLOW_MA_RANGE if x > individual[0]]))
    elif idx == 2:  # adx_period
        individual[idx] = int(random.choice(ADX_PERIOD_RANGE))
    else:  # adx_threshold
        individual[idx] = int(random.choice(ADX_THRESH_RANGE))
    return individual,

def optimize_stock(stock_code):
    """对单个股票进行参数优化"""
    try:
        print(f'[开始] 处理股票: {stock_code}')
        
        # 创建reports目录（如果不存在）
        os.makedirs('reports', exist_ok=True)
        
        # 获取股票数据
        df = get_30m_kline_data('wfq', stock_code, START_DATE, END_DATE)
        
        # 检查数据是否为空
        if df is None or len(df) == 0:
            print(f"[错误] 股票 {stock_code} 数据为空")
            return None
            
        # 检查必要的列是否存在
        required_columns = ['trade_time', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"[错误] 股票 {stock_code} 缺少列: {missing_columns}")
            return None
            
        # 数据预处理
        df['trade_time'] = pd.to_datetime(df['trade_time'])
        df.set_index('trade_time', inplace=True)
        
        print(f"[信息] {stock_code} 数据范围: {df.index.min()} 到 {df.index.max()}, 行数: {len(df)}")
        
        # 获取基准数据
        benchmark_sql = """
        SELECT trade_time, close 
        FROM a_index_1day_kline_baostock 
        WHERE ts_code = '000300.SH' 
        AND trade_time BETWEEN %s AND %s 
        ORDER BY trade_time
        """
        benchmark_df = pd.read_sql(benchmark_sql, engine, params=(START_DATE, END_DATE))
        
        # 检查基准数据
        if len(benchmark_df) == 0:
            print("[错误] 基准数据为空")
            return None
            
        benchmark_df['trade_time'] = pd.to_datetime(benchmark_df['trade_time'])
        benchmark_df.set_index('trade_time', inplace=True)
        benchmark_returns = benchmark_df['close'].pct_change()
        benchmark_returns.name = '000300.SH'

        # 设置遗传算法工具箱
        toolbox = base.Toolbox()
        
        # 修改个体生成函数，确保生成有效的参数
        def create_valid_fast_ma():
            return int(random.choice(FAST_MA_RANGE))
            
        def create_valid_slow_ma(fast_ma):
            valid_slow_ma = [x for x in SLOW_MA_RANGE if x > fast_ma]
            if not valid_slow_ma:  # 如果没有有效的slow_ma值
                return int(max(SLOW_MA_RANGE))  # 返回最大值
            return int(random.choice(valid_slow_ma))
            
        def create_valid_adx_period():
            return int(random.choice(ADX_PERIOD_RANGE))
            
        def create_valid_adx_threshold():
            return int(random.choice(ADX_THRESH_RANGE))
            
        def create_individual():
            fast_ma = create_valid_fast_ma()
            slow_ma = create_valid_slow_ma(fast_ma)
            adx_period = create_valid_adx_period()
            adx_threshold = create_valid_adx_threshold()
            return [fast_ma, slow_ma, adx_period, adx_threshold]
        
        # 注册参数生成函数
        toolbox.register("individual", tools.initIterate, creator.Individual, create_individual)
        toolbox.register("population", tools.initRepeat, list, toolbox.individual)
        
        # 注册遗传算法操作
        toolbox.register("evaluate", evaluate_strategy, df=df)
        toolbox.register("mate", tools.cxTwoPoint)
        toolbox.register("mutate", mutate_params)  # 使用自定义变异操作
        toolbox.register("select", tools.selTournament, tournsize=TOURNAMENT_SIZE)
        
        # 创建初始种群
        print(f'[信息] 开始 {stock_code} 参数优化')
        pop = toolbox.population(n=POPULATION_SIZE)
        
        # 验证初始种群的参数
        for ind in pop:
            if not (5 <= ind[0] <= 30 and ind[0] < ind[1] <= 60 and 10 <= ind[2] <= 30 and 20 <= ind[3] <= 40):
                print(f"[警告] 无效参数组合: {ind}")
                
        hof = tools.HallOfFame(5)  # 保存5个最优个体
        stats = tools.Statistics(lambda ind: ind.fitness.values)
        stats.register("avg", np.mean)
        stats.register("min", np.min)
        stats.register("max", np.max)
        
        # 运行遗传算法
        pop, logbook = algorithms.eaSimple(pop, toolbox,
                                         cxpb=P_CROSSOVER,
                                         mutpb=P_MUTATION,
                                         ngen=N_GENERATIONS,
                                         stats=stats,
                                         halloffame=hof,
                                         verbose=True)
        
        # 获取最优参数
        if not hof:
            print(f'未能找到有效的参数组合: {stock_code}')
            return None
            
        best_params = {
            'fast_ma': hof[0][0],
            'slow_ma': hof[0][1],
            'adx_period': hof[0][2],
            'adx_threshold': hof[0][3]
        }
        best_score = hof[0].fitness.values[0]
        
        print(f'最优参数组合: {best_params}, 夏普比率: {best_score:.4f}')
        print('\n前5个最佳参数组合:')
        for idx, ind in enumerate(hof):
            params = {
                'fast_ma': ind[0],
                'slow_ma': ind[1],
                'adx_period': ind[2],
                'adx_threshold': ind[3]
            }
            score = ind.fitness.values[0]
            print(f"参数: {params}, 夏普比率: {score:.4f}")
        print()
        
        # 使用最优参数进行回测
        cerebro = bt.Cerebro()
        
        # 计算最优参数的指标
        final_df = df.copy()
        final_df = calculate_indicators(
            final_df,
            best_params['fast_ma'],
            best_params['slow_ma'],
            best_params['adx_period']
        )
        
        data = MAData(dataname=final_df)
        cerebro.adddata(data)
        cerebro.broker.setcash(100000)
        cerebro.broker.setcommission(commission=0.0003)
        cerebro.addstrategy(MAAdxStrategy,
                           fast_ma=best_params['fast_ma'],
                           slow_ma=best_params['slow_ma'],
                           adx_period=best_params['adx_period'],
                           adx_threshold=best_params['adx_threshold'])
        cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn')
        
        results = cerebro.run()
        strat = results[0]
        returns = pd.Series(strat.analyzers.timereturn.get_analysis())
        
        # 将30分钟收益聚合为日度收益
        returns.index = pd.to_datetime(returns.index)
        daily_returns = (1 + returns).groupby(returns.index.date).prod() - 1
        daily_returns.index = pd.to_datetime(daily_returns.index)
        daily_returns.name = 'MA_ADX'
        
        # 使用已获取的基准数据
        benchmark_returns = benchmark_returns

        # 确保两个时间序列的索引对齐
        aligned_dates = daily_returns.index.intersection(benchmark_returns.index)
        daily_returns = daily_returns[aligned_dates]
        benchmark_returns = benchmark_returns[aligned_dates]
        
        # 删除任何包含NaN的数据
        valid_data = ~(daily_returns.isna() | benchmark_returns.isna())
        daily_returns = daily_returns[valid_data]
        benchmark_returns = benchmark_returns[valid_data]
        
        # 确保数据类型一致
        daily_returns = daily_returns.astype(float)
        benchmark_returns = benchmark_returns.astype(float)
        
        # 生成quantstats报告
        report_title = f'{stock_code} MA+ADX策略回测报告 (基准: 沪深300)'
        report_file = os.path.join('reports', f'{stock_code}_report.html')
        
        try:
            qs.reports.html(
                daily_returns, 
                benchmark=benchmark_returns,
                output=report_file,
                title=report_title,
                download_filename=f'{stock_code}_report.html'
            )
            print(f'已生成 {stock_code} 的策略报告: {report_file}')
        except Exception as e:
            print(f'生成 {stock_code} 的策略报告时发生错误: {str(e)}')
        
        # 计算指标
        metrics = {
            'stock_code': stock_code,
            'fast_ma': best_params['fast_ma'],
            'slow_ma': best_params['slow_ma'],
            'adx_period': best_params['adx_period'],
            'adx_threshold': best_params['adx_threshold'],
            # 风险调整收益
            'sharpe': qs.stats.sharpe(daily_returns, rf=0.0, periods=252, annualize=True),
            'sortino': qs.stats.sortino(daily_returns),
            'calmar': qs.stats.calmar(daily_returns),
            'adjusted_sortino': qs.stats.adjusted_sortino(daily_returns),
            'gain_to_pain_ratio': qs.stats.gain_to_pain_ratio(daily_returns),
            'risk_of_ruin': qs.stats.risk_of_ruin(daily_returns),
            'risk_return_ratio': qs.stats.risk_return_ratio(daily_returns),
            # 胜率 & 盈亏比
            'win_rate': qs.stats.win_rate(daily_returns) * 100,
            'profit_factor': qs.stats.profit_factor(daily_returns),
            'profit_ratio': qs.stats.profit_ratio(daily_returns),
            'win_loss_ratio': qs.stats.win_loss_ratio(daily_returns),
            'payoff_ratio': qs.stats.payoff_ratio(daily_returns),
            'consecutive_losses': qs.stats.consecutive_losses(daily_returns),
            'consecutive_wins': qs.stats.consecutive_wins(daily_returns),
            # 收益相关指标
            'avg_return': qs.stats.avg_return(daily_returns) * 100,
            'avg_win': qs.stats.avg_win(daily_returns) * 100,
            'avg_loss': qs.stats.avg_loss(daily_returns) * 100,
            'best': qs.stats.best(daily_returns) * 100,
            'worst': qs.stats.worst(daily_returns) * 100,
            'expected_return': qs.stats.expected_return(daily_returns) * 100,
            'expected_shortfall': qs.stats.expected_shortfall(daily_returns),
            'rar': qs.stats.rar(daily_returns),
            # 风险 & 回撤
            'volatility': qs.stats.volatility(daily_returns) * 100,
            'max_drawdown': qs.stats.max_drawdown(daily_returns) * 100,
            'ulcer_index': qs.stats.ulcer_index(daily_returns),
            'ulcer_performance_index': qs.stats.ulcer_performance_index(daily_returns),
            'value_at_risk': qs.stats.value_at_risk(daily_returns),
            'tail_ratio': qs.stats.tail_ratio(daily_returns),
            'recovery_factor': qs.stats.recovery_factor(daily_returns),
            # 年化收益 & 复利相关
            'cagr': qs.stats.cagr(daily_returns) * 100,
            'compsum': qs.stats.compsum(daily_returns).iloc[-1] * 100,
            # 统计指标
            'skew': qs.stats.skew(daily_returns),
            'kurtosis': qs.stats.kurtosis(daily_returns),
            'outlier_loss_ratio': qs.stats.outlier_loss_ratio(daily_returns),
            'outlier_win_ratio': qs.stats.outlier_win_ratio(daily_returns),
            'geometric_mean': qs.stats.geometric_mean(daily_returns) * 100,
            # 组合管理 & 风险衡量
            'cpc_index': qs.stats.cpc_index(daily_returns),
            'kelly_criterion': qs.stats.kelly_criterion(daily_returns),
            'common_sense_ratio': qs.stats.common_sense_ratio(daily_returns),
            'exposure': qs.stats.exposure(daily_returns) * 100,
            'ghpr': qs.stats.ghpr(daily_returns),
            # 需要基准数据的指标
            'information_ratio': qs.stats.information_ratio(daily_returns, benchmark_returns),
            'r_squared': qs.stats.r_squared(daily_returns, benchmark_returns)
        }
        
        print(f'股票 {stock_code} 处理完成')
        return metrics
        
    except Exception as e:
        print(f'处理股票 {stock_code} 时发生错误: {str(e)}')
        return None

def main():
    # 读取股票列表
    try:
        stock_list_df = pd.read_csv('上证50_stock_list.csv', header=None, names=['ts_code']).head(5)
        stock_codes = stock_list_df['ts_code'].tolist()
        print(f'共读取到 {len(stock_codes)} 只股票')
    except Exception as e:
        print(f'读取股票列表时发生错误: {str(e)}')
        return

    # 创建进程池
    pool = mp.Pool(processes=MAX_PROCESSES)
    
    try:
        # 并行处理所有股票
        results = []
        for result in pool.imap_unordered(optimize_stock, stock_codes):
            if result is not None:
                results.append(result)
                
        # 关闭进程池
        pool.close()
        pool.join()
        
        # 将结果保存到CSV
        if results:
            results_df = pd.DataFrame(results)
            try:
                # 尝试保存到当前目录
                results_df.to_csv('MA_ADX_Strategy_Metrics.csv', index=False)
                print(f'结果已保存到 MA_ADX_Strategy_Metrics.csv，共处理成功 {len(results)} 只股票')
            except PermissionError:
                # 如果当前目录无写入权限，尝试保存到reports目录
                output_path = os.path.join('reports', 'MA_ADX_Strategy_Metrics.csv')
                results_df.to_csv(output_path, index=False)
                print(f'结果已保存到 {output_path}，共处理成功 {len(results)} 只股票')
        else:
            print('没有成功处理任何股票')
            
    except Exception as e:
        print(f'处理过程中发生错误: {str(e)}')
        pool.close()
        pool.join()

if __name__ == '__main__':
    main()
