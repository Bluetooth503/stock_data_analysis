# -*- coding: utf-8 -*-
from common import *
from xtquant import xtdata
from multiprocessing import Pool
from sqlalchemy import text
import schedule
xtdata.enable_hello = False
import pandas as pd
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.width', 180)


# 读取配置文件
config = load_config()
engine = create_engine(get_pg_connection_string(config))

def get_top_stocks():
    """从数据库获取表现最好的股票"""
    query = """
    SELECT ts_code, period, multiplier, sharpe, sortino, win_rate, profit_factor
    FROM heikin_ashi_supertrend_metrics
    ORDER BY sortino DESC
    LIMIT 30
    """
    df = pd.read_sql(query, engine)
    return df


def check_signal_change(df, code):
    """检查direction的变化，返回信号类型"""
    if len(df) < 2:
        return None
    last_two = df.tail(2)
    if last_two['direction'].iloc[0] == -1 and last_two['direction'].iloc[1] == 1:
        return 'BUY'
    elif last_two['direction'].iloc[0] == 1 and last_two['direction'].iloc[1] == -1:
        return 'SELL'
    return None


def check_if_notified(engine, trade_time, ts_code):
    """检查是否已经发送过通知"""
    query = "SELECT EXISTS (SELECT 1 FROM signal_notifications WHERE trade_time = %s AND ts_code = %s)"
    with engine.connect() as conn:
        result = conn.execute(text(query), [trade_time, ts_code]).scalar()
    return result

def add_notification_record(engine, trade_time, ts_code):
    """添加通知记录"""
    query = "INSERT INTO signal_notifications (trade_time, ts_code) VALUES (%s, %s) ON CONFLICT (trade_time, ts_code) DO NOTHING"
    with engine.connect() as conn:
        conn.execute(text(query), [trade_time, ts_code])
        conn.commit()

def process_stock_data(args):
    """处理单个股票的数据"""
    code, stock_data, stock_params = args
    
    # 在进程内创建数据库连接
    config = load_config()
    engine = create_engine(get_pg_connection_string(config))
    
    stock_data['trade_time'] = pd.to_datetime(stock_data['time'].apply(lambda x: datetime.fromtimestamp(x / 1000.0)))
    stock_data = heikin_ashi(stock_data)
    stock_data = supertrend(stock_data, stock_params['period'], stock_params['multiplier'])
    print(f"{code}\n",stock_data[['trade_time','direction']].tail(2).to_string(index=False),"\n")
    
    # 检查信号
    signal = check_signal_change(stock_data, code)
    if signal:
        trade_time = stock_data['trade_time'].iloc[-1]
        
        # 检查是否已经通知过该组合
        if not check_if_notified(engine, trade_time, code):
            signal_type = "买入" if signal == "BUY" else "卖出"
            subject = f"{code} - {signal_type}信号"
            content = f"""
            信号类型: {signal_type}
            股票代码: {code}
            信号时间: {trade_time}
            当前价格: {stock_data['close'].iloc[-1]}
            Sortino比率: {stock_params['sortino']}
            胜率: {stock_params['win_rate']}
            盈亏比: {stock_params['profit_factor']}
            """
            # 发送通知
            send_notification(subject, content)
            add_notification_record(engine, trade_time, code)
            return (code, signal_type, subject, content)
    return None

def run_market_analysis():
    """执行市场分析的主函数"""
    print("开始执行市场分析...")
    
    # 获取合成周期数据
    df = xtdata.get_market_data_ex([], code_list, period='30m', start_time='20240101')
    
    # 准备并行处理的参数
    process_args = []
    for code in code_list:
        if code in df:
            stock_params = top_stocks[top_stocks['ts_code'] == code].iloc[0].to_dict()
            process_args.append((code, df[code], stock_params))
    
    # 并行处理所有股票数据
    results = pool.map(process_stock_data, process_args)
    
    # 处理信号结果
    for result in results:
        if result:
            code, signal_type, subject, content = result
            print(f"发现{signal_type}信号: {code}")
            print(content)

if __name__ == "__main__":
    # 获取股票列表
    top_stocks = get_top_stocks()
    code_list = top_stocks['ts_code'].tolist()
    
    # 下载基础周期数据
    for code in code_list:
        xtdata.download_history_data(code, period='5m', incrementally=True)
        xtdata.subscribe_quote(code, '5m')

    # 创建进程池
    pool = Pool(processes=10)

    # 设置定时任务
    for hour in range(9, 15):   # 9点到15点
        for minute in [0, 30]:  # 每个小时的00分和30分
            schedule_time = f"{hour:02d}:{minute:02d}:01"
            schedule.every().day.at(schedule_time).do(run_market_analysis)
    
    # 运行定时任务
    while True:
        now = datetime.now()
        now_time = now.strftime('%H%M%S')
        
        # 只在交易时间内运行
        if '093000' <= now_time <= '150000':
            schedule.run_pending()
        
        time.sleep(1)

    pool.close()
    pool.join()
    xtdata.run()
