# -*- coding: utf-8 -*-
from common import *
import tushare as ts
import traceback
import schedule

# ================================= 配置 =================================
run_time = "17:00"
n_days = 5
table_name = "a_stock_trade_dates"

# ================================= 配置日志 =================================
logger = setup_logger()

# ================================= 更新交易日期 =================================
def update_trade_dates(n_days=30, table_name="a_stock_trade_dates"):
    """从 Tushare 获取交易日期并保存到数据库"""
    try:
        logger.info(f"开始获取最近{n_days}个交易日并保存到数据库...")

        # 加载配置
        config = load_config()
        token = config.get('tushare', 'token')
        pro = ts.pro_api(token)

        # 设置日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')  # 获取更多天数以确保有足够的交易日

        # 获取交易日历
        calendar = pro.trade_cal(start_date=start_date, end_date=end_date)

        # 筛选交易日并按日期降序排序
        trade_dates = calendar[calendar['is_open'] == 1]['cal_date'].sort_values(ascending=False)

        # 获取最近N个交易日
        latest_dates = trade_dates[:n_days].tolist()

        # 转换日期格式为 yyyy-MM-dd
        formatted_dates = [datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d') for date in latest_dates]

        if not formatted_dates:
            logger.warning("没有获取到交易日期，更新失败")
            return

        logger.info(f"获取到最近{n_days}个交易日: {formatted_dates}")

        # 创建DataFrame，只包含trade_date列
        df = pd.DataFrame({
            'trade_date': [datetime.strptime(date, '%Y-%m-%d').date() for date in formatted_dates]
        })

        # 获取数据库连接
        engine = create_engine(get_pg_connection_string(config))

        # 保存到数据库（先清空再插入，避免删除表结构）
        with engine.begin() as conn:
            # 清空表中的数据
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            # 插入新数据
            df.to_sql(table_name, conn, if_exists='append', index=False)

        logger.info(f"成功更新{len(formatted_dates)}个交易日期到数据库表 {table_name}")

        # 刷新物化视图
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT refresh_stock_5days_avg_volume_trans()"))
            logger.info("成功刷新物化视图 refresh_stock_5days_avg_volume_trans")
        except Exception as e:
            error_msg = f"刷新物化视图过程中发生错误: {str(e)}"
            logger.error(error_msg)
            send_notification_wecom("刷新物化视图错误", error_msg)

    except Exception as e:
        error_msg = f"更新交易日期过程中发生错误: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_msg)
        send_notification_wecom("更新交易日期错误", error_msg)

# ================================= 主函数 =================================
def main():
    """主函数"""
    logger.info(f"交易日期更新器已启动，将在每天 {run_time} 运行")

    # 设置定时任务
    schedule.every().day.at(run_time).do(update_trade_dates, n_days, table_name)

    # 启动时先运行一次
    logger.info("首次运行更新...")
    update_trade_dates(n_days, table_name)

    # 持续运行定时任务
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次

if __name__ == "__main__":
    main()
