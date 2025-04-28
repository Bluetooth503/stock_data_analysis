from common import *

# ================================= 记录日志 =================================
logger = setup_logger()

# ================================= 执行 =================================
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
def run_script(script_name):
    script_path = os.path.join(BASE_PATH, script_name)
    if not os.path.exists(script_path):
        logger.error(f"错误: 脚本文件 {script_path} 不存在")
        return
    subprocess.run([sys.executable, script_path])

# ================================= 设置定时计划 =================================
schedule.every().day.at("09:00").do(run_script, "01_a_stock_level1_data.py")
schedule.every().day.at("16:30").do(run_script, "01_ods_a_stock_level1_data.py")

# 其他定时任务示例
# schedule.every(10).minutes.do(run_script, "your_script.py")
# schedule.every().hour.do(run_script, "hourly_task.py")
# schedule.every().monday.at("12:30").do(run_script, "weekly_report.py")

# ================================= 一直运行 =================================
while True:
    schedule.run_pending()
    time.sleep(1)