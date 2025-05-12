from common import *

# ================================= 记录日志 =================================
logger = setup_logger()

# ================================= 任务状态管理 =================================
class TaskManager:
    def __init__(self):
        self.task_dependencies: Dict[str, List[str]] = {}
        self.completed_tasks: Set[str] = set()
        self.task_last_run: Dict[str, datetime] = {}

    def add_dependency(self, task: str, depends_on: List[str]) -> None:
        self.task_dependencies[task] = depends_on

    def mark_completed(self, task: str) -> None:
        self.completed_tasks.add(task)
        self.task_last_run[task] = datetime.now()

    def can_run_task(self, task: str) -> bool:
        if task not in self.task_dependencies:
            return True
        return all(dep in self.completed_tasks for dep in self.task_dependencies[task])

    def reset_daily(self) -> None:
        self.completed_tasks.clear()

task_manager = TaskManager()

# ================================= 执行 =================================
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
def run_script_no_check(script_name, conda_env=None):
    """执行脚本，不检查任务依赖状态"""
    script_path = os.path.join(BASE_PATH, script_name)
    if not os.path.exists(script_path):
        logger.error(f"错误: 脚本文件 {script_path} 不存在")
        return

    # 使用指定的conda环境或当前环境
    conda_prefix = os.environ.get('CONDA_PREFIX', '')
    if conda_env:
        # 检查CONDA_PREFIX是否已经包含了指定的环境
        if conda_prefix.endswith(os.path.join('envs', conda_env)):
            conda_python = os.path.join(conda_prefix, 'python.exe')
        else:
            conda_python = os.path.join(conda_prefix, 'envs', conda_env, 'python.exe')
    else:
        conda_python = os.path.join(conda_prefix, 'python.exe')

    if not os.path.exists(conda_python):
        logger.error(f"错误: 未找到conda环境中的Python解释器: {conda_python}")
        return

    try:
        result = subprocess.run([conda_python, script_path], check=True)
        if result.returncode == 0:
            logger.info(f"任务 {script_name} 执行成功")
        else:
            logger.error(f"任务 {script_name} 执行失败")
    except subprocess.CalledProcessError as e:
        logger.error(f"任务 {script_name} 执行出错: {str(e)}")

def run_script(script_name, conda_env=None):
    """执行脚本，检查任务依赖状态"""
    # 检查任务依赖
    if not task_manager.can_run_task(script_name):
        logger.info(f"任务 {script_name} 的依赖任务尚未完成，暂不执行")
        return

    script_path = os.path.join(BASE_PATH, script_name)
    if not os.path.exists(script_path):
        logger.error(f"错误: 脚本文件 {script_path} 不存在")
        return

    # 使用指定的conda环境或当前环境
    conda_prefix = os.environ.get('CONDA_PREFIX', '')
    if conda_env:
        # 检查CONDA_PREFIX是否已经包含了指定的环境
        if conda_prefix.endswith(os.path.join('envs', conda_env)):
            conda_python = os.path.join(conda_prefix, 'python.exe')
        else:
            conda_python = os.path.join(conda_prefix, 'envs', conda_env, 'python.exe')
    else:
        conda_python = os.path.join(conda_prefix, 'python.exe')

    if not os.path.exists(conda_python):
        logger.error(f"错误: 未找到conda环境中的Python解释器: {conda_python}")
        return

    try:
        result = subprocess.run([conda_python, script_path], check=True)
        if result.returncode == 0:
            logger.info(f"任务 {script_name} 执行成功")
            task_manager.mark_completed(script_name)
        else:
            logger.error(f"任务 {script_name} 执行失败")
    except subprocess.CalledProcessError as e:
        logger.error(f"任务 {script_name} 执行出错: {str(e)}")

# ================================= 配置任务依赖 =================================
# 设置任务依赖关系
# task_manager.add_dependency("04_qmt_monitor_ha_st_30m.py", ["01_a_stock_level1_data.py"])
# task_manager.add_dependency("01_ods_a_stock_level1_data.py", ["01_a_stock_level1_data.py"])

# ================================= 天任务 =================================
# 每天0点重置任务状态
schedule.every().day.at("00:00").do(task_manager.reset_daily)

# 无依赖关系的任务
schedule.every().day.at("09:10").do(lambda: run_script_no_check("01_a_stock_level1_data.py", "stock"))
schedule.every().day.at("09:15").do(lambda: run_script_no_check("04_qmt_monitor_ha_st_30m.py", "stock"))
schedule.every().day.at("16:30").do(lambda: run_script_no_check("01_ods_a_stock_level1_data.py", "stock"))
schedule.every().day.at("16:40").do(lambda: run_script_no_check("01_a_stock_daily_basic.py", "stock"))
schedule.every().day.at("16:50").do(lambda: run_script_no_check("01_ths_limit_list.py", "stock"))
schedule.every().day.at("18:00").do(lambda: run_script_no_check("01_a_stock_30m_kline_wfq_baostock.py", "stock"))
schedule.every().day.at("19:00").do(lambda: run_script_no_check("01_a_index_1day_kline_baostock.py", "stock"))

# ================================= 周任务 =================================
# 无依赖关系的周任务
schedule.every().saturday.at("02:00").do(lambda: run_script_no_check("01_ths_index_members.py", "stock"))
schedule.every().saturday.at("03:00").do(lambda: run_script_no_check("01_a_stock_trade_cal.py", "stock"))
schedule.every().saturday.at("03:00").do(lambda: run_script_no_check("01_a_stock_basic.py", "stock"))


# schedule.every(10).minutes.do(run_script, "your_script.py")
# schedule.every().hour.do(run_script, "hourly_task.py")

# ================================= 一直运行 =================================
while True:
    schedule.run_pending()
    time.sleep(1)