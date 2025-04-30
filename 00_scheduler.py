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
def run_script(script_name):
    # 检查任务依赖
    if not task_manager.can_run_task(script_name):
        logger.info(f"任务 {script_name} 的依赖任务尚未完成，暂不执行")
        return
    
    script_path = os.path.join(BASE_PATH, script_name)
    if not os.path.exists(script_path):
        logger.error(f"错误: 脚本文件 {script_path} 不存在")
        return
    
    # 使用conda环境中的Python解释器
    conda_python = os.path.join(os.environ.get('CONDA_PREFIX', ''), 'python.exe')
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

# 设置任务调度
schedule.every().day.at("09:00").do(run_script, "01_a_stock_level1_data.py")
schedule.every().day.at("09:00").do(run_script, "04_qmt_monitor_ha_st_30m.py")
schedule.every().day.at("16:30").do(run_script, "01_ods_a_stock_level1_data.py")

# ================================= 周任务 =================================
schedule.every().monday.at("02:00").do(run_script, "01_ths_index_members.py")
schedule.every().monday.at("02:00").do(run_script, "01_a_stock_trade_cal.py")


# schedule.every(10).minutes.do(run_script, "your_script.py")
# schedule.every().hour.do(run_script, "hourly_task.py")

# ================================= 一直运行 =================================
while True:
    schedule.run_pending()
    time.sleep(1)