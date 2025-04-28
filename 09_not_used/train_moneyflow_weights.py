from common import *
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
import tushare as ts
import joblib
import os

# ================================= 读取配置文件 =================================
config = load_config()
token = config.get('tushare', 'token')
pro = ts.pro_api(token)
engine = create_engine(get_pg_connection_string(config))

# ================================= 配置日志 =================================
logger = setup_logger()

# ================================= 创建输出目录 =================================
OUTPUT_DIR = "model_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)




def send_notification(subject, content):
    """发送微信通知"""
    try:
        config = load_config()
        token = config.get('wxpusher', 'token')
        uids = [config.get('wxpusher', 'uid')]
        WxPusher.send_message(
            content=f"{subject}\n\n{content}",
            uids=uids,
            token=token)
    except Exception as e:
        logger.error(f"微信通知发送失败: {str(e)}")

class MoneyflowWeightTrainer:
    def __init__(self, start_date, end_date, min_samples=252):
        """
        初始化训练器
        
        Args:
            start_date (str): 开始日期，格式：YYYYMMDD
            end_date (str): 结束日期，格式：YYYYMMDD
            min_samples (int): 最小样本数量，默认252个交易日(一年)
        """
        self.config = load_config()
        self.pro = ts.pro_api(self.config.get('tushare', 'token'))
        self.engine = create_engine(get_pg_connection_string(self.config))
        self.start_date = start_date
        self.end_date = end_date
        self.min_samples = min_samples
        self.logger = setup_logger()
        self.scaler = None
        self.model = None
        
    def get_training_data(self):
        """获取训练数据"""
        self.logger.info("开始获取训练数据...")
        
        # 获取资金流数据
        moneyflow_sql = f"""
        SELECT ts_code, trade_date,
               buy_elg_amount - sell_elg_amount as elg_net,
               buy_lg_amount - sell_lg_amount as lg_net,
               buy_md_amount - sell_md_amount as md_net,
               buy_sm_amount - sell_sm_amount as sm_net
        FROM a_stock_moneyflow
        WHERE trade_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        """
        moneyflow_df = pd.read_sql(moneyflow_sql, self.engine)
        
        # 获取日线行情数据
        daily_sql = f"""
        SELECT ts_code, trade_date, close, pre_close, amount
        FROM a_stock_daily_k
        WHERE trade_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        """
        daily_df = pd.read_sql(daily_sql, self.engine)
        
        # 获取每日基本面数据(用于获取流通市值和换手率)
        basic_sql = f"""
        SELECT ts_code, trade_date, circ_mv, turnover_rate, volume_ratio
        FROM a_stock_daily_basic
        WHERE trade_date BETWEEN '{self.start_date}' AND '{self.end_date}'
        """
        basic_df = pd.read_sql(basic_sql, self.engine)
        
        # 合并数据
        df = pd.merge(moneyflow_df, daily_df, on=['ts_code', 'trade_date'])
        df = pd.merge(df, basic_df, on=['ts_code', 'trade_date'])
                
        # 数据清洗：删除任何包含NaN的行
        initial_rows = len(df)
        nan_df = df[df.isnull().any(axis=1)].copy()
        df = df.dropna()
        dropped_rows = initial_rows - len(df)
        if dropped_rows > 0:
            self.logger.warning(f"删除了 {dropped_rows} 行含有NaN的数据")
            # 保存含有NaN的数据
            nan_df.to_csv(os.path.join(OUTPUT_DIR, 'dropped_nan_data.csv'), index=False)
        
        # 数据清洗：删除异常值
        invalid_df = df[df['circ_mv'] <= 0].copy()  # 保存流通市值异常的数据
        df = df[df['circ_mv'] > 0]  # 删除流通市值小于等于0的记录
        if len(invalid_df) > 0:
            invalid_df.to_csv(os.path.join(OUTPUT_DIR, 'dropped_invalid_mv_data.csv'), index=False)
        
        # 计算特征和标签
        df['return'] = (df['close'] - df['pre_close']) / df['pre_close']
        df['label'] = (df['return'] > 0).astype(int)  # 涨跌作为二分类标签
        
        # 计算资金流/市值特征
        for col in ['elg_net', 'lg_net', 'md_net', 'sm_net']:
            df[f'{col}_mv'] = df[col] / df['circ_mv']
            
        # # 保存极端值数据
        # extreme_df = pd.DataFrame()
        # for col in ['elg_net_mv', 'lg_net_mv', 'md_net_mv', 'sm_net_mv', 'volume_ratio', 'turnover_rate']:
        #     # 获取极端值数据
        #     lower = df[col].quantile(0.01)
        #     upper = df[col].quantile(0.99)
        #     extreme_data = df[~df[col].between(lower, upper)].copy()
        #     extreme_data['extreme_feature'] = col
        #     extreme_data['lower_bound'] = lower
        #     extreme_data['upper_bound'] = upper
        #     extreme_df = pd.concat([extreme_df, extreme_data])
        #     # 删除极端值
        #     df = df[df[col].between(lower, upper)]
            
        # if not extreme_df.empty:
        #     extreme_df.to_csv(os.path.join(OUTPUT_DIR, 'dropped_extreme_data.csv'), index=False)
            
        # 按股票分组，确保每只股票都有足够的样本
        valid_stocks = df.groupby('ts_code').size()[df.groupby('ts_code').size() >= self.min_samples].index
        insufficient_samples_df = df[~df['ts_code'].isin(valid_stocks)].copy()
        df = df[df['ts_code'].isin(valid_stocks)]
        
        if not insufficient_samples_df.empty:
            insufficient_samples_df.to_csv(os.path.join(OUTPUT_DIR, 'dropped_insufficient_samples.csv'), index=False)
        
        final_rows = len(df)
        self.logger.info(f"数据清洗完成: 初始数据量 {initial_rows}, 最终数据量 {final_rows}")
                
        return df
        
    def prepare_features(self, df):
        """准备特征数据"""
        feature_cols = [
            'elg_net_mv', 'lg_net_mv', 'md_net_mv', 'sm_net_mv',
            'volume_ratio', 'turnover_rate'
        ]
        
        # 再次检查是否有NaN值
        if df[feature_cols].isnull().any().any():
            self.logger.warning("特征中仍然存在NaN值，将被删除")
            df = df.dropna(subset=feature_cols)
        
        # 标准化特征
        self.scaler = StandardScaler()
        X = self.scaler.fit_transform(df[feature_cols])
        y = df['label']
        
        return X, y, feature_cols
        
    def evaluate_model(self, X, y, feature_cols):
        """评估模型性能"""
        # 分割训练集和测试集
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # 训练模型
        self.model = LogisticRegression(random_state=42, max_iter=1000)
        self.model.fit(X_train, y_train)
        
        # 在测试集上评估
        y_pred = self.model.predict(X_test)
        y_prob = self.model.predict_proba(X_test)
        
        # 计算评估指标
        report = classification_report(y_test, y_pred, output_dict=True)
        conf_matrix = confusion_matrix(y_test, y_pred)
        
        # 打印详细的评估指标
        self.logger.info("\n模型评估指标:")
        self.logger.info(f"准确率(Accuracy): {report['accuracy']:.4f}")
        self.logger.info("\n涨跌分类详细指标:")
        self.logger.info(f"跌(0)类 - 查准率(Precision): {report['0']['precision']:.4f}, " + \
                        f"查全率(Recall): {report['0']['recall']:.4f}, " + \
                        f"F1分数: {report['0']['f1-score']:.4f}")
        self.logger.info(f"涨(1)类 - 查准率(Precision): {report['1']['precision']:.4f}, " + \
                        f"查全率(Recall): {report['1']['recall']:.4f}, " + \
                        f"F1分数: {report['1']['f1-score']:.4f}")
        self.logger.info(f"\n混淆矩阵:")
        self.logger.info(f"True Negative(实际跌-预测跌): {conf_matrix[0][0]}")
        self.logger.info(f"False Positive(实际跌-预测涨): {conf_matrix[0][1]}")
        self.logger.info(f"False Negative(实际涨-预测跌): {conf_matrix[1][0]}")
        self.logger.info(f"True Positive(实际涨-预测涨): {conf_matrix[1][1]}")
        
        # 保存评估结果
        with open(os.path.join(OUTPUT_DIR, 'model_evaluation.txt'), 'w', encoding='utf-8') as f:
            f.write("模型评估报告\n")
            f.write("=" * 50 + "\n\n")
            
            f.write("1. 整体准确率\n")
            f.write("-" * 30 + "\n")
            f.write(f"Accuracy: {report['accuracy']:.4f}\n\n")
            
            f.write("2. 分类详细指标\n")
            f.write("-" * 30 + "\n")
            f.write("跌(0)类:\n")
            f.write(f"  - Precision(查准率): {report['0']['precision']:.4f}\n")
            f.write(f"  - Recall(查全率): {report['0']['recall']:.4f}\n")
            f.write(f"  - F1-score: {report['0']['f1-score']:.4f}\n")
            f.write(f"  - Support(样本数): {report['0']['support']}\n\n")
            
            f.write("涨(1)类:\n")
            f.write(f"  - Precision(查准率): {report['1']['precision']:.4f}\n")
            f.write(f"  - Recall(查全率): {report['1']['recall']:.4f}\n")
            f.write(f"  - F1-score: {report['1']['f1-score']:.4f}\n")
            f.write(f"  - Support(样本数): {report['1']['support']}\n\n")
            
            f.write("3. 混淆矩阵\n")
            f.write("-" * 30 + "\n")
            f.write("预测跌    预测涨\n")
            f.write(f"实际跌  {conf_matrix[0][0]:8d} {conf_matrix[0][1]:8d}\n")
            f.write(f"实际涨  {conf_matrix[1][0]:8d} {conf_matrix[1][1]:8d}\n\n")
            
            f.write("4. 模型参数\n")
            f.write("-" * 30 + "\n")
            f.write(f"特征数量: {len(feature_cols)}\n")
            f.write(f"训练集大小: {len(X_train)}\n")
            f.write(f"测试集大小: {len(X_test)}\n")
            
        # # 保存预测概率
        # test_results = pd.DataFrame({
        #     'true_label': y_test,
        #     'predicted_label': y_pred,
        #     'prob_negative': y_prob[:, 0],
        #     'prob_positive': y_prob[:, 1]
        # })
        # test_results.to_csv(os.path.join(OUTPUT_DIR, 'test_predictions.csv'), index=False)
        
        # 计算并返回准确率
        accuracy = (y_pred == y_test).mean()
        return accuracy, feature_cols
        
    def save_model(self):
        """保存模型和相关组件"""
        if self.model is None or self.scaler is None:
            self.logger.error("模型或标准化器未初始化，无法保存")
            return
            
        # 保存模型
        joblib.dump(self.model, os.path.join(OUTPUT_DIR, 'logistic_model.joblib'))
        # 保存标准化器
        joblib.dump(self.scaler, os.path.join(OUTPUT_DIR, 'scaler.joblib'))
        
    def train_model(self):
        """训练模型并获取权重"""
        # 获取数据
        df = self.get_training_data()
        if df.empty:
            self.logger.error("没有获取到有效的训练数据")
            return None
            
        # 准备特征
        X, y, feature_cols = self.prepare_features(df)
        
        # 评估模型
        self.logger.info("开始训练和评估模型...")
        accuracy, feature_cols = self.evaluate_model(X, y, feature_cols)
        self.logger.info(f"模型准确率: {accuracy:.4f}")
        
        # 获取特征权重
        weights = dict(zip(feature_cols, self.model.coef_[0]))
        
        # 将权重归一化到总和为1
        total = sum(abs(w) for w in weights.values())
        normalized_weights = {k: abs(v)/total for k, v in weights.items()}
        
        # 转换为原始指标的权重
        final_weights = {
            '特大单/市值_Z': normalized_weights['elg_net_mv'],
            '大单/市值_Z': normalized_weights['lg_net_mv'],
            '中单/市值_Z': normalized_weights['md_net_mv'],
            '小单/市值_Z': normalized_weights['sm_net_mv'],
            '量比_Z': normalized_weights['volume_ratio'],
            '换手率_Z': normalized_weights['turnover_rate']
        }
        
        # 保存权重到CSV
        weights_df = pd.DataFrame([
            {'feature': k, 'raw_weight': weights[k], 'normalized_weight': normalized_weights[k]}
            for k in feature_cols
        ])
        weights_df.to_csv(os.path.join(OUTPUT_DIR, 'feature_weights.csv'), index=False)
        
        self.logger.info("模型训练完成，最终权重:")
        # 按权重大小排序显示
        sorted_weights = dict(sorted(final_weights.items(), key=lambda x: x[1], reverse=True))
        for k, v in sorted_weights.items():
            self.logger.info(f"{k}: {v:.4f}")
            
        # 保存模型和组件
        self.save_model()
            
        return final_weights

def main():
    # 设置训练时间范围
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = '20100105'
    
    trainer = MoneyflowWeightTrainer(start_date, end_date)
    weights = trainer.train_model()
    
    if weights:
        send_notification(
            "资金流指标权重更新完成",
            f"训练日期: {end_date}\n" + \
            "\n".join([f"{k}: {v:.4f}" for k, v in weights.items()])
        )

if __name__ == "__main__":
    main() 