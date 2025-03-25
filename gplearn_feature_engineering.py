# -*- coding: utf-8 -*-
from common import *
from gplearn.genetic import SymbolicTransformer
from sklearn.preprocessing import StandardScaler
import joblib


def engineer_features(X_train, y_train, feature_names):
    """使用gplearn进行特征工程并选择重要特征"""
    # 简化函数集，只保留基础运算
    function_set = ['add', 'sub', 'mul', 'div', 'sqrt', 'log', 'abs', 'neg', 'inv', 'max', 'min', 'sin', 'cos', 'tan']
    
    # 创建SymbolicTransformer实例
    st = SymbolicTransformer(
        population_size=200,   # 增加种群大小以提高特征质量
        generations=10,        # 增加迭代次数
        function_set=function_set,
        parsimony_coefficient=0.001,
        n_components=20,        # 生成更多特征以便后续筛选
        hall_of_fame=100,      # 增加名人堂大小
        n_jobs=-1,
        verbose=1,
        random_state=42
    )
    
    # 训练特征转换器
    logger.info("开始训练特征转换器...")
    st.fit(X_train, y_train)
    
    # 获取特征重要性
    feature_importance = {}
    for i, program in enumerate(st._best_programs):
        program_str = str(program)
        for feature in feature_names:
            if feature not in feature_importance:
                feature_importance[feature] = 0
            feature_importance[feature] += program_str.count(feature)
    
    # 按重要性排序并选择前10个特征
    sorted_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
    selected_features = [feature for feature, importance in sorted_features[:10] if importance > 0]
    selected_indices = [feature_names.index(feature) for feature in selected_features]
    
    # 打印特征重要性
    logger.info("\n特征重要性排名:")
    logger.info("-" * 50)
    logger.info(f"{'特征名称':<30} {'重要性得分':<10} {'使用次数':<10}")
    logger.info("-" * 50)
    
    for feature, importance in sorted_features:
        if importance > 0:
            logger.info(f"{feature:<30} {importance/len(st._best_programs):.4f} {importance:>10}")
    
    logger.info("-" * 50)
    logger.info("\n选择的前10个特征:")
    for i, feature in enumerate(selected_features, 1):
        logger.info(f"{i}. {feature}")
    
    # 打印生成的特征公式
    logger.info("\n生成的特征转换公式:")
    logger.info("-" * 50)
    for i, program in enumerate(st._best_programs):
        formula = str(program)
        # 将X0, X1等替换为实际的特征名
        for j, feat_name in enumerate(feature_names):
            formula = formula.replace(f'X{j}', feat_name)
        logger.info(f"特征 {i+1}: {formula}")
    
    return st, selected_indices, selected_features

def save_components(transformer, selected_indices, selected_features, feature_names, save_dir='models'):
    """保存特征工程组件"""
    os.makedirs(save_dir, exist_ok=True)
    
    # 保存特征转换器
    joblib.dump(transformer, os.path.join(save_dir, 'feature_transformer.pkl'))
    
    # 保存特征选择信息
    feature_selection = {
        'selected_indices': selected_indices,
        'selected_features': selected_features,
        'all_features': feature_names
    }
    joblib.dump(feature_selection, os.path.join(save_dir, 'feature_selection.pkl'))
    
    logger.info(f"\n特征工程组件已保存到 {save_dir} 目录")
    logger.info(f"- feature_transformer.pkl: 特征转换器")
    logger.info(f"- feature_selection.pkl: 特征选择信息")

def main():
    # 获取数据
    logger.info("\n获取股票数据...")
    df = pd.read_csv('tmp_stock_data.csv')

    # # 只保留50只股票的数据
    # unique_stocks = df['ts_code'].unique()
    # selected_stocks = unique_stocks[:50]
    # df = df[df['ts_code'].isin(selected_stocks)]
    # df.to_csv('tmp_stock_data.csv', index=False)
    
    # 计算特征
    logger.info("\n计算特征...")
    # 获取所有数值型列
    numeric_columns = df_features.select_dtypes(include=[np.number]).columns
    
    # 移除不需要的列（如日期、代码等）
    exclude_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'amount',
                      'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv',
                      'target', 'next_day_return']  # 添加target和next_day_return到排除列
    feature_columns = [col for col in numeric_columns if col not in exclude_columns]
    
    # 创建最终的特征DataFrame
    final_df = df_features[feature_columns].copy()
    final_df['target'] = df_features['target']  # 单独添加target列
    


    df_features, feature_names = calculate_features(df)
    
    # 数据预处理
    X = df_features.drop('target', axis=1)
    y = df_features['target']
    
    # 获取时间范围信息
    timestamps = pd.to_datetime(df['trade_date'])
    logger.info(f"\n数据集信息:")
    logger.info(f"时间范围: {timestamps.min()} 到 {timestamps.max()}")
    logger.info(f"样本数量: {len(X)}")
    
    # 标准化
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # 特征工程和特征选择
    logger.info("\n开始特征工程...")
    transformer, selected_indices, selected_features = engineer_features(X_scaled, y, feature_names)
    
    # 保存组件
    save_components(transformer, selected_indices, selected_features, feature_names)
    
    logger.info("完成!")

if __name__ == "__main__":
    main() 