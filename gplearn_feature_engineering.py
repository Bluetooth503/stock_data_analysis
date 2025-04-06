# -*- coding: utf-8 -*-
from common import *
from gplearn.genetic import SymbolicTransformer
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import joblib
import pandas as pd
import numpy as np
import json

def engineer_features(X_train, y_train, feature_names):
    """基于遗传编程的特征工程"""
    logger.info("开始遗传编程特征生成...")
    st = SymbolicTransformer(
        population_size=2000,
        generations=20,
        function_set=['add', 'sub', 'mul', 'div', 'sqrt', 'log', 'abs', 'neg', 'max', 'min'],
        n_components=30,
        hall_of_fame=200,
        parsimony_coefficient=0.01,
        n_jobs=30,
        random_state=42,
        verbose=1
    )
    
    # 生成新特征
    X_new = st.fit_transform(X_train, y_train)
    
    # 合并原始特征和新生成特征
    combined_features = np.hstack([X_train, X_new])
    new_feature_names = feature_names + [f'GP_Feature_{i}' for i in range(X_new.shape[1])]
    
    logger.info(f"共生成 {X_new.shape[1]} 个新特征，开始特征筛选...")
    
    # 使用随机森林评估特征重要性
    rf = RandomForestClassifier(
        n_estimators=100,
        max_depth=8,
        min_samples_split=100,
        min_samples_leaf=50,
        max_features='sqrt',
        n_jobs=30,
        random_state=42,
        verbose=1
    )
    rf.fit(combined_features, y_train)
    
    # 获取特征重要性并筛选Top20
    importances = rf.feature_importances_
    indices = np.argsort(importances)[::-1][:20]  # 取前20个
    selected_features = [new_feature_names[i] for i in indices]
    
    # 打印特征重要性
    logger.info("Top20特征重要性排名:")
    logger.info("-" * 60)
    formulas = []  # 用于存储特征公式
    
    # 获取所有有效的GP生成特征公式
    gp_programs = st._programs
    logger.info(f"GP程序总数: {len(gp_programs)}")

    # 创建特征名称映射字典
    feature_map = {f'X{i}': name for i, name in enumerate(feature_names)}
    
    # 简化特征公式映射逻辑
    formula_map = {}
    programs = st._best_programs if hasattr(st, '_best_programs') else gp_programs[0]
    
    for i, program in enumerate(programs):
        if program is not None:
            raw_formula = str(program)
            readable_formula = raw_formula
            for symbol, name in feature_map.items():
                readable_formula = readable_formula.replace(symbol, f"'{name}'")
            formula_map[i] = readable_formula
    
    logger.info(f"创建了 {len(formula_map)} 个公式映射")
    
    # 简化特征重要性和公式输出逻辑
    for i, idx in enumerate(indices, 1):
        feat_name = new_feature_names[idx]
        importance = float(importances[idx])
        
        formula_info = {
            'feature_name': feat_name,
            'importance': importance,
            'type': 'original' if idx < len(feature_names) else 'generated',
            'index': int(idx if idx < len(feature_names) else idx - len(feature_names))
        }
        
        if formula_info['type'] == 'generated':
            gp_idx = idx - len(feature_names)
            formula = formula_map.get(gp_idx, 'unavailable')
            formula_info['formula'] = formula
            
            if formula == 'unavailable':
                logger.warning(f"特征 {feat_name} (idx={gp_idx}) 的公式不可用")
        else:
            formula_info['formula'] = feat_name
            
        formulas.append(formula_info)
        logger.info(f"{i:>2}. {feat_name:<40} {importance:.6f}")
        if formula_info['type'] == 'generated' and formula != 'unavailable':
            logger.info(f"    Formula: {formula}")
    
    logger.info("-" * 60)
    
    # 添加调试信息
    logger.info(f"原始特征数量: {len(feature_names)}")
    logger.info(f"生成特征数量: {X_new.shape[1]}")
    logger.info(f"有效公式数量: {len(formula_map)}")
    
    return st, indices, selected_features, formulas

def save_components(transformer, selected_indices, selected_features, feature_names, formulas, save_dir='models'):
    """保存特征工程组件"""
    try:
        os.makedirs(save_dir, exist_ok=True)
        
        # 整合所有需要保存的数据
        components = {
            'transformer': transformer,
            'feature_selection': {
                'selected_indices': [int(i) for i in selected_indices],
                'selected_features': selected_features,
                'all_features': list(feature_names),
                'feature_formulas': formulas,
                'save_time': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            },
            'symbol_mapping': {f'X{i}': name for i, name in enumerate(feature_names)}
        }
        
        # 保存各个组件
        for name, data in components.items():
            if name == 'transformer':
                joblib.dump(data, os.path.join(save_dir, 'symbolic_transformer.pkl'))
            elif name == 'feature_selection':
                joblib.dump(data, os.path.join(save_dir, 'feature_selection.pkl'))
                # 额外保存JSON格式的公式
                with open(os.path.join(save_dir, 'feature_formulas.json'), 'w', encoding='utf-8') as f:
                    json.dump(data['feature_formulas'], f, ensure_ascii=False, indent=2)
            else:  # symbol_mapping
                with open(os.path.join(save_dir, f'{name}.json'), 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"特征工程组件已保存到 {save_dir} 目录")

    except Exception as e:
        logger.error(f"保存组件时发生错误: {str(e)}")
        raise

def main():
    """主函数"""
    logger.info("开始数据处理流程...")
    
    # 数据加载
    parquet_path = 'tmp_stock_features_df.parquet'
    if os.path.exists(parquet_path):
        df_features = pd.read_parquet(parquet_path, engine='pyarrow')
    else:
        raise FileNotFoundError("未找到数据文件")
    
    # 数据预处理
    numeric_columns = df_features.select_dtypes(include=[np.number]).columns
    exclude_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'amount',
                       'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv',
                       'target', 'next_day_return']
    feature_columns = [col for col in numeric_columns if col not in exclude_columns]
    
    # 增强版日期解析
    try:
        df_features['trade_date'] = pd.to_datetime(
            df_features['trade_date'].astype(str).str.replace(r'\D', '', regex=True).str[:8],
            format='%Y%m%d',
            errors='coerce'
        ).fillna(method='ffill')
    except Exception as e:
        logger.error(f"日期解析失败: {str(e)}")
        raise
    
    # 数据准备
    X = df_features[feature_columns].ffill().bfill()
    y = df_features['target'].ffill().bfill()
    
    # 特征标准化
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # 使用默认配置即可，无需重复定义
    gp_transformer, indices, selected_features, formulas = engineer_features(
        X_scaled, 
        y,
        feature_columns
    )
    
    # 生成完整特征名称列表（原始+新生成）
    new_feature_names = feature_columns + [f'GP_Feature_{i}' for i in range(50)]
    
    # 保存完整组件
    save_components(
        transformer=gp_transformer,
        selected_indices=indices,
        selected_features=selected_features,
        feature_names=new_feature_names,
        formulas=formulas
    )
    
    logger.info("流程完成！")

if __name__ == "__main__":
    main()
