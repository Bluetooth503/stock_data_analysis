# -*- coding: utf-8 -*-
import os
import pandas as pd
import json
import numpy as np
from scipy.stats import rankdata
import sys
import re

# 获取当前脚本的绝对路径
script_path = os.path.abspath(__file__)
# 获取脚本所在目录
path = os.path.dirname(script_path)
# 更改工作目录
os.chdir(path)

folder_name = os.path.basename(path)

os.chdir(path)
files = [file_name for file_name in os.listdir(path) if file_name.endswith(".csv") and "_max_" in file_name]

dfs = []

def process_file(file):
    df = pd.read_csv(file)
    df = df.replace("N/A",0).fillna(0)
    df["Symbol"] = file.replace("deep backtesting ","").split(" ")[0]
    df["Strategy"] = file.replace("deep backtesting ","").split(" ")[1]

    # 删除 "__Return Table" 列
    if "__Return Table" in df.columns:
        df.drop(columns=["__Return Table"], inplace=True)
    
    # 将 "__Vol" 和 "__MA" 列中的值转换为字符串，然后处理
    vol_ma_columns = ["__Vol", "__MA"]
    for col in vol_ma_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("0", "false")
    
    # __Vol和__MA去重
    if "__Vol" in df.columns and "__Multi" in df.columns:
        df.loc[df["__Vol"].astype(str).str.lower() == "false", "__Multi"] = 0.0
    
    if "__MA" in df.columns and "__Len" in df.columns:
        df.loc[df["__MA"].astype(str).str.lower() == "false", "__Len"] = 0.0

    info_columns = [col for col in df.columns if col.startswith("__")]
    df["Parameter"] = df[info_columns].apply(lambda row: json.dumps(dict(row.astype(str))), axis=1)
    df.drop(columns=info_columns, inplace=True)
    df.drop(columns=["comment"], inplace=True)
    
    column_order = ["Strategy","Symbol","Parameter","Net Profit: All","Net Profit %: All","Net Profit: Long","Net Profit %: Long","Net Profit: Short","Net Profit %: Short","Gross Profit: All","Gross Profit %: All","Gross Profit: Long","Gross Profit %: Long","Gross Profit: Short","Gross Profit %: Short","Gross Loss: All","Gross Loss %: All","Gross Loss: Long","Gross Loss %: Long","Gross Loss: Short","Gross Loss %: Short","Max Run-up","Max Run-up %","Max Drawdown","Max Drawdown %","Buy & Hold Return","Buy & Hold Return %","Sharpe Ratio","Sortino Ratio","Profit Factor: All","Profit Factor: Long","Profit Factor: Short","Max Contracts Held: All","Max Contracts Held: Long","Max Contracts Held: Short","Open PL","Open PL %","Commission Paid: All","Commission Paid: Long","Commission Paid: Short","Total Closed Trades: All","Total Closed Trades: Long","Total Closed Trades: Short","Total Open Trades: All","Total Open Trades: Long","Total Open Trades: Short","Number Winning Trades: All","Number Winning Trades: Long","Number Winning Trades: Short","Number Losing Trades: All","Number Losing Trades: Long","Number Losing Trades: Short","Percent Profitable: All","Percent Profitable: Long","Percent Profitable: Short","Avg Trade: All","Avg Trade %: All","Avg Trade: Long","Avg Trade %: Long","Avg Trade: Short","Avg Winning Trade: All","Avg Winning Trade %: All","Avg Winning Trade: Long","Avg Winning Trade %: Long","Avg Winning Trade: Short","Avg Losing Trade: All","Avg Losing Trade %: All","Avg Losing Trade: Long","Avg Losing Trade %: Long","Avg Losing Trade: Short","Ratio Avg Win / Avg Loss: All","Ratio Avg Win / Avg Loss: Long","Ratio Avg Win / Avg Loss: Short","Largest Winning Trade: All","Largest Winning Trade %: All","Largest Winning Trade: Long","Largest Winning Trade %: Long","Largest Winning Trade: Short","Largest Losing Trade: All","Largest Losing Trade %: All","Largest Losing Trade: Long","Largest Losing Trade %: Long","Largest Losing Trade: Short","Avg # Bars in Trades: All","Avg # Bars in Trades: Long","Avg # Bars in Trades: Short","Avg # Bars in Winning Trades: All","Avg # Bars in Winning Trades: Long","Avg # Bars in Winning Trades: Short","Avg # Bars in Losing Trades: All","Avg # Bars in Losing Trades: Long","Avg # Bars in Losing Trades: Short","Margin Calls: All","Margin Calls: Long","Margin Calls: Short"]
    df = df.reindex(columns = column_order)
    df = df.drop_duplicates(subset=["Strategy", "Symbol", "Parameter"], keep="first")
    df["综合利润指标"] = df["Total Closed Trades: All"] * df["Avg Trade %: All"]/100 * df["Percent Profitable: All"]/100
    return df


for file in files:
    df = process_file(file)
    dfs.append(df)

combined_df = pd.concat(dfs, ignore_index=True)
# combined_df.to_csv(f"{folder_name}.csv", index=False)



def topsis_ranking(df):    
    # 定义评价指标
    criteria = [
        "Net Profit %: All",
        "Percent Profitable: All",
        "Profit Factor: All",
        "Sharpe Ratio",
        "Sortino Ratio"
    ]
    
    # 权重设置为相等
    weights = np.array([0.2, 0.2, 0.2, 0.2, 0.2])
    
    def calculate_topsis(data_matrix):
        # 归一化
        normalized = data_matrix / np.sqrt(np.sum(data_matrix**2, axis=0))
        
        # 加权归一化
        weighted = normalized * weights
        
        # 确定正理想解和负理想解
        ideal_best = np.max(weighted, axis=0)
        ideal_worst = np.min(weighted, axis=0)
        
        # 计算到理想解的距离
        s_best = np.sqrt(np.sum((weighted - ideal_best)**2, axis=1))
        s_worst = np.sqrt(np.sum((weighted - ideal_worst)**2, axis=1))
        
        # 计算得分
        scores = s_worst / (s_best + s_worst)
        
        # 计算排名（降序）
        ranks = len(scores) - rankdata(scores) + 1
        
        return ranks.astype(int), scores
    
    # 初始化结果DataFrame
    result_df = df.copy()
    
    # 对每个Strategy和Symbol组合分别计算TOPSIS
    for (strategy, symbol), group in df.groupby(["Strategy", "Symbol"]):
        data = group[criteria].values
        if len(data) > 0:
            ranks, scores = calculate_topsis(data)
            
            # 将ranks和scores添加到对应的行
            result_df.loc[group.index, "Rank"] = ranks
            result_df.loc[group.index, "Score"] = scores
    
    # 选择并重命名需要的列
    result_df = result_df[[
        "Strategy", "Symbol", "Parameter", "Rank", "Score", "综合利润指标", 
        "Net Profit %: All", "Percent Profitable: All", "Profit Factor: All",
        "Sharpe Ratio", "Sortino Ratio", "Total Closed Trades: All",
        "Avg Trade %: All", "Avg Winning Trade %: All", "Avg Losing Trade %: All",
        "Max Drawdown %"
    ]].rename(columns={
        "Net Profit %: All": "总利润%",
        "Percent Profitable: All": "胜率%",
        "Profit Factor: All": "盈利因子",
        "Sharpe Ratio": "夏普率",
        "Sortino Ratio": "索提诺率",
        "Total Closed Trades: All": "总交易次数",
        "Avg Trade %: All": "平均交易获利%",
        "Avg Winning Trade %: All": "WinningTrade平均获利%",
        "Avg Losing Trade %: All": "LosingTrade平均亏损%",
        "Max Drawdown %": "最大回撤%"
    })
    
    # 设置需要保留3位小数的列
    decimal_columns = [
        "Score", "综合利润指标", "总利润%", "胜率%", "盈利因子", "夏普率", "索提诺率",
        "平均交易获利%", "WinningTrade平均获利%", "LosingTrade平均亏损%", "最大回撤%"
    ]
    
    # 保留3位小数
    for col in decimal_columns:
        result_df[col] = result_df[col].round(3)
    
    # 按Score降序排序并只保留每个Symbol的前5条记录
    result_df = (result_df.sort_values("Score", ascending=False)
                          .groupby(["Symbol"])
                          .head(2)
                          .sort_values("Score", ascending=False))
    
    result_df = result_df.sort_values(by=['索提诺率'], ascending=False)
    return result_df

# 使用示例
result_df = topsis_ranking(combined_df)
result_df.to_csv(f"{folder_name}_TOPSIS.csv", index=False)
