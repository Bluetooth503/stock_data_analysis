@echo off
D:
cd D:\data_01\data_stock\stock_data_analysis
call conda activate stock
python 00_scheduler.py
