@echo off
D:
cd D:\data_01\data_stock\stock_data_integration
call conda activate stock
python db_query_api.py