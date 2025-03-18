Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "cmd /c D:\data_01\data_stock\stock_data_integration\db_query_api_v2.bat", 0, False
WshShell.Run "cmd /c D:\data_01\data_stock\stock_data_integration\a_stock_daily_task.bat", 0, False