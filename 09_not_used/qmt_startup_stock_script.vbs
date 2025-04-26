Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "cmd /c C:\stock\qmt_monitor_ha_st_30m.bat", 0, False
WshShell.Run "cmd /c C:\stock\qmt_data_publisher.bat", 0, False