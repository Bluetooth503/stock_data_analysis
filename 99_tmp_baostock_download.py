import baostock as bs
import pandas as pd

#### 参数 ####
ts_code = "sh.603003"
frequency = "30"
start_date = "2025-04-08"
end_date = "2025-04-08"

#### 登陆系统 ####
lg = bs.login()
rs = bs.query_history_k_data_plus(ts_code,
    "date,time,code,open,high,low,close,volume,amount,adjustflag",
    start_date=start_date, end_date=end_date,
    frequency=frequency, adjustflag="3")
print('query_history_k_data_plus respond error_code:'+rs.error_code)
print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)

#### 打印结果集 ####
data_list = []
while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
    data_list.append(rs.get_row_data())
result = pd.DataFrame(data_list, columns=rs.fields)

#### 结果集输出到csv文件 ####   
result.to_csv("history_A_stock_k_data.csv", index=False)

#### 登出系统 ####
bs.logout()