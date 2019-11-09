# coding:utf-8

# system
import sys

# ログファイル整形系
import csv
import pprint as p
import apache_log_parser

# OSディレクトリファイル取得
import glob

# 時間操作系
from datetime import time

# 時間をグループ毎に集計する
import pandas as pd

# プログレスバー
from tqdm import tqdm

# dict ファイルごと、時間別の dict を格納する
sort_day_logs = {}

# setting apache log format
parser = apache_log_parser.make_parser(
    '%h %l %u %t \"%r\" %>s %b pt:%D \"%{Referer}i\" \"%{User-Agent}i\"')

# 集計用の配列
sum_all_dict_access = {}

# ディレクトリ内に存在するログファイル名一覧を取得する
apache_logfile_lists = glob.glob('./log_file/*')

# データフレーム
rtn_daily_access_log = pd.DataFrame()

big_logs_dict = []
read_file_cnt = 0
for log_file in tqdm(apache_logfile_lists):
    data_frame_log_data_list = pd.DataFrame()
    # ログフィルを展開する
    with open(log_file) as f:

        # デバッグ用 10000カウントで強制終了
        i = 0

        for line in f.readlines():
           
            try:
                log_data = parser(line)
                big_logs_dict.append(log_data)

            except:
                print ("logfiled turned over")
                sys.stdout.write("line read Error ")

            finally:
                i += 1
                if i == 100:
                    break
        # 取得したログデータを、データフレーム形式に変換



data_frame_log_data_list = pd.DataFrame(big_logs_dict)
# data_frame_log_data_list['time_received_utc_datetimeobj'] = pd.to_datetime(data_frame_log_data_list['time_received_utc_datetimeobj'])
data_frame_log_data_list = data_frame_log_data_list.set_index('time_received_datetimeobj')

# three_time_logs = data_frame_log_data_list.between_time('3:00','3:59')
three_time_logs = data_frame_log_data_list['time_received_datetimeobj'].hour
print(three_time_logs)

three_time_logs_maxaccess_remote_host_address = three_time_logs['remote_host'].value_counts()
print(three_time_logs_maxaccess_remote_host_address)

# print(data_frame_log_data_list.info())



#         print('------------------------------------\n')
#         print('list -> convert -> dataFrame')

#         # dict convert dataframe
#         try:
#             logs_dataframe = pd.DataFrame(request_urls_array)

#         except:
#             print('------------------------------------')
#             print('conert failed')
#             print('------------------------------------')

#         # アクセスログを各時間帯のデータフレームに変換する
#         # 時間毎のアクセスログOBJを格納する dict を定義

#         # 0 -23 時のログを、各時間帯の dict に格納する
#         access_address_log = {}

#         for time_cnt in range(0, 24):

#             # 時間帯ごとの LOG オブジェクトが sort_time_logs に格納される
#             this_time_cnt_logs = logs_dataframe[logs_dataframe['access_time'].dt.hour == time_cnt]
            
#             # p.pprint(this_time_cnt_logs)
#             # IPアドレスごとの集計
#             access_address_log = this_time_cnt_logs['access_address'].value_counts().max()
#             # access_address_log = access_address_log.max()
#             p.pprint(access_address_log)

# '''
# データフレーム？
# 列と行から構成される２次元の表
# これに値を追加する？

# 縦軸が時間で横軸が項目名
# 1. アクセスログファイルごとに、時間帯別最大アクセス数を取得
# 2. 以下のフォーマットで、DataFrame 形式にデータを変換する
# 3. 

# time  | 9/1  | 9/2  | 9/3  |
# ----- | -----| ---- | ---- |
# 1     | 1000 | 2000 | 1200 |
# ----- | -----|----- |------|
# 2     | 2000 | 3000 | 1000 |
# ----- | -----|------|------|
# 3     | 3000 | 1200 | 8000 |
# ----- | -----|------|------|


# 上記表を作成するためのサンプルコード

# list1=[[1,2,3], [21,22,23], [31,32,33]]

# 「index1」は、列名になる
# index1 = [
#     "0時", "1時", "2時", "3時", "4時", "5時", "6時", "7時", "8時", "9時", "10時",
#     11時", "12時", "13時", "14時", "15時", "16時", "17時", "18時", "19時", "20時",
#     "21時", "22時", "23時"
#     ]
# columns1 =["Col1", "Col2", "Col3"]
# pd.DataFrame(data=list1, index=index1, columns=columns1)

# '''
read_file_cnt = read_file_cnt + 1