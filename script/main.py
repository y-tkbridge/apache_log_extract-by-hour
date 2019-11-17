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
import numpy as np

from collections import defaultdict
# setting apache log format
parser = apache_log_parser.make_parser(
    '%h %l %u %t \"%r\" %>s %b pt:%D \"%{Referer}i\" \"%{User-Agent}i\"')

# 集計用の配列
sum_all_dict_access = np.array([])

# ディレクトリ内に存在するログファイル名一覧を取得する
apache_logfile_lists = glob.glob('./log_file/*')

# 全ログファイルの時間帯ごとの最大数が格納される辞書
sum_all_access_log = {}

# ログファイル数分繰り返す（tqdmはプログレスバー）
for log_file in tqdm(apache_logfile_lists):
    
    logfile_row_dict = []
    # ログフィルを展開する
    with open(log_file) as f:

        # デバッグ用 10000カウントで強制終了
        i = 0

        for line in tqdm(f.readlines()):
           
            try:
                log_data = parser(line)
                logfile_row_dict.append(log_data)

            except:
                print ("logfiled turned over")
                sys.stdout.write("line read Error ")
            finally:
                i += 1
                # if i == 2000:
                #     break

        data_frame_log_data_list = pd.DataFrame(logfile_row_dict)
        data_frame_log_data_list = data_frame_log_data_list.set_index('time_received_datetimeobj')

        print("{} file is reading".format(log_file))

        file_access_log_cnt_list = {}
        for i in range(0,24):
            # 各時間ごとのIPアドレスの集計を取得
            max_access_address_list = data_frame_log_data_list[data_frame_log_data_list.index.hour == i]

            #辞書形式に値を格納していく
            file_logs_max_cnt = {"{} time".format(i):max_access_address_list['remote_host'].value_counts().max()}
            file_access_log_cnt_list.update(file_logs_max_cnt)

        result_logfile_max_cnt = {log_file:file_access_log_cnt_list}
        
        
    sum_all_access_log.update(result_logfile_max_cnt)
# print(sum_all_access_log)
access_cnt_sum_df = pd.DataFrame(sum_all_access_log)
print(access_cnt_sum_df.mean(axis="columns"))
