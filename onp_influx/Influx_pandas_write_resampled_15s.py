import pandas as pd
from influxdb import DataFrameClient
import os
import time

# DataFrame을 write할 client 객체 생성
client = DataFrameClient('tinyos.asuscomm.com', 20052, 'root', 'root', 'nexg')

# 파일 list
file_names = []
for file in os.listdir("C:\\Users\\KETIS\\Desktop\\2018년 4월 11일 ~ 5월 15일 KP_line_PI\\"):
    if file.endswith(".xlsx"):  # 엑셀 파일일 경우
        file_names.append('C:\\Users\\KETIS\\Desktop\\2018년 4월 11일 ~ 5월 15일 KP_line_PI\\{0}'.format(file))  # 디렉토리 내 모든 파일 이름 추가

file_names.sort()
# 엑셀 파일 하나씩
for file_name in file_names:
    start_time = time.time()
    print(file_name)

    # excel to dataframe
    df = pd.read_excel(file_name)

    # 약간의 전처리
    df = df.loc[2:]
    df = df[df.columns[2:]]
    df.columns = df.loc[2]
    df = df.loc[3:]
    df = df[:-1]
    df.index = pd.DatetimeIndex(df['time'])
    del df['time']

    # 하나의 column씩 write
    for i in range(len(df.columns)):
        print(i)

        tmp_df = pd.DataFrame(df[df.columns[i]])
        # 오류 메세지 제거
        tmp_df = tmp_df[~tmp_df.isin(['Shutdown', 'Calc Failed', 'Bad Input', 'Out of Serv', 'I/O Timeout', 'Pt Created', 'Bad Output', 'Intf Shut', 'Not Connect', 'Scan Timeout', 'Configure', 'No Data'])]

        tmp_df = tmp_df.dropna()
        tmp_df = tmp_df.astype(float)

        print(len(tmp_df))

        # 'PI_sampled_by_15s' measurement에 write
        client.write_points(tmp_df, 'PI_sampled_by_15s', protocol='json')

    print('local elapsed time : ', time.time() - start_time)