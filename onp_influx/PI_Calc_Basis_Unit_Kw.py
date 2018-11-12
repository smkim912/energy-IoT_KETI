from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import time


client = InfluxDBClient('tinyos.asuscomm.com', port=20052, username='root', password='root', database='nexg')
DBNAME = 'nexg'
client_df = DataFrameClient('tinyos.asuscomm.com', port=20052, username='root', password='root', database='nexg')



def get_data(client, q_field_name, q_measurement_name, start_date, end_date):
    query = 'SELECT "{0}" FROM "{1}" WHERE time >= \'{2}\' and time <= \'{3}\''.format(q_field_name, q_measurement_name, start_date, end_date)
    db_data = client.query(query, database=DBNAME)

    return pd.DataFrame(db_data.get_points())


def get_KW(client, q_measurement_name, start_date, end_date):
    query = 'SELECT "KW1", "KW2" FROM "{0}" WHERE time >= \'{1}\' and time <= \'{2}\''.format(q_measurement_name, start_date, end_date)
    db_data = client.query(query, database=DBNAME)

    return pd.DataFrame(db_data.get_points())


def field_preprocessing(df): #field값 전처리
    df = df.set_index(pd.to_datetime(df['time'])) #데이터 시간 값을 index로
    del df['time']

    return df



start_date_lst = ['2018-04-11T00:00:00Z']
end_date_lst = ['2018-05-15T23:59:59Z']
q_measurement_lst = ['LB_A_PI', 'LB_B_PI', 'LB_C_PI', 'NB_A_PI', 'NB_C_PI']

if __name__ == '__main__':
    for measurement_name in q_measurement_lst:
        print(measurement_name)
        for start_date, end_date in zip(start_date_lst, end_date_lst):
            local_time = time.time()

            productivity = get_data(client, q_field_name='PROD', q_measurement_name=measurement_name,
                                    start_date=start_date, end_date=end_date)
            kw = get_KW(client, q_measurement_name=measurement_name, start_date=start_date, end_date=end_date)

            productivity = field_preprocessing(productivity)
            kw = field_preprocessing(kw)

            basis_unit_kw_df1 = pd.DataFrame((kw['KW1']) / ((productivity['PROD'] * 60 * 1 / 1000)), columns=['KW_BU1'])  # 원단위 전력 계산 공식
            basis_unit_kw_df1 = basis_unit_kw_df1.applymap(lambda x: 0 if x > 200 else x)   # 계산된 원단위 전력 값이 200이상이면 0으로, 원단위 전력은 항상 이 값 이하이므로
            basis_unit_kw_df1.fillna(0, inplace=True)

            basis_unit_kw_df2 = pd.DataFrame((kw['KW2']) / ((productivity['PROD'] * 60 * 1 / 1000)), columns=['KW_BU2'])
            basis_unit_kw_df2 = basis_unit_kw_df2.applymap(lambda x: 0 if x > 200 else x)
            basis_unit_kw_df2.fillna(0, inplace=True)

            basis_unit_kw = pd.concat([basis_unit_kw_df1, basis_unit_kw_df2], axis=1) # 원단위 전력1과 원단위 전력2 join

            client_df.write_points(basis_unit_kw, measurement_name, protocol='json')

            print('elapsed time :', time.time() - local_time)