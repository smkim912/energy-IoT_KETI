from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd

client = InfluxDBClient('tinyos.asuscomm.com', port=20052, username='root', password='root', database='nexg')
DBNAME = 'nexg'
client_df = DataFrameClient('tinyos.asuscomm.com', port=20052, username='root', password='root', database='nexg')



def get_data(client, q_measurement_name, start_date, end_date): # 농도는 상수 값을 사용할 것이므로 유량_M 값만 불러옴.
    query = 'SELECT "FLOW_M" FROM "{0}" WHERE time >= \'{1}\' and time <= \'{2}\''.format(q_measurement_name, start_date, end_date)
    db_data = client.query(query, database=DBNAME)

    return pd.DataFrame(db_data.get_points())


def field_preprocessing(df): #field값 전처리
    df = df.set_index(pd.to_datetime(df['time'])) #데이터 시간 값을 index로
    del df['time']

    return df



refiner_list = ['LB_A_PI', 'LB_B_PI', 'LB_C_PI', 'NB_A_PI', 'NB_B_PI', 'NB_C_PI']
start_date_lst = ['2017-01-01T00:00:00Z', '2017-03-01T00:00:00Z', '2017-05-01T00:00:00Z', '2017-07-01T00:00:00Z', '2017-09-01T00:00:00Z', '2017-11-01T00:00:00Z']
end_date_lst = ['2017-02-28T23:59:59Z', '2017-04-30T23:59:59Z', '2017-06-30T23:59:59Z', '2017-08-31T23:59:59Z', '2017-10-31T23:59:59Z', '2017-12-31T23:59:59Z']


# 생산량 = (농도(%) / 100) * 유량
if __name__ == '__main__':
    for measurement_name in refiner_list:
        print(measurement_name)
        for start_date, end_date in zip(start_date_lst, end_date_lst):

            tmp_df = get_data(client, measurement_name, start_date, end_date)
            tmp_df = field_preprocessing(tmp_df)

            if 'LB' in measurement_name:
                tmp_df['PROD'] = 4.0 / 100 * tmp_df['FLOW_M']   # LB Refiner는 농도 4.0으로 고정
            elif 'NB' in measurement_name:
                tmp_df['PROD'] = 4.3 / 100 * tmp_df['FLOW_M']   # NB Refiner는 농도 4.3으로 고정

            client_df.write_points(pd.DataFrame(tmp_df['PROD'], columns=['PROD']), measurement_name, protocol='json')