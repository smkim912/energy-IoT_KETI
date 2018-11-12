from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
from datetime import datetime

# influx -> dataframe 용
client = InfluxDBClient('tinyos.asuscomm.com', port=20052, username='root', password='root', database='nexg')
DBNAME = 'nexg'
# dataframe -> influx용
client_df = DataFrameClient('tinyos.asuscomm.com', port=20052, username='root', password='root', database='nexg')

# influx -> dataframe
def get_data(client, q_measurement_name):
    query = 'SELECT "AM1", "AM2", "CON", "FLOW_M", "FLOW_S", "KW1", "KW2", "KW_HT1", "KW_HT2" FROM "{0}" WHERE time >= \'2018-04-11T00:00:00Z\' and time <= \'2018-05-15T23:59:59Z\''.format(q_measurement_name)
    db_data = client.query(query, database=DBNAME)

    return pd.DataFrame(db_data.get_points())


def field_preprocessing(df): #field값 전처리
    df = df.set_index(pd.to_datetime(df['time'])) #데이터 시간 값을 index로
    del df['time']

    return df



if __name__ == '__main__':
    measurement_name = 'LB_A_PI'
    data = get_data(client, measurement_name)
    data = field_preprocessing(data)

    new_index = pd.date_range(datetime(2018, 4, 11), datetime(2018, 5, 15, 23, 59, 59), freq='15s') # 2018년 4월 11일 ~ 2018년 5월 15일 23시 59분 59초 까지를 15초 단위로 쪼개 index화
    influx_df = data.reindex(new_index) # 새로 만든 index를 influx에서 불러온 dataframe에 적용 => 15초 단위 PI 데이터에 빈 부분들이 포함됨(처음에 PI로 write할 때 오류 메세지가 빠진 부분들)

    interpolated_df = influx_df.interpolate() # interpolation
    b = interpolated_df[~interpolated_df.isin(influx_df).all(1)] # interpolated된 값들만 가져온 객체 b

    # 기존 AM2, FLOW_S와 type 통일
    b['AM2'] = b['AM2'].astype(int)
    b['FLOW_S'] = b['FLOW_S'].astype(int)

    client_df.write_points(b, measurement_name, protocol='json')