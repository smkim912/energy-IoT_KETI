from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd

HOST = '127.0.0.1'
PORT = 8086
DBNAME = 'o_line'
TAG_PROD = 'production'
TAG_TEMP = 'RAD_2TIC6552_M'
TAG_EPOW = 'RAD_2EIC6854_M'

TABLE_PROD = 'prod'
TABLE_EPOW = 'onp'

RESULT_TABLE = 'pow_per_product'
RESULT_COL1 = ['product']
RESULT_COL2 = ['power']

client = InfluxDBClient(HOST, PORT, '', '', DBNAME)
client_df = DataFrameClient(HOST, PORT, '', '', DBNAME)

start_date = '2017-01-01T00:00:00Z'
#end_date = '2018-05-15T23:59:59Z' 
duration = 12000  #hard coding value (500-days * 24-hours)



def get_hour_mean(client, f_name, q_measurement_name, start_point, idx):
	query = 'SELECT MEAN("{0}") FROM "{1}" WHERE time >= \'{2}\' + {3}h and time < \'{2}\' + {4}h'.format(f_name, q_measurement_name, start_point, idx, idx+1)
	db_data = client.query(query, database=DBNAME)
	return pd.DataFrame(db_data.get_points())


def get_hour_sum(client, f_name, q_measurement_name, start_point, idx):
	query = 'SELECT SUM("{0}") FROM "{1}" WHERE time >= \'{2}\' + {3}h and time < \'{2}\' + {4}h'.format(f_name, q_measurement_name, start_point, idx, idx+1)
	db_data = client.query(query, database=DBNAME)
	return pd.DataFrame(db_data.get_points())



def field_preprocessing(df, col_name):
	df = df.set_index(pd.to_datetime(df['time']))
	del df['time']
	df.columns = col_name
	return df



# 생산량 = (농도(%) / 100) * 유량
if __name__ == '__main__':
	day_idx = 0
	p_df = pd.DataFrame()
	t_df = pd.DataFrame()
	for day_idx in range(0, duration):	#in future, need fix to compare with end_date
		print('progress (' + str(day_idx) + '/' + str(duration) + ')')
		tmp_p_df = get_hour_mean(client, TAG_PROD, TABLE_PROD, start_date, day_idx)
		tmp_p_df = field_preprocessing(tmp_p_df, RESULT_COL1)
		p_df = pd.concat([p_df,tmp_p_df])
		tmp_t_df = get_hour_mean(client, TAG_EPOW, TABLE_EPOW, start_date, day_idx)
		tmp_t_df = field_preprocessing(tmp_t_df, RESULT_COL2)
		t_df = pd.concat([t_df,tmp_t_df])
	
	#p_df['ton'] = p_df['ton'] * 60 * 0.001	
	t_df = pd.concat([t_df, p_df], axis=1)
	#print(t_df)
	t_df.to_csv('pow_prod_eda.csv')
