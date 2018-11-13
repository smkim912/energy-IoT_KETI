from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd

HOST = '125.140.110.217'
PORT = 48086
DBNAME = 'onp_dcs'
TABLE_NAME = 'onp_dcs_dist'
RESULT_TABLE = 'onp_dcs_dist_60s_sample'

client = InfluxDBClient(HOST, PORT, '', '', DBNAME)
client_df = DataFrameClient(HOST, PORT, '', '', DBNAME)

#start_date = '2018-10-22T16:42:00Z' #Local timezone
start_date = '2018-10-22T07:43:00Z' #GMT ;influx problem
#end_date = '2018-11-11T16:42:00Z' 
duration = 28800  #hard coding value (20-days * 24-hours * 60-mins)



def get_hour_mean(client, q_measurement_name, start_point, idx):
	query = 'SELECT MEAN(*) FROM "{0}" WHERE time >= \'{1}\' + {2}m and time < \'{1}\' + {3}m'.format(q_measurement_name, start_point, idx, idx+1)
	db_data = client.query(query, database=DBNAME)
	if len(db_data) < 1:
		return None
	return pd.DataFrame(db_data.get_points())



def field_preprocessing(df):
	df = df.set_index(pd.to_datetime(df['time']))
	del df['time']
	return df



if __name__ == '__main__':
	day_idx = 0
	p_df = pd.DataFrame()
	for day_idx in range(0, duration):	#in future, need fix to compare with end_date
		print('progress (' + str(day_idx) + '/' + str(duration) + ')')
		tmp_p_df = get_hour_mean(client, TABLE_NAME, start_date, day_idx)
		if tmp_p_df is None: continue
		tmp_p_df = field_preprocessing(tmp_p_df)
		p_df = pd.concat([p_df,tmp_p_df])
	
	p_df.to_csv(RESULT_TABLE + '.csv')
