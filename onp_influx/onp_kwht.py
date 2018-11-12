from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import numpy as np

HOST = '127.0.0.1'
PORT = 8086
DBNAME = 'o_line'
TAG_FLOW = 'RAD_2FIC6260_M'
TAG_CCNT = 'RAD_2CIC6153_M'
TAG_EPOW = 'RAD_2EIC6854_M'

client = InfluxDBClient(HOST, PORT, '', '', DBNAME)
client_df = DataFrameClient(HOST, PORT, '', '', DBNAME)


start_date_lst = ['2017-01-01T00:00:00Z', '2017-03-01T00:00:00Z', '2017-05-01T00:00:00Z', '2017-07-01T00:00:00Z', '2017-09-01T00:00:00Z', '2017-11-01T00:00:00Z', '2018-01-01T00:00:00Z', '2018-03-01T00:00:00Z', '2018-05-01T00:00:00Z']
end_date_lst = ['2017-02-28T23:59:59Z', '2017-04-30T23:59:59Z', '2017-06-30T23:59:59Z', '2017-08-31T23:59:59Z', '2017-10-31T23:59:59Z', '2017-12-31T23:59:59Z', '2018-02-28T23:59:59Z', '2018-04-30T23:59:59Z', '2018-05-15T23:59:59Z']
#start_date_lst = ['2017-01-30T00:00:00Z']
#end_date_lst = ['2017-01-31T23:59:59Z']
#start_date_lst = ['2017-01-31T06:04:00Z']
#end_date_lst = ['2017-01-31T06:05:59Z']



def get_data_prod(client, q_measurement_name, start_date, end_date):
	query = 'SELECT "production" FROM "{0}" WHERE time >= \'{1}\' and time <= \'{2}\''.format(q_measurement_name, start_date, end_date)
	db_data = client.query(query, database=DBNAME)
	return pd.DataFrame(db_data.get_points())



def get_data_epow(client, q_measurement_name, start_date, end_date):
	query = 'SELECT "RAD_2EIC6854_M" FROM "{0}" WHERE time >= \'{1}\' and time <= \'{2}\''.format(q_measurement_name, start_date, end_date)
	db_data = client.query(query, database=DBNAME)
	return pd.DataFrame(db_data.get_points())



def field_preprocessing(df):
	df = df.set_index(pd.to_datetime(df['time']))
	del df['time']
	return df



# measure e-power / production => kwh/t
if __name__ == '__main__':
	for start_date, end_date in zip(start_date_lst, end_date_lst):
		print(start_date + " ~ " + end_date)
		tmp_df_prod = get_data_prod(client, 'prod', start_date, end_date)
		tmp_df_prod = field_preprocessing(tmp_df_prod)
		#print(tmp_df_prod)
		#print("cnt: " + str(len(tmp_df_prod.index)))

		tmp_df = get_data_epow(client, 'onp', start_date, end_date)
		tmp_df = field_preprocessing(tmp_df)
		#print(tmp_df)
		#print("cnt: " + str(len(tmp_df.index)))

		if len(tmp_df_prod.index) != len(tmp_df.index):
			print("[[[Error: Mismatch row counts]]]")
			break

		tmp_df['kwht'] = tmp_df[TAG_EPOW] / (tmp_df_prod['production'] * 60 * 0.001)
		tmp_df = tmp_df.replace([np.inf, -np.inf, np.nan], 0)
		#print(tmp_df['kwht'])
	
		client_df.write_points(pd.DataFrame(tmp_df['kwht']), 'kwht_calc',  protocol='json')
