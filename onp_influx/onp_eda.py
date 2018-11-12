from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd

HOST = '127.0.0.1'
PORT = 8086
DBNAME = 'o_line'
TAG_PROD = 'production'
TAG_TEMP = 'RAD_2TIC6552_M'
TAG_EPOW = 'RAD_2EIC6854_M'


client = InfluxDBClient(HOST, PORT, '', '', DBNAME)
client_df = DataFrameClient(HOST, PORT, '', '', DBNAME)


value_list = [TAG_PROD, TAG_TEMP, TAG_EPOW]
start_date_lst = ['2017-01-01T00:00:00Z', '2017-02-01T00:00:00Z', '2017-03-01T00:00:00Z', '2017-04-01T00:00:00Z', '2017-05-01T00:00:00Z', '2017-06-01T00:00:00Z', '2017-07-01T00:00:00Z', '2017-08-01T00:00:00Z', '2017-09-01T00:00:00Z', '2017-10-01T00:00:00Z', '2017-11-01T00:00:00Z', '2017-12-01T00:00:00Z', '2018-01-01T00:00:00Z', '2018-02-01T00:00:00Z', '2018-03-01T00:00:00Z', '2018-04-01T00:00:00Z', '2018-05-01T00:00:00Z']
end_date_lst = ['2017-01-31T23:59:59Z', '2017-02-28T23:59:59Z', '2017-03-31T23:59:59Z', '2017-04-30T23:59:59Z', '2017-05-31T23:59:59Z', '2017-06-30T23:59:59Z', '2017-07-31T23:59:59Z', '2017-08-31T23:59:59Z', '2017-09-30T23:59:59Z', '2017-10-31T23:59:59Z', '2017-11-30T23:59:59Z', '2017-12-31T23:59:59Z', '2018-01-31T23:59:59Z', '2018-02-28T23:59:59Z', '2018-03-31T23:59:59Z', '2018-04-30T23:59:59Z', '2018-05-15T23:59:59Z']



def get_mean(client, f_name, q_measurement_name, start_date, end_date):
	query = 'SELECT MEAN("{0}") FROM "{1}" WHERE time >= \'{2}\' and time <= \'{3}\''.format(f_name, q_measurement_name, start_date, end_date)
	db_data = client.query(query, database=DBNAME)

	return pd.DataFrame(db_data.get_points())



def get_median(client, f_name, q_measurement_name, start_date, end_date):
	query = 'SELECT MEDIAN("{0}") FROM "{1}" WHERE time >= \'{2}\' and time <= \'{3}\''.format(f_name, q_measurement_name, start_date, end_date)
	db_data = client.query(query, database=DBNAME)

	return pd.DataFrame(db_data.get_points())



def get_min(client, f_name, q_measurement_name, start_date, end_date):
	query = 'SELECT MIN("{0}") FROM "{1}" WHERE time >= \'{2}\' and time <= \'{3}\''.format(f_name, q_measurement_name, start_date, end_date)
	db_data = client.query(query, database=DBNAME)

	return pd.DataFrame(db_data.get_points())



def get_max(client, f_name, q_measurement_name, start_date, end_date):
	query = 'SELECT MAX("{0}") FROM "{1}" WHERE time >= \'{2}\' and time <= \'{3}\''.format(f_name, q_measurement_name, start_date, end_date)
	db_data = client.query(query, database=DBNAME)

	return pd.DataFrame(db_data.get_points())



def field_preprocessing(df):
	df = df.set_index(pd.to_datetime(df['time']))
	del df['time']

	return df



# 생산량 = (농도(%) / 100) * 유량
if __name__ == '__main__':
	for field_name in value_list:
		if field_name == 'production':
			measurement_name = 'prod'
		else:
			measurement_name = 'onp'

		print('<MEAN>[' + measurement_name + ']: ' + field_name)
		for start_date, end_date in zip(start_date_lst, end_date_lst):
			print(start_date + " ~ " + end_date)
			tmp_df = get_mean(client, field_name, measurement_name, start_date, end_date)
			tmp_df = field_preprocessing(tmp_df)
			client_df.write_points(pd.DataFrame(tmp_df['mean']), field_name+'_eda',  protocol='json')

		print('<MEDIAN>[' + measurement_name + ']: ' + field_name)
		for start_date, end_date in zip(start_date_lst, end_date_lst):
			print(start_date + " ~ " + end_date)
			tmp_df = get_median(client, field_name, measurement_name, start_date, end_date)
			tmp_df = field_preprocessing(tmp_df)
			client_df.write_points(pd.DataFrame(tmp_df['median']), field_name+'_eda',  protocol='json')

		print('<MIN>[' + measurement_name + ']: ' + field_name)
		for start_date, end_date in zip(start_date_lst, end_date_lst):
			print(start_date + " ~ " + end_date)
			tmp_df = get_min(client, field_name, measurement_name, start_date, end_date)
			tmp_df = field_preprocessing(tmp_df)
			client_df.write_points(pd.DataFrame(tmp_df['min']), field_name+'_eda',  protocol='json')
		
		print('<MAX>[' + measurement_name + ']: ' + field_name)
		for start_date, end_date in zip(start_date_lst, end_date_lst):
			print(start_date + " ~ " + end_date)
			tmp_df = get_max(client, field_name, measurement_name, start_date, end_date)
			tmp_df = field_preprocessing(tmp_df)
			client_df.write_points(pd.DataFrame(tmp_df['max']), field_name+'_eda',  protocol='json')
