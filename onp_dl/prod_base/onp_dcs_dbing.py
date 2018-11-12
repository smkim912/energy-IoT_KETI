import pandas as pd
import numpy as np
from influxdb import DataFrameClient
import os
import time

ABS_FILE = '/home/smkim/smkim912/eiot/onp_dl/prod_base/data/ONP_DISP_FLT_DATETIME_dist.csv'

client = DataFrameClient('127.0.0.1', 8086, '', '', 'onp_dcs')
start_time = 0



# Original excel format
def preproc(df):
	df = df.dropna()
	df = df.set_index(pd.to_datetime(df['DATETIME']))
	del df['DATETIME']
	return df



def db_writer(_df):
	global start_time
	# column by column
	print(len(_df.columns))
	for i in range(len(_df.columns)):
		print('(' + str(i) + ')-column processing...')

		tmp_df = pd.DataFrame(_df[_df.columns[i]])
		try:
			tmp_df = tmp_df.astype(float)
		except ValueError:
			print(tmp_df)

		print(len(tmp_df))
		client.write_points(tmp_df, 'onp_dcs_1', protocol='json')

	print('local elapsed time : ', time.time() - start_time)



if __name__ == '__main__':
	start_time = time.time()
	print(ABS_FILE)
	df = pd.read_csv(ABS_FILE)
	df = preproc(df)
	db_writer(df)
