import pandas as pd
import numpy as np
from influxdb import DataFrameClient
import os
import time
import sys

#ABS_FILE = '/home/smkim/smkim912/eiot/onp_dl/prod_base/data/ONP_DISP_FLT_DATETIME_dist.csv'
ABS_FILE = '/home/smkim/smkim912/eiot/onp_dl/prod_base/data/ONP_DISP_FLT.csv'

client = DataFrameClient('127.0.0.1', 8086, '', '', 'onp_dcs')
start_time = 0



# Excel preprocessed format
'''
def preproc(df):
	df = df.dropna()
	df = df.set_index(pd.to_datetime(df['DATETIME']))
	del df['DATETIME']
	return df
'''

# Original csv(from machbase) format
def preproc(df):
	df = df.dropna()
	df = df.drop(df[df.WP_IN_M < 100].index)
	df = df.drop(df[df.DISP_PWR_M < 100].index)
	df = df.set_index(pd.to_datetime(df['TIMESTAMP']))
	del df['TIMESTAMP']
	del df['_ARRIVAL_TIME']
	return df


def db_writer(_df):
	global start_time
	# column by column
	maxlen = len(_df.columns)
	for i in range(len(_df.columns)):
		print('(' + str(i) + '/' + str(maxlen) + ')-column processing...')

		tmp_df = pd.DataFrame(_df[_df.columns[i]])
		try:
			tmp_df = tmp_df.astype(float)
		except ValueError:
			print(tmp_df)

		print(str(len(tmp_df)) + '-lines')
		#client.write_points(tmp_df, 'onp_dcs_1', protocol='json')
		client.write_points(tmp_df, 'onp_dcs_dist', protocol='json')

	print('local elapsed time : ', time.time() - start_time)



if __name__ == '__main__':
	start_time = time.time()
	print(ABS_FILE)
	df = pd.read_csv(ABS_FILE)
	df = preproc(df)
	div = 5		#for very long data
	idx = int(len(df) / div)
	for i in range(div):
		if i == div-1:						
			_df = df.iloc[i*idx:,:]
		else:
			_df = df.iloc[i*idx:(i+1)*idx,:]
		db_writer(_df)

