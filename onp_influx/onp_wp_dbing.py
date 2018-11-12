import pandas as pd
import numpy as np
from influxdb import DataFrameClient
import os
import time

ABS_FILE = '/home/smkim/eiot/onp_influx/data/o_wp/wp.xlsx'
SHEET_NAME = '13_ONP Wire'
#SHEET_NAME = '스팀 사용량'

client = DataFrameClient('127.0.0.1', 8086, '', '', 'o_line')
start_time = 0



# Original excel format
def preproc(df):
	df = df.drop(df.index[5115])
	print(df.loc[5114:5116, :9])	# delete row that includes hangul
	df = df.loc[4240:6034, :9]		#hard coding #date range (2017-01-01~2018-08-28)
	#df = df.loc[4713:4714, :9])

	df = df.dropna()
	df_header = ['time', 'in_conce_S', 'in_conce_M', 'in_conce_R', 'in_charge_S', 'in_charge_M', 'Drive RL', 'out_conce_R', 'steam_sampling', 'speed']
	df.columns = df_header
	df = df.set_index(pd.to_datetime(df['time'], errors='coerce'))
	del df['time']
	#print(df)
	return df



def db_writer(_df):
	global start_time
	# column by column
	for i in range(len(_df.columns)):
		print('(' + str(i) + ')-column processing...')

		tmp_df = pd.DataFrame(_df[_df.columns[i]])
		#tmp_df = tmp_df[~tmp_df.isin([' ', '막힘'])]
		#tmp_df = tmp_df.dropna()
		tmp_df = tmp_df.replace('\.\.','.',regex=True)	# fix typo
		try:
			tmp_df = tmp_df.astype(float)
		except ValueError:
			print(tmp_df)

		print(len(tmp_df))
		client.write_points(tmp_df, 'onp_wp', protocol='json')

	print('local elapsed time : ', time.time() - start_time)



if __name__ == '__main__':
	start_time = time.time()
	xls = pd.ExcelFile(ABS_FILE)
	df = xls.parse(SHEET_NAME, header=None)
	df = preproc(df)
	db_writer(df)
