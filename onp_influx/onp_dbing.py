import pandas as pd
from influxdb import DataFrameClient
import os
import time

DATA_PATH = '/home/smkim/eiot/onp_influx/data/onp/'
DATA2_PATH = '/home/smkim/eiot/onp_influx/data/onp_temp/'

#client = DataFrameClient('tinyos.asuscomm.com', 20052, 'root', 'root', 'nexg')
client = DataFrameClient('127.0.0.1', 8086, '', '', 'o_line')

file_names = []
for file in os.listdir(DATA_PATH):
    if file.endswith(".csv"): 
        file_names.append(os.path.join(DATA_PATH,file))
file_names.sort()

temp_file_names = []
for file in os.listdir(DATA2_PATH):
    if file.endswith(".xlsx"):
        temp_file_names.append(os.path.join(DATA2_PATH,file))
temp_file_names.sort()

start_time = 0



# Original excel format
def preproc(df):
#	print(df)
	df_header = df.loc[3, 2:].dropna()
#	print(df_header)
	df = df.loc[4:, 2:5]
	df = df.dropna(how='all')
	df = df[:-1]	# remove 'NaT' row within whitespace 
	df.columns = df_header
	df = df.set_index(pd.to_datetime(df['time'], errors='coerce'))
	del df['time']
#	print(df)
	return df


# Second excel format
def preproc_second(df):
#	print(df)
	df = df.set_index(pd.to_datetime(df['time']))
	del df['time']
#	print(df)
	return df



def db_writer(_df):
	global start_time
	# 하나의 column씩 write
	for i in range(len(_df.columns)):
		print('(' + str(i) + ')-column processing...')

		tmp_df = pd.DataFrame(_df[_df.columns[i]])
		# 오류 메세지 제거
		tmp_df = tmp_df[~tmp_df.isin([' ', 'Shutdown', 'Calc Failed', 'Bad Input', 'Out of Serv', 'I/O Timeout', 'Pt Created', 'Bad Output', 'Intf Shut', 'Not Connect', 'Scan Timeout', 'Configure', 'No Data'])]
	
		tmp_df = tmp_df.dropna()
		tmp_df = tmp_df.astype(float)

		print(len(tmp_df))

		# 'PI_sampled_by_15s' measurement에 write
		client.write_points(tmp_df, 'onp', protocol='json')

	print('local elapsed time : ', time.time() - start_time)


'''
# data_path(.csv) // second format excel
for file_name in file_names:
	start_time = time.time()
	print(file_name)
	df = pd.read_csv(file_name)
	df = preproc_second(df)
	db_writer(df)
'''

# data2_path(.xlsx) // original format excel
for file_name in temp_file_names:
	start_time = time.time()
	print(file_name)
	xls = pd.ExcelFile(file_name)
	df = xls.parse('Sheet1', header=None)
	#df = pd.read_excel(xls, 'Sheet1')
	df = preproc(df)
	db_writer(df)
