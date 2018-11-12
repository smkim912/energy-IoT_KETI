# -*- coding: utf-8 -*-
# Author : Seongmin Kim , https://github.com/smkim912

import time
import datetime
import os
import sys
import requests
import threading
import json
import socket
import math
import pandas as pd
import numpy as np
from decimal import Decimal
from influxdb import InfluxDBClient
from influxdb import DataFrameClient


START_TS = 1462028400
CHUNK_SIZE = 2500
PATH = '/home/smkim/eiot/kcl_2018/analysis_perf/data/'
RESULTPATH = '/home/smkim/eiot/kcl_2018/analysis_perf/result/'
DATA_ROWS = 400000

influx_pd = DataFrameClient('127.0.0.1', 8086, '', '', 'analyze')

MIN_STARTTIME = 0
MAX_ENDTIME = 0



def one_insert():
	ts_idx = 0
#	file = '/home/smkim/eiot/201807_perf/sample_201701_1.xlsx'
	for fn in os.listdir(PATH):
		file = os.path.join(PATH,fn)
		table_name = (file.split('.')[0]).split('/')
		table_name = table_name[len(table_name)-1]
		if os.path.isfile(file):
			print(" "+file+": load start.")
			xlsx_file = pd.ExcelFile(file)
			print(" "+file+": load complete. -> insert to influxDB start.")
			exdf = xlsx_file.parse('Sheet1', header=None)
			df_header = exdf[:1].values[0]
			df_data = exdf[1:]
	
			for i in range(0,DATA_ROWS):
				key = START_TS+ts_idx
				ts_idx += 1
				try: 
					_frame = pd.DataFrame(df_data.values[i])
				except IndexError:
					break
				_frame = _frame.transpose()
				_frame.columns = df_header
				_frame['time'] = key
				_frame = _frame.set_index(pd.DatetimeIndex(_frame['time']))
				influx_pd.write_points(_frame, measurement=table_name, protocol='json')

			print(" "+file+": DB insert complete.")


def write_points_thread(timeStart, table_name, thd_idx):
	global MIN_STARTTIME, MAX_ENDTIME
	if (MIN_STARTTIME == 0) or (MIN_STARTTIME > timeStart):
		MIN_STARTTIME = timeStart

	new_table_name = table_name+'_'+str(thd_idx)

	print(" "+new_table_name+": select query start.")
	result = influx_pd.query('select A1, B1 from "'+table_name+'" limit 150000')
#	result = influx_pd.query('select mean("A1") as A1_AVG, mean("B1") as B1_AVG, mean("C1") as C1_AVG from "'+table_name+'"')
	print(" "+new_table_name+": select query complete. -> calc. start.")
	
	####### some calc #######
#	print(result)
	try: 
		calc_result = pd.DataFrame(result[table_name])
	except ValueError:
		print("         Error!!!!!")
		return

	time.sleep(0.1)
	# Compute 1: Sum up
	calc_result['CALC'] = calc_result.sum(axis=1)
	del calc_result['A1']
	del calc_result['B1']
	
	# Compute 2: 1000-Sampling
	calc_result = calc_result.sample(n=1000)

	print(" "+new_table_name+": calc. complete. -> insert to analyzer table.")

	time.sleep(0.1)
	# 1) export to csv file
	'''
	calc_result.to_csv(RESULTPATH+new_table_name+'.csv', mode='w')
	print(" "+new_table_name+": result file(.csv) complete. -> save to result/")
	'''
	# 2) write to another influxDB table
	
	influx_pd.write_points(calc_result, measurement=new_table_name, protocol='json')
	print(" "+new_table_name+": result table complete. -> (del table)")
	influx_pd.query('drop measurement "'+new_table_name+'"')
	time.sleep(0.1)

	timeEnd = time.time()
#	print (" insert start time =", timeStart)
#	print (" insert done time =", timeEnd)
	ctime = timeEnd - timeStart
	print (" Avg. run-time of data analysis = ", ctime, "s")
	if (MAX_ENDTIME == 0) or (MAX_ENDTIME < timeEnd):
		MAX_ENDTIME = timeEnd




def compute():
	print ("\n Start Test .......\n")

	timeStart = time.time()
	####################  Analysis Section  ####################
	for i in range(0, 100):		
		for fn in os.listdir(PATH):
			file = os.path.join(PATH,fn)
			table_name = (file.split('.')[0]).split('/')
			table_name = table_name[len(table_name)-1]	
			t = threading.Thread(target=write_points_thread, args=(timeStart, table_name, i))
			t.start()
			time.sleep(0.1)
	return




# main function
if __name__ == "__main__":
	#one_insert()
	compute()
	print (" \n ... __main__ finish ... ")
