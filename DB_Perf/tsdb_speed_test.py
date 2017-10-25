# -*- coding: utf-8 -*-
# Author : Seongmin Kim , https://github.com/smkim912

import time
import datetime
import os
import sys
import requests
import json
import time, datetime
import socket
import math
import csv
from decimal import Decimal
from influxdb import InfluxDBClient


START_TS = 1462028400
CHUNK_SIZE = 2500
#TABLE_NAME = "test0"

class InfluxDB :
	m_conn = None
	
	def __init__(self) :
		pass

	def open(self) :
		bConti = True
		while bConti :
			try :
				#self.m_dConn = InfluxDBClient('tinyos.asuscomm.com', 20052, '', '', 'speed')
				self.m_dConn = InfluxDBClient('127.0.0.1', 8086, '', '', 'speed')
				bConti = False
			except Exception, e :
				#time.sleep(CmpGlobal.g_nConnectionRetryInterval)
				time.sleep(5)
				print "-------------------------- influxdb connect fail -----------------------------"

	def insertData(self, jsondata) :
		#print("Write points: {0}".format(jsondata))
		self.m_dConn.write_points(jsondata)



class InfluxDBManager :
	m_oDBConn = None
	
	def __init__(self) :
		self.m_oDBConn = InfluxDB() 
		self.m_oDBConn.open()

	def insert(self, deviceid, json_value) :
		json_body = [
			{ 
				"measurement": deviceid, 
				"fields": json.loads(json_value)
			}
		]
		#print json_body
		self.m_oDBConn.insertData(json_body)
		return 0



g_influxdbconn = InfluxDBManager()

def update():
	_dict_list = []
	_data_dict = {}

	for i in range(1,11):
		reader = csv.reader(open('_data_dict_30k.csv'))
		for row in reader:
			key = long(row[0])
			_data_dict[key] = float(row[i])
		_dict_list.append(_data_dict.copy())
		_data_dict.clear()
		print "  reading csv data ", i

	print "\n Start Test .......\n"

	_temp_list = []

	print " Total data length =", (len(_dict_list) * len(_dict_list[0]))
	print

	for list_idx in range(0,10):
		field_name = "dataSet_" + str(list_idx)
		tmp_list = []
		tmp_dict = {}
		for idx in range(0, 30000/CHUNK_SIZE):
			for chunk_idx in range((idx*CHUNK_SIZE), (idx*CHUNK_SIZE+CHUNK_SIZE)):
				key = START_TS+chunk_idx
				tmp_dict[key] = _dict_list[list_idx][START_TS+chunk_idx]
			tmp_list.append(tmp_dict.copy())
			tmp_dict.clear()
			#print " chunk ", idx

		############## Measurement Start #####################
		timeStart = time.time()
		
		for insert_idx in range(0,30000/CHUNK_SIZE):
			#influx_ts = (START_TS+idx) * 1000000000
			json_dict = {}
			json_dict = tmp_list[insert_idx]
			json_val = json.dumps(json_dict)
			g_influxdbconn.insert(field_name, json_val)
	
		timeEnd = time.time()
		############## Measurement End   #####################
	
		#print " insert start time =", timeStart
		#print " insert done time =", timeEnd
		ctime = timeEnd - timeStart
		print " Avg. run-time of 30,000 data insertion=", ctime, "seconds"
		print
		#print str(list_idx+1) + " Set" + " time Done"
		_temp_list.append(ctime)

	_avg = sum(_temp_list) / 10.0
	print " Avg ="+ str(_avg)
	_temp_list[:] = []

	return 42



# main function
if __name__ == "__main__":
    print
    timestamp = time.localtime()
    print " " + time.asctime(timestamp)
    donecheck = update()
    print (" \n ... Finishing , Closing ... ")
