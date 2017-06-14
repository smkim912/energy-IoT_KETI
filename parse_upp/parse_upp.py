#!/usr/bin/python
#-*- coding: utf-8 -*-
# Author : Seongmin Kim, https://github.com/smkim912

#server program
import time
import json
from influxdb import InfluxDBClient
from datetime import datetime



class InfluxDB :
	m_conn = None
	
	def __init__(self) :
		pass

	def open(self) :
		bConti = True
		while bConti :
			try :
				self.m_dConn = InfluxDBClient('127.0.0.1', 8086, 'keti', 'itek', 'hansoldb')
				bConti = False
			except Exception, e :
#time.sleep(CmpGlobal.g_nConnectionRetryInterval)
				time.sleep(5)
				print "-------------------------- influxdb connect fail -----------------------------"

	def insertData(self, jsondata) :
#		print("Write points: {0}".format(jsondata))
		self.m_dConn.write_points(jsondata)



class InfluxDBManager :
	m_oDBConn = None
	
	def __init__(self) :
		self.m_oDBConn = InfluxDB() 
		self.m_oDBConn.open()

	def insert(self, json_upp, timestamp) :
		json_body = [
			{ 
				"measurement": "UPP", 
				"fields": json.loads(json_upp),
				"time": timestamp
			}
		]
#		print json_body
		self.m_oDBConn.insertData(json_body)
		return 0



g_influxdbconn = InfluxDBManager()

date = datetime.today().strftime("%Y%m%d")
date = "16_UPP.txt"
fp = open(date, 'r')

upp_arr = []
lines = fp.readlines()
for line in lines:
	line_list = line.split("\t")
	line_list[len(line_list)-1] = line_list[len(line_list)-1].replace("\r\n", "")
	upp_arr.append(line_list)

my_dict = {}
for i in range(1,len(upp_arr)):
	for k in range(1,len(upp_arr[i])):
		try:
			my_dict[upp_arr[0][k]] = float(upp_arr[i][k])
		except ValueError:
			pass
	json_val = json.dumps(my_dict)
	ts = time.mktime(time.strptime(upp_arr[i][0], '%Y-%m-%d %H:%M'))
	influx_ts = int(ts) * 1000000000  # add decimal part
	g_influxdbconn.insert(json_val, influx_ts)
#	print "json_val = %s" % json_val

fp.close()
