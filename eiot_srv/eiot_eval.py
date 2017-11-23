#!/usr/bin/python
#-*- coding: utf-8 -*-
#Author: Seongmin Kim(KETI), https://github.com//smkim912

#e-IoT Server Program
import paho.mqtt.client as mqtt
import time
import json
import copy
from datetime import datetime
from influxdb import InfluxDBClient

VERSION = '171115.01'
print 'Start a server of e-IoT analyzer system. ver_' + VERSION

DBNAME = 'eiotdb'
MQTTIP = '10.200.4.56'
MQTTPORT = 1883
CNT = 1				#evaluate the insert performance of 66 devices
start_time = 0
end_time = 0

RESP_OK = {"rsc":2001,"to":"/Mobius","fr":"material","rqi":"","pc":""}
RESP_PUB_TOPIC = "/oneM2M/resp/Mobius/material/json"
REQ_SUB_TOPIC = "/oneM2M/req/#"


class InfluxDB :
	m_conn = None
	
	def __init__(self) :
		pass

	def open(self, dbname) :
		bConti = True
		while bConti :
			try :
				self.m_dConn = InfluxDBClient('127.0.0.1', 8086, '', '', dbname)
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
	
	def __init__(self, dbname) :
		self.m_oDBConn = InfluxDB() 
		self.m_oDBConn.open(dbname)

	def insert(self, timestamp, aeid, loadname, i_value, kw_value) :
		json_body = [
			{ 
				"measurement": aeid, 
				"tags": {
					"C_LOAD_NAME": loadname
				},
				"fields": {
					"C_I_3PM_AVG": i_value,
					"C_KW_TOT_AVG": kw_value
				},
				"time": timestamp
			}
		]
#		print json_body
		self.m_oDBConn.insertData(json_body)
		return 0


g_influxdbconn = InfluxDBManager(DBNAME)


def payloadParser(payload):
	global CNT
	try:
		op = payload['op']
		if op != 5: 
			return
	except KeyError, e:
		return
	sur = payload['pc']['m2m:sgn']['sur']
	sur_lst = sur.split('/')
	aeid = sur_lst[3] 
	loadname = sur_lst[4]
	#print 'aeid: ' + aeid + ', loadname: ' + loadname
	cin = payload['pc']['m2m:sgn']['nev']['rep']['m2m:cin']
	i_value = float(cin['con']['C_I_3PM_AVG'])
	kw_value = float(cin['con']['C_KW_TOT_AVG'])
	#print 'i: '+ str(i_value) + ', kw: ' + str(kw_value)
	influx_ts = int(cin['rn']) * 1000000000 /1000
	
	if loadname[:9] == "31HT-MAIN":
		print "Inserting ", int(cin['rn'])
		CNT = 1

	g_influxdbconn.insert(influx_ts, aeid, loadname, i_value, kw_value)
	
	resp_dict = copy.deepcopy(RESP_OK)
	resp_dict["rqi"] = payload['rqi']
	#print resp_dict
	client.publish(RESP_PUB_TOPIC, json.dumps(resp_dict))

	if CNT >= 66:
		end_time = datetime.now()
		e_time = end_time - start_time
		#print "Elapsed time: ", e_time
		CNT = 1



def on_disconnect(client, userdata, rc):
	print "Disconnected from MQTT server with code: %s" % rc + "\n"
	while rc != 0:
		time.sleep(1)
		print "Reconnecting..."
		rc=client.reconnect()

def on_connect(client, userdata, flags, rc):
	print ("Connected with result coe " + str(rc))
	client.subscribe(REQ_SUB_TOPIC)

def on_message(client, userdata, msg):
	global CNT, start_time, end_time
	#print "Topic: ", msg.topic + '\nMessage: ' + str(msg.payload)
	payload = json.loads(msg.payload)
	payloadParser(payload)
	if CNT == 1:
		start_time = datetime.now()
		CNT += 1
	else:
		CNT += 1

client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message
client.connect(MQTTIP, MQTTPORT, 60)
client.loop_forever()
