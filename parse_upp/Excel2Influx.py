from influxdb import InfluxDBClient
import simplejson as json
import xlrd
from datetime import datetime
import time

class InfluxDB:

    m_conn = None

    def __init__(self):
        pass

    def open(self):
        bConti = True

        while bConti:
            try:
                self.m_conn=InfluxDBClient('127.0.0.1', 8086, 'root', 'root', 'test6') #on local influxDB
                bConti = False
            except Exception, e:
                time.sleep(5)
                print "fail to connect"

    def insertData(self, jsondata):
        print("Write points: {0}".format(jsondata))
        self.m_conn.write_points(jsondata)

class InfluxDBManager:

    m_oDBConn = None

    def __init__(self):
        self.m_oDBConn = InfluxDB()
        self.m_oDBConn.open()

    def insert(self, json_upp, timestamp):
        json_body = [
            {
                "measurement":"UPP",
                "fields":json.loads(json_upp),
                "time":timestamp
            }
        ]
        self.m_oDBConn.insertData(json_body)
        return 0

g_influxdbconn = InfluxDBManager()

wb = xlrd.open_workbook('/home/yeom/test/16_UPP.xlsx') #opening the excel(.xlsx) file
sh = wb.sheet_by_index(1) #excel's second sheet

upp_col_names = sh.row_values(2)[1:] #(2):sheet's third row, [1:] :column names without datetime

upp_dict = {}
for rownum in range(3, sh.nrows): #from 4th line
    row_values = sh.row_values(rownum) #row_values is one row

    for i in range(0, len(row_values)-1):
        try:
            upp_dict[upp_col_names[i]] = float(row_values[i+1]) #excluding datetime field
        except ValueError: #because of I/O Timeout data?
            pass

    json_val = json.dumps(upp_dict)
    as_datetime = datetime(*xlrd.xldate_as_tuple(row_values[0], wb.datemode)) #making datetime(type of float, For example 45678.1234567) into tuple
    influx_ts = as_datetime.strftime('%Y-%m-%d %H:%M:%S')
    # influx_ts = int(time.mktime(as_datetime.timetuple())) * 1000 #as seconds
    g_influxdbconn.insert(json_val, influx_ts)