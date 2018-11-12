import json
import ast
import sys
from datetime import datetime, timedelta
import time
import re
from machbaseAPI.machbaseAPI import machbase

# Variables to move to the configureation file
TS_FLAG = 'DATE'	#'EPOCH' or 'DATE'
SRC_TABLE = 'IF_OMG'
MERGE_TABLE = 'OMG_DISP'
COL_NAME_PREFIX = 'OMG'
CREATE_TABLE_QUERY = "create table OMG_DISP(TIMESTAMP datetime)"
START_DATE = '2018-10-18 12:39:10'
#START_TS = (1532530800-1) * 1000000000 #equal to START_DATE
DEFAULT_QUERY_RANGE = 300 #seconds
QUERY_INTERVAL = 10 #seconds
MAX_SUSP_TIME = 60*60*24*30 #1-month


correct_range = DEFAULT_QUERY_RANGE
saved_date = ""
saved_ts = 0 


# get DB data from hansol machbase server
def query_to_src_table(db):
	#global saved_ts, saved_date, correct_range
	if TS_FLAG == 'EPOCH':
		ts = str(saved_ts)
		range_ts = str(saved_ts + (correct_range * 1000000000))
		print("query: " + ts + " ~ " + range_ts)
	else:
		ts = saved_date
		range_ts = str(datetime.strptime(saved_date, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=correct_range))
		print("query: " + ts + " ~ " + range_ts)

	if db.execute("select TIMESTAMP, TAG_ID, VALUE from " + SRC_TABLE + " where TIMESTAMP > '" + ts + "' and TIMESTAMP <= '" + range_ts + "' order by TIMESTAMP") is 0:
		print(db.result())
		return 0
	return ast.literal_eval(db.result())



# 1) Merge data with the same timestamp into a single row
# 2) Merged row inserts to the other merge-table in machbase
def same_time_merge(_list):
	ins_db = machbase()
	if ins_db.open('127.0.0.1', 'SYS', 'MANAGER', 5656) is 0:
		print("Fatal error: " + ins_db.result())
		sys.exit()
	final_ts = 0
	merge_list = []
	while True:
		_max_ts = _list[len(_list)-1]['TIMESTAMP']
		_min_ts = _list[0]['TIMESTAMP']
		if TS_FLAG == 'DATE':
			lst = _min_ts.split(' ')
			min_lst = lst[0] + ' ' + lst[1]
			lst = _max_ts.split(' ')
			max_lst = lst[0] + ' ' + lst[1]
			min_ts = int(time.mktime(datetime.strptime(min_lst, '%Y-%m-%d %H:%M:%S').timetuple())) * 1000000000
			max_ts = int(time.mktime(datetime.strptime(max_lst, '%Y-%m-%d %H:%M:%S').timetuple())) * 1000000000
		else:
			min_ts = _min_ts
			max_ts = _max_ts
		diff_ts = int(max_ts) - int(min_ts)
		#print('min:'+str(min_ts)+' max:'+str(max_ts)+' diff:'+str(diff_ts))
		if diff_ts is 0:
			break
		#min_ts data to the other list
		for n in range(len(_list)-1, -1, -1):	#reversed loop
			if _list[n]['TIMESTAMP'] == _min_ts:
				merge_list.append(_list.pop(n))
		#merge data and insert to machbase
		sql = "insert into " + MERGE_TABLE + "(TIMESTAMP,"
		for m in range(0, len(merge_list)):
			#sql += re.sub("[:_.]", "", merge_list[m]['TAG_ID'][merge_list[m]['TAG_ID'].find(COL_NAME_PREFIX):])
			sql += merge_list[m]['TAG_ID']
			if m+1 < len(merge_list):
				sql += ","
		sql += ") values ('"
		sql += str(datetime.fromtimestamp(int(min_ts)/1000000000))
		sql += "',"
		for o in range(0, len(merge_list)):
			sql += merge_list[o]['VALUE']
			if o+1 < len(merge_list):
				sql += ","
		sql += ")"
		#print(sql)
		if ins_db.execute(sql) is 0:
			print(ins_db.result() + " -> execute alter table query.")
			for l in range(0, len(merge_list)):
				alt_sql = "alter table " + MERGE_TABLE + " add column ("
				#alt_sql += re.sub("[:_.]", "", merge_list[l]['TAG_ID'][merge_list[l]['TAG_ID'].find(COL_NAME_PREFIX):])
				alt_sql += merge_list[l]['TAG_ID']
				alt_sql += " float)"
				ins_db.execute(alt_sql)
				print(ins_db.result())
			if ins_db.execute(sql) is 0:
				print("fatal error: can't insert data.")
				print(ins_db.result())
				break
			print(ins_db.result())
		#inserted data's timestamp -> final_ts
		final_ts = int(min_ts)
		del merge_list[:]

	ins_db.close()
	return final_ts



# daemon loop 
def proc_merge_table(db):
	global saved_ts, saved_date, correct_range
	while True:
		ret = query_to_src_table(db)
		if ret is 0:
			print('No src data. -> Delta correction')
			if correct_range < MAX_SUSP_TIME:
				correct_range += QUERY_INTERVAL
			continue
		else:
			_list = list(ret)
		print('[' + saved_date + ']:Begin:same_time_merge()')
		#print(_list)
		ret_ = same_time_merge(_list)
		if ret_ >= saved_ts:
			saved_ts = ret_
			correct_range = DEFAULT_QUERY_RANGE
		else:
			print('No multiple data: did not works. -> Delta correction')
			if correct_range < MAX_SUSP_TIME:
				correct_range += DEFAULT_QUERY_RANGE
		saved_date = str(datetime.fromtimestamp(saved_ts/1000000000))
		print('[' + saved_date + ']:Complete:same_time_merge()')
		time.sleep(QUERY_INTERVAL)	

	return saved_ts



if __name__ == "__main__":
	# mach init
	db = machbase()
	if db.open('127.0.0.1', 'SYS', 'MANAGER', 5656) is 0:
		print(db.result())
		sys.exit()
	db.execute(CREATE_TABLE_QUERY)
	print(db.result())

	lastest_ts = 0	

	# if merge table is exist, get a timestamp of lastest data point
	if db.execute("select max(timestamp) from " + MERGE_TABLE) is not 0:
		lst = db.result().split('"')
		if lst[3] == '':
			lastest_ts = 0
		else: 
			lst_ = lst[3].split(' ')
			lastest_ts = lst_[0] + ' ' + lst_[1]
	else: 
		print(db.result())
	if lastest_ts is 0:
		saved_date = START_DATE
		saved_ts = (int(time.mktime(datetime.strptime(saved_date, '%Y-%m-%d %H:%M:%S').timetuple())) -1) * 1000000000
	else:
		saved_date = lastest_ts 
		saved_ts = int(time.mktime(datetime.strptime(saved_date, '%Y-%m-%d %H:%M:%S').timetuple())) * 1000000000
	print('Since ' + saved_date + ' =' + str(saved_ts))
	'''
	else:
		if TS_FLAG == 'EPOCH':
			saved_ts = int(lastest_ts)
			saved_date = str(datetime.fromtimestamp(saved_ts/1000000000))
		else:
			saved_date = db.result()
			saved_ts = int(time.mktime(datetime.strptime(saved_date, '%Y-%m-%d %H:%M:%S').timetuple())) * 1000000000
	'''
	print('Start timestamp is ' + saved_date + '(' + str(saved_ts) + ')')
	print(proc_merge_table(db))
