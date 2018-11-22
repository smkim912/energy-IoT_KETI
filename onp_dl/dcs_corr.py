import os
import sys
import tensorflow as tf
import numpy as np
import pandas as pd
import time
import json
from datetime import datetime, timedelta
from influxdb import InfluxDBClient, DataFrameClient
from scipy import stats
from collections import OrderedDict
from math import isnan

# Variables for Machbase
'''
MERGE_TABLE = 'DISP_ONP'
DL_START_DATE = '2018-07-26 00:00:00'
DL_END_DATE = '2018-10-16 23:59:58'

tag_list = ['DISP_PWR_S', 'DISP_PWR_M', 'DISP_DIL', 'DISP_DIL_S',
		 'DISP_DIL_M', 'HS_STM', 'WP_LOAD_B', 'WP_LOAD_T',
		 'WP_SPD_S', 'WP_SPD_M', 'WP_IN_S', 'WP_IN_M',
		 'DC1_LEV', 'DC_OUT_S', 'DC_OUT_M', 'DISP_ENG',
		 'DC2_LEV', 'DISP_VIB', 'HS_TMPT_S', 'HS_TMPT_M',
		 'DISP_GAP', 'HS_TMPT_O']
'''
# Variables for InfluxDB
HOST = '127.0.0.1'
PORT = 8086
DBNAME = 'onp_dcs'
MEASUREMENT = 'onp_dcs_dist'
DL_MEASUREMENT = '_deepl'
DL_START_DATE = '2018-10-22T16:42:00Z'
DL_END_DATE = '2018-11-11T16:42:00Z'
RESAMPLE_UNIT = 60	#1-min

client = InfluxDBClient(HOST, PORT, '', '', DBNAME)
client_df = DataFrameClient(HOST, PORT, '', '', DBNAME)
#x_tag_list = ['RAD_2CIC6153_M','RAD_2FIC6260_M','RAD_2SIC6853_M','RAD_2SP_WATT_ONP:EIC6854AUTO.RO03','RAD_2TIC6552_M'] #for tf
#x_tag_list = ['production']

#Disperser Power
'''
x_tag_list = ['DISP_PWR_S', 'DISP_DIL', 'DISP_DIL_S',
		 'DISP_DIL_M', 'HS_STM', 'WP_LOAD_B', 'WP_LOAD_T',
		 'WP_SPD_S', 'WP_SPD_M', 'WP_IN_S', 'WP_IN_M',
		 'DC1_LEV', 'DC_OUT_S', 'DC_OUT_M', 'DISP_ENG',
		 'DC2_LEV', 'DISP_VIB', 'HS_TMPT_S', 'HS_TMPT_M',
		 'DISP_GAP', 'HS_TMPT_O', 'PULP_TMPT_M']

y_tag = 'DISP_PWR_M'

EXCEL_PATH = 'dcs_power_corr.xlsx'
'''
#Heating Screw Steam
x_tag_list = ['DISP_PWR_M', 'DISP_PWR_S', 'DISP_DIL', 'DISP_DIL_S',
		 'DISP_DIL_M', 'WP_LOAD_B', 'WP_LOAD_T',
		 'WP_SPD_S', 'WP_SPD_M', 'WP_IN_S', 'WP_IN_M',
		 'DC1_LEV', 'DC_OUT_S', 'DC_OUT_M', 'DISP_ENG',
		 'DC2_LEV', 'DISP_VIB', 'HS_TMPT_S', 'HS_TMPT_M',
		 'DISP_GAP', 'HS_TMPT_O', 'PULP_TMPT_M']

y_tag = 'HS_STM'

EXCEL_PATH = 'dcs_stm_corr.xlsx'

def field_preprocessing(df):
	df = df.set_index(pd.to_datetime(df['time']))
	del df['time']
	return df



def MinMaxScaler(data):
	numerator = data - np.min(data, 0)
	denominator = np.max(data, 0) - np.min(data, 0)
	return numerator / (denominator + 1e-7)



def get_mean(client, f_name, q_measurement_name, start_date, end_date):
	query = 'SELECT MEAN("{0}") FROM "{1}" WHERE time >= \'{2}\' and time < \'{3}\''.format(f_name, q_measurement_name, start_date, end_date)
	if f_name == y_tag:
		print(query)
	db_data = client.query(query, database=DBNAME)
	if len(db_data) < 1:
		return None
	return pd.DataFrame(db_data.get_points())



def calc_correlation(X_df, Y_df):
	pearson_dict = OrderedDict()
	spearman_dict = OrderedDict()

	for one_field in x_tag_list:
		print(one_field)
		#pearson 상관계수
		_x = X_df[one_field]
		_y = Y_df[y_tag]
		#if stats.pearsonr(_x, _y)[1] >= 0.05: continue #P-value 테스트
		pearsonr = stats.pearsonr(_x, _y)[0]
		#print('pearson:')
		#print(pearsonr)
		if isnan(pearsonr): #값이 nan이면 상관관계가 0인 것으로 설정
			pearsonr = 0
		pearson_dict[one_field] = pearsonr #dict에 insert
						   
		#spearman 상관계수
		#if stats.spearmanr(_x, _y)[1] >=0.05: continue
		spearmanr = stats.spearmanr(_x, _y)[0]
		#print('spearman: ')
		#print(spearmanr)
		if isnan(spearmanr):
			spearmanr = 0
		spearman_dict[one_field] = spearmanr

	# 내림 차순으로 dict 정렬
	pearson_dict=sorted(pearson_dict.items(), key=lambda value : abs(value[1]), reverse=True)
	spearman_dict=sorted(spearman_dict.items(), key=lambda value : abs(value[1]), reverse=True)
	return pearson_dict, spearman_dict



def correlation_to_excel(pearson_result, spearman_result, object_field_name):
    pearson_df = pd.DataFrame(pearson_result, columns=[object_field_name, 'pearson_correlation'])
    spearman_df = pd.DataFrame(spearman_result, columns=[object_field_name, 'spearman_correlation'])

    pearson_df = pearson_df.set_index(pearson_df[object_field_name])
    spearman_df = spearman_df.set_index(spearman_df[object_field_name])

    pearson_df = pd.DataFrame(pearson_df[pearson_df.columns[1]])
    spearman_df = pd.DataFrame(spearman_df[spearman_df.columns[1]])

    correlations = pd.concat([pearson_df, spearman_df], axis=1, sort=True) #merge by index
    correlations = correlations.reindex(correlations['pearson_correlation'].abs().sort_values(ascending=False).index)

    writer = pd.ExcelWriter(EXCEL_PATH)
    correlations.to_excel(writer, 'Sheet1')
    writer.save()



if __name__ == '__main__':
	ts_walker = datetime.strptime(DL_START_DATE, '%Y-%m-%dT%H:%M:%SZ')
	end_ts = datetime.strptime(DL_END_DATE, '%Y-%m-%dT%H:%M:%SZ')
	cnt=0
	X_df = pd.DataFrame()
	Y_df = pd.DataFrame()
	while ts_walker < end_ts:		
		ts_range = ts_walker + timedelta(seconds=RESAMPLE_UNIT)
		target_df = pd.DataFrame()
		tmp_df = get_mean(client, y_tag, MEASUREMENT, ts_walker, ts_range)
		if tmp_df is None:
			ts_walker = ts_range
			continue
		tmp_df = field_preprocessing(tmp_df)
		tmp_df.columns = [y_tag]
		Y_df = pd.concat([Y_df, tmp_df])

		for field_name in x_tag_list:
			tmp_df = get_mean(client, field_name, MEASUREMENT, ts_walker, ts_range)
			tmp_df = field_preprocessing(tmp_df)
			tmp_df.columns = [field_name]
			target_df = pd.concat([target_df, tmp_df], axis=1)
			#client_df.write_points(pd.DataFrame(tmp_df['mean']), DL_MEASUREMENT, protocol='json')
		X_df = pd.concat([X_df, target_df])
		ts_walker = ts_range
		#if cnt == 50: break
		cnt += 1

	#X_df = MinMaxScaler(X_df)
	#Y_df = MinMaxScaler(Y_df)
	#print(X_df)
	#print(Y_df)
	pearson_result, spearman_result = calc_correlation(X_df, Y_df)
	print('=======Pearson Result=========')
	print(pearson_result)
	print('=======Spearman Result========')
	print(spearman_result)

	correlation_to_excel(pearson_result, spearman_result, y_tag)
	#influx_ts = "{:%Y-%m-%dT%H:%M:%SZ}".format(ts_range)
	#print(influx_ts)
