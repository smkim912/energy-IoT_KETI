import os
import sys
import tensorflow as tf
import numpy as np
import pandas as pd
import time
import json
import xlsxwriter
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
DBNAME = 'o_line'
MEASUREMENT = 'onp'
DL_MEASUREMENT = '_dl'
DL_START_DATE = '2017-05-15T00:00:00Z'
DL_END_DATE = '2018-05-15T23:59:59Z'
RESAMPLE_UNIT = 60 * 60 #1-hour

client = InfluxDBClient(HOST, PORT, '', '', DBNAME)
client_df = DataFrameClient(HOST, PORT, '', '', DBNAME)
x_tag_list = ['RAD_2FIC6260_M','RAD_2SIC6853_M','RAD_2SP_HIC_ONP:2HCV6962.OUT','RAD_2SP_WATT_ONP:EIC6854AUTO.RO03'] #list from correlation rank
y_tag = 'RAD_2EIC6854_M'

EXCEL_PATH = 'power_predict.xlsx'



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
	db_data = client.query(query, database=DBNAME)
	return pd.DataFrame(db_data.get_points())



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
		#if cnt == 1000: break
		cnt += 1
	
	'''
	writer = pd.ExcelWriter('onp_train.xlsx', engine='xlsxwriter')
	X_df.to_excel(writer, sheet_name='Sheet1')
	writer.save()

	writer = pd.ExcelWriter('onp_train.xlsx', engine='xlsxwriter')
	Y_df.to_excel(writer, sheet_name='Sheet1')
	writer.save()
	sys.exit()
	'''
	
	
	X_df = MinMaxScaler(X_df)
	Y_df = MinMaxScaler(Y_df)
	#print(X_df)
	#print(Y_df)
	x = X_df.values
	y = Y_df.values

	learning_rate = 0.001
	training_iters = 1
	batch_size = 128
	seq_length = 10
	display_step = 10
	num_layers = 5
	output_dim = 1
	hidden_dim = 10

	dataX = []
	dataY = []
	for i in range(0, len(y) - seq_length, seq_length):
		_x = x[i:i+seq_length]
		_y = y[i:i+seq_length]
		dataX.append(_x)
		dataY.append(_y)

	train_size = int(len(dataY) * 0.7)
	test_size = len(dataY) - train_size
	trainX, testX = np.array(dataX[0:train_size]), np.array(dataX[train_size:len(dataX)])
	trainY, testY = np.array(dataY[0:train_size]), np.array(dataY[train_size:len(dataY)])

	X = tf.placeholder(tf.float32, [None, seq_length, len(x_tag_list)])
	Y = tf.placeholder(tf.float32, [None, seq_length, 1])


	def lstm_cell():
	    cell = tf.contrib.rnn.LSTMCell(num_units=hidden_dim, state_is_tuple=True, activation=tf.tanh, reuse=tf.get_variable_scope().reuse)
	    return cell

	cell = tf.contrib.rnn.MultiRNNCell([lstm_cell() for _ in range(num_layers)], state_is_tuple=True)
	outputs, states = tf.nn.dynamic_rnn(cell, X, dtype=tf.float32, initial_state=None)
	Y_pred = tf.contrib.layers.fully_connected(outputs[:, -1], output_dim, activation_fn=None) 

	loss = tf.reduce_sum(tf.square(Y_pred - Y))
	train = tf.train.AdamOptimizer(learning_rate=learning_rate).minimize(loss)

	# RMSE
	targets = tf.placeholder(tf.float32, [None, 1])
	predictions = tf.placeholder(tf.float32, [None, 1])
	rmse = tf.sqrt(tf.reduce_mean(tf.square(targets - predictions)))

	correct_pred = tf.equal(tf.argmax(predictions, 1), tf.argmax(Y, 1))
	accuracy = tf.reduce_mean(tf.cast(correct_pred, tf.float32))

	with tf.Session() as sess:
		init = tf.global_variables_initializer()
		sess.run(init)

		for i in range(training_iters):
			total_batch = int(len(trainY) / batch_size)
			for j in range(total_batch):
				batch_trainX = trainX[j*batch_size:(j+1)*batch_size]
				batch_trainY = trainY[j*batch_size:(j+1)*batch_size]
				_, step_loss = sess.run(train, feed_dict={X: batch_trainX, Y: batch_trainY})
				print("[Step: {} loss: {}". format(i, step_loss))

		test_predict = sess.run(Y_pred, feed_dict={X: testX})
		print("Prediction:", test_predict)
		#print("Accuracy:", sess.run(accuracy, feed_dict={X: testX, Y: testY}))
		#rmse_val = sess.run(rmse, feed_dict={targets:testY, predictions:test_predict})
		#print("RMSE: {}".format(rmse_val))
	
	'''
	pearson_result, spearman_result = calc_correlation(X_df, Y_df)
	print('=======Pearson Result=========')
	print(pearson_result)
	print('=======Spearman Result========')
	print(spearman_result)

	correlation_to_excel(pearson_result, spearman_result, y_tag)
	#influx_ts = "{:%Y-%m-%dT%H:%M:%SZ}".format(ts_range)
	#print(influx_ts)
	'''
