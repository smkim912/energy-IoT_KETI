import os
import sys
import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.layers import LSTM
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
import math
from sklearn.metrics import mean_squared_error

np.set_printoptions(threshold=np.nan, formatter=dict(float=lambda x: "%.3f" % x))
log = open('keras.log', 'w')

def create_dataset(dataset, look_back=1):
	dataX, dataY = [], []
	for i in range(len(dataset)-look_back-1):
		a = dataset[i:(i + look_back), :]
		dataX.append(a)
		dataY.append(dataset[i + look_back, 4])	
	return np.array(dataX), np.array(dataY)
									 
# file loader
sydtpath = "./"
naturalEndoTekCode = "onp_train"
fullpath = sydtpath + os.path.sep + naturalEndoTekCode + '.csv'
pandf = pd.read_csv(fullpath, index_col="time")
									 
# convert nparray
#nparr = pandf['Close'].values[::-1]
#nparr = pandf.values[::-1].reshape(-1, 1)
#nparr = pandf['RAD_2EIC6854_M'].values.reshape(-1, 1)
nparr = pandf.values
nparr.astype('float32')
#print(nparr)

# normalization
scaler = MinMaxScaler(feature_range=(0, 1))
nptf = scaler.fit_transform(nparr)
	 
# split train, test
train_size = int(len(nptf) * 0.7)
test_size = len(nptf) - train_size
train, test = nptf[0:train_size, :], nptf[train_size:len(nptf), :]
#print(len(train), len(test))
	 
# create dataset for learning
look_back = 3
trainX, trainY = create_dataset(train, look_back)
testX, testY = create_dataset(test, look_back)

#print(trainX)
#print('-------------------')
#print(trainY)
#print('-------------------')
#sys.exit()

# reshape input to be [samples, time steps, features]
trainX = np.reshape(trainX, (trainX.shape[0], look_back, 5))
testX = np.reshape(testX, (testX.shape[0], look_back, 5))

# simple lstm network learning
model = Sequential()
model.add(LSTM(4, input_shape=(look_back, 5)))
model.add(Dense(1))
model.compile(loss='mean_squared_error', optimizer='adam')
model.fit(trainX, trainY, epochs=100, batch_size=128, verbose=2)
 
# make prediction
testPredict = model.predict(testX)
testPredict_ext = np.zeros((len(testPredict), 5))
testPredict_ext[:,-1:] = testPredict
testPredict = scaler.inverse_transform(testPredict_ext)[:,-1:]
log.write('\ntestPredict>\n')
log.write(str(testPredict))
testY_ext = np.zeros((len(testY), 5))
testY_ext[:,-1] = testY
testY = scaler.inverse_transform(testY_ext)[:,-1]
log.write('\ntestY>\n')
log.write(str(testY))
testScore = math.sqrt(mean_squared_error(testY, testPredict))
print('Train Score: %.2f RMSE' % testScore)
log.write('Train Score: %.2f RMSE' % testScore)

'''
# predict last value (or tomorrow?)
lastX = nptf[-1]
lastX = np.reshape(lastX, (1, 1, 1))
lastY = model.predict(lastX)
lastY = scaler.inverse_transform(lastY)
print('Predict the Close value of final day: %d' % lastY)  # 데이터 입력 마지막 다음날 종가 예측
'''
# plot
#plt.plot(testPredict)
#plt.plot(testY)
#plt.show()
