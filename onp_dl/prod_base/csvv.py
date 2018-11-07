import pandas as pd
from datetime import datetime
import time

data = pd.read_csv('DISP_ONP.csv')
ex_data = data[["TIMESTAMP","DC_OUT_S","DC_OUT_M","WP_IN_M","WP_IN_S","WP_SPD_M","WP_SPD_S","WP_LOAD_T","WP_LOAD_B","HS_STM","DISP_DIL_M","DISP_DIL_S","DISP_DIL","DISP_PWR_M","DISP_PWR_S","DISP_GAP","HS_TMPT_M","HS_TMPT_S","DC1_LEV","DISP_VIB","DC2_LEV","DISP_ENG","HS_TMPT_O","PULP_TMPT_M"]]
ex_data = ex_data.sort_values(["TIMESTAMP"], ascending=[True])
#print(ex_data.shape)
'''
for i, row in ex_data.iterrows():
	
	_ts = ex_data.loc[i,'TIMESTAMP'] 
	ex_data.loc[i,'TIMESTAMP'] = str(datetime.fromtimestamp(int(_ts)/1000000000))
	ts = ex_data.loc[i,'TIMESTAMP']
	print(_ts, ' -> ', ts)
	#print(ex_data)
'''
j = 0
i = 1000000
idx = 0
#print(ex_data.iloc[i:i+5])
while True:
	_save = ex_data.iloc[j:i]
	filename = "prod_out_" + str(idx) + ".csv"
	with open(filename, 'a') as f:
		_save.to_csv(f, index=False, mode='a', header=f.tell()==0)
	if i > 7000000: break
	j = i
	if i+1000000 > 7079463: i = 7079463
	else: i += 1000000
	idx += 1
