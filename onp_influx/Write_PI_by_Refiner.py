from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import time

DBNAME = 'nexg'
client = DataFrameClient('tinyos.asuscomm.com', 20052, 'root', 'root', DBNAME)

# Influx -> dataframe
def field_to_dataframe(field_name, start_date, end_date='2017-06-30T23:59:59Z', measurement_name='raw_material'): #개별 field 값들 -> DataFrame
    query = 'SELECT \"{0}\" FROM {1} WHERE time >= \'{2}\' and time <= \'{3}\''.format(field_name, measurement_name, start_date, end_date)
    df = client.query(query)
    return df


# Refiner별 PI 태그명 통일
# LB-A Refiner
LB_A_PI = {
    'RAD_1SP_WATT_LB:2ET5853.PNT':'KW1',
    'RAD_AT1048_M':'AM1',
    'RAD_2EIC5853_M':'KW_HT1',
    'RAD_1SP_WATT_LB:2ET5854.PNT':'KW2',
    'RAD_AT1051_M':'AM2',
    'RAD_2EIC5854_M':'KW_HT2',
    'RAD_2CIC5151_M':'CON',
    'RAD_2FIC5251_M':'FLOW_M',
    'RAD_2FIC5251_S':'FLOW_S'
}

# LB-B Refiner
LB_B_PI ={
    'RAD_1SP_WATT_LB:2ET5856.PNT':'KW1',
    'RAD_AT1054_M':'AM1',
    'RAD_2EIC5856_M':'KW_HT1',
    'RAD_1SP_WATT_LB:2ET5857.PNT':'KW2',
    'RAD_AT1057_M':'AM2',
    'RAD_2EIC5857_M':'KW_HT2',
    'RAD_2CIC5152_M':'CON',
    'RAD_2FIC5252_M':'FLOW_M',
    'RAD_2FIC5252_S':'FLOW_S'
}

# LB-C Refiner
LB_C_PI = {
    'RAD_3SP_DDR:EIC_5845.MEAS':'KW1',
    'RAD_1EIC5845.MV':'KW_HT1',
    'RAD_3SP_DDR:EIC_5846.MEAS':'KW2',
    'RAD_1EIC5846.MV':'KW_HT2',
    'RAD_3SP_DDR:CIC_5142.MEAS':'CON',
    'RAD_3SP_DDR:FIC_5242.MEAS':'FLOW_M'
}

# LB-D Refiner
LB_D_PI = {
    'RAD_1EIC5831':'KW1',
    'RAD_1EIC5832':'KW2'
}

# NB-A Refiner
NB_A_PI = {
    'RAD_2EIC5803S_M':'KW1',
    'RAD_AT1015_M':'AM1',
    'RAD_2EIC5803_M':'KW_HT1',
    'RAD_1SP_WATT_NB:2ET5804.PNT':'KW2',
    'RAD_AT1018_M':'AM2',
    'RAD_2EIC5804':'KW_HT2',
    'RAD_2CIC5101_M':'CON',
    'RAD_2FIC5201_M':'FLOW_M',
    'RAD_2FIC5201_S':'FLOW_S'
}

# NB-B Refiner
NB_B_PI = {
    'RAD_AT1071_M':'AM1',
    'RAD_2CIC5101B_M':'CON',
    'RAD_2FIC5201B_M':'FLOW_M',
    'RAD_2FIC5201B_S':'FLOW_S'
}

# NB-C Refiner
NB_C_PI = {
    'RAD_1SP_WATT_LB:2ET5853C.PNT':'KW1',
    'RAD_AT1078_M':'AM1',
    'RAD_2EIC5853C_M':'KW_HT1',
    'RAD_1SP_WATT_LB:2ET5854C.PNT':'KW2',
    'RAD_AT1080_M':'AM2',
    'RAD_2EIC5854C_M':'KW_HT2',
    'RAD_2CIC5153_M':'CON',
    'RAD_2FIC5253_M':'FLOW_M',
    'RAD_2FIC5253_S':'FLOW_S'
}


start_date_lst = ['2017-01-01T00:00:00Z', '2017-03-01T00:00:00Z', '2017-05-01T00:00:00Z', '2017-07-01T00:00:00Z', '2017-09-01T00:00:00Z', '2017-11-01T00:00:00Z']
end_date_lst = ['2017-02-28T23:59:59Z', '2017-04-30T23:59:59Z', '2017-06-30T23:59:59Z', '2017-08-31T23:59:59Z', '2017-10-31T23:59:59Z', '2017-12-31T23:59:59Z']

if __name__ == '__main__':

    # NB_C에 대해서만 따로 new measurement write
    measurement_name = 'PI_sampled_by_15s'
    new_measurement_name = 'NB_C_PI'
    for start_date, end_date in zip(start_date_lst, end_date_lst):
        for key in NB_C_PI.keys():
            local_time = time.time()

            df = field_to_dataframe(key, start_date, end_date, measurement_name)
            df = df[measurement_name]
            df.columns = [NB_C_PI[df.columns[0]]]
            client.write_points(df, new_measurement_name, protocol='json')

            print('elapsed time:', time.time() - local_time)