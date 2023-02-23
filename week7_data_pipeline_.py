import pandas as pd
import psycopg2 
from sqlalchemy import create_engine
import numpy as np

#path for the dataset
equip_sensor = 'equipment_sensor.csv'
maint_records = 'maintenance_records.csv'
network_sensor= 'network_sensor.csv'
equip_sensor_table = 'equip_sensor'
network_sensor_table = 'network_sensor'

def read_data_df(file_path):
    #read the data into pandas dataframe
    df = pd.read_csv(file_path)
    return df

def clean_data(data:pd.DataFrame):
    #standardise the columns names
    data.columns = data.columns.str.lower().str.replace(' ', '_')
    #check for missing data, duplicated data and remove
    data = data.dropna(axis=0, how='any')
    data = data.drop_duplicates(subset=['id', 'date', 'time'])
    return data

def sensor_maintenance_merge(sensor:pd.DataFrame, maintenance:pd.DataFrame):
    #vlookup the daily sensor maintenance type on date and sensor id
    #get date from datetime in equip_df
    sensor = sensor.merge(maintenance.loc[:,['equipment_id', 'date', 'maintenance_type']], how='left', left_on=['id', 'date'], right_on=['equipment_id', 'date'])
    sensor = sensor.drop(columns='equipment_id')
    sensor = sensor.fillna({'maintenance_type':'No Maintenance'})
    return sensor

def hourly_data(data:pd.DataFrame):
    #aggregate the 15min sensor reading to per hour reading by getting average over the hour
    #create a column with both date and time
    data['datetime'] = data['date'] + " " + data['time'].str[0:2] 
    data['datetime'] = pd.to_datetime(data['datetime'], format='%Y-%m-%d %H',) 
    data = data.groupby(['id', 'datetime',],).agg({'sensor_reading':[np.mean], 'maintenance_type':[np.unique]}).reset_index()
    #drop the agg function multindex
    data = data.droplevel(level=1, axis=1)
    data['maintenance_type'] = data['maintenance_type'].apply(lambda x: x[0])
    return data

def database_setup():
    '''store the data into postgres database
    host = localhost  docker container
    port 5442
    database = maintenance
    table1 = equip_sensor for equipment sensor reading
    table2 = network_sensor for network sesor reading
    '''
    #create connection to db
    conn = psycopg2.connect(host='localhost', port=5442, database='maintenance', user='postgres', password='fundi')
    cur = conn.cursor()
    cur.execute('create table if not exists equip_sensor (id integer, datetime date, sensor_reading float8, maintenance_type TEXT) ')
    cur.execute('create table if not exists network_sensor (id integer, datetime date, sensor_reading float8, maintenance_type TEXT)')
    conn.commit()
    cur.close()
    conn.close()
    return 1

def load_data(data_df:pd.DataFrame, table_name):
    #create connection to db
    # conn = psycopg2.connect(host='localhost', port=5442, database='maintenance', user='postgres', password='fundi')
    # cur = conn.cursor()
    #sqlalchemy connection object for pandas to sql
    engine = create_engine('postgresql+psycopg2://postgres:fundi@localhost:5442/maintenance')
    data_df.to_sql(name=table_name, con=engine, index=False, if_exists='append')
    return 1

if __name__ == '__main__':
    #read the data
    equip_df = read_data_df(equip_sensor)
    network_df = read_data_df(network_sensor)
    maint_df = read_data_df(maint_records)
    
    #clean and standardise the data
    equip_df = clean_data(equip_df)
    network_df = clean_data(network_df)
    maint_df = clean_data(maint_df)
    
    #merge the equipment and maintenance records to get the maintenance type done for an equipment
    equip_df = sensor_maintenance_merge(equip_df, maint_df)
    network_df = sensor_maintenance_merge(network_df, maint_df)

    #hourly aggregation of data
    equip_df = hourly_data(equip_df)
    network_df = hourly_data(network_df)
 
    #check the hourly aggregated data
    print(equip_df.head(),'\n' , equip_df.dtypes)
    print(network_df.head() ,'\n', network_df.dtypes)
    print(maint_df.head(),'\n' , maint_df.dtypes)

    #create the tables in database 'maintenance' for equipment and network sensors data
    database_setup()

    #load the equipment sensor and network sensor data into postgres
    load_data(equip_df, equip_sensor_table)
    load_data(network_df, network_sensor_table)