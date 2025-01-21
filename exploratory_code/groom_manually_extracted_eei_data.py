#import eei data from input_data/EEI/activity.xlsx
#%%
import pandas as pd
import numpy as np
import os
import re
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
# Load data from 'input_data/EEI/stocks_by_economy.xlsx' by looking at the sheets names and then loading the data and grroming it
sheet_names = pd.ExcelFile('input_data/EEI/stocks_by_economy.xlsx').sheet_names
data_dict = {}
for sheet in sheet_names:
    print(sheet)
    data = pd.read_excel('input_data/EEI/stocks_by_economy.xlsx', sheet_name=sheet)
    #put in dict
    data_dict[sheet] = data

#%%
#as of the time of first coding this up we had data for sheets: New Zealand, Japan and USA. Ther rows were:
# Vehicle stocks (number of vehicles in use)
# Cars, SUV and personal light trucks
#      - gasoline and other spark ignition engines
#      - diesel (compression ignition) engine
#      - battery and plug-in hybrid electric - voluntary
#      Of which cars - VOLUNTARY 
#            gasoline and other spark ignition engines
#            diesel (compression ignition) engine
#            battery and plug-in hybrid electric 
#      Of which SUV and personal light trucks - VOLUNTARY
#            gasoline and other spark ignition engines
#            diesel (compression ignition) engine
#            battery and plug-in hybrid electric 
# Motorcycles (2-4 wheelers < 400 kg)
# Buses
# Passenger Trains
#     Of which metro and light rail - voluntary
#     Of which conventional rail - voluntary
#     Of which high-speed rail - voluntary
# Domestic passenger airplanes
# Domestic passenger ships

# Freight & Commercial road transport
#      - gasoline and other spark ignition engines
#      - diesel (compression ignition) engine
#      Of which Light Commercial Vehicle (<3.5 t) - voluntary
#                gasoline and other spark ignition engines
#                diesel (compression ignition) engine
#      Of which Medium Freight Trucks  ( 3.5 t -12 t) - voluntary
#                gasoline and other spark ignition engines
#                diesel (compression ignition) engine
#      Of which Heavy Freight Trucks (> 12 t) - voluntary
#                gasoline and other spark ignition engines
#                diesel (compression ignition) engine
# Freight trains
# Domestic freight airplanes
# Domestic freight ships

#the columns were
# Vehicle stocks (number of vehicles in use) code	unit
# 1990	1991	1992	1993	1994	1995	1996	1997	1998	1999	2000	2001	2002	2003	2004	2005	2006	2007	2008	2009	2010	2011	2012	2013	2014	2015	2016	2017	2018	2019	2020

#so we wantto drop the seconda nd third columns then set the index to the first column. Then map the first columns rows to the vehicle type, drive and transprot types we use in the model:
vehicle_type_mapping_dict = {'Cars, SUV and personal light trucks': 'lpv', 'Motorcycles (2-4 wheelers < 400 kg)': 'motorcycle', 'Buses': 'bus', 'Passenger Trains': 'train', 'Domestic passenger airplanes': 'air', 'Domestic passenger ships': 'ship', 'Freight & Commercial road transport': 'all', 'Freight trains': 'train', 'Domestic freight airplanes': 'air', 'Domestic freight ships': 'ship','     Of which Light Commercial Vehicle (<3.5 t) - voluntary': 'lcvs', '     Of which Medium Freight Trucks  ( 3.5 t -12 t) - voluntary': 'mt', '     Of which Heavy Freight Trucks (> 12 t) - voluntary': 'ht', '     Of which cars - VOLUNTARY': 'car', '     Of which SUV and personal light trucks - VOLUNTARY': 'suv'} #using all for freight and commercial road transport since we have light commercial vehicle, medium freight trucks and heavy freight trucks which will get split upby unfiltered_combined_data = pre_selection_estimation_functions.split_vehicle_types_using_distributions(unfiltered_combined_data) in main.py

drive_mapping_dict = {'     - gasoline and other spark ignition engines': 'ice_g', '     - diesel (compression ignition) engine': 'ice_d', '     - battery and plug-in hybrid electric - voluntary': 'ev'}

transport_type_mapping_dict = {'Cars, SUV and personal light trucks': 'passenger', 'Motorcycles (2-4 wheelers < 400 kg)': 'passenger', 'Buses': 'passenger', 'Passenger Trains': 'passenger', 'Domestic passenger airplanes': 'passenger', 'Domestic passenger ships': 'passenger', 'Freight & Commercial road transport': 'freight', 'Freight trains': 'freight', 'Domestic freight airplanes': 'freight', 'Domestic freight ships': 'freight'}

medium_mapping_dict = {'Cars, SUV and personal light trucks': 'road', 'Motorcycles (2-4 wheelers < 400 kg)': 'road', 'Buses': 'road', 'Passenger Trains': 'rail', 'Domestic passenger airplanes': 'air', 'Domestic passenger ships': 'ship', 'Freight & Commercial road transport': 'road', 'Freight trains': 'rail', 'Domestic freight airplanes': 'air', 'Domestic freight ships': 'ship'}
economy_name_to_code_dict = {'New Zealand': '12_NZ', 'Japan': '08_JPN', 'USA': '20_USA', 'Chile': '04_CHL', 'Canada':'03_CDA'}
#%%
all_Data = pd.DataFrame()
#now based on this create new columns in the data which are the mapped values
for sheet in sheet_names:
    data = data_dict[sheet].copy()
    data['vehicle_type'] = data['Vehicle stocks (number of vehicles in use)'].map(vehicle_type_mapping_dict)
    data['drive'] = data['Vehicle stocks (number of vehicles in use)'].map(drive_mapping_dict)
    data['transport_type'] = data['Vehicle stocks (number of vehicles in use)'].map(transport_type_mapping_dict)
    data['medium'] = data['Vehicle stocks (number of vehicles in use)'].map(medium_mapping_dict)
    
    #because of the way the the data is ordered we can set the vehicle types based on the previous non na value:
    data['vehicle_type'] = data['vehicle_type'].fillna(method='ffill')
    #same for medium
    data['medium'] = data['medium'].fillna(method='ffill')
    #and transport type
    data['transport_type'] = data['transport_type'].fillna(method='ffill')
    
    #now drop the columns we don't need
    data = data.drop(columns=['Vehicle stocks (number of vehicles in use)', 'unit', 'code'])
    
    #then melt the data date wise
    data = data.melt(id_vars=['vehicle_type', 'drive', 'transport_type', 'medium'], var_name='date', value_name='value')
    
    #set the measur eot stocks
    data['measure'] = 'stocks'
    data['unit'] = 'stocks'
    #unit will be stocks and we will times value by 1e6 to get real values
    data['value'] = data['value']*1e6
    data['comment'] = 'no_comment' 
    #set dataset to 'eei_manually_extracted'
    data['dataset'] = 'eei_manually_extracted'
    #make date have the format YYYY-MM-DD, with mm=12, dd=31
    data['date'] = data['date'].astype(str) + '-12-31'
    #set frequency to yearly
    data['frequency'] = 'yearly'
    data['fuel'] = 'all'
    data['scope'] = 'national'
    data['economy'] = economy_name_to_code_dict[sheet]
    #where drive is na then set it to all
    data['drive'] = data['drive'].fillna('all')
    #drop rows which have no data or have 0s
    data = data.dropna(subset=['value'])
    data = data[data['value']!=0]
    
    #lastly, where there is a vehicle type, transport type, year, economy comboination that has a row where drive is all as well as row where drive is ice_g and ice_d, then drop the row where drive is all so we only have the more detailed data
    #     e.g. vehicle_type	drive	transport_type	medium
    # lpv	all	passenger	road
    # lpv	ice_g	passenger	road
    # lpv	ice_d	passenger	road
    #becomes
    #     e.g. vehicle_type	drive	transport_type	medium
    # lpv	ice_g	passenger	road
    # lpv	ice_d	passenger	road
    for vehicle_type in data['vehicle_type'].unique():
        for transport_type in data['transport_type'].unique():
            for date in data['date'].unique():
                for economy in data['economy'].unique():
                    data_to_check = data[(data['vehicle_type']==vehicle_type) & (data['transport_type']==transport_type) & (data['date']==date) & (data['economy']==economy)]
                    if data_to_check.shape[0]>1:
                        if data_to_check['drive'].str.contains('all').any() and (data_to_check['drive'].str.contains('ice_g').any() and data_to_check['drive'].str.contains('ice_d').any()):
                            data = data.drop(data_to_check[data_to_check['drive'].str.contains('all')].index)
                        elif data_to_check['drive'].str.contains('all').any() and (data_to_check['drive'].str.contains('ev').any()):
                            raise ValueError('WASNT EXPECTING THIS: There is a row with drive all and drive ev for vehicle_type: {}, transport_type: {}, date: {}, economy: {}'.format(vehicle_type, transport_type, date, economy))
                        elif data_to_check['drive'].str.contains('all').any() and (data_to_check['drive'].str.contains('ice_g').any()):
                            raise ValueError('WASNT EXPECTING THIS: There is a row with drive all and drive ice_g for vehicle_type: {}, transport_type: {}, date: {}, economy: {}'.format(vehicle_type, transport_type, date, economy))
                        elif data_to_check['drive'].str.contains('all').any() and (data_to_check['drive'].str.contains('ice_d').any()):
                            raise ValueError('WASNT EXPECTING THIS: There is a row with drive all and drive ice_d for vehicle_type: {}, transport_type: {}, date: {}, economy: {}'.format(vehicle_type, transport_type, date, economy))   
    all_Data = pd.concat([all_Data, data])
#%%
import datetime
#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
all_Data.to_csv('intermediate_data/estimated/{}_eei_stocks.csv'.format(FILE_DATE_ID), index=False)

#drop any 0s since that is where there is no data (not where the data is 0)

#%%