#%%
import pandas as pd
import numpy as np
import re
import os
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')

#usign this to help (scroll to the bottom) https://chatgpt.com/share/67b2fbaf-7aec-8000-b575-5dc371ae01f9
#we need data for lpv, ht, bus and 2w. we can set the drive to all for now. we can set the dataset to manually_estimated_peru_stocks
#set source to google

#use the following as the columns:
default_values = {
    'economy': '14_PE',
    'medium': 'Road',
    'measure': 'Stocks',
    'dataset': 'manually_estimated_peru_stocks',
    'unit': 'Stocks',
    'fuel': 'All',
    'comment': None,
    'scope': 'national',
    'frequency': 'yearly',
    'drive': 'All',
    'source': '',
    'date': 2018
}
#total fleet: we have data from this https://www.ceicdata.com/en/indicator/peru/motor-vehicle-registered?utm_source=chatgpt.com which has data for 2015: 2.4million, 2020: 2.9million, so 2018 should be around (2.9-2.4)/(2020-2015) * (2018-2015) + 2.4 = 2.7million < i.e. 0.1 million extra stocks per year. so 2.7million
# ht: 217,900
# cars: compact cars: 43.4% of total fleet = 1,172,000
#corporate fleet: 11.2% of total fleet = 302,400 - asked chatgpt and it seems likely these are mostly lcvs, so we'll go wtih that. 
# bus: 7.2% of total fleet = 194,400
# 2w: = 2,700000-217900-1172000-194400-302400 = 813300
#actually the bus total is quite high. we can assume a lot of these are actually vans which would be lt's so why dont we split them into lts and buses. we can say 1/3 are buses and 2/3 are lts. so 194400*2/3 = 129600 are lts and 194400*1/3 = 64800 are buses
#%%
#then we will need to split lpvs into cars, suvs and vans. we will use the latest distributions from here:

folder_path = './aggregation_code'  # Replace with the actual path of the folder you want to add
import sys
sys.path.append(folder_path)
import utility_functions 
date_id = utility_functions.get_latest_date_for_data_file('./intermediate_data/estimated/','vehicle_type_distributions')
vehicle_type_distributions_file_path = './intermediate_data/estimated/vehicle_type_distributionsDATE{}.csv'.format(date_id)
vehicle_type_distributions = pd.read_csv(vehicle_type_distributions_file_path)

#extract the row that we need"
	# Source	Dataset	Comments	Date	Measure	Economy	Medium	Transport Type	Vehicle Type	Vehicle1_name	Vehicle2_name	Vehicle3_name	Vehicle1	Vehicle2	Vehicle3	Drive

# europe_suvs	Manually_inputted_data	no_comment	12/31/2018	Vehicle_type_distribution	14_PE	road	passenger	lpv	car	suv	lt	0.9	0.075	0.025	ice_g

lpv_dist = vehicle_type_distributions[(vehicle_type_distributions['Economy'] == '14_PE') & (vehicle_type_distributions['Vehicle Type'] == 'lpv') & (vehicle_type_distributions['Transport Type'] == 'passenger') & (vehicle_type_distributions['Date'] == '2018-12-31') & (vehicle_type_distributions['Drive'] == 'ice_g')]
#check its only 1 row then extract the values fpr cols Vehicle1_name	Vehicle2_name	Vehicle3_name	Vehicle1	Vehicle2	Vehicle3
if len(lpv_dist) != 1:
    raise ValueError('lpv_dist should have only 1 row')
if lpv_dist['Vehicle1_name'].values[0] != 'car' or lpv_dist['Vehicle2_name'].values[0] != 'suv' or lpv_dist['Vehicle3_name'].values[0] != 'lt':
    raise ValueError('lpv_dist should have car, suv and lt as the vehicle names')
lpv_car_dist = lpv_dist['Vehicle1'].values[0]
lpv_suv_dist = lpv_dist['Vehicle2'].values[0]
lpv_lt_dist = lpv_dist['Vehicle3'].values[0]

#%%
#we can set the source to google
estimates_by_vehicle_type = {
    'car': 1172000*lpv_car_dist,
    'suv': 1172000*lpv_suv_dist,
    'lt': 1172000*lpv_lt_dist + 194400*2/3,
    'ht': 217900,#will get split by vehicle_type_distributions fucntion
    'lcv': 302400,
    'bus': 194400*1/3,
    '2w': 813300
}
vehicle_type_to_transport_type = {
    'car': 'passenger',
    'suv': 'passenger',
    'lt': 'passenger',
    'ht': 'freight',
    'lcv': 'freight',
    'bus': 'passenger',
    '2w': 'passenger'
}   
#make a dataframe
pe_df = pd.DataFrame()

#add the estimates row by row with theirvehicle type
for vehicle_type, estimate in estimates_by_vehicle_type.items():
    row = default_values.copy()
    row['vehicle_type'] = vehicle_type
    row['value'] = estimate
    row['transport_type'] = vehicle_type_to_transport_type[vehicle_type]
    pe_df = pd.concat([pe_df, pd.DataFrame([row])], ignore_index=True)

#%%
    
#save the dataframe
import datetime
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

pe_df.to_csv('intermediate_data/Peru/manually_estimated_peru_stocks_{}.csv'.format(FILE_DATE_ID), index=False)

#%%C:\Users\finbar.maunsell\github\transport_data_system\grooming_code\1_create_estimates_for_peru_stocks.py