#we're missing occ and laod data so we will fill it in. Problem is that we cant really calcualted it using the data we have right now. We will instead just use estrrimates from the internet.


#%%
#set working directory as one folder back so that config works
import os
import re
import pandas as pd
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')
#%%
#we're oging to take in the data we have from the 8th edition transport model and convert it into LDV data so it can be compared to other datasets which are commonly reported as LDV data. 
#LDV data is efectively the sum of the lt and lv values

#we will also look inot importing data from other datasets since that might have data on lv/lt when it can be added to create ldv's

import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data = pd.read_csv('./intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))

#load datafrom 9th
# APERC\transport_data_system\intermediate_data\9th_dataset
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/9th_dataset/', 'combined_dataset_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data_9th = pd.read_csv('./intermediate_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID))

#%%
#each vehciel type will have a unique value.
#occupance will needed for all unique rows where medium is road and the Transport type is passenger
#load will be needed for all unique rows where medium is road and the Transport type is freight

#OCCUPANCY
#we will set this to 1.5 for ldvs,
#15 for buses
#1.1 for 2w
occupancy = combined_data_9th.copy()
occupancy = occupancy.loc[occupancy.Medium == 'road']
occupancy = occupancy.loc[occupancy['Transport Type'] == 'passenger']
#set measure to occupancy
occupancy.Measure = 'Occupancy'
#set unit to occupancy
occupancy.Unit = 'Passengers'
#set dataset to occupancy
occupancy.Dataset = 'Occupancy'
#set source to 'guess'
occupancy['Source']  = 'guess'

#now according to the vehicle type we will set the value to the correct value
#set all ldv's to 1.5
occupancy.loc[occupancy['Vehicle Type'] == 'ldv', 'Value'] = 1.5
#set all buses to 15
occupancy.loc[occupancy['Vehicle Type'] == 'bus', 'Value'] = 15
#set all 2w to 1.1
occupancy.loc[occupancy['Vehicle Type'] == '2w', 'Value'] = 1.1

occupancy = occupancy.drop_duplicates()
#LOAD
#set using Table 26 in the AUS Survey of Motor Vehicle Use 2020 
#ldvs = 365kg
#ht = 5625kg #this is for rigid trucks and not atriculted trucks which are basiclly dump trucks
#in tonne the above vlaues are:
#ldv = 0.365
#ht = 5.625

load = combined_data_9th.copy()
load = load.loc[load.Medium == 'road']
load = load.loc[load['Transport Type'] == 'freight']
#set measure to load
load.Measure = 'Load'
#set unit to load
load.Unit = 'Tonnes'
#set dataset to load
load.Dataset = 'Load'
#create source col and set to guess
load['Source'] = 'guess'

#now according to the vehicle type we will set the value to the correct value
#set all ldv's to 0.365
load.loc[load['Vehicle Type'] == 'ldv', 'Value'] = 0.365
#set all hts to 5.625
load.loc[load['Vehicle Type'] == 'ht', 'Value'] = 5.625
#give 2w a value of 20kg?
load.loc[load['Vehicle Type'] == '2w', 'Value'] = 0.02

load = load.drop_duplicates()
#%%
#take a look at how this affects travel km per stock 
#%%
#concat the data and then save
occ_load = pd.concat([occupancy, load])

#%%
occ_load.to_csv('./intermediate_data/estimated/occ_load_guesses{}.csv'.format(FILE_DATE_ID), index=False)
#%%