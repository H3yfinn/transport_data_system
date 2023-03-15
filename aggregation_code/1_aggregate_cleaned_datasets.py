# Use this to gather together all non-aggregated data that is already cleaned and put it into the same dataset. Once thats been done we can pass it to the next script to select the best data for each time period. There  is some data manipulation to make quick fixes to make themany sources of data fit better together... but an efforts been made to keep most of that in the original cleaning files.
# It will also create a concordance which will essentially create rows in the dataset where you would expect them, like between missing years.

#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re


#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False

#%%

#load in the cleaned datasets here and then deal with them in a cell each
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/ATO_data/', 'ATO_data_cleaned_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
ATO_dataset_clean = pd.read_csv('intermediate_data/ATO_data/ATO_data_cleaned_{}.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/8th_edition_transport_model/', 'eigth_edition_transport_data_final_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
eigth_edition_transport_data = pd.read_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_final_{}.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/item_data/', 'item_dataset_clean_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
item_data_apec_tall = pd.read_csv('intermediate_data/item_data/item_dataset_clean_' + FILE_DATE_ID + '.csv')

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/estimated/', '_8th_ATO_passenger_road_updates.csv')
FILE_DATE_ID = 'DATE{}'.format(file_date)
passenger_road_updates = pd.read_csv('./intermediate_data/estimated/{}_8th_ATO_passenger_road_updates.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/estimated/', '_8th_ATO_bus_update.csv')
FILE_DATE_ID = 'DATE{}'.format(file_date)
bus_passengerkm_updates = pd.read_csv('./intermediate_data/estimated/{}_8th_ATO_bus_update.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/estimated/', '_8th_ATO_road_freight_tonne_km.csv')
FILE_DATE_ID = 'DATE{}'.format(file_date)
freight_tonne_km_updates = pd.read_csv('./intermediate_data/estimated/{}_8th_ATO_road_freight_tonne_km.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/estimated/', '_8th_iea_ev_all_stock_updates.csv')
FILE_DATE_ID = 'DATE{}'.format(file_date)
iea_ev_all_stock_updates = pd.read_csv('./intermediate_data/estimated/{}_8th_iea_ev_all_stock_updates.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/estimated/', '_8th_ATO_vehicle_type_update.csv')
FILE_DATE_ID = 'DATE{}'.format(file_date)
eighth_ATO_vehicle_type_update = pd.read_csv('./intermediate_data/estimated/{}_8th_ATO_vehicle_type_update.csv'.format(FILE_DATE_ID))

#load egeda data
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/EGEDA/', 'EGEDA_transport_output')
FILE_DATE_ID = 'DATE{}'.format(file_date)
EGEDA_transport_output = pd.read_csv('intermediate_data/EGEDA/EGEDA_transport_output' + FILE_DATE_ID + '.csv')

#load estimates using egeda data
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/estimated/', 'EGEDA_merged')
FILE_DATE_ID = 'DATE{}'.format(file_date)
EGEDA_transport_output_estimates = pd.read_csv('intermediate_data/estimated/EGEDA_merged' + FILE_DATE_ID + '.csv')

#load revenue passenger km estimates for air
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/estimated/', 'ATO_revenue_pkm')
FILE_DATE_ID = 'DATE{}'.format(file_date)
ATO_revenue_pkm = pd.read_csv('intermediate_data/estimated/ATO_revenue_pkm' + FILE_DATE_ID + '.csv')

#Get data which contains data from nearest available date, where data is misssing
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/estimated/', 'nearest_available_date')
FILE_DATE_ID = 'DATE{}'.format(file_date)
nearest_available_date = pd.read_csv('./intermediate_data/estimated/nearest_available_date{}.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/IEA/', '_iea_fuel_economy.csv')
FILE_DATE_ID = 'DATE{}'.format(file_date)
iea_fuel_economy = pd.read_csv('intermediate_data/IEA/{}_iea_fuel_economy.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/IEA/', '_evs.csv')
FILE_DATE_ID = 'DATE{}'.format(file_date)
iea_evs = pd.read_csv('intermediate_data/IEA/{}_evs.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/estimated/filled_missing_values/', 'missing_drive_values_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
missing_drive_values = pd.read_csv('intermediate_data/estimated/filled_missing_values/missing_drive_values_{}.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/estimated/', 'occ_load_guesses')
FILE_DATE_ID = 'DATE{}'.format(file_date)
occ_load = pd.read_csv('intermediate_data/estimated/occ_load_guesses{}.csv'.format(FILE_DATE_ID))
############################################################

#HANDLE SPECIFIC DATASETS

############################################################


#%%
#drop sheet column
ATO_dataset_clean = ATO_dataset_clean.drop(columns=['Sheet'])
#now drop duplicates since they are only the ones where the saemw vlaue is in multiple sheets
ATO_dataset_clean = ATO_dataset_clean.drop_duplicates()

#%%
#remove na values in value column
item_data_apec_tall = item_data_apec_tall[item_data_apec_tall['Value'].notna()]
#create a date column with month and day set to 1
item_data_apec_tall['Date'] = item_data_apec_tall['Year'].astype(str) + '-12-31'
#make frequency column and set to yearly
item_data_apec_tall['Frequency'] = 'Yearly'
#remove Year column
item_data_apec_tall = item_data_apec_tall.drop(columns=['Year'])

#%%
#handle iea evs dataset
iea_evs['Dataset'] = 'IEA EVs'
#make the first letter of words in columns uppercase
iea_evs.columns = iea_evs.columns.str.title()
#remove na values in value column
iea_evs = iea_evs[iea_evs['Value'].notna()]
#create a date column with month and day set to 12-31
iea_evs['Date'] = iea_evs['Year'].astype(str) + '-12-31'
#make frequency column and set to yearly
iea_evs['Frequency'] = 'Yearly'
#remove Year column
iea_evs = iea_evs.drop(columns=['Year'])

#%%
#handle iea fuel economy dataset

#%%
#HANDLE ESTIMATES
#nothing to do here yet as they are clean
############################################################

#COMBINE DATASETS

############################################################
#%%
#join data together using concat
combined_data = pd.concat([ATO_dataset_clean, eigth_edition_transport_data, item_data_apec_tall, iea_evs, iea_fuel_economy,  bus_passengerkm_updates, passenger_road_updates, iea_ev_all_stock_updates,eighth_ATO_vehicle_type_update,EGEDA_transport_output,EGEDA_transport_output_estimates,ATO_revenue_pkm,nearest_available_date,missing_drive_values], ignore_index=True)
#if scope col is na then set it to 'national'
combined_data['Scope'] = combined_data['Scope'].fillna('National')

# #MAKE SURE ALL COLUMNS ARE STRINGS EXCEPT FOR VALUE WHICH IS FLOAT
# #TODO REMOVING THIS BECASUSE I DONT THINK IT IS NEEDED
# for col in combined_data.columns:
#     if col != 'Value':
#         combined_data[col] = combined_data[col].astype(str)
#     else:
#         combined_data[col] = combined_data[col].astype(float)

#%%
#remove all na values in value column
combined_data = combined_data[combined_data['Value'].notna()]

#%%

#To make things faster in the manual dataseelection process, for any rows in the eighth edition dataset where the data for both the carbon neutral and reference scenarios (in source column) is the same, we will remove the carbon neutral scenario data, as we would always choose the reference data anyways.
combined_data = combined_data[combined_data['Source'] != 'Carbon neutrality']
# #set any na's to strs
# combined_data = combined_data.fillna('nan')
# combined_data[combined_data.duplicated()]
############################################################

#CHECK FOR ERRORS AND HANDLE THEM

############################################################
#%%
#Important step: make sure that units are the same for each measure so that they can be compared. If they are not then the measure should be different.
#For example, if one measure is in tonnes and another is in kg then they should just be converted. But if one is in tonnes and another is in number of vehicles then they should be different measures.
for measure in combined_data['Measure'].unique():
    if len(combined_data[combined_data['Measure'] == measure]['Unit'].unique()) > 1:
        print(measure)
        print(combined_data[combined_data['Measure'] == measure]['Unit'].unique())
        raise Exception('There are multiple units for this measure. This is not allowed. Please fix this before continuing.')

#check for any duplicates
if len(combined_data[combined_data.duplicated()]) > 0:
    raise Exception('There are duplicates in the combined data. Please fix this before continuing.')

#A fix to make thigns easier, we will concatenate the Source and Dataset columns into one column called Dataset. But if source is na then we will just use the dataset column
combined_data['Dataset'] = combined_data.apply(lambda row: row['Dataset'] if pd.isna(row['Source']) else row['Dataset'] + ' $ ' + row['Source'], axis=1)
#then drop source column
combined_data = combined_data.drop(columns=['Source'])

############################################################
 
#SAVE DATA

############################################################
#%%
#save 
combined_data.to_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)
#also save combined data as 'all data' in case we need to use it later
combined_data.to_csv('output_data/all_unfiltered_data_{}.csv'.format(FILE_DATE_ID), index=False)
combined_data_concordance_new.to_csv('intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID), index=False)
#%%