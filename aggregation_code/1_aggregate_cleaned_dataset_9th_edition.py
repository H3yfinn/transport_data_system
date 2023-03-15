#intention is to build script that will keep track of updates made to the 8th dataset, in the ./data_mixing_code/ scripts. This will allow usto avoid having to go through the process of selecting best data and interpoalting when we already know what the best data is. If this makes the data selection process ive built irrelevant then so be it i guess...

# The strength of doing this is that we can more easily keep track of the changes i intend to make to the 8th dataset, since the data selection process doesnt allow for that. The weakness is that im not using the manually select best data process, which is a bit of a shame. But perhaps i will find a way to use that process to keep track of the changes i make to the 8th dataset, or use it when im not sure what the best data is for datasets outside of the 8th dataset.
#this can also allow me to qucikly update the 8th dataset with changes, make more changes and iterate. 


# ACTUALLY WHY NOT MAKE USE OF THE DATA SELECTION PROCESS BY USING IT TO CHOOSE WHICH DATA POINTS TO KEEP OF THE CAHNGES I MAKE TO EH 8TH DATASET? tHIS WAY I COULD MAKE MULTIPLE CHANGES TO THE SAME VLAUE, KEEP ALL STAGES OF THE CHANGES AND THEN COMPARE THEM USING THE SELECTION PROCESS? 
#SO BAsically im changin the selection process so that the input data has to be within the specified structure and then the selection process will choose the best data pooints to =keep wihtin that?
##I should make sure to keep it so that the structure can be changed easily?
#but then this is really only useful for the 9th model as i ahve designed the input data. i may easily change this.how to ensure that this is a long term solution?
#i guess it should be desigined in a way that is easy to change and aslo is fast to create. 

#If i was to make the automatic and manual scripts into functions then perhaps i could fix these issues. This would allow me to do the following sueful things:
#-name data files according to the pruprose of the data, rather than the date of creation
#-more easily repurpose the code 

#anyway that can all be done in the future i reckon. For nwo i will just make a copy of 1_aggregate_cleaned_datasets.py but make it so it fitlers only for data needed for the 9th model.

#%%

# This will also create a concordance which will essentially create rows in the dataset where you would expect them, like between missing years.

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

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/8th_edition_transport_model/', 'eigth_edition_transport_data_final_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
eigth_edition_transport_data = pd.read_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_final_{}.csv'.format(FILE_DATE_ID))

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

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/ATO/', 'ATO_data_cleaned_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
ATO_dataset_clean = pd.read_csv('intermediate_data/ATO/ATO_data_cleaned_{}.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/item_data/', 'item_dataset_clean_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
item_data_apec_tall = pd.read_csv('intermediate_data/item_data/item_dataset_clean_' + FILE_DATE_ID + '.csv')

# {}_turnover_rate_3pct.csv'.format(FILE_DATE_ID)
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/estimated/', '_turnover_rate_3pct.csv')
FILE_DATE_ID = 'DATE{}'.format(file_date)
turnover_rate_3pct = pd.read_csv('./intermediate_data/estimated/{}_turnover_rate_3pct.csv'.format(FILE_DATE_ID))

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

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/estimated/', 'new_vehicle_efficiency_estimates_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
vehicle_eff = pd.read_csv('intermediate_data/estimated/new_vehicle_efficiency_estimates_{}.csv'.format(FILE_DATE_ID))

#load macro data
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/Macro/', 'all_macro_data_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
all_macro_data = pd.read_csv('intermediate_data/Macro/all_macro_data_{}.csv'.format(FILE_DATE_ID))

#recreate datasets so it matches all the datasets loaded above. for example, parse the following into a three element tuple:
#file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/8th_edition_transport_model/', 'eigth_edition_transport_data_final_')
#FILE_DATE_ID = 'DATE{}'.format(file_date)
#eigth_edition_transport_data = pd.read_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_final_{}.csv'.format(FILE_DATE_ID))
#so it looks like this:
#('intermediate_data/8th_edition_transport_model/', 'eigth_edition_transport_data_final_', 'intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_final_FILE_DATE_ID.csv')
#now do it for all:

############################################################

#COMBINE DATASETS

############################################################
#%%

#join data together using concat
combined_data = pd.concat([eigth_edition_transport_data, bus_passengerkm_updates, passenger_road_updates, freight_tonne_km_updates, iea_ev_all_stock_updates,eighth_ATO_vehicle_type_update,ATO_dataset_clean,item_data_apec_tall,turnover_rate_3pct,EGEDA_transport_output,EGEDA_transport_output_estimates,ATO_revenue_pkm,nearest_available_date,missing_drive_values,occ_load,vehicle_eff,all_macro_data], ignore_index=True)
#if scope col is na then set it to 'national'
combined_data['Scope'] = combined_data['Scope'].fillna('National')

#MAKE SURE ALL COLUMNS ARE STRINGS EXCEPT FOR VALUE WHICH IS FLOAT
# #TODO NOT SURE IF THIS IS THE RIGHT THING TO DO. GOING TO REMOVVE THIS FOR NOW
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

#%%
#check the index rows for duplicates and then remove the ones we dont want

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
filtered_combined_data.to_csv('intermediate_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)
#also save combined data as 'all data' in case we need to use it later
# combined_data.to_csv('output_data/all_unfiltered_data_{}.csv'.format(FILE_DATE_ID), index=False)
combined_data_concordance_new.to_csv('intermediate_data/9th_dataset/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID), index=False)
#%%