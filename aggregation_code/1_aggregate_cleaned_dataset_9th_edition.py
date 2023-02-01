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

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/8th_edition_transport_model/', 'eigth_edition_transport_data_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
eigth_edition_transport_data = pd.read_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_{}.csv'.format(FILE_DATE_ID))

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

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/ATO_data/', 'ATO_data_cleaned_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
ATO_dataset_clean = pd.read_csv('intermediate_data/ATO_data/ATO_data_cleaned_{}.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/item_data/', 'item_dataset_clean_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
item_data_apec_tall = pd.read_csv('intermediate_data/item_data/item_dataset_clean_' + FILE_DATE_ID + '.csv')

############################################################

#HANDLE SPECIFIC DATASETS

############################################################
#%%
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

############################################################

#COMBINE DATASETS

############################################################
#%%
#join data together using concat
combined_data = pd.concat([eigth_edition_transport_data, bus_passengerkm_updates, passenger_road_updates, freight_tonne_km_updates, iea_ev_all_stock_updates,eighth_ATO_vehicle_type_update,ATO_dataset_clean,item_data_apec_tall], ignore_index=True)
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

#FILTER FOR 9th data only

############################################################
#%%

#import snapshot of 9th concordance
model_concordances_base_year_measures_file_name = 'model_concordances_measures.csv'
model_concordances_measures = pd.read_csv('./intermediate_data/9th_dataset/{}'.format(model_concordances_base_year_measures_file_name))

#In the concordance, let's include all data we can between 2000 and 2022, because it will be easier to filter out the data we don't want later and this may allow for interpolation of data
#so create a copy, set the date to 2000-12-31 and then append it to the original dataset. Do this for every year between 2000 and 2022, except for the year that is already in the dataset
original_years = model_concordances_measures['Date'].unique()
for year in range(2010, 2023):
    if year not in original_years:
        temp = model_concordances_measures.copy()
        temp['Date'] = year
        model_concordances_measures = pd.concat([model_concordances_measures,temp])

#set Date
model_concordances_measures['Date'] = model_concordances_measures['Date'].astype(str) + '-12-31'
#%%
new_eigth_edition_transport_data = eigth_edition_transport_data.copy()

#%%
# #filter for rail ship and air data for energy freight and passenger activiy
# x = x.reset_index()
# x = x[x['Medium'].isin(['rail', 'ship', 'air'])]
# x = x[x['Measure'].isin(['Energy', 'passenger_km', 'freight_tonne_km'])]
# y = filtered_combined_data.reset_index()
# y = y[y['Medium'].isin(['rail', 'ship', 'air'])]
# y = y[y['Measure'].isin(['Energy', 'passenger_km', 'freight_tonne_km'])]
#%%
#Easiest way to do this is to loop through the unique rows in model_concordances_measures and then if there are any rows that are not in the 8th dataset then add them in with 0 values. 
INDEX_COLS_no_scope_no_fuel_type = ['Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Date', 'Economy','Frequency', 'Measure', 'Unit']

#%%
#set index
model_concordances_measures = model_concordances_measures.set_index(INDEX_COLS_no_scope_no_fuel_type)
combined_data = combined_data.set_index(INDEX_COLS_no_scope_no_fuel_type)

#%%
#Use diff to remove data that isnt in the 9th edition concordance
extra_rows = combined_data.index.difference(model_concordances_measures.index)
filtered_combined_data = combined_data.drop(extra_rows)

#%%
#now see what we are missing:
missing_rows = model_concordances_measures.index.difference(filtered_combined_data.index)
#create a new dataframe with the missing rows
missing_rows_df = pd.DataFrame(index=missing_rows)
# save them to a csv
print('Saving missing rows to /intermediate_data/9th_dataset/missing_rows.csv')
missing_rows_df.to_csv('./intermediate_data/9th_dataset/missing_rows.csv')

filtered_combined_data.reset_index(inplace=True)
#%%

############################################################

#CREATE ANOTHER DATAFRAME AND REMOVE THE 0'S, TO SEE WHAT IS MISSING IF WE DO THAT

############################################################
#%%

#import snapshot of 9th concordance
model_concordances_base_year_measures_file_name = 'model_concordances_measures.csv'
model_concordances_measures = pd.read_csv('./intermediate_data/9th_dataset/{}'.format(model_concordances_base_year_measures_file_name))

#In the concordance, let's include all data we can between 2000 and 2022, because it will be easier to filter out the data we don't want later and this may allow for interpolation of data
#so create a copy, set the date to 2000-12-31 and then append it to the original dataset. Do this for every year between 2000 and 2022, except for the year that is already in the dataset
original_years = model_concordances_measures['Date'].unique()
for year in range(2010, 2023):
    if year not in original_years:
        temp = model_concordances_measures.copy()
        temp['Date'] = year
        model_concordances_measures = pd.concat([model_concordances_measures,temp])

#set Date
model_concordances_measures['Date'] = model_concordances_measures['Date'].astype(str) + '-12-31'
#%%
new_eigth_edition_transport_data = eigth_edition_transport_data.copy()

#%%
#set index
model_concordances_measures = model_concordances_measures.set_index(INDEX_COLS_no_scope_no_fuel_type)
# combined_data = combined_data.set_index(INDEX_COLS_no_scope_no_fuel_type)
#DROP THE 0's
combined_data_no_zeros = combined_data[combined_data['Value'] != 0]
#%%
#Use diff to remove data that isnt in the 9th edition concordance
extra_rows = combined_data_no_zeros.index.difference(model_concordances_measures.index)
filtered_combined_data_no_zeros = combined_data_no_zeros.drop(extra_rows)

#%%
#now see what we are missing:
missing_rows = model_concordances_measures.index.difference(filtered_combined_data_no_zeros.index)
#create a new dataframe with the missing rows
missing_rows_df = pd.DataFrame(index=missing_rows)
# save them to a csv
print('Saving missing rows to /intermediate_data/9th_dataset/missing_rows_no_zeros.csv')
missing_rows_df.to_csv('./intermediate_data/9th_dataset/missing_rows_no_zeros.csv')

filtered_combined_data_no_zeros.reset_index(inplace=True)
#%%

############################################################

#CREATE CONCORDANCE

############################################################
#%%
#CREATE CONCORDANCE
#create a concordance which contains all the unique rows in the combined data df, when you remove the Dataset source and value columns.
combined_data_concordance = filtered_combined_data.drop(columns=['Dataset','Comments', 'Value']).drop_duplicates()
#we will also have to split the frequency column by its type: Yearly, Quarterly, Monthly, Daily
#YEARLY
yearly = combined_data_concordance[combined_data_concordance['Frequency'] == 'Yearly']
#YEARS:
MAX = yearly['Date'].max()
MIN = yearly['Date'].min()
#using datetime creates a range of dates, separated by year with the first year being the MIN and the last year being the MAX
years = pd.date_range(start=MIN, end=MAX, freq='Y')
#drop date from ATO_data_years
yearly = yearly.drop(columns=['Date']).drop_duplicates()
#now do a cross join between the concordance and the years array
combined_data_concordance_new = yearly.merge(pd.DataFrame(years, columns=['Date']), how='cross')

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