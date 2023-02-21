#%%
import pandas as pd
import numpy as np
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import os
import re
import datetime
#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
# file_date = datetime.datetime.now().strftime("%Y%m%d")
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/8th_edition_transport_model/', 'eigth_edition_transport_data_aggregated_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
# FILE_DATE_ID = 'DATE20221214'
#%%
#load 8th data
eigth_edition_transport_data = pd.read_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_aggregated_{}.csv'.format(FILE_DATE_ID))
#%%
#rename year to date
eigth_edition_transport_data = eigth_edition_transport_data.rename(columns={'Year':'Date'})
#%%
#there are too many missing values for 2017 in new_vehicle_efficiency, we will jsut fill them in with the values for 2018
new_vehicle_efficiency = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'new_vehicle_efficiency') & (eigth_edition_transport_data['Dataset'] == '8th edition transport model'),:]
#filter out anny values for 2017
new_vehicle_efficiency_not_2017 = new_vehicle_efficiency[new_vehicle_efficiency['Date'] != 2017]
new_vehicle_efficiency_2017 = new_vehicle_efficiency.loc[new_vehicle_efficiency['Date'] == 2018]
#set year to 2017
new_vehicle_efficiency_2017['Date'] = 2017
#concat
new_vehicle_efficiency = pd.concat([new_vehicle_efficiency_not_2017, new_vehicle_efficiency_2017])

#add data back to eigth_edition_transport_data
eigth_edition_transport_data = eigth_edition_transport_data.loc[~((eigth_edition_transport_data['Measure'] == 'new_vehicle_efficiency') & (eigth_edition_transport_data['Dataset'] == '8th edition transport model')),:]
eigth_edition_transport_data = pd.concat([eigth_edition_transport_data, new_vehicle_efficiency])


####################################################################################################################################
#%%
#Replace million_stocks with stocks
#timeseries data for stocks is in millions, so we need to multiply by 1,000,000
eigth_edition_transport_data.loc[eigth_edition_transport_data['Unit'] == 'million_stocks', 'Value'] = eigth_edition_transport_data.loc[eigth_edition_transport_data['Unit'] == 'million_stocks', 'Value'] * 1000000

eigth_edition_transport_data.loc[eigth_edition_transport_data['Unit'] == 'million_stocks', 'Unit'] = 'Stocks'

#units are currently in thousands for activity, and PJ for energy. We want to convert activity to singlular units#NOTE IT SEEMS THAT PERHAPS THE VALUES are IN MILLIONS FOR ACTIVITY!!!
eigth_edition_transport_data['Value'] = eigth_edition_transport_data['Value'].astype(float)
eigth_edition_transport_data.loc[eigth_edition_transport_data['Unit'].isin(['passenger_km', 'freight_tonne_km']), 'Value'] = eigth_edition_transport_data.loc[eigth_edition_transport_data['Unit'].isin(['passenger_km', 'freight_tonne_km']), 'Value'] * 1000000

#perhaps we should do efficiency as well. as it is in PJ per 1000 passenger km and PJ per 1000 freight tonne km. We want to convert to PJ per passenger km and PJ per freight tonne km
eigth_edition_transport_data.loc[eigth_edition_transport_data['Measure'] == 'Efficiency', 'Value'] = eigth_edition_transport_data.loc[eigth_edition_transport_data['Measure'] == 'Efficiency', 'Value'] / 1000000#NOTE IT SEEMS THAT PERHAPS THE VALUES SHOULD BE IN MILLIONS FOR ACTIVITY!!!
#%%
#if we have a value in vehicle type but medium is nan we can set it based on the vehicle type
#frst seperate out the data that we want to change
temp = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Vehicle Type'].notnull()) & (eigth_edition_transport_data['Medium'].isnull()),:]
#if vtype =air then medium = air
temp.loc[temp['Vehicle Type'] == 'air', 'Medium'] = 'air'
#if vtype = rail then medium = rail
temp.loc[temp['Vehicle Type'] == 'rail', 'Medium'] = 'rail'
#if vtype = nonspecified then medium = nonspecified
temp.loc[temp['Vehicle Type'] == 'nonspecified', 'Medium'] = 'nonspecified'
#if vtype = ship then medium = ship
temp.loc[temp['Vehicle Type'] == 'ship', 'Medium'] = 'ship'
#for all the rest we will set medium = road
temp.loc[temp['Medium'].isnull(), 'Medium'] = 'road'

#add data back to eigth_edition_transport_data
eigth_edition_transport_data = eigth_edition_transport_data.loc[~((eigth_edition_transport_data['Vehicle Type'].notnull()) & (eigth_edition_transport_data['Medium'].isnull())),:]
eigth_edition_transport_data = pd.concat([eigth_edition_transport_data, temp])
##########################################################################
#%%

############################################################
#calcaulte ldv data now instead of in the data_mixing_code folder. 
############################################################

#%%
INDEX_COLS = ['Date',
 'Economy',
 'Medium',
 'Measure',
 'Transport Type',
 'Drive',
 'Dataset',
 'Unit']

#%%
#we want to add up the data for lv's and lt's in energy, activity and stocks
measures = ['freight_tonne_km','passenger_km','Energy', 'Stocks']#, 'Sales']
vtypes = ['lv', 'lt']

ldv_data = eigth_edition_transport_data[eigth_edition_transport_data['Vehicle Type'].isin(vtypes)]
ldv_data = ldv_data[ldv_data['Measure'].isin(measures)]

#%%
#now pivot so we have lv and lt as columns
ldv_data_wide = ldv_data.pivot(index=INDEX_COLS, columns='Vehicle Type', values='Value')

#TAKE A LOOK at rows where one of lt or lv is na
ldv_data_wide_na = ldv_data_wide.reset_index()
ldv_data_wide_na = ldv_data_wide_na[ldv_data_wide_na['lv'].isna() | ldv_data_wide_na['lt'].isna()]
#there are a couple of instances where this could be an issue but mostly it looks like an oversight in the data. Perhaps we should plot the sum of lv and lt and see if it matches the ldv data we do have
ldv_data_wide = ldv_data_wide.reset_index()
#add so that NA values are 0
ldv_data_wide['ldv'] = ldv_data_wide['lv'].fillna(0) + ldv_data_wide['lt'].fillna(0)

#%%
#DATA TO SAVE
ldv_data_new = ldv_data_wide.drop(['lv', 'lt'], axis=1)
#make vehicle type ldv
ldv_data_new['Vehicle Type'] = 'ldv'
#make ldv into value
ldv_data_new = ldv_data_new.rename(columns={'ldv':'Value'})
#drop na from vlaue col
ldv_data_new = ldv_data_new[ldv_data_new['Value'].notna()]

#join back to main
eigth_edition_transport_data = pd.concat([eigth_edition_transport_data, ldv_data_new])

#
###################################################################
#
#%%

#WE ONLY HAVE PRE-2017 DATA FOR OCC_LOAD. We also want to create it for lt/lvs = ldv
occupance_load = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'occupancy_or_load') & (eigth_edition_transport_data['Dataset'] == '8th edition transport model'),:]
#we will set all values for BASE_YEAR to the values for 2016 in occupance_load.
occupance_load = occupance_load.loc[occupance_load.Date == 2016,:]
occupance_load['Date'] = 2017
#and also where transport type is passenger, set Measure to occupancy and where transport type is freight, set Measure to load
occupance_load.loc[occupance_load['Transport Type'] == 'passenger', 'Measure'] = 'Occupancy'
occupance_load.loc[occupance_load['Transport Type'] == 'freight', 'Measure'] = 'Load'
#set units to Passengers and Tonnes
occupance_load.loc[occupance_load['Transport Type'] == 'passenger', 'Unit'] = 'Passengers'
occupance_load.loc[occupance_load['Transport Type'] == 'freight', 'Unit'] = 'Tonnes'
#%%
#create ldv data
ldv_occ_load = occupance_load.loc[occupance_load['Vehicle Type'].isin(['lv','lt'])]
ldv_occ_load['Vehicle Type'] = 'ldv'
#group by all cols except vlaue and calcualte mean
cols = ldv_occ_load.columns.tolist()
cols.remove('Value')
cols.remove('Drive')
ldv_occ_load = ldv_occ_load.groupby(cols)['Value'].mean().reset_index()
#add ldv data back to occupance_load
occupance_load = pd.concat([occupance_load, ldv_occ_load])
#we also want this data for every drive type in the vehicle types, so we will join on drive types from eigth_edition_transport_data
#drop drive
occupance_load = occupance_load.drop(columns = ['Drive'])
drive_types = eigth_edition_transport_data[['Drive', 'Vehicle Type','Transport Type', 'Medium']].drop_duplicates()
#keep only Medium = Road
drive_types = drive_types.loc[drive_types['Medium'] == 'road',:]
occupance_load = pd.merge(occupance_load, drive_types, how = 'outer', on = ['Vehicle Type','Transport Type','Medium'])
#theres also no 2w freight drive types. Since these are constants we will jsut create them (for bev and g)
two_w = occupance_load.loc[(occupance_load['Vehicle Type'] == '2w') & (occupance_load['Transport Type'] == 'freight'),:]
two_w['Drive'] = 'bev'
occupance_load.loc[(occupance_load['Vehicle Type'] == '2w') & (occupance_load['Transport Type'] == 'freight'),'Drive'] = 'g'
occupance_load = pd.concat([occupance_load, two_w])
#add data back to eigth_edition_transport_data
eigth_edition_transport_data = eigth_edition_transport_data.loc[~((eigth_edition_transport_data['Measure'] == 'occupancy_or_load')),:]
eigth_edition_transport_data = pd.concat([eigth_edition_transport_data, occupance_load])
###################################################################
#%%


############################################################
############################################################
############################################################






# ##########################################################

#%%
#now where medium is air, rail, ship or nonspecified we will set vehicle type to nan, as well as drive
eigth_edition_transport_data.loc[eigth_edition_transport_data['Medium'].isin(['air', 'rail', 'ship', 'nonspecified']), 'Vehicle Type'] = np.nan
eigth_edition_transport_data.loc[eigth_edition_transport_data['Medium'].isin(['air', 'rail', 'ship', 'nonspecified']), 'Drive'] = np.nan
#%%
#because we have a lot of totals in our data and it would be good to compare this data agisnt them, we will calcualte some totals, for example:
#total passenger km for road
#total freight km for road
#total energy use for road
#total energy use for road passenger
#total energy use for road freight
#and eprhaps some more later

total_road_passenger_km = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'passenger_km') & (eigth_edition_transport_data['Medium'] == 'road'),:]
cols_to_group = ['Economy', 'Transport Type', 'Date','Medium', 'Unit', 'Measure', 'Dataset']
total_road_passenger_km = total_road_passenger_km.groupby(cols_to_group, as_index = False).sum(numeric_only=True)
total_road_passenger_km['Vehicle Type'] =  np.nan
total_road_passenger_km['Drive'] = np.nan

total_road_freight_km = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'freight_tonne_km') & (eigth_edition_transport_data['Medium'] == 'road'),:]
cols_to_group = ['Economy', 'Transport Type', 'Date','Medium', 'Unit', 'Measure', 'Dataset']
total_road_freight_km = total_road_freight_km.groupby(cols_to_group, as_index = False).sum(numeric_only=True)
total_road_freight_km['Vehicle Type'] =  np.nan
total_road_freight_km['Drive'] =  np.nan

total_road_energy_use = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'Energy') & (eigth_edition_transport_data['Medium'] == 'road'),:]
cols_to_group = ['Economy', 'Date','Medium', 'Unit', 'Measure', 'Dataset']
total_road_energy_use = total_road_energy_use.groupby(cols_to_group, as_index = False).sum(numeric_only=True)
total_road_energy_use['Vehicle Type'] =  np.nan
total_road_energy_use['Drive'] =  np.nan
total_road_energy_use['Transport Type'] = 'combined'

total_road_passenger_energy_use = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'Energy') & (eigth_edition_transport_data['Medium'] == 'road') & (eigth_edition_transport_data['Transport Type'] == 'passenger'),:]
cols_to_group = ['Economy', 'Transport Type', 'Date','Medium', 'Unit', 'Measure', 'Dataset']
total_road_passenger_energy_use = total_road_passenger_energy_use.groupby(cols_to_group, as_index = False).sum(numeric_only=True)
total_road_passenger_energy_use['Vehicle Type'] = np.nan
total_road_passenger_energy_use['Drive'] =  np.nan

total_road_freight_energy_use = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'Energy') & (eigth_edition_transport_data['Medium'] == 'road') & (eigth_edition_transport_data['Transport Type'] == 'freight'),:]
cols_to_group = ['Economy', 'Transport Type', 'Date','Medium', 'Unit', 'Measure', 'Dataset']
total_road_freight_energy_use = total_road_freight_energy_use.groupby(cols_to_group, as_index = False).sum(numeric_only=True)
total_road_freight_energy_use['Vehicle Type'] = np.nan
total_road_freight_energy_use['Drive'] = np.nan

#%%
#now add these totals to the data
eigth_edition_transport_data = pd.concat([eigth_edition_transport_data, total_road_passenger_km, total_road_freight_km, total_road_energy_use, total_road_passenger_energy_use, total_road_freight_energy_use])

#%%
#where dataset is 8th edition transport model, make Source col = 'Reference'
eigth_edition_transport_data.loc[eigth_edition_transport_data['Dataset'] == '8th edition transport model', 'Source'] = 'Reference'
#where dataset is 8th edition transport model (forecasted carbon neutrality scenario) make Source col = 'Carbon neutrality' and make dataset col = '8th edition transport model'
eigth_edition_transport_data.loc[eigth_edition_transport_data['Dataset'] == '8th edition transport model (forecasted carbon neutrality scenario)', 'Source'] = 'Carbon neutrality'
eigth_edition_transport_data.loc[eigth_edition_transport_data['Dataset'] == '8th edition transport model (forecasted carbon neutrality scenario)', 'Dataset'] = '8th edition transport model'
#where dataset is 8th edition transport model (forecasted reference scenario) maek Source col = 'Reference' and make dataset col = '8th edition transport model'
eigth_edition_transport_data.loc[eigth_edition_transport_data['Dataset'] == '8th edition transport model (forecasted reference scenario)', 'Source'] = 'Reference'
eigth_edition_transport_data.loc[eigth_edition_transport_data['Dataset'] == '8th edition transport model (forecasted reference scenario)', 'Dataset'] = '8th edition transport model'


#%%
#######################


#%%
#one issue is that the 8th data for road stocks is much more spoecific than the publicly available data. So we  will create aggregates of the stocks data to the same level as the publicly available data (no drive) so it can be directly compared when we merge the two datasets
#first we need to get the stocks data
stocks_data = eigth_edition_transport_data.copy()
stocks_data = stocks_data.loc[stocks_data['Measure'] == 'Stocks',:]
#print the unique vehicle types
print(stocks_data['Vehicle Type'].unique())
# #we want to sum up lt and ldv to get ldv's
# ldv_stocks_data = stocks_data.loc[stocks_data['Vehicle Type'].isin(['lt','lv'])]
no_drive_stocks_data = stocks_data.copy()
cols  = no_drive_stocks_data.columns.to_list()
cols.remove('Value')
#we'll remove drive as we want to sum up all drives
cols.remove('Drive')
no_drive_stocks_data = no_drive_stocks_data.groupby(cols, as_index = False).sum(numeric_only=True)
#set drive to nan for all of these
no_drive_stocks_data['Drive'] = np.nan

#%%
#and also do one for 'road' where we set all vehicle types to nan where medium is road
road_stocks_data = stocks_data.loc[stocks_data['Medium'] == 'road',:]
cols = road_stocks_data.columns.to_list()
cols.remove('Value')
cols.remove('Vehicle Type')
cols.remove('Drive')
road_stocks_data = road_stocks_data.groupby(cols, as_index = False).sum(numeric_only=True)
#add the vehicle type col and drive as nan
road_stocks_data['Vehicle Type'] = np.nan
road_stocks_data['Drive'] = np.nan

#and just concat the two together
stocks_data = pd.concat([no_drive_stocks_data, road_stocks_data])
#%%
#and concat to main
eigth_edition_transport_data = pd.concat([eigth_edition_transport_data, stocks_data])


#%%
#make filtered_eigth_edition_transport_data['Frequency']=='Yearly'
eigth_edition_transport_data['Frequency']='Yearly'

#%%
############################################################
############################################################
#we also want to make sure that this dataset is full, even if the vlaues we inclkudee are 0. this is so that when merged with other data in the process of estimating data based on it, it wont be missing rows and therefore cause data we want to introudce to not be introudced because we did an inner join or something. 0 values will solve this and we can then jsut drop 0 values before selecting data so it doesnt cause issues
#this is a bit hacky and could cause oversights but it seems liek a net positive. 
#so import the 9th model concordances and identify missing rows in the 8th edition data

############################################################

#FILTER FOR 9th data only

############################################################


#this will import the mdoel concordance from the 9th edition model and then filter the combined data to only include data that will fit into the concordance. Then the selection process can continue as if normal (perhaps some changes to suit this case)

#import snapshot of 9th concordance
model_concordances_base_year_measures_file_name = 'model_concordances_measures.csv'
model_concordances_measures = pd.read_csv('./intermediate_data/9th_dataset/{}'.format(model_concordances_base_year_measures_file_name))
#make Frequency yearly
model_concordances_measures['Frequency'] = 'Yearly'
#%%
new_eigth_edition_transport_data = eigth_edition_transport_data.copy()
#%%
#Easiest way to do this is to loop through the unique rows in model_concordances_measures and then if there are any rows that are not in the 8th dataset then add them in with 0 values. 
INDEX_COLS = ['Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Date', 'Economy','Frequency', 'Measure', 'Unit']

#set index
model_concordances_measures = model_concordances_measures.set_index(INDEX_COLS)
new_eigth_edition_transport_data = new_eigth_edition_transport_data.set_index(INDEX_COLS)
#%%
#use diff to identify rows that are in model_concordances_measures but not in new_eigth_edition_transport_data
missing_rows = model_concordances_measures.index.difference(new_eigth_edition_transport_data.index)
#create a new dataframe with the missing rows
missing_rows_df = pd.DataFrame(index=missing_rows)
#set Value, dataset and source to 0, 8th edition transport model and reference
missing_rows_df['Value'] = 0
missing_rows_df['Dataset'] = '8th edition transport model'
missing_rows_df['Source'] = 'Reference'
#now concat this to the new_eigth_edition_transport_data
concatted_eigth_edition_transport_data = pd.concat([new_eigth_edition_transport_data, missing_rows_df])
#%%
#now we will also remove data that isnt in the 9th edition concordance
extra_rows = concatted_eigth_edition_transport_data.index.difference(model_concordances_measures.index)
new_eigth_edition_transport_data = concatted_eigth_edition_transport_data.drop(extra_rows)
new_eigth_edition_transport_data.reset_index(inplace=True)
############################################################
############################################################
#%%
new_eigth_edition_transport_data['Date'] = new_eigth_edition_transport_data['Date'].astype(str) + '-12-31'
eigth_edition_transport_data['Date'] = eigth_edition_transport_data['Date'].astype(str) + '-12-31'
#drop turmover rrate and New_vehicle_efficiency from new_eigth_edition_transport_data Measuree col
new_eigth_edition_transport_data = new_eigth_edition_transport_data[new_eigth_edition_transport_data['Measure']!='Turnover_rate']
new_eigth_edition_transport_data = new_eigth_edition_transport_data[new_eigth_edition_transport_data['Measure']!='New_vehicle_efficiency']
#%%
#
#save data to same file
new_eigth_edition_transport_data.to_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_final_{}.csv'.format(FILE_DATE_ID), index = False)
eigth_edition_transport_data.to_csv('intermediate_data/8th_edition_transport_model/non_filtered_eigth_edition_transport_data_final_{}.csv'.format(FILE_DATE_ID), index = False)
#%%