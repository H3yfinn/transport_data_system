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

#there are too many missing values for 2017 in new_vehicle_efficiency, we will jsut fill them in with the values for 2018
new_vehicle_efficiency = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'new_vehicle_efficiency') & (eigth_edition_transport_data['Dataset'] == '8th edition transport model'),:]
#filter out anny values for 2017
new_vehicle_efficiency_not_2017 = new_vehicle_efficiency[new_vehicle_efficiency['Year'] != 2017]
new_vehicle_efficiency_2017 = new_vehicle_efficiency.loc[new_vehicle_efficiency['Year'] == 2018]
#set year to 2017
new_vehicle_efficiency_2017['Year'] = 2017
#concat
new_vehicle_efficiency = pd.concat([new_vehicle_efficiency_not_2017, new_vehicle_efficiency_2017])

#add data back to eigth_edition_transport_data
eigth_edition_transport_data = eigth_edition_transport_data.loc[~((eigth_edition_transport_data['Measure'] == 'new_vehicle_efficiency') & (eigth_edition_transport_data['Dataset'] == '8th edition transport model')),:]
eigth_edition_transport_data = pd.concat([eigth_edition_transport_data, new_vehicle_efficiency])

###################################################################

#%%

#WE ONLY HAVE PRE-2017 DATA FOR OCC_LOAD.
occupance_load = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'occupancy_or_load') & (eigth_edition_transport_data['Dataset'] == '8th edition transport model'),:]
#we will set all values for BASE_YEAR to the values for 2016 in occupance_load.
occupance_load = occupance_load.loc[occupance_load.Year == 2016,:]
occupance_load['Year'] = 2017

#add data back to eigth_edition_transport_data
eigth_edition_transport_data = eigth_edition_transport_data.loc[~((eigth_edition_transport_data['Measure'] == 'occupancy_or_load') & (eigth_edition_transport_data['Dataset'] == '8th edition transport model')),:]
eigth_edition_transport_data = pd.concat([eigth_edition_transport_data, occupance_load])
####################################################################################################################################
#%%
#Replace million_stocks with stocks
#timeseries data for stocks is in millions, so we need to multiply by 1,000,000
eigth_edition_transport_data.loc[eigth_edition_transport_data['Unit'] == 'million_stocks', 'Value'] = eigth_edition_transport_data.loc[eigth_edition_transport_data['Unit'] == 'million_stocks', 'Value'] * 1000000

eigth_edition_transport_data.loc[eigth_edition_transport_data['Unit'] == 'million_stocks', 'Unit'] = 'Stocks'

#units are currently in thousands for activity, and PJ for energy. We want to convert activity to singlular units#NOTE IT SEEMS THAT PERHAPS THE VALUES SHOULD BE IN MILLIONS FOR ACTIVITY!!!
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
####################################################################################################################################

#%%
#because we have a lot of totals in our data and it would be good to compare this data agisnt them, we will calcualte some totals, for example:
#total passenger km for road
#total freight km for road
#total energy use for road
#total energy use for road passenger
#total energy use for road freight
#and eprhaps some more later

total_road_passenger_km = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'passenger_km') & (eigth_edition_transport_data['Medium'] == 'road'),:]
cols_to_group = ['Economy', 'Transport Type', 'Year','Medium', 'Unit', 'Measure', 'Dataset']
total_road_passenger_km = total_road_passenger_km.groupby(cols_to_group, as_index = False).sum(numeric_only=True)
total_road_passenger_km['Vehicle Type'] = 'road'
total_road_passenger_km['Drive'] = 'road'

total_road_freight_km = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'freight_tonne_km') & (eigth_edition_transport_data['Medium'] == 'road'),:]
cols_to_group = ['Economy', 'Transport Type', 'Year','Medium', 'Unit', 'Measure', 'Dataset']
total_road_freight_km = total_road_freight_km.groupby(cols_to_group, as_index = False).sum(numeric_only=True)
total_road_freight_km['Vehicle Type'] = 'road'
total_road_freight_km['Drive'] = 'road'

total_road_energy_use = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'Energy') & (eigth_edition_transport_data['Medium'] == 'road'),:]
cols_to_group = ['Economy', 'Year','Medium', 'Unit', 'Measure', 'Dataset']
total_road_energy_use = total_road_energy_use.groupby(cols_to_group, as_index = False).sum(numeric_only=True)
total_road_energy_use['Vehicle Type'] = 'road'
total_road_energy_use['Drive'] = 'road'
total_road_energy_use['Transport Type'] = 'combined'

total_road_passenger_energy_use = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'Energy') & (eigth_edition_transport_data['Medium'] == 'road') & (eigth_edition_transport_data['Transport Type'] == 'passenger'),:]
cols_to_group = ['Economy', 'Transport Type', 'Year','Medium', 'Unit', 'Measure', 'Dataset']
total_road_passenger_energy_use = total_road_passenger_energy_use.groupby(cols_to_group, as_index = False).sum(numeric_only=True)
total_road_passenger_energy_use['Vehicle Type'] = 'road'
total_road_passenger_energy_use['Drive'] = 'road'

total_road_freight_energy_use = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'Energy') & (eigth_edition_transport_data['Medium'] == 'road') & (eigth_edition_transport_data['Transport Type'] == 'freight'),:]
cols_to_group = ['Economy', 'Transport Type', 'Year','Medium', 'Unit', 'Measure', 'Dataset']
total_road_freight_energy_use = total_road_freight_energy_use.groupby(cols_to_group, as_index = False).sum(numeric_only=True)
total_road_freight_energy_use['Vehicle Type'] = 'road'
total_road_freight_energy_use['Drive'] = 'road'

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
#one issue is that the data for stocks is much more spoecific than the publicly available data. So we  will aggregate the stocks data to the same level as the publicly available data so it can be directly compared when we merge the two datasets
#first we need to get the stocks data
stocks_data = eigth_edition_transport_data.loc[eigth_edition_transport_data['Measure'] == 'Stocks',:]
#print the unique vehicle types
print(stocks_data['Vehicle Type'].unique())
#we want to sum up lt and ldv to get ldv's
ldv_stocks_data = stocks_data.loc[stocks_data['Vehicle Type'].isin(['lt','lv'])]
ldv_stocks_data['Vehicle Type'] = 'ldv'
#set drive to road for all of these
ldv_stocks_data['Drive'] = 'road'
#sum up the datas
cols  = ldv_stocks_data.columns.to_list()
cols.remove('Value')
ldv_stocks_data = ldv_stocks_data.groupby(cols, as_index = False).sum(numeric_only=True)

#and also do one for 'road' where we set all vehicle types to 'road' where medium is road
road_stocks_data = stocks_data.loc[stocks_data['Medium'] == 'road',:]
road_stocks_data['Vehicle Type'] = 'road'
road_stocks_data['Drive'] = 'road'
road_stocks_data = road_stocks_data.groupby(cols, as_index = False).sum(numeric_only=True)

#and just concat the two together
stocks_data = pd.concat([ldv_stocks_data, road_stocks_data])
#%%
#and concat to main
eigth_edition_transport_data = pd.concat([eigth_edition_transport_data, stocks_data])


#%%
#
#save data to same file
eigth_edition_transport_data.to_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_{}.csv'.format(FILE_DATE_ID), index = False)

#%%