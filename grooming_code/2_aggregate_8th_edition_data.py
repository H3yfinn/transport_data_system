
#%%
import pandas as pd
import os
import re
import datetime
#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = '_DATE{}'.format(file_date)
# FILE_DATE_ID = ''

#%%
#load 8th edition data
road_stocks= pd.read_csv('intermediate_data/8th_edition_transport_model/road_stocks.csv')
activity= pd.read_csv('intermediate_data/8th_edition_transport_model/activity.csv')
energy= pd.read_csv('intermediate_data/8th_edition_transport_model/energy.csv')

turnover_rate = pd.read_csv('intermediate_data/8th_edition_transport_model/turnover_rate.csv')
new_vehicle_efficiency = pd.read_csv('intermediate_data/8th_edition_transport_model/new_vehicle_efficiency.csv')
occupance_load = pd.read_csv('intermediate_data/8th_edition_transport_model/occupance_load.csv')
#%%
#create unit cols
road_stocks['Unit'] = 'million_stocks'
#depending on if transport type is passenger or freight, the unit will be different
activity['Unit'] = activity['Transport Type'].apply(lambda x: 'thousand_passenger_km' if x == 'passenger' else 'thousand_tonne_km')
#energy is pj
energy['Unit'] = 'PJ'

turnover_rate['Unit'] = 'turnover_rate'
new_vehicle_efficiency['Unit'] = 'efficiency'
occupance_load['Unit'] = 'occupancy_or_load'
#%%
#merge data
activity_energy = activity.merge(energy, on=['Scenario', 'Economy', 'Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Year','Unit'], how='left')
activity_energy_road_stocks = activity_energy.merge(road_stocks, on=['Scenario', 'Economy', 'Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Year','Unit'], how='left')
eigth_edition_data = activity_energy_road_stocks.merge(turnover_rate, on=['Scenario', 'Economy', 'Transport Type', 'Vehicle Type', 'Year','Unit'], how='left')
eigth_edition_data = eigth_edition_data.merge(new_vehicle_efficiency, on=['Scenario', 'Economy', 'Transport Type', 'Vehicle Type','Drive', 'Year','Unit'], how='left')
eigth_edition_data = eigth_edition_data.merge(occupance_load, on=['Scenario', 'Economy', 'Transport Type', 'Vehicle Type', 'Year','Unit'], how='left')

#%%
#melt so we have a measure and value column
eigth_edition_data_melted = eigth_edition_data.melt(id_vars=['Scenario', 'Economy', 'Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Year','Unit'], value_vars=['Activity', 'Energy', 'Stocks'], var_name='Measure', value_name='Value') 

#%%
#create source column and label as 8th edition transport model. Except where the date is greater than 2017 label the data as:
# 8th edition transport model (forecasted reference scenario)
# 8th edition transport model (forecasted carbon neutrality scenario)
#depdning on the year and the scenario
eigth_edition_data_melted['Dataset'] = eigth_edition_data_melted.apply(lambda x: '8th edition transport model (forecasted reference scenario)' if x['Year'] > 2017 and x['Scenario'] == 'Reference' else '8th edition transport model (forecasted carbon neutrality scenario)' if x['Year'] > 2017 and x['Scenario'] == 'Carbon Neutral' else '8th edition transport model', axis=1)

#remove scenario
eigth_edition_data_melted = eigth_edition_data_melted.drop(columns=['Scenario'])
#%%
#save
eigth_edition_data_melted.to_csv('output_data/8th_edition_transport_model/eigth_edition_transport_data_{}.csv'.format(FILE_DATE_ID), index=False)

#%%


# %%
