
#%%
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import os
import re
import datetime
#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
# FILE_DATE_ID = ''

#%%
#load 8th edition data
road_stocks= pd.read_csv('intermediate_data/8th_edition_transport_model/road_stocks.csv')
activity= pd.read_csv('intermediate_data/8th_edition_transport_model/activity.csv')
energy= pd.read_csv('intermediate_data/8th_edition_transport_model/energy.csv')

# turnover_rate = pd.read_csv('intermediate_data/8th_edition_transport_model/turnover_rate.csv')
new_vehicle_efficiency = pd.read_csv('intermediate_data/8th_edition_transport_model/new_vehicle_efficiency.csv')
occupance_load = pd.read_csv('intermediate_data/8th_edition_transport_model/occupance_load.csv')
#%%
#create unit cols
road_stocks['Unit'] = 'million_stocks'
#energy is pj
energy['Unit'] = 'PJ'
# turnover_rate['Unit'] = 'turnover_rate'8th edition transport model
new_vehicle_efficiency['Unit'] = 'efficiency'
occupance_load['Unit'] = 'occupancy_or_load'

#%%
#since activity is in thousands, units, we need to multiply by 1000 to get the correct units
activity['Activity'] = activity['Activity']*1000
#change units to be passenger_km or freight_tonne_km depending on the transport type
activity['Unit'] = activity['Transport Type'].apply(lambda x: 'passenger_km' if x == 'passenger' else 'freight_tonne_km')
#create two dataframes, one for passenger and one for freight and change the name of Activity column to the correct unit
activity_passenger = activity[activity['Transport Type'] == 'passenger'].rename(columns={'Activity':'passenger_km'})
activity_freight = activity[activity['Transport Type'] == 'freight'].rename(columns={'Activity':'freight_tonne_km'})

#%%
# Now in every dataframe change the Value column to be called 'Value', and a Measure column to be correct measure name
#road_stocks
road_stocks = road_stocks.rename(columns={'Stocks':'Value'})
road_stocks['Measure'] = 'Stocks'
#energy
energy = energy.rename(columns={'Energy':'Value'})
energy['Measure'] = 'Energy'
#activity
activity_passenger = activity_passenger.rename(columns={'passenger_km':'Value'})
activity_passenger['Measure'] = 'passenger_km'
activity_freight = activity_freight.rename(columns={'freight_tonne_km':'Value'})
activity_freight['Measure'] = 'freight_tonne_km'
#turnover_rate
# turnover_rate = turnover_rate.rename(columns={'Turnover_rate':'Value'})
# turnover_rate['Measure'] = 'turnover_rate'
#new_vehicle_efficiency
new_vehicle_efficiency = new_vehicle_efficiency.rename(columns={'New_vehicle_efficiency':'Value'})
new_vehicle_efficiency['Measure'] = 'new_vehicle_efficiency'
#occupance_load
occupance_load = occupance_load.rename(columns={'Occupancy_or_load':'Value'})
occupance_load['Measure'] = 'occupancy_or_load'

#%%
#concatenate data
eigth_edition_data = pd.concat([road_stocks, energy, activity_passenger, activity_freight,  new_vehicle_efficiency, occupance_load], ignore_index=True)#turnover_rate,

#%%
#create source column and label as 8th edition transport model. Except where the date is greater than 2017 label the data as:
# 8th edition transport model (forecasted reference scenario)
# 8th edition transport model (forecasted carbon neutrality scenario) #include 2017 data from this scenario as being from this dataset
#depdning on the year and the scenario
eigth_edition_data['Dataset'] = eigth_edition_data.apply(lambda x: '8th edition transport model (forecasted reference scenario)' if x['Year'] > 2017 and x['Scenario'] == 'Reference' else '8th edition transport model (forecasted carbon neutrality scenario)' if x['Year'] >= 2017 and x['Scenario'] == 'Carbon Neutral' else '8th edition transport model', axis=1)

#remove scenario
eigth_edition_data = eigth_edition_data.drop(columns=['Scenario'])
#drop duplicates:
eigth_edition_data = eigth_edition_data.drop_duplicates()

#%%
#make sure there is only one unit for each measure
eigth_edition_data.groupby(['Measure', 'Unit']).size().reset_index().rename(columns={0:'count'})
if len(eigth_edition_data.groupby(['Measure', 'Unit']).size().reset_index().rename(columns={0:'count'})) == len(eigth_edition_data['Measure'].unique()):
    print('All measures have one unit')
else:
    print('There are measures with more than one unit')
#%%
#save
eigth_edition_data.to_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_aggregated_{}.csv'.format(FILE_DATE_ID), index=False)

#%%


# %%
