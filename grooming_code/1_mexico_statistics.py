#%%
#take in data files. they all have around about the same structure:
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import re
import datetime
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')

#please note that the code here was developed using this: https://chat.openai.com/share/0eea0832-9b13-48ef-8d3c-9a093e229d6d > the data files that were used are in  transport_data_system\input_data\Korea\chatgpt
#%%

# Load raw data and templates
inegi_export_path = 'input_data/Mexico/stocks_INEGI_exporta_12_10_2023_19_19_6.xlsx'
data_structure_mexico_path = 'input_data/Mexico/chatgpt/data_structure_mexico.xlsx'

inegi_export_data = pd.read_excel(inegi_export_path)
data_structure_mexico = pd.read_excel(data_structure_mexico_path)

# Clean INEGI export data
inegi_export_data = inegi_export_data.dropna(how='all').reset_index(drop=True)
header_row_idx = inegi_export_data.index[inegi_export_data.iloc[:, 1] == 'Total'][0]
inegi_export_data.columns = inegi_export_data.iloc[header_row_idx]
inegi_export_data = inegi_export_data[header_row_idx + 1:].reset_index(drop=True)
inegi_export_data = inegi_export_data[inegi_export_data.iloc[:, 0].str.isnumeric()]

# Translate column names
column_translation = {
    'Automóviles': 'lpv',
    'Camiones para pasajeros': 'bus',
    'Camiones y camionetas para carga': 'ht',
    'Motocicletas': '2w'
}

inegi_export_data.drop(columns=['Total'], inplace=True)

inegi_export_data.rename(columns=column_translation, inplace=True)

# Transform the data (melt)
inegi_melted = inegi_export_data.melt(id_vars=inegi_export_data.columns[0], 
                                      var_name='vehicle_type', 
                                      value_name='value')
inegi_melted['value'] = inegi_melted['value'].str.replace(',', '').astype(float)
inegi_melted['date'] = inegi_melted[inegi_export_data.columns[0]].astype(int)
inegi_melted.drop(columns=[inegi_export_data.columns[0]], inplace=True)

#transport type: passenger for cars, bus, 2w and freight for ht
#also drop total
transport_type_map = {
    'lpv': 'passenger',
    'bus': 'passenger',
    '2w': 'passenger',
    'ht': 'freight'
}
# Apply initial mappings for vehicle_type
inegi_melted['transport_type'] = inegi_melted['vehicle_type'].map(transport_type_map)

# Add missing columns
missing_columns = set(data_structure_mexico.columns) - set(inegi_melted.columns)
default_values = {
    'economy': data_structure_mexico['economy'].iloc[0],
    'medium': 'Road',
    'measure': 'Stocks',
    'dataset': 'statistics_mexico_stocks',
    'unit': 'Stocks',
    'fuel': 'All',
    'comment': None,
    'scope': 'national',
    'frequency': 'Annual',
    'drive': 'All',
    'source': ''
}
for column in missing_columns:
    inegi_melted[column] = default_values.get(column, None)

# Reorder columns to match data_structure_mexico
inegi_melted = inegi_melted[data_structure_mexico.columns]
#%%
###################################
#we have a problem in the modelling where the proportional decrease in diesel use is actually pretty high. To fix this we are going to try anmd allocate more ice_d to passenger vehicles and less to freight vehicles. To make it easy we will just assume that all freight vehicles are ice_d



# #try and split the proportions of ice_d/ice_g in the vehicle types. We dont have much to go off of except avg's of other economies and the fact that diesel was affected quite a lot by covid, which implies a higher proportion of diesel vehicles in passenger than you would expect: (since passenger is more affected by covid than freight)
# #my generaly hunch is that, at least to make the modelling play nice, we should increase the proportion of lcvs that are gasoline and increase proprotion of cars and buses taht use diesel. We can calcualte the proportions by looking at the averages we have in our pre modelled data, and then adjust them according to how much tehy need to increae/decrease to make the modelled data match the real data
# mexico_prop_decreases_from_covid_by_drive = {
#     'ice_d':(563-317)/563,
#     'ice_g':(1457-1107)/1457

# }
# mexico_current_prop_increases_after_covid_by_drive = {
#     'ice_d':449/563,
#     'ice_g':1457/1457,
# }#use this to identify how much we need to increase the proportion of diesel vehicles in passenger vehicles

# #take in road modelled data
# road_modelled_data_path = 'input_data/mexico/11_MEX_road_model_output20240618.csv'
# road_modelled_data_stocks = pd.read_csv(road_modelled_data_path)[['Economy', 'Date', 'Medium', 'Vehicle Type', 'Transport Type', 'Drive', 'Scenario', 'Stocks']]
# #filter for just the earliest date
# road_modelled_data_stocks = road_modelled_data_stocks[road_modelled_data_stocks['Date'] == road_modelled_data_stocks['Date'].min()]
# #filter for just ice_g and ice_d
# road_modelled_data_stocks = road_modelled_data_stocks[road_modelled_data_stocks['Vehicle Type'].isin(['ice_g', 'ice_d'])]
# #calcualte proportion of each drive type within each vehicle type/transport type , scenario comboniation
# road_modelled_data_stocks['proportion'] = road_modelled_data_stocks.groupby(['Vehicle Type', 'Transport Type', 'Scenario', 'Drive'])['Stocks'].transform(lambda x: x/x.sum())

#%%

# reduce ice_g stocks in freight to 0. This is because freight diesel use is too high in the modelled data. do this by jsut setting the drive to ice_d for all freight vehicles
#double check there are no ice_g or ice_d vehicles in freight
if inegi_melted.loc[inegi_melted['transport_type'] == 'freight', 'drive'].nunique() > 1:
    raise ValueError('There are multiple drive types for freight vehicles. This is not expected.')
#set all freight vehicles to ice_d
inegi_melted.loc[(inegi_melted['transport_type'] == 'freight') & (inegi_melted['drive'] == 'All'), 'drive'] = 'ice_d'

#also set vehicle type to trucks for freight vehicles so the aggregation code main..py will split these into lcvs/hts/mts
if inegi_melted.loc[inegi_melted['transport_type'] == 'freight', 'vehicle_type'].nunique() > 1:
    raise ValueError('There are multiple vehicle types for freight vehicles. This is not expected.')
inegi_melted.loc[inegi_melted['transport_type'] == 'freight', 'vehicle_type'] = 'all'
###################################
#%%
# date id to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

inegi_melted.to_csv('intermediate_data/MEX/{}_statistics_mexico_stocks.csv'.format(FILE_DATE_ID), index=False) 
#%%