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
    'Automóviles': 'cars',
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
    'cars': 'passenger',
    'bus': 'passenger',
    '2w': 'passenger',
    'ht': 'freight'
}
# Apply initial mappings for vehicle_type
inegi_melted['vehicle_type'] = inegi_melted['vehicle_type'].map(transport_type_map)

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
    'scope': 'All',
    'frequency': 'Annual',
    'drive': 'All',
    'source': ''
}
for column in missing_columns:
    inegi_melted[column] = default_values.get(column, None)

# Reorder columns to match data_structure_mexico
inegi_melted = inegi_melted[data_structure_mexico.columns]
#%%
# date id to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

inegi_melted.to_csv('intermediate_data/MEX/{}_statistics_mexico_stocks.csv'.format(FILE_DATE_ID), index=False) 
#%%