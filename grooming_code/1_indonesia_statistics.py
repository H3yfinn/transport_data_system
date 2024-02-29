#take in data files. they all have around about the same structure:
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import re
import datetime
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')

#unlike other similar scripts in this folder, this will just take manually reocrded data from a spreadsheet, to cut down on the code in this script. Partly this is just because the data from ina is simple to record, compared to the other countries.

#%%
#load in the files:
# C:\Users\finbar.maunsell\github\transport_data_system\input_data\indonesia\indonesia_non_road.xlsx
non_road = pd.read_excel('input_data/indonesia/indonesia_non_road.xlsx')
#these are the columns in the file:
# Index(['Date', 'medium', 'value', 'measure', 'transport type', 'unit'], dtype='object')
#these are the cols we want
# 'economy', 'date', 'medium', 'measure', 'dataset', 'unit', 'fuel', 
#     'comment', 'scope', 'frequency', 'vehicle_type', 'transport_type', 'value', 'source'
# Define the default values
default_values = {
    'vehicle_type': 'all',
    'dataset': 'statistics_indonesia',
    'economy': '07_INA',
    'source': '',
    'fuel': 'all',
    'scope': '',
    'frequency': 'yearly',
    'drive': 'all'
}

# Identify the missing columns
missing_columns = set(default_values.keys()) - set(non_road.columns)

# Add the missing columns with their default values
for column in missing_columns:
    non_road[column] = default_values.get(column, None)

#%%
# date id to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

non_road.to_csv('intermediate_data/INA/{}_statistics_indonesia_non_road.csv'.format(FILE_DATE_ID), index=False) 
#%%