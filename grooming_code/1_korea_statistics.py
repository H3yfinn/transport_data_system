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

#please note that the code here was developed using this: https://chat.openai.com/share/fcdd1287-7c70-4371-a288-847ed815ed66 > the data files that were used are in  transport_data_system\input_data\Korea\chatgpt
#%%
# Read the Motor_Vehicle CSV data
motor_vehicle_df = pd.read_csv('input_data/Korea/Motor_Vehicle_Registration_by_Year_20231010122552.csv')

# Define initial mappings for vehicle_type
vehicle_type_map = {
    'Sedan': 'car',
    'Vans': None,  # This will be handled separately due to conditions
    'Freight': 'ht'
}

# Apply initial mappings for vehicle_type
motor_vehicle_df['vehicle_type'] = motor_vehicle_df['Level01(1)'].map(vehicle_type_map)

# Handle conditional mappings for 'Vans'
motor_vehicle_df.loc[
    (motor_vehicle_df['Level01(1)'] == 'Vans') & 
    (motor_vehicle_df['Item'].isin(['Commercial vehicle', 'Official use vehicle'])), 
    'vehicle_type'
] = 'lcv'

motor_vehicle_df.loc[
    (motor_vehicle_df['Level01(1)'] == 'Vans') & 
    (~motor_vehicle_df['Item'].isin(['Commercial vehicle', 'Official use vehicle'])), 
    'vehicle_type'
] = 'lt'

# Handle "Special" by splitting the value and appending new rows
special_rows = motor_vehicle_df[motor_vehicle_df['Level01(1)'] == 'Special'].copy()
special_rows['Total'] = special_rows['Total'] / 2  # Divide the total value by 2
special_rows['vehicle_type'] = 'ht'  # Half to 'ht'

# Duplicate these rows for 'lcv'
special_rows_lcv = special_rows.copy()
special_rows_lcv['vehicle_type'] = 'lcv'  # The other half to 'lcv'

# Append these new rows back to original DataFrame
motor_vehicle_df = pd.concat([motor_vehicle_df, special_rows, special_rows_lcv], ignore_index=True)
motor_vehicle_df = motor_vehicle_df[motor_vehicle_df['Level01(1)'] != 'Special']  # Remove original 'Special' rows

# Define mappings for transport_type
transport_type_map = {
    'car': 'passenger',
    'lpv': 'passenger',
    'suv': 'passenger',
    'lt': 'passenger',
    'ht': 'freight',
    'mt': 'freight',
    'lcv': 'freight'
}

# Apply mappings for transport_type
motor_vehicle_df['transport_type'] = motor_vehicle_df['vehicle_type'].map(transport_type_map)

#rename cols:
motor_vehicle_df = motor_vehicle_df.rename(columns={'Period': 'date', 'Total': 'value'})

# Read the Korea Data Structure from Excel
korea_data_df = pd.read_excel('input_data/Korea/chatgpt/korea_data_structure.xlsx')

# Use values from the Korea Data Structure DataFrame for default values
default_values_korea = {col: korea_data_df[col].iloc[0] for col in korea_data_df.columns if col not in ['vehicle_type', 'transport_type', 'value', 'date']}

# Override the 'dataset' default value
default_values_korea['dataset'] = 'statistics_korea_stocks'
default_values_korea['economy'] = '09_ROK'

# Update the missing columns with these new default values
for column, default_value in default_values_korea.items():
    motor_vehicle_df[column] = default_value

# Reorder columns to match Korea Data Structure
final_columns_order = [
    'economy', 'date', 'medium', 'measure', 'dataset', 'unit', 'fuel', 
    'comment', 'scope', 'frequency', 'vehicle_type', 'transport_type', 'value'
]

motor_vehicle_df = motor_vehicle_df[final_columns_order]

#set drive to all
motor_vehicle_df['drive'] = 'all'
#%%
#group and sum up:
motor_vehicle_df = motor_vehicle_df.groupby(['economy', 'date', 'medium', 'measure', 'dataset', 'unit', 'fuel', 'comment', 'scope', 'frequency', 'vehicle_type', 'transport_type', 'drive'], as_index=False).agg({'value': 'sum'})
#%%


#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

motor_vehicle_df.to_csv('intermediate_data/KOR/{}_statistics_korea_stocks.csv'.format(FILE_DATE_ID), index=False)
#%%