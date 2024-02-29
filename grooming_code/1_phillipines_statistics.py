#%%
#https://chat.openai.com/g/g-LegtYG6gQ-transport-data-system-gpt/c/77234ee3-193e-4e44-8d42-434e56475252
#take in data files. they all have around about the same structure:
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import re
import datetime
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
#%%
# File paths for the data files
ev_stocks_path = 'input_data/phillipines/chatgpt/20230919_PH_Vehicle Fleet_FINAL.xlsx'
other_stocks_path = 'input_data/phillipines/chatgpt/PH Registered MV as of Dec 2022.xlsx'
phil_data_structure_path = 'input_data/phillipines/chatgpt/phil_data_structure.xlsx'

# Load the data on all other stocks
other_stocks_data = pd.read_excel(other_stocks_path)
#drop frist row
other_stocks_data = other_stocks_data.drop([0])

# Load the target data structure
phil_data_structure = pd.read_excel(phil_data_structure_path)

# Load the EV stocks data and set the correct header for dates
ev_stocks_data = pd.read_excel(ev_stocks_path, header=2)  # Setting header to second row

#########################
#PLEASE NOTE THAT THIS TRSUTS THAT THE DATA KEEPS THE SAME STRUCTURE. IF IT CHANGES THEN THIS CODE WILL RMEOVE TOO MANY ROWS
#%%
# Filter out data for the 'Total' region in the other stocks data
other_stocks_total = other_stocks_data[other_stocks_data.columns.drop(list(other_stocks_data.filter(regex='R')))]
other_stocks_total = other_stocks_total.rename(columns={'TOTAL': 'value'})

other_stocks_data['All MV Class'] = other_stocks_data['All MV Class'].ffill()
#find index where All MV Class is TOTAL, and then drop all rows after that
TOTAL_index = other_stocks_data.index[other_stocks_data['All MV Class'] == 'TOTAL'][0]
other_stocks_data = other_stocks_data[other_stocks_data.index < TOTAL_index]  # Ignoring totals
other_stocks_total = other_stocks_data[['All MV Class', 'Unnamed: 1', 'TOTAL']]
other_stocks_total = other_stocks_total.rename(columns={'TOTAL': 'value', 'Unnamed: 1': 'drive', 'All MV Class': 'vehicle_type'})
#%%
# Map vehicle types and drive types
#'CARS', 'SUV', 'UV', 'TRUCK', 'BUSES', 'MC', 'TRL', 'TRM', 'TRH'
other_stocks_total['vehicle_type'] = other_stocks_total['vehicle_type'].map({'CARS': 'car', 'SUV': 'suv', 'UV': 'lcv', 'TRUCK': 'ht', 'BUSES': 'bus', 'MC': '2w', 'TRL': 'lcv', 'TRM': 'mt', 'TRH': 'ht'})
other_stocks_total['drive'] = other_stocks_total['drive'].map({'G': 'ice_g', 'D': 'ice_d', 'Hybrid': 'phev_g', 'Electric': 'bev', 'LPG': 'lpg', 'CNG': 'cng', 'Others': 'other'})
#where vehicle_type is lcv,mt and ht and drive is na then set it to ice_d
other_stocks_total.loc[(other_stocks_total['vehicle_type'].isin(['lcv','mt','ht'])) & (other_stocks_total['drive'].isna()),'drive'] = 'ice_d'

#%%

##############################

# Load the EV stocks data with default headers first to identify rows
ev_stocks_data = pd.read_excel(ev_stocks_path)

# Find the row indices for the 'Grand Total' and '% share of total fleet'
grand_total_start_index = ev_stocks_data.index[ev_stocks_data.iloc[:, 0] == 'Grand Total'][0] +1
share_of_total_fleet_start_index = ev_stocks_data.index[ev_stocks_data.iloc[:, 0] == 'Grand Total'][-1]+1
total_vehicle_fleet_start_index = ev_stocks_data.index[ev_stocks_data.iloc[:, 0] == 'Grand Total'][-2] +1
# Load the EV stocks data again but only the required rows for the 'Grand Total'
ev_stocks_data_grand_total = pd.read_excel(ev_stocks_path, skiprows=grand_total_start_index+1, nrows=16, header=None)

# Load the EV stocks data again but only the required rows for '% share of total fleet'
ev_stocks_data_share_total_fleet = pd.read_excel(ev_stocks_path, skiprows=share_of_total_fleet_start_index+1, nrows=6, header=None)

total_vehicle_fleet = pd.read_excel(ev_stocks_path, skiprows=total_vehicle_fleet_start_index+1, nrows=6, header=None) 

columns = pd.read_excel(ev_stocks_path, skiprows=1, nrows=0, header=1).rename(columns={'Unnamed: 0': 'vehicle_type'}).columns

# Set the correct headers for both datasets by using the columns from the top 
ev_stocks_data_grand_total.columns = columns
ev_stocks_data_share_total_fleet.columns = columns
total_vehicle_fleet.columns = columns
# Process the 'vehicle_type' column to separate the vehicle types and drive types
# Initialize the drive column with empty strings
ev_stocks_data_grand_total['drive'] = ''

# Handle 'TC - BEV' and 'MC - BEV' based on your knowledge or glossary
# Assuming 'TC' refers to some kind of three-wheeler and 'MC' refers to motorcycles
ev_stocks_data_grand_total['drive'] = ev_stocks_data_grand_total['vehicle_type'].map({'BEV': 'bev', 'PHEV': 'phev', 'HEV': 'phev', 'E-Bus': 'bev', 'TC - BEV': 'bev', 'MC - BEV': 'bev'})
ev_stocks_data_grand_total['vehicle_type'] =  ev_stocks_data_grand_total['vehicle_type'].map({'Cars': 'car', 'SUV': 'suv', 'UV': 'lcv', 'TC': '2w', 'MC': '2w', 'E-Bus': 'bus', 'TC - BEV': '2w', 'MC - BEV': '2w'})
#FFILL the vehicle_type column
ev_stocks_data_grand_total['vehicle_type'] = ev_stocks_data_grand_total['vehicle_type'].ffill()


#%%
#PLEASE NOTE THAT THIS TRSUTS THAT THE DATA KEEPS THE SAME STRUCTURE. IF IT CHANGES THEN THIS CODE WILL RMEOVE TOO MANY ROWS 
#remove rows that are nan in the drive column
ev_stocks_data_grand_total = ev_stocks_data_grand_total[ev_stocks_data_grand_total['drive'].notna()]

#melt to have date, vehicle_type, drive and value
ev_stocks_data_grand_total = ev_stocks_data_grand_total.melt(id_vars=['vehicle_type', 'drive'], 
                                        var_name='date', 
                                        value_name='value')
#%%

#now do the same for the share of total fleet. Note that there is no drive types here, but we want to split into them. To do that we will need to calcualte the share of each drive tye for each vehicle type in teh grand total data, then apply that to the share of total fleet data:
# Shares data contains these vehicle types, which we will map:
# Cars
# UV
# SUV
# TC
# MC
# Bus
ev_stocks_data_share_total_fleet['vehicle_type'] = ev_stocks_data_share_total_fleet['vehicle_type'].map({'Cars': 'car', 'SUV': 'suv', 'UV': 'lcv', 'TC': '2w', 'MC': '2w', 'Bus': 'bus'})
#melt to have date, vehicle_type and value
ev_stocks_data_share_total_fleet = ev_stocks_data_share_total_fleet.melt(id_vars=['vehicle_type'], var_name='date', value_name='value')
#do same for total_vehicle_fleet
total_vehicle_fleet['vehicle_type'] = total_vehicle_fleet['vehicle_type'].map({'Cars': 'car', 'SUV': 'suv', 'UV': 'lcv', 'TC': '2w', 'MC': '2w', 'Bus': 'bus'})
#melt to have date, vehicle_type and value
total_vehicle_fleet = total_vehicle_fleet.melt(id_vars=['vehicle_type'], var_name='date', value_name='value')

#%%

#calcaulte the shares for each vehicle type and drive type
ev_stocks_data_grand_total_shares = ev_stocks_data_grand_total.groupby(['vehicle_type', 'drive'])['value'].sum().reset_index()
ev_stocks_data_grand_total_shares['share'] = ev_stocks_data_grand_total_shares.groupby(['vehicle_type'])['value'].transform(lambda x: x / x.sum())
ev_stocks_data_grand_total_shares = ev_stocks_data_grand_total_shares.drop(columns=['value'])

#now do a right join on the shares data to the share of total fleet data
ev_stocks_data_share_total_fleet = ev_stocks_data_share_total_fleet.merge(ev_stocks_data_grand_total_shares, how='left', on=['vehicle_type'])

#times value by share to get the share for each drive type
ev_stocks_data_share_total_fleet['value'] = ev_stocks_data_share_total_fleet['value'] * ev_stocks_data_share_total_fleet['share']

#drop share
ev_stocks_data_share_total_fleet = ev_stocks_data_share_total_fleet.drop(columns=['share'])

#%%
#now we wnat to split phev into phev_g or phev_d based on the ice_g, ice_d split in each vehicle type within other_stocks_total, then apply that to the ev_stocks_data_grand_total and ev_stocks_data_share_total_fleet:
#first we need to get the split for each vehicle type in other_stocks_total
#first get the total for each vehicle type
ice_stocks = other_stocks_total.loc[other_stocks_total['drive'].isin(['ice_g','ice_d'])]
other_stocks_total_sum = ice_stocks.groupby(['vehicle_type'])['value'].sum().reset_index()
#now get the split for each vehicle type
other_stocks_total_split = ice_stocks.merge(other_stocks_total_sum, how='left', on=['vehicle_type'])
other_stocks_total_split['split'] = other_stocks_total_split['value_x'] / other_stocks_total_split['value_y']
other_stocks_total_split = other_stocks_total_split.drop(columns=['value_x','value_y'])
#now join those onto the phev rows in ev_stocks_data_grand_total and ev_stocks_data_share_total_fleet
phev_grand_total = ev_stocks_data_grand_total.loc[ev_stocks_data_grand_total['drive'] == 'phev']
phev_share_total_fleet = ev_stocks_data_share_total_fleet.loc[ev_stocks_data_share_total_fleet['drive'] == 'phev']
phev_grand_total = phev_grand_total.merge(other_stocks_total_split, how='left', on=['vehicle_type'], suffixes=('','_y'))
phev_share_total_fleet = phev_share_total_fleet.merge(other_stocks_total_split, how='left', on=['vehicle_type'], suffixes=('','_y'))
#now times the value by the split
phev_grand_total['value'] = phev_grand_total['value'] * phev_grand_total['split']
phev_share_total_fleet['value'] = phev_share_total_fleet['value'] * phev_share_total_fleet['split']
#set drive to phed_g and phev_d based on the drive_y
phev_grand_total.loc[phev_grand_total['drive_y'] == 'ice_g','drive'] = 'phev_g'
phev_grand_total.loc[phev_grand_total['drive_y'] == 'ice_d','drive'] = 'phev_d'
phev_share_total_fleet.loc[phev_share_total_fleet['drive_y'] == 'ice_g','drive'] = 'phev_g'
phev_share_total_fleet.loc[phev_share_total_fleet['drive_y'] == 'ice_d','drive'] = 'phev_d'
#drop drive_y and split
phev_grand_total = phev_grand_total.drop(columns=['drive_y','split'])
phev_share_total_fleet = phev_share_total_fleet.drop(columns=['drive_y','split'])
#now concat back onto the original dataframes
ev_stocks_data_grand_total = pd.concat([ev_stocks_data_grand_total.loc[ev_stocks_data_grand_total['drive'] != 'phev'], phev_grand_total])
ev_stocks_data_share_total_fleet = pd.concat([ev_stocks_data_share_total_fleet.loc[ev_stocks_data_share_total_fleet['drive'] != 'phev'], phev_share_total_fleet])

# %%
#great now 
# Define default values for the missing columns
default_values = {
    'economy': '15_PHL',  # Philippines
    'medium': 'road',  # Assuming all vehicles are for road use
    'measure': 'stocks',  # Assuming we're measuring vehicle stocks
    'dataset': 'ph_govt_data_stocks',  # Name of the dataset
    'unit': 'number',  # Assuming the unit is the actual number of vehicles
    'fuel': 'all',  # If specific fuel data is not available
    'comment': 'no_comment',  # Placeholder for any comments
    'scope': 'national',  # Assuming national scope
    'frequency': 'annual',  # Assuming data is collected annually
    # Additional default columns can be added as needed
}

# Apply default values to the grand total dataframe
for column, default in default_values.items():
    if column in ev_stocks_data_grand_total.columns:
        continue
    ev_stocks_data_grand_total[column] = default

# Apply default values to the share of total fleet dataframe
for column, default in default_values.items():
    if column in ev_stocks_data_share_total_fleet.columns:
        continue
    ev_stocks_data_share_total_fleet[column] = default

for column,default in default_values.items():
    if column in total_vehicle_fleet.columns:
        continue
    total_vehicle_fleet[column] = default
    
for column,default in default_values.items():
    if column in other_stocks_total.columns:
        continue
    other_stocks_total[column] = default
    
other_stocks_total['date'] = 2022
#make a copy for 2021 too. this is cheating but for now it is fine
other_stocks_total_2021 = other_stocks_total.copy()
other_stocks_total_2021['date'] = 2021
#make a comment that this is based off 2022 data
other_stocks_total_2021['comment'] = 'Same as 2022 data'
other_stocks_total = pd.concat([other_stocks_total,other_stocks_total_2021])
# Set transport type based on vehicle type
# This is an example, adjust the logic according to your actual data
transport_type_mapping = {
    'car': 'passenger',
    'suv': 'passenger',
    'lcv': 'freight',
    'bus': 'passenger',
    '2w': 'passenger',
    'mt': 'freight',
    'ht': 'freight'
    # Add more mappings as required
}
ev_stocks_data_grand_total['transport_type'] = ev_stocks_data_grand_total['vehicle_type'].map(transport_type_mapping)
ev_stocks_data_share_total_fleet['transport_type'] = ev_stocks_data_share_total_fleet['vehicle_type'].map(transport_type_mapping)
other_stocks_total['transport_type'] = other_stocks_total['vehicle_type'].map(transport_type_mapping)
total_vehicle_fleet['transport_type'] = total_vehicle_fleet['vehicle_type'].map(transport_type_mapping)
#check for any missing transport types
nas = ev_stocks_data_grand_total[ev_stocks_data_grand_total['transport_type'].isna()]
if len(nas) > 0:
    print('There are missing transport types in the grand total data')
    print(nas)
nas = ev_stocks_data_share_total_fleet[ev_stocks_data_share_total_fleet['transport_type'].isna()]
if len(nas) > 0:
    print('There are missing transport types in the share of total fleet data')
    print(nas)
nas = other_stocks_total[other_stocks_total['transport_type'].isna()]
if len(nas) > 0:
    print('There are missing transport types in the other stocks data')
    print(nas)
nas = total_vehicle_fleet[total_vehicle_fleet['transport_type'].isna()]
if len(nas) > 0:
    print('There are missing transport types in the total vehicle fleet data')
    print(nas)
#%%
# Now, align the columns of the dataframes with the structure file
# We assume that the phil_data_structure has specific column order that we need to match
final_column_order = phil_data_structure.columns.tolist() +['drive']
ev_stocks_data_grand_total = ev_stocks_data_grand_total[final_column_order]
ev_stocks_data_share_total_fleet = ev_stocks_data_share_total_fleet[final_column_order]
other_stocks_total = other_stocks_total[final_column_order]
total_vehicle_fleet = total_vehicle_fleet[phil_data_structure.columns.tolist()]
#group and sum all by their cols
group_cols = final_column_order 
group_cols.remove('value')
ev_stocks_data_grand_total = ev_stocks_data_grand_total.groupby(group_cols)['value'].sum().reset_index()
ev_stocks_data_share_total_fleet = ev_stocks_data_share_total_fleet.groupby(group_cols)['value'].sum().reset_index()
other_stocks_total = other_stocks_total.groupby(group_cols)['value'].sum().reset_index()
group_cols.remove('drive')
total_vehicle_fleet = total_vehicle_fleet.groupby(group_cols)['value'].sum().reset_index()

# Concatenate the two dataframes if needed
# ev_stocks_data_grand_total
# ev_stocks_data_share_total_fleet

#as an additional task, we want to convert the files with shares so be used as inputs to the transport model. so we will have to adjsut somethingsto have the same cols as below:
# Economy	Scenario	Transport Type	Medium	Vehicle Type	Drive	Date	Share
# 19_THA	Reference	freight	air	all	air_av_gas	2017	0.010406353
#and we will aslo need to find the share within each vehicle type, rather than within the entire road fleet. so we will need to do some more processing using the data frpm total_vehicle_fleet (which contains the total fleet for each vehicle type). we will find the portion of the total vehicle fleet that each drive makes up for each vehicle type:
#merge the total_vehicle_fleet with the ev_stocks_data_share_total_fleet on the date and vehicle_type columns
total_sales = total_vehicle_fleet.rename(columns={'value':'total_fleet'})[['date','vehicle_type', 'total_fleet']].copy()
#clacualte total sales as the change in the total fleet from the previous year
#sort
total_sales = total_sales.sort_values(by=['vehicle_type','date'])
total_sales['sales'] = total_sales.groupby(['vehicle_type'])['total_fleet'].transform(lambda x: x.diff())
total_sales.drop('total_fleet', axis=1, inplace=True)
#drop nas
total_sales = total_sales.dropna()
ev_stocks_share_of_vtype = ev_stocks_data_grand_total.merge(total_sales, how='left', on=['date','vehicle_type'])
#now calculate the share of the total fleet that each drive type makes up
ev_stocks_share_of_vtype['Share'] = ev_stocks_share_of_vtype['value'] / ev_stocks_share_of_vtype['sales']
#%%
#now sort out the colymns so we only have the ones we need (Economy	Scenario	Transport Type	Medium	Vehicle Type	Drive	Date	Share)
ev_stocks_share_of_vtype = ev_stocks_share_of_vtype[['economy','transport_type','medium','vehicle_type','drive','date','Share']]
#make the scenario 'Target'
ev_stocks_share_of_vtype['scenario'] = 'Target'
#rename cols to match exactly:
ev_stocks_share_of_vtype = ev_stocks_share_of_vtype.rename(columns={'economy':'Economy','scenario':'Scenario','transport_type':'Transport Type','medium':'Medium','vehicle_type':'Vehicle Type','drive':'Drive','date':'Date','Share':'Share'})

#%%
#save data to files

file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

ev_stocks_data_grand_total.to_csv(f'intermediate_data/PHL/{FILE_DATE_ID}_ev_stocks_data_grand_total_forecast.csv', index=False)
ev_stocks_data_share_total_fleet.to_csv(f'intermediate_data/PHL/{FILE_DATE_ID}_ev_stocks_data_share_total_fleet_forecast.csv', index=False)
other_stocks_total.to_csv(f'intermediate_data/PHL/{FILE_DATE_ID}phillipines_2022_stocks_total.csv', index=False)
total_vehicle_fleet.to_csv(f'intermediate_data/PHL/{FILE_DATE_ID}_total_vehicle_fleet_forecast.csv', index=False)

#save as csv
ev_stocks_share_of_vtype.to_csv(f'intermediate_data/PHL/{FILE_DATE_ID}_ev_stocks_share_of_vtype.csv', index=False)
#%%


#%%


#%%

