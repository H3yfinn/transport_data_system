#we will extract this data without going through the process in '1_extract_data_from_EIA_API.py' because we want this data now, since we're struggling to get the code to work for the other data.
#i have manually formatted it to have the following columns. we jsut need to do some averaging and then we can use it, probably!
#the data is from https://www.eia.gov/outlooks/aeo/tables_ref.php > Table 51. New Light-Duty Vehicle Fuel Economy
# dataset	unit	original_drive	vehicle_type	drive	original_vehicle_type	2022 2023... etc.

#location: input_data\EIA\fuel_economy_manually_formatted.xlsx
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

#%%
#load in the data:
fuel_economy = pd.read_excel('input_data/EIA/fuel_economy_manually_formatted.xlsx', sheet_name='ref2023.0206a')
#%%
#melt so that the years are in a single column
fuel_economy = fuel_economy.melt(id_vars=['dataset','unit','original_drive','vehicle_type','drive','original_vehicle_type', 'source'], var_name='year', value_name='Value')
#%%
#now do unit conversion from mpg to kmpmj

# unit_conversions = C:\Users\finbar.maunsell\github\transport_data_system\config\conversion_factors.csv
unit_conversions = pd.read_csv('config/conversion_factors.csv')
unit_conversions = unit_conversions[unit_conversions['original_unit']=='mpg']
unit_conversions = unit_conversions[unit_conversions['final_unit']=='km/PJ']
#we will have to do some manual categorising to link conversion factors to their drive types (note that any electricity or fuel cells (since the hydrogen isnt actually burnt!) is measured in mpge which is gasoline equivalent!): 'Gasoline', 'Turbo Direct Injection Diesel',
#    'Plug-in 20 Gasoline Hybrid', 'Plug-in 50 Gasoline Hybrid',
#    'Ethanol Flex', 'Compressed/Liquefied Natural Gas', 'Propane',
#    '100-Mile Electric Vehicle', '200-Mile Electric Vehicle',
#    '300-Mile Electric Vehicle', 'Gasoline-Electric Hybrid',
#    'Fuel Cell Hydrogen'
#unit_conversions.fuel .unique(): 'hydrogen', 'methanol', 'petrol', 'diesel', 'crude_oil', 'lpg',
#    'natural_gas', 'ethanol', 'biodiesel', 'jet_fuel', 'biojet'
original_drive_to_fuel_conversion_factor = {}
original_drive_to_fuel_conversion_factor['Gasoline'] = 'petrol'
original_drive_to_fuel_conversion_factor['Turbo Direct Injection Diesel'] = 'diesel'
original_drive_to_fuel_conversion_factor['Plug-in 20 Gasoline Hybrid'] = 'petrol'
original_drive_to_fuel_conversion_factor['Plug-in 50 Gasoline Hybrid'] = 'petrol'
original_drive_to_fuel_conversion_factor['Ethanol Flex'] = 'ethanol'
original_drive_to_fuel_conversion_factor['Compressed/Liquefied Natural Gas'] = 'natural_gas'
original_drive_to_fuel_conversion_factor['Propane'] = 'lpg'
original_drive_to_fuel_conversion_factor['100-Mile Electric Vehicle'] = 'petrol'
original_drive_to_fuel_conversion_factor['200-Mile Electric Vehicle'] = 'petrol'
original_drive_to_fuel_conversion_factor['300-Mile Electric Vehicle'] = 'petrol'
original_drive_to_fuel_conversion_factor['Gasoline-Electric Hybrid'] = 'petrol'
original_drive_to_fuel_conversion_factor['Fuel Cell Hydrogen'] = 'petrol'
 
#first add a column to the fuel_economy df that is the fuel type
fuel_economy['fuel'] = fuel_economy['original_drive'].map(original_drive_to_fuel_conversion_factor)

#merge with the fuel_economy data
fuel_economy = fuel_economy.merge(unit_conversions, how='left', on=['fuel'])

#now we can convert the values
fuel_economy['Value'] = fuel_economy['Value'] * fuel_economy['value']
#drop value as it is unit conversion factor
fuel_economy = fuel_economy.drop(columns=['value'])

# #inverse the value to get the conversion from mpg to PJ/km
# fuel_economy['Value'] = 1/fuel_economy['Value']

#set unit t km_per_pj
fuel_economy['unit'] = 'km_per_pj'

#also save the origianl version where we have the original drive and vehicle type:
fuel_economy_original = fuel_economy.groupby(['dataset', 'source','unit','vehicle_type','drive','year', 'original_drive', 'original_vehicle_type']).mean(numeric_only=True).reset_index().copy()

#%%
#group by and average numeric columns:
fuel_economy = fuel_economy.groupby(['dataset', 'source','unit','vehicle_type','drive','year']).mean(numeric_only=True).reset_index()

#if vehicle type is lcv then set transport_type to frieght else set to passenger
fuel_economy['transport_type'] = np.where(fuel_economy['vehicle_type']=='lcv', 'freight', 'passenger')

#set the other columns:
fuel_economy['frequency'] = 'yearly'
fuel_economy['scope'] = 'national'
fuel_economy['comment'] = 'no_comment'
fuel_economy['fuel'] = 'all'
fuel_economy['medium'] = 'road'
fuel_economy['measure'] = 'efficiency'
fuel_economy['unit'] = 'km_per_pj'
#save the data:
fuel_economy.to_csv('intermediate_data/EIA/eia_2023_weo_fuel_economy.csv', index=False)
fuel_economy_original.to_csv('intermediate_data/EIA/eia_2023_weo_fuel_economy_original.csv', index=False)
# if the vehicle type is 
#%%
#do a quick visualisation to check it looks ok:
fig = px.line(fuel_economy, x='year', y='Value', color='drive', facet_col='vehicle_type', facet_col_wrap=3)
# fig.show()

title = 'fuel economy from EIA weo 2023 suing original vehicle type'
fig = px.line(fuel_economy_original.groupby(['original_vehicle_type', 'drive', 'year']).mean(numeric_only=True).reset_index()
              , x='year', y='Value', color='drive', facet_col='original_vehicle_type', facet_col_wrap=3)
# fig.show()
fig.write_html("plotting_output/analysis/usa/{}.html".format(title))
#%%