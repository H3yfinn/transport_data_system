#%%
#eia data is probably all in the same format so we can just use the same code for all of them, hopefully... It was a bit difficult to write the cpde through Chatgpt but i still tried: https://chat.openai.com/share/51f97ae6-e276-4e49-b0d0-acd229c745b9

#take in data files. they all have around about the same structure:
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import re
import datetime
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')

# Load the CSV file
file_path = 'input_data/EIA/Freight.csv'
#the 5th row is the header row, so we will skip the first 4 rows
freight_data = pd.read_csv(file_path, header=4)

#anything in the following correspod to their drive categories, and their mapped value is our categories for thiose:
# Diesel
# Motor Gasoline
# Propane
# Compressed/Liquefied Natural Gas
# Ethanol-Flex Fuel
# Electric
# Plug-in Diesel Hybrid
# Plug-in Gasoline Hybrid
# Fuel Cell

drive_types = {
    'Diesel': 'ice_d',
    'Motor Gasoline': 'ice_g',
    'Propane': 'lpg',
    'Compressed/Liquefied Natural Gas': 'cng',
    'Ethanol-Flex Fuel': 'ethanol_flex_fuel',
    'Electric': 'bev',
    'Plug-in Diesel Hybrid': 'phev_d',
    'Plug-in Gasoline Hybrid': 'phev_g',
    'Fuel Cell': 'fcev'
}
    
#in below is their vehicle types and our mapped values for those:
# Light Medium
# Medium
# Heavy
# Railroads
# Domestic Shipping
# International Shipping

vehicle_types = {
    'Light Medium': 'lcv',
    'Medium': 'mt',
    'Heavy': 'ht',
    'Railroads': 'rail',
    'Domestic Shipping': 'ship_domestic',
    'International Shipping': 'ship_international'
}

# Distillate Fuel Oil (diesel)
# Residual Fuel Oil
# Compressed Natural Gas
# Liquefied Natural Gas
Fuels = {
    'Distillate Fuel Oil (diesel)': 'diesel',
    'Residual Fuel Oil': 'fuel_oil',
    'Compressed Natural Gas': 'cng',
    'Liquefied Natural Gas': 'lng'
}
    
# Vehicle Miles Traveled (billion miles)
# Consumption (trillion Btu)
# Fuel Efficiency (miles per gallon)
# Stock (millions)
# Fuel Efficiency (miles per gallon)
# Fuel Consumption (trillion Btu)
# Ton Miles by Rail (billion)
# Fuel Efficiency (ton miles per thousand Btu)
# Ton Miles Shipping (billion)
# Gross Trade (billion 2012 dollars)
# Exports (billion 2012 dollars)
# Imports (billion 2012 dollars)
measures = {
    'Vehicle Miles Traveled (billion miles)': 'travel_km',
    'Consumption (trillion Btu)': 'energy',
    'Fuel Efficiency (miles per gallon)': 'efficiency',
    'Stock (millions)': 'vehicle_stock',
    'Fuel Consumption (trillion Btu)': 'fuel_consumption',
    'Ton Miles by Rail (billion)': 'activity',
    'Fuel Efficiency (ton miles per thousand Btu)': 'efficiency',
    'Ton Miles Shipping (billion)': 'activity',
    'Gross Trade (billion 2012 dollars)': 'gross_trade_value',
    'Exports (billion 2012 dollars)': 'export_value',
    'Imports (billion 2012 dollars)': 'import_value'
}
  
rows_to_skip = ['Freight Truck Stock by Size Class']

years = [str(num) for num in range(2021, 2051)]#the data has years for 2021 to 2050
#using these, we will go down the first column of freight.csv and extract and reformat rows based on the pattern:
# e.g:
# measure: Vehicle Miles Traveled (billion miles)
# vehicle type: Light Medium
# drive (until a non drive col is reached [normally 9 rows]): Diesel
# ...
# Vehicle type subtotal: Light Medium subtotal (can search for this by looking for subtotal in the name)
# Vehicle type: Medium (pattern restarts here)
# ...
# Measure subtotal: Total Vehicle Miles Traveled (after all the vehicle types fr that measure , a subtotal row is reached, note that sometimes it might be an avg row instead of a subtotal row)
# Measure: Consumption (trillion Btu) (after all the vehicle types fr that measure , snother measure starts and the pattern restarts)
# ...
# New Trucks by Size Class: this represents data for new vehicles, rahter than pre-existing ones, we should label measures accordingly
# Fuel Efficiency (miles per gallon)
# Light Medium
# pattern continues as before...
# ...
# Railroads: Once railroads are met the pattern changes, it goes:
# Ton Miles by Rail (billion)
# Fuel Efficiency (ton miles per thousand Btu)
# Fuel Consumption (trillion Btu)
# Distillate Fuel Oil (diesel)
# Residual Fuel Oil
# Compressed Natural Gas
# Liquefied Natural Gas
#Domestic Shipping: pattern for rail is copied for domestic shipping
# ...
# International Shipping: (pattern is a bit different here)
# Gross Trade (billion 2012 dollars)
# Exports (billion 2012 dollars)
# Imports (billion 2012 dollars)
# Fuel Consumption (trillion Btu)
# Distillate Fuel Oil (diesel)
# Residual Fuel Oil
# Compressed Natural Gas
# Liquefied Natural Gas

# Dictionary to hold the processed data for each vehicle type
current_measure = None
current_vehicle_type = None
current_drive_type = None
new_vehicles = False 
medium = 'road'#will change to false when we reach railroads
unit = None

new_df = pd.DataFrame(columns=['measure', 'vehicle_type', 'drive', 'year', 'value', 'unit', 'new_vehicles', 'medium'])

for index, row in freight_data.iterrows():
    col1 = row[0]
    if col1 in rows_to_skip:
        continue
    #check if row is a measure
    if col1 in measures.keys():
        current_measure = measures[col1]
        continue
    #check if row is a vehicle type
    if col1 in vehicle_types.keys():
        current_vehicle_type = vehicle_types[col1]
        continue
    if col1 in Fuels.keys():
        current_drive_type = Fuels[col1]
        continue
    #if col contains Total or Subtotal then we can skip it
    if 'Total' in col1 or 'Subtotal' in col1:
        continue
    
    if col1 == 'New Trucks by Size Class':
        new_vehicles = True
        continue
    #check if row is a drive type. if so start adding data to the new df
    if col1 in drive_types.keys():
        current_drive_type = drive_types[col1]
        #if we are doing drives then we want to create a new row for each year using the variables we have:
        if medium == 'road':
            for col in row.index:
                new_row = {'measure': current_measure, 'vehicle_type': current_vehicle_type, 'drive': current_drive_type, 'new_vehicles': new_vehicles, 'medium': medium, 'unit': unit}
                if col == 'units':
                    unit = row[col]
                    continue
                elif str(col) in years:
                    new_row['year'] =int(col)
                    new_row['value'] =row[col] 
                else:
                    continue
                new_df = pd.concat([new_df, pd.DataFrame([new_row])])
        else:
            raise Exception('Something went wrong, we should not be here')
    
    if col1 == 'Railroads':
        medium = 'rail'
        current_vehicle_type=medium
        new_vehicles = False
        continue
    if col1 == 'Domestic Shipping':
        medium = 'ship'
        current_vehicle_type=medium
        new_vehicles = False
        continue
    
    if col1 == 'International Shipping':
        medium = 'int_ship'
        current_vehicle_type=medium
        new_vehicles = False
        continue
    if medium!= 'road' and col1 in measures.keys():
        #add row
        current_drive_type=None
        for col in row.index:
            new_row = {'measure': current_measure, 'vehicle_type': current_vehicle_type, 'drive': current_drive_type, 'new_vehicles': new_vehicles, 'medium': medium, 'unit': unit}
            if col == 'units':
                unit = row[col]
                continue
            elif str(col) in years:
                new_row['year'] =int(col)
                new_row['value'] =row[col] 
            else:
                continue
            new_df = pd.concat([new_df, pd.DataFrame([new_row])])
            
    if medium!= 'road' and col1 in Fuels.keys():
        for col in row.index:
            new_row = {'measure': current_measure, 'vehicle_type': current_vehicle_type, 'drive': current_drive_type, 'new_vehicles': new_vehicles, 'medium': medium, 'unit': unit}
            if col == 'units':
                unit = row[col]
                continue
            elif str(col) in years:
                new_row['year'] =int(col)
                new_row['value'] =row[col] 
            else:
                continue
            new_df = pd.concat([new_df, pd.DataFrame([new_row])])
    
#now where new_vehicles is true and efficiency is the measure, we want to rename measure to new_vehicle_efficiency
new_df.loc[(new_df['new_vehicles']==True) & (new_df['measure']=='efficiency'), 'measure'] = 'new_vehicle_efficiency'


#conversions:
#):
#%%