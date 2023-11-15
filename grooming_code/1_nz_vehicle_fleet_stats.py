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

#please note that the code here was developed using this: https://chat.openai.com/share/5ffcfe0c-5f79-421f-b7c4-fb3ba1d96202 > the data files that were used are in  transport_data_system\input_data\new zealand\chatgpt
#%%
# Step 1: Load all the sheets from the New Zealand vehicle fleet data
nz_vehicle_fleet_extract = pd.read_excel('input_data/New Zealand/nz_vehicle_fleet_extract.xlsx', sheet_name=None)

# Step 2: Load the data structure template
data_structure_nz_df = pd.read_excel('input_data/New Zealand/chatgpt/data_structure_nz.xlsx')

# Step 3: Prepare mapping dictionaries for vehicle and drive types
vehicle_type_mapping = {
    'Light passenger': 'LPV',
    'Light commercial': 'LCV',
    'Trucks': 'HT',
    'Bus': 'Bus',
    'MCycle': '2W',
    'Other': 'Other',
    'Total': 'Total',
    'Total light': 'Total Light'
}
drive_to_vehicle_type_mapping = {
    'Light passenger petrol vehicles': 'LPV',
    'Light passenger diesel vehicles': 'LPV',
    'Light pure electric vehicles ': 'LPV',
    'Light commercial petrol vehicles': 'LCV',
    'Light commercial diesel vehicles': 'LCV',
    ' Petrol trucks': 'HT',
    'Diesel trucks': 'HT',
    'Motorcycles': '2W',
    'Petrol Buses': 'Bus',
    'Diesel buses': 'Bus',
    'Electric bus': 'Bus'
}
drive_type_mapping = {
    'Light passenger petrol vehicles': 'ICE_G',
    'Light passenger diesel vehicles': 'ICE_D',
    'Light pure electric vehicles ': 'BEV',
    'Light commercial petrol vehicles': 'ICE_G',
    'Light commercial diesel vehicles': 'ICE_D',
    ' Petrol trucks': 'ICE_G',
    'Diesel trucks': 'ICE_D',
    'Motorcycles': 'ICE_G',
    'Petrol Buses': 'ICE_G',
    'Diesel buses': 'ICE_D',
    'Electric bus': 'BEV'
}

# Step 4: Melt and map the 'vehicle_type' and 'drive' sheets
vehicle_type_df = nz_vehicle_fleet_extract['vehicle_type']
drive_df = nz_vehicle_fleet_extract['drive']
melted_vehicle_type_df = pd.melt(vehicle_type_df, id_vars=['Period'], var_name='Original_Vehicle_Type', value_name='Stocks')
melted_drive_df = pd.melt(drive_df, id_vars=['Period'], var_name='Original_Vehicle_Type', value_name='Stocks')

# Apply the mappings
melted_vehicle_type_df['Vehicle_Type'] = melted_vehicle_type_df['Original_Vehicle_Type'].map(vehicle_type_mapping)
melted_drive_df['Vehicle_Type'] = melted_drive_df['Original_Vehicle_Type'].map(drive_to_vehicle_type_mapping)
melted_drive_df['Drive'] = melted_drive_df['Original_Vehicle_Type'].map(drive_type_mapping)

# Concatenate the melted 'vehicle_type' and 'drive' data
melted_vehicle_type_df['Measure'] = 'Stocks'
melted_drive_df['Measure'] = 'Stocks'
#%%
all_stocks_df = pd.concat([melted_vehicle_type_df, melted_drive_df])

#rename Stocks to Value
all_stocks_df = all_stocks_df.rename(columns={'Stocks': 'Value'})
# Step 5: Melt and map the 'mileage_calc' sheet
mileage_calc_df = nz_vehicle_fleet_extract['mileage_calc']
melted_mileage_calc_df = pd.melt(mileage_calc_df, id_vars=['Period'], var_name='Original_Vehicle_Type', value_name='Value')

# Apply the mappings specific to 'mileage_calc'
mileage_calc_mapping = {
    'Light passenger travel': 'LPV',
    'Light commercial travel': 'LCV',
    'Heavy truck travel': 'HT',
    'Heavy bus travel': 'Bus',
    'Motorcycle / moped travel': '2W',
    'Other travel': 'Other'
}
melted_mileage_calc_df['Vehicle_Type'] = melted_mileage_calc_df['Original_Vehicle_Type'].map(mileage_calc_mapping)
melted_mileage_calc_df['Measure'] = 'Mileage'

#%%
# Step 6: Melt and map the 'travel_km' sheet
travel_km_df = nz_vehicle_fleet_extract['travel_km']
melted_travel_km_df = pd.melt(travel_km_df, id_vars=['Period'], var_name='Original_Vehicle_Type', value_name='Value')

# Apply the mappings specific to 'travel_km'
travel_km_mapping = mileage_calc_mapping 

melted_travel_km_df['Vehicle_Type'] = melted_travel_km_df['Original_Vehicle_Type'].map(travel_km_mapping)
melted_travel_km_df['Measure'] = 'Travel_KM'

# Step 7: Melt and map the 'average_age' sheet
average_age_df = nz_vehicle_fleet_extract['average_age']
melted_average_age_df = pd.melt(average_age_df, id_vars=['Period'], var_name='Original_Vehicle_Type', value_name='Value')

# Apply the mappings specific to 'average_age'
average_age_mapping = {
    ' Light Passenger': 'LPV',
    'Light Commercial': 'LCV',
    'Light New': 'LPV',  # Assuming new light vehicles are mostly passenger vehicles
    'Light Used Imports': 'LPV',  # Assuming used imports are mostly passenger vehicles
    'Total Light Vehicles': 'Total Light',
    'MCycle': '2W',
    'Heavy Commercial': 'HT',
    'Bus': 'Bus',
    'Other': 'Other',
    'Overall': 'Overall'
}

melted_average_age_df['Vehicle_Type'] = melted_average_age_df['Original_Vehicle_Type'].map(average_age_mapping)
melted_average_age_df['Measure'] = 'Average_Age'

# Step 8: Concatenate all the melted DataFrames
all_data_df = pd.concat([all_stocks_df, melted_mileage_calc_df, melted_travel_km_df, melted_average_age_df])

# Drop the rows where 'Vehicle_Type' is 'Overall', 'Total', or 'Total Light'
all_data_df = all_data_df[~all_data_df['Vehicle_Type'].isin(['Overall', 'Total', 'Total Light', 'Other', ' Overall'])]

# Drop the rows where 'Vehicle_Type' is 'Overall', 'Total', or 'Total Light'
all_data_df = all_data_df[~all_data_df['Original_Vehicle_Type'].isin(['Overall', 'Total', 'Total Light', 'Other', ' Overall'])]

#drop original vehicle type
all_data_df = all_data_df.drop(columns=['Original_Vehicle_Type'])


# Set additional metadata columns
transport_type_mapping = {
    'LPV': 'Passenger',
    'LCV': 'Freight',
    'HT': 'Freight',
    'Bus': 'Passenger',
    '2W': 'Passenger'
}

all_data_df['Transport_Type'] = all_data_df['Vehicle_Type'].map(transport_type_mapping)
all_data_df['Frequency'] = 'yearly'
all_data_df['Medium'] = 'Road'
all_data_df['Dataset'] = 'nz_fleet_stats'
all_data_df['Unit'] = all_data_df['Measure'].map({'Stocks': 'Stocks', 'Mileage': 'Km_per_stock', 'Travel_KM': 'Km', 'Average_Age': 'Age'})
all_data_df['Fuel'] = 'All'
all_data_df['Economy']='12_NZ'
all_data_df['Scope']='National'
all_data_df['Source']=''

#where Drive is na set it to All
all_data_df['Drive'] = all_data_df['Drive'].fillna('All')
#rename Period to date
all_data_df = all_data_df.rename(columns={'Period': 'Date'})
#make date an integer so we dont haver .0s
all_data_df['Date'] = all_data_df['Date'].astype(int)

#%%

#save 
# date id to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

all_data_df.to_csv('intermediate_data/NZ/{}_nz_fleet_stats.csv'.format(FILE_DATE_ID), index=False)
#%%