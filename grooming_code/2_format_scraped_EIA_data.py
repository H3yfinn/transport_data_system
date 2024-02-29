#%%
#eia data is accessible via api so we will use that to get the data we need. some formatting will still need to be done but hopefully a lot will be easy using the vlaues in the data/call

# the prev file for this is be C:\Users\finbar.maunsell\github\transport_data_system\grooming_code\1_extract_data_from_EIA_API.py
#%%
 

#take in data files. they all have around about the same structure:
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import re
import datetime
import urllib.parse
import requests
import time
import json
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
# key = 'tCnavZWYLeKCs7nKHvKVGpnAqMKXIXoXVoxa0GXF'
data_source = 'aeo/2023'

concatenated_data = pd.read_pickle(f'input_data/EIA/all_scraped_data_{data_source.replace("/", "_")}.pkl')

#%%
#now using the output we can set it inot the format we want. for this its useufl to use the seriesName, Period, scenarioDescription, unit as it often contians all the info we need, as long as we have prepared cateorisations for that info.
#for example, series name is 'New Light-Duty Vehicle : 200-Mile Electric Vehicle : Small Utility' and scenarioDescription is 'Reference' and period is 2021. from this we can extract the following: vehicle type: light-duty vehicle, range: 200, fuel type: electric,drive: electric, new_or_used: new, period: 2021, scenario: reference, unit: mpg, economy: 20_USA

#there are often other useful datapoints but for now we will just use these. we can then use these to create a dataframe with the following columns: vehicle type, range, fuel type, drive, new_or_used, period, scenario, unit, economy, value

#and we will amp them to more simialr values to our data, e.g.
# vehicle type: light-duty vehicle -> lpv
# fuel type: electric -> electric
# drive: electric -> bev
# new_or_used: new -> new
# period: 2021 -> 2021
# scenario: reference -> reference
# unit: mpg -> mpg (this will neeed to be converted to pj.km later)
# economy: 20_USA -> 20_USA (this will be a bit mroe difficult than that, basically the api returns region ids, where eg. 0-0 is 20_USA. we will need to convert this to the correct economy value)
# value:NOT SURE YET

# mappings: (search for the keys in the seriesName, scenarioDescription, unit, period to extract them)
#%%

#PLEASE NOTE THAT THIS CODE ASSUMES THE DATA SOURCE IS AEO/2023/DATA. IF IT IS NOT THEN IT MIGHT NEED TO BE CHANGED. HOPEFULLY IT CAN BE KEPT SIMILAR SO THE MAPPINGSSTILL WORK FOR THE AEO/2023/DATA DATA SOURCE.

#%%
#some of the dicts need to be ordered dicts because they have overlapping keys. e.g. 'diesel' and 'diesel-electric hybrid' both have 'diesel' in them. so we need to check for the less specific one first, so that if it is matching it can still be overriden by the more specific one.
from collections import OrderedDict
drive_types = OrderedDict([
    ('diesel', 'ice_d'),
    ('gasoline', 'ice_g'),
    ('propane', 'lpg'),
    ('compressed/liquefied natural gas', 'cng'),
    ('ethanol-flex fuel', 'ethanol_flex_fuel'),
    ('electric', 'bev'),
    ('fuel cell', 'fcev'),
    #starting more specific ones:
    ('diesel-electric hybrid', 'phev_d'),
    ('plug-in diesel hybrid', 'phev_d'),
    ('plug-in gasoline hybrid', 'phev_g'),
    ('ethanol flex', 'ethanol_flex_fuel'),
])

vehicle_types = OrderedDict([
    ('cars', 'car'),
    ('light medium', 'lcv'),
    ('medium', 'mt'),
    ('heavy', 'ht'),
    ('railroads', 'rail'),
    ('domestic shipping', 'ship_domestic'),
    ('international shipping', 'ship_international'),
    ('light-duty vehicle', 'lpv'),
    ('large crossover', 'suv'),
    ('small crossover', 'suv'), 
    ('small van', 'lcv'),
    ('large van', 'lcv'),
    ('subcompact car', 'car'),
    ('mini-compact car', 'car'),
    ('two seater car', 'car'),
    ('conventional cars', 'car'),
    ('light trucks', 'lt')
    ])
    
fuels = {
    'distillate fuel oil (diesel)': 'diesel',
    'residual fuel oil': 'fuel_oil',
    'compressed natural gas': 'cng',
    'liquefied natural gas': 'lng',
    'electric': 'electric'
}

measures = {
    # 'vehicle miles traveled (billion miles)': 'travel_km',
    # 'consumption (trillion btu)': 'energy',
    # 'fuel efficiency (miles per gallon)': 'efficiency',
    # 'stock (millions)': 'vehicle_stock',
    # 'fuel consumption (trillion btu)': 'fuel_consumption',
    # 'ton miles by rail (billion)': 'activity',
    # 'fuel efficiency (ton miles per thousand btu)': 'efficiency',
    # 'ton miles shipping (billion)': 'activity',
    # 'gross trade (billion 2012 dollars)': 'gross_trade_value',
    # 'exports (billion 2012 dollars)': 'export_value',
    # 'imports (billion 2012 dollars)': 'import_value'
    'fleet vehicle sales': 'sales',
    'fuel economy': 'efficiency',
    
}
#use this where there is nothing to indicate the neasure in the seriesName
#note that we will fix units later, so we can just use the units that are in the data for now
units_to_measure_mapping = {
    'mpg': 'efficiency',
}


# mapping_dictionaries_to_column_name = {
#     'drive': drive_types,
#     'vehicle_type': vehicle_types,
#     'fuel': fuels,
#     'measure': measures,
#     'unit': units_to_measure_mapping
# }
mapping_dictionaries_to_column_name = OrderedDict([
    ('drive', drive_types),
    ('vehicle_type', vehicle_types),
    # ('fuel', fuels),
    ('measure', measures),
    ('unit', units_to_measure_mapping)
])

region_id_to_economy_mapping = {
    '0-0': '20_usa'
}

words_in_series_names_to_ignore = ['liquefaction', 'energy expenditures',  'production', 'net imports', 'total supply']
# things to ignore for now:
words_in_series_names_to_ignore += ['aircraft stock','energy use', 'use by sector', 'energy prices', 'end-use prices','vehicle range']
# anything in this list will cause that series to be ignored. e.g. 'liquefaction' will ignore any series with 'liquefaction' in the seriesname

new_df = pd.DataFrame(columns=['drive', 'vehicle_type', 'measure', 'period', 'scenario', 'unit', 'economy', 'value'])#'fuel', 

#make the value col into a float:
#find any nopn num,eric characters in the value column and replace them with np.nan
concatenated_data['value'] = concatenated_data['value'].replace(regex=r'[^\d.]', value=np.nan)
concatenated_data['value'] = concatenated_data['value'].astype(float)
#decapitalaise everythin in concatenated_data:
concatenated_data = concatenated_data.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
#%%
######################################
######################################
def extract_values_from_mapping_dictionaries(mapping, string_to_search):
    matching_string = None
    for key in mapping.keys():
        if key in string_to_search:
            matching_string = mapping[key]
    
    return matching_string

def process_row(row):
    new_row = {}
    
    if any(word in row['seriesName'] for word in words_in_series_names_to_ignore):
        return None
    
    #go through the mapping dictionaries and extract the values in column_to_extract_from that mathc the keys in the mapping dictionary
    for column_name, mapping in mapping_dictionaries_to_column_name.items():
        column_to_extract_from = 'seriesName'
        if column_name == 'unit':#check that measrue for this row hasnt already been set, if so, skip this row, else, set measure using the unit as column_to_extract_from
            if 'measure' in new_row.keys():
                if new_row['measure'] is not None:
                    continue
                else:
                    column_name = 'measure'
                    column_to_extract_from = 'unit'
            else:
                column_name = 'measure'
                column_to_extract_from = 'unit'
        
        new_row[column_name] = extract_values_from_mapping_dictionaries(mapping, row[column_to_extract_from])
        
    new_row['period'] = row['period']
    new_row['scenario'] = row['scenario']
    new_row['unit'] = row['unit']
    #where region maps to economy then use that, otherwise use the region id
    new_row['economy'] = region_id_to_economy_mapping[row['regionId']] if row['regionId'] in region_id_to_economy_mapping else row['regionId']
    # new_row['value'] = entry['data'][0]['value']
    
    new_row['seriesName'] = row['seriesName']    
    new_row['value'] = row['value']
    if new_row['value'] == 'nan' or new_row['value'] == 'na' or new_row['value'] == np.nan:
        breakpoint()#why doesnt vlaue have non na
    breakpoint()
    return new_row

# Function to save checkpoints
def save_checkpoint(index, temp_df):
    temp_df.to_csv(f'intermediate_data/EIA/temp_checkpoint_{index}.csv', index=False)
    with open('intermediate_data/EIA/last_checkpoint.json', 'w') as f:
        json.dump({'last_index': index}, f)

# Function to load the last checkpoint
def load_last_checkpoint():
    try:
        with open('intermediate_data/EIA/last_checkpoint.json', 'r') as f:
            try:
                last_checkpoint = json.load(f)
            except:
                return 0
            if 'last_index' in last_checkpoint:
                last_index = last_checkpoint['last_index']
            else:
                last_index = 0
            return last_index
    except FileNotFoundError:
        return 0
    
######################################
######################################
#%%
# Load the last checkpoint
last_index_processed = load_last_checkpoint()
last_index_processed = 0

# Assuming you've defined your mapping functions and dictionaries above

# Process data in chunks
chunk_size = 100  # Define a reasonable chunk size
total_rows = len(concatenated_data)

for start_row in range(last_index_processed, total_rows, chunk_size):
    end_row = min(start_row + chunk_size, total_rows)
    temp_df = pd.DataFrame()  # Temporary dataframe to store processed chunk

    # Process chunk
    for index, row in concatenated_data.iloc[start_row:end_row].iterrows():
        # Your data processing logic here
        breakpoint()
        new_row = process_row(row)  # Assuming you have a function to process each row
        if new_row is not None:
            temp_df = pd.concat([temp_df, pd.DataFrame(new_row, index=[0])])

    # Save processed chunk
    save_checkpoint(end_row, temp_df)
    print(f"Processed and saved up to row {end_row}")

# Combine all chunks into the final DataFrame after all chunks are processed
# This step can be done after all processing is complete or in a separate script
final_df = pd.DataFrame()
for i in range(0, total_rows, chunk_size):
    if i == 0:
        continue
    try:
        temp_df = pd.read_csv(f'intermediate_data/EIA/temp_checkpoint_{i}.csv')
    except:
        print(f"File intermediate_data/EIA/temp_checkpoint_{i}.csv not found with error")
        breakpoint()
        continue
    final_df = pd.concat([final_df, temp_df], ignore_index=True)

# Save the final combined DataFrame
final_df.to_csv(f'intermediate_data/EIA/{data_source.replace("/", "_")}_partly_formatted.csv', index=False)

#%%
# #save new_df:
# new_df.to_csv(f'intermediate_data/EIA/{data_source.replace("/", "_")}_partly_formatted.csv', index=False) #this data will be used by 2_format_scraped_EIA_data


######################################
######################################



#%%
new_df = pd.read_csv(f'intermediate_data/EIA/{data_source.replace("/", "_")}_partly_formatted.csv')
#where the unit is mpg convert to pj.km. can do this using the config/conversion_factors.csv file. 
#we will have to convert from the drove to the fuel type:
drive_to_fuel_type = {
    'ice_d': 'diesel',
    'ice_g': 'petrol',
    'phev_d': 'diesel',
    'phev_g': 'petrol',
    'bev': 'electric',
    'lpg': 'lpg',
    'ethanol_flex_fuel': 'ethanol',
    'fcev': 'hydrogen'}
    
new_df_mpg = new_df[new_df['unit'] == 'mpg']
new_df_mpg['fuel'] = new_df_mpg['drive'].map(drive_to_fuel_type)
#where there ar enas show them then if there are not nas fro drive, print the user so they know:
nas = new_df_mpg[new_df_mpg['fuel'].isna() & new_df_mpg['drive'].notna()]

if len(nas) > 0:
    print('nas found for drive to fuel type mapping')
    print(nas)
    breakpoint()

conversion_factors = pd.read_csv('config/conversion_factors.csv')
conversion_factors = conversion_factors[(conversion_factors['original_unit'] == 'mpg')&(conversion_factors['final_unit'] == 'km/PJ')]

#merge with conversion factors:
new_df_mpg = new_df_mpg.merge(conversion_factors, left_on=['fuel', 'unit'], right_on=['fuel', 'original_unit'], how='left', suffixes=('', '_conversion_factor'), indicator=True)
#identify any left_onlys
left_onlys = new_df_mpg[new_df_mpg['_merge'] == 'left_only']
if len(left_onlys) > 0:
    print('left_onlys found in merge with conversion factors')
    print(left_onlys)
    # raise ValueError('left_onlys found')

#times the conversion factor by the value:
new_df_mpg['value'] = new_df_mpg['value']*new_df_mpg['value_conversion_factor']

#drop the columns we dont need anymore:
new_df_mpg = new_df_mpg.drop(columns=['value_conversion_factor', 'original_unit', 'final_unit'])

#rename unit to Km_per_pj 
new_df_mpg['unit'] = 'km_per_pj'

#add this back to the main df:
new_df = new_df[new_df['unit'] != 'mpg']
new_df = pd.concat([new_df, new_df_mpg])


#%%
#save the data:
new_df.to_csv(f'intermediate_data/EIA/{data_source.replace("/", "_")}_formatted.csv', index=False) #this data will be used by 2_format_scraped_EIA_data
#%%
#Do some quick analysis to show user what is available:
#hosw the different valuesin new_df.seriesName.unique() and new_df.measure.unique()
print('We have the following unique values in seriesName and measure:')
for i in new_df['seriesName'].unique():
    print(i)
print(new_df['measure'].unique())









# %%
