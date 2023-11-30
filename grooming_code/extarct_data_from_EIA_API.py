#%%
#eia data is accessible via api so we will use that to get the data we need. some formatting will still need to be done but hopefully a lot will be easy using the vlaues in the data/call
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
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
key = 'tCnavZWYLeKCs7nKHvKVGpnAqMKXIXoXVoxa0GXF'
data_source = 'aeo/2023/data'
#%%
def make_api_call(data_source, api_key, query_string, headers=None, url_start='https://api.eia.gov/v2'):
    """

    Args:
        url (_type_): the start of the url that is always the same unless you are using a different api
        headers (_type_): dont really know. too complicated
        data_source (_type_): the data source to be used in the request. so for example if you want to get data from the international energy outlook, you would put 'ieo/2023/data', or aeo/2023/data/ or the american outlook
    e.g.
    make_api_call(data_source, key, query_string, headers=None, url_start='https://api.eia.gov/v2')
    """
    
    # API endpoint URL
    url = f'{url_start}/{data_source}/?api_key={api_key}&{query_string}'

    # Making the GET request
    response = requests.get(f'{url}')

    # Extracting the data
    data = response.json()
    
    return data


def convert_to_query_string(params):
    """
    Convert dictionary parameters to a URL-encoded query string.

    It aseeems this will beuseful because the EIA ewbsite returns this kind of params data when you choose data in the website. so we can just copy and paste it into the code and it will work?
    Args:
        params (dict): Dictionary of parameters.

    Returns:
        str: URL-encoded query string.
        
    # Example usage
    header= {
        "frequency": "annual",
        "data": [
            "value"
        ],
        "facets": {
            "scenario": [
                "ref2023"
            ]
        }
    }

    query_string = convert_to_query_string(header)
    """
    query_string = ''
    for key, value in params.items():
        if isinstance(value, dict):
            for inner_key, inner_value in value.items():
                if isinstance(inner_value, list):
                    for val in inner_value:
                        query_string += f'{key}[{inner_key}][]={urllib.parse.quote(str(val))}&'
                else:
                    query_string += f'{key}[{inner_key}]={urllib.parse.quote(str(inner_value))}&'
        else:
            query_string += f'{key}={urllib.parse.quote(str(value))}&'

    return query_string[:-1]


#%%
#we want to run through the results from the api call and extract potential series that we can use. to do this we will grab all data for the header, and if it is met with a warning like so: 

# {'response': {'warnings': [{'warning': 'incomplete return',
# 'description': 'The API can only return 5000 rows in JSON format.  Please consider constraining your request with facet, start, or end, or using offset to paginate results.'}],
# it will provide &offset=5000 at the end of the next query_string to grab the next 5000. do the same for 10,000 and so on.

#specific search features:
# in the returns we will search the seriesId for '_tr_'  which could represent any transport series
# search for one of [transport,vehicle,travel, air, ship, road ]       in seriesName.lower(). hopfully this will get us all of the transport data!
#from those extract their seriesId. these are specific to each series and will be used to get the data for that series

def query_all_applicable_data(header, data_source, key,
    offset = 0):
    all_rows = []
    ALL_ROWS_RETURNED = False
    while not ALL_ROWS_RETURNED:
        query_string = convert_to_query_string(header)
        query_string = query_string + '&offset=' + str(offset)
        data = make_api_call(data_source, key, query_string, headers=None, url_start='https://api.eia.gov/v2')
        
        if 'error' in data.keys():
            try:
                if data['error']['code'] == 'OVER_RATE_LIMIT':
                    print(f"offset: {offset}, data['error']: {data['error']}")
                    complete=False
                    breakpoint()
                    return all_rows, offset, complete
                else:
                    raise Exception('Something went wrong, error from api call was not as expected: {}'.format(data['error']))
            except:
                raise Exception('Something went wrong, error from api call was not as expected: {}'.format(data['error']))
        
        all_rows.extend(data['response']['data'])
        
        if 'warnings' in data['response']:
            if 'The API can only return 5000 rows in JSON format.' in data['response']['warnings'][0]['description']:
                offset += 5000
            else:
                raise Exception('Something went wrong, warning from api call was not as expected: {}'.format(data['response']['warnings'][0]['description']))
        else:
            ALL_ROWS_RETURNED = True
    complete=True
    return all_rows, offset, complete

#%%
header= {
    "frequency": "annual",
    "data": [
        "value"
    ],
    "facets": {
        "scenario": [
            "ref2023"
        ]
    },
    "start": "2021",
    "end": "2022",
}
complete = False
delay = 0
offset = 0
all_rows = []
all_rows_df = pd.DataFrame()
#%%
all_rows_df = pd.read_pickle('input_data/EIA/all_rows_200000.pkl')
offset = 75000
#%%
do_this = False
if do_this:
    while not complete:
        #wait delay seconds. it will start at 0 and increase by 60 each time we get a warning about the api call being too fast. eventually it will get all the data!
        print('delay: {}'.format(delay))
        time.sleep(delay)
        all_rows_, offset, complete = query_all_applicable_data(header, data_source, key, offset)
        
        all_rows += all_rows_
        
        #add to delay
        
        delay += 60
        
        if delay >= 200:#TODO: we are finding that at 25000 offset we always get over the rate limit. so we will just stop there for now. we can always come back to this later
            print('Max offset reached: {}'.format(offset))
            breakpoint()
            break
        
    all_rows_df = pd.concat([all_rows_df, pd.DataFrame(all_rows)])
    pd.DataFrame(all_rows_df).to_pickle(f'input_data/EIA/all_rows_{offset}.pkl')

#%%
#now we have all the rows, we can search for the series we want:
applicable_series = []
for index, row in all_rows_df.iterrows():
    if 'tr' in row['seriesId'] or any(x in row['seriesName'].lower() for x in ['transport','vehicle','travel', 'air', 'ship', 'road']):
        applicable_series.append(row['seriesId'])
#%%
#now we have the series we want, we can extract the data for each series:
del all_rows
#create new header with all the series in it and no period restriction
header = {
    "frequency": "annual",
    "data": [
        "value"
    ],
    "facets": {
        "scenario": [
            "ref2023"
        ],
        "seriesId": [series_id for series_id in applicable_series]
    }
}

all_rows = query_all_applicable_data(header, data_source, key)
#%%
all_scraped_data = all_rows[0]
#TODO HOW TO GET THE VALUE???
pd.DataFrame(all_scraped_data).to_pickle(f'input_data/EIA/all_scraped_data_{data_source}.pkl')

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

vehicle_types = {
    'Light Medium': 'lcv',
    'Medium': 'mt',
    'Heavy': 'ht',
    'Railroads': 'rail',
    'Domestic Shipping': 'ship_domestic',
    'International Shipping': 'ship_international',
    'light-Duty Vehicle': 'lpv',
}

Fuels = {
    'Distillate Fuel Oil (diesel)': 'diesel',
    'Residual Fuel Oil': 'fuel_oil',
    'Compressed Natural Gas': 'cng',
    'Liquefied Natural Gas': 'lng',
    'Electric': 'electric'
}


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
mapping_dictionaries_to_column_name = {
    'drive': drive_types,
    'vehicle_type': vehicle_types,
    'fuel': Fuels,
    'measure': measures 
}

region_id_to_economy_mapping = {
    '0-0': '20_USA'
}

new_df = pd.DataFrame(columns=['drive', 'vehicle_type', 'fuel', 'measure', 'period', 'scenario', 'unit', 'economy', 'value'])

def extract_values_from_mapping_dictionaries(mapping_dictionary, string_to_search, column_name):
    for mapping in mapping_dictionary[column_name]:
        for key in mapping.keys():
            if key in string_to_search:
                return mapping[key]
    return None
    
for entry in all_scraped_data:
    new_row = {}
    
    for column_name, mapping_dictionary in mapping_dictionaries_to_column_name.items():
        new_row[column_name] = extract_values_from_mapping_dictionaries(mapping_dictionary, entry['seriesName'], column_name)
        
    new_row['period'] = entry['period']
    new_row['scenario'] = entry['scenarioDescription']
    new_row['unit'] = entry['unit']
    #where region maps to economy then use that, otherwise use the region id
    new_row['economy'] = region_id_to_economy_mapping[entry['region']] if entry['region'] in region_id_to_economy_mapping else entry['region']
    # new_row['value'] = entry['data'][0]['value']
    
    
#%%