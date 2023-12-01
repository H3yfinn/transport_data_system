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
    offset = 0, THROW_ERROR=True):
    all_rows = []
    ALL_ROWS_RETURNED = False
    while not ALL_ROWS_RETURNED:
        query_string = convert_to_query_string(header)
        query_string = query_string + '&offset=' + str(offset)
        try:
            data = make_api_call(data_source, key, query_string, headers=None, url_start='https://api.eia.gov/v2')
        except Exception as e:
            #hope to catch JSONDecodeError: Expecting value: line 1 column 1 (char 0)
            if THROW_ERROR:
                raise Exception('Error in api call. Exception was: {}'.format(e))
            else: 
                complete = False
                error_code=e
                return all_rows, offset, complete, error_code
        
        if 'error' in data.keys():
            try:
                if data['error']['code'] == 'OVER_RATE_LIMIT':
                    print(f"offset: {offset}, data['error']: {data['error']}")
                    complete=False
                    error_code = data['error']['code']
                    breakpoint()
                    return all_rows, offset, complete, error_code
                else:
                    if THROW_ERROR:
                        raise Exception('Something went wrong, error from api call was not as expected: {}'.format(data['error']))
                    else: 
                        complete = False
                        error_code=data['error']
                        return all_rows, offset, complete, error_code
            except:
                if THROW_ERROR:
                    raise Exception('Something went wrong, error from api call was not as expected: {}'.format(data['error']))
                else: 
                    complete = False
                    error_code=data['error']
                    return all_rows, offset, complete, error_code
        
        all_rows.extend(data['response']['data'])
        
        if 'warnings' in data['response']:
            if 'The API can only return 5000 rows in JSON format.' in data['response']['warnings'][0]['description']:
                offset += 5000
            else:
                if THROW_ERROR:
                    raise Exception('Something went wrong, warning from api call was not as expected: {}'.format(data['response']['warnings'][0]['description']))
                else: 
                    complete = False
                    error_code=data['response']
                    return all_rows, offset, complete, error_code
        else:
            ALL_ROWS_RETURNED = True
    complete=True
    error_code=None
    return all_rows, offset, complete, error_code

#%%
def brute_force_scrape_all(offset, header, data_source, key):
    
    complete = False
    delay = 0
    all_rows = []
    all_rows_df = pd.DataFrame()
    while not complete:
        #wait delay seconds. it will start at 0 and increase by 60 each time we get a warning about the api call being too fast. eventually it will get all the data!
        print('delay: {}'.format(delay))
        time.sleep(delay)
        all_rows_, offset, complete, error_code = query_all_applicable_data(header, data_source, key, offset)
        if not complete and error_code == 'OVER_RATE_LIMIT':
            #add to delay
            delay += 60
            if delay >= 200:#TODO: we are finding that at 25000 offset we always get over the rate limit. so we will just stop there for now. we can always come back to this later
                print('Max offset reached: {}'.format(offset))
                complete = True
            continue
        elif not complete: 
            breakpoint()
            raise Exception('Something went wrong, error from api call was not as expected: {}'.format(error_code))
        else:
            all_rows += all_rows_        
        
    all_rows_df = pd.concat([all_rows_df, pd.DataFrame(all_rows)])
    pd.DataFrame(all_rows_df).to_pickle(f'input_data/EIA/all_rows_{offset}.pkl')

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

offset = 200000
all_rows_df = pd.read_pickle(f'input_data/EIA/all_rows_{offset}.pkl')

do_this = False
if do_this:
    brute_force_scrape_all(offset, header, data_source, key)
#%%
#now we have all the rows, we can search for the series we want:
applicable_series = []
for index, row in all_rows_df.iterrows():
    if 'tr' in row['seriesId'] or any(x in row['seriesName'].lower() for x in ['transport','vehicle','travel', 'air', 'ship', 'road']):
        applicable_series.append(row['seriesId'])
#%%
#now we have the series we want, we can extract the data for each series:
# del all_rows
#create new header with all the series in it and no period restriction
#for some reason it doesnt work with every series id so we'll jsut run it series by series:


def run_query_by_series_id(data_source, key, applicable_series, header, MAX_DELAY=600):
    #first create a folder for the data source (with slashes replaced with _), if it doesnt already exist:
    subfolder = data_source.replace("/", "_")
    if not os.path.exists('input_data/EIA/{}'.format(subfolder)):
        os.makedirs('input_data/EIA/{}'.format(subfolder))
        completed_series_ids = []
        bad_series_ids = []
    else:
        try:#FileNotFoundError
            #extrract series that have already been completed:
            completed_series_ids = pd.read_pickle(f'input_data/EIA/{subfolder}/completed_series_ids.pkl').tolist()
            bad_series_ids = pd.read_pickle(f'input_data/EIA/{subfolder}/bad_series_ids.pkl').tolist()
            applicable_series = [x for x in applicable_series if x not in completed_series_ids and x not in bad_series_ids]
        except:
            completed_series_ids = []
            bad_series_ids = []
        
    delay = 0
    all_rows = []
    #now run the query series by series and save the data in the folder:
    for series_id in applicable_series:
        print('delay: {}'.format(delay))
        time.sleep(delay)
        # header = {
        #     "frequency": "annual",
        #     "data": [
        #         "value"
        #     ],
        #     "facets": {
        #         "scenario": [
        #             "ref2023"
        #         ],
        #         "series_id": [
        #             series_id
        #         ]
        #     }
        # }
        header_x = header.copy()
        header_x['facets']['seriesId'] = [series_id]
        all_rows_, offset, complete, error_code = query_all_applicable_data(header_x, data_source, key, THROW_ERROR=False)
        if error_code == 'OVER_RATE_LIMIT':
            #add to delay
            delay += 60
            if delay >= MAX_DELAY:
                print('Max delay {} reached with offset: {}. Progress saved.'.format(delay,offset))
                bad_series_ids += series_id
                break
        elif not complete:
            breakpoint()
            print('Something went wrong, error from api call was not as expected: {}'.format(error_code))
            #add the series_id to a list of series that didnt work
            bad_series_ids += series_id
            continue
        else:
            breakpoint()
            delay = 0
            #save the data and 
            pd.DataFrame(all_rows_).to_pickle(f'input_data/EIA/{subfolder}/{series_id}.pkl')
            all_rows += all_rows_
            completed_series_ids += series_id
            
    #save the bad series ids and the completed series ids:
    pd.DataFrame(bad_series_ids).to_pickle(f'input_data/EIA/{subfolder}/bad_series_ids.pkl')
    pd.DataFrame(completed_series_ids).to_pickle(f'input_data/EIA/{subfolder}/completed_series_ids.pkl')
    
    return bad_series_ids, completed_series_ids, all_rows

def concat_saved_pickle_files(data_source):
    subfolder = data_source.replace("/", "_")
    directory = f'input_data/EIA/{subfolder}/'

    all_data_frames = []

    for filename in os.listdir(directory):
        if filename.endswith('.pkl') and filename not in ['bad_series_ids.pkl', 'completed_series_ids.pkl']:
            file_path = os.path.join(directory, filename)
            df = pd.read_pickle(file_path)
            all_data_frames.append(df)

    concatenated_df = pd.concat(all_data_frames, ignore_index=True)
    
    return concatenated_df

     
#%%
header = {
    "frequency": "annual",
    "data": [
        "value"
    ],
    "facets": {
        "scenario": [
            "ref2023"
        ],
    }
}
#%%

bad_series_ids, completed_series_ids, all_rows = run_query_by_series_id(data_source, key, applicable_series, header)

# Example usage
concatenated_data = concat_saved_pickle_files(data_source)
#%%
#add concatenated_data to all_rows
concatenated_data = pd.concat( [concatenated_data, pd.DataFrame(all_rows)] )
#%%
# all_scraped_data = all_rows[0]
#TODO HOW TO GET THE VALUE???
concatenated_data.to_pickle(f'input_data/EIA/all_scraped_data_{data_source}.pkl')

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