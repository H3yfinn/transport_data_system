#%%
#eia data is accessible via api so we will use that to get the data we need. some formatting will still need to be done but hopefully a lot will be easy using the vlaues in the data/call

# the next file for this will be 2_format_scraped_EIA_data
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
data_source = 'aeo/2023'
#%%

################################################################################
#START OF FUNCTIONS
################################################################################
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
    url = f'{url_start}/{data_source}/?api_key={api_key}'
    #if we are searching for anything then we need to add the query string to the url, as well as adding /data/ to the data_source
    if query_string != '':
        url = f'{url_start}/{data_source}/data/?api_key={api_key}&{query_string}'
    try:
        response = requests.get(f'{url}')
    except Exception as e:
        if 'CERTIFICATE_VERIFY_FAILED' in str(e):
            raise Exception('You may need to activate the aperc guest wifi. Exception was: {}'.format(e))
        else:
            raise Exception('Exception was: {}'.format(e))
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
        elif isinstance(value, list):
            for val in value:
                query_string += f'{key}[]={urllib.parse.quote(str(val))}&'
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

def query_all_applicable_data(data_source, key,
    header={}, offset = 0,PURPOSE='', THROW_ERROR=True, ALL_DATA_QUERIED=True):
    all_rows = []
    ALL_ROWS_RETURNED = False
    error_code = None
    while not ALL_ROWS_RETURNED:
        query_string = convert_to_query_string(header)
        if (query_string != ''):
            query_string += '&offset=' + str(offset)
            
        try:
            
            data = make_api_call(data_source, key, query_string, headers=None, url_start='https://api.eia.gov/v2')
            
        #######ERROR HANDLING######
        except Exception as e:
            #hope to catch JSONDecodeError: Expecting value: line 1 column 1 (char 0)
            if THROW_ERROR:
                raise Exception('Error in api call. Exception was: {}'.format(e))
            else: 
                error_code=e
                return all_rows, error_code, offset, ALL_DATA_QUERIED
        
        if 'error' in data.keys():
            try:
                if data['error']['code'] == 'OVER_RATE_LIMIT':
                    print(f"data['error']: {data['error']}")
                    error_code = data['error']['code']
                    return all_rows, error_code, offset, ALL_DATA_QUERIED
                else:
                    if THROW_ERROR:
                        raise Exception('Something went wrong, error from api call was not as expected: {}'.format(data['error']))
                    else: 
                        error_code=data['error']
                        return all_rows, error_code, offset, ALL_DATA_QUERIED
            except:
                if THROW_ERROR:
                    raise Exception('Something went wrong, error from api call was not as expected: {}'.format(data['error']))
                else: 
                    error_code=data['error']
                    return all_rows, error_code, offset, ALL_DATA_QUERIED
        #######ERROR HANDLING######
        
        if PURPOSE == 'data':
            all_rows.extend(data['response']['data'])
        elif PURPOSE == 'seriesid':
            all_rows.extend(data['response']['facets'])
        else:
            raise Exception('Purpose was not recognised. Purpose was: {}'.format(PURPOSE))
        
        if 'warnings' in data['response']:
            if 'The API can only return 5000 rows in JSON format.' in data['response']['warnings'][0]['description']:
                print('offset: {}'.format(offset))
                offset += 5000
                ALL_DATA_QUERIED = False
                return all_rows, error_code, offset, ALL_DATA_QUERIED
                # raise Exception('Removed offset handling. Have to reinstate it: {}'.format(data['response']['warnings'][0]['description']))
            else:
                if THROW_ERROR:
                    raise Exception('Something went wrong, warning from api call was not as expected: {}'.format(data['response']['warnings'][0]['description']))
                else: 
                    error_code=data['response']
                    return all_rows, error_code, offset, ALL_DATA_QUERIED
        else:
            ALL_ROWS_RETURNED = True
    error_code=None
    return all_rows, error_code, offset, ALL_DATA_QUERIED

#%%
def brute_force_scrape_all_for_data_source(data_source, key, data_source_addition):
    """will search for all data given the data_source_addition and the data source. it will save the data in a pickle file in the input_data/EIA folder

    Args:
    
    Raises:
        Exception: _description_
        Exception: _description_

    Returns:
        _type_: _description_
    """
    all_rows = []
    all_rows_df = pd.DataFrame()
    new_data_source = data_source + data_source_addition
    all_rows, error_code, offset, ALL_DATA_QUERIED = query_all_applicable_data(new_data_source, key, PURPOSE='seriesid', THROW_ERROR=False)
        
    all_rows_df = pd.concat([all_rows_df, pd.DataFrame(all_rows)])
    #find num of duplicates:
    num_duplicates = all_rows_df.duplicated().sum()
    print('num_duplicates: {}'.format(num_duplicates))
    #drop duplicates:
    all_rows_df = all_rows_df.drop_duplicates()
    #save the data:   
    if all_rows_df.empty:
        breakpoint()#why does this end up as 0 rows??     
        raise Exception('all_rows_df is empty when saving to pickle file')
    all_rows_df.to_pickle(f'input_data/EIA/all_series_ids_{data_source.replace("/", "_")}.pkl')
    
    return all_rows_df

def setup_for_datasource(data_source):
    
    #find the offset that we are up to by finding file with grandest offset:
    if not os.path.exists('input_data/EIA/{}'.format(data_source.replace("/", "_"))):
        os.makedirs('input_data/EIA/{}'.format(data_source.replace("/", "_")))
        
    if not os.path.exists('input_data/EIA/{}/{}'.format(data_source.replace("/", "_"), 'series_id_data')):
        os.makedirs('input_data/EIA/{}/{}'.format(data_source.replace("/", "_"), 'series_id_data'))
    
def run_query_by_series_id(data_source, key, applicable_series, header, MAX_DELAY=600):
    """load data from api using the series ids in  applicable_series and save it in the folder input_data/EIA/{data_source.replace("/", "_")}/series_id_data. If this process breaks it will just need to be rerun, and any fiels that have already been downloaded will be skipped because of running remove_already_downloaded_series_ids_from_applicable_series

    Args:
        data_source (_type_): _description_
        key (_type_): _description_
        applicable_series (_type_): _description_
        header (_type_): _description_
        MAX_DELAY (int, optional): _description_. Defaults to 600.

    Returns:
        _type_: _description_
    """
    #first create a folder for the data source (with slashes replaced with _), if it doesnt already exist:
    subfolder = data_source.replace("/", "_")  
    delay = 0
    all_rows = []
    #now run the query series by series and save the data in the folder:
    for series_id in applicable_series:
        id = series_id[0]
        name = series_id[1]
        if id == '#N/A':
            # breakpoint()
            # identifier = name
            # header_x = header.copy()
            # header_x['facets']['seriesName'] = [name]
            print(f'series_id is #N/A for seriesName {name}, skipping')
            continue    
        # .contains('NAME') #check if the series name contains the string 'NAME'
        elif 'NAME' in id or 'NAME' in name:
            breakpoint()
            continue
        else:
            identifier = id
            header_x = header.copy()
            header_x['facets']['seriesId'] = [id]
            
        print('delay: {}'.format(delay))
        time.sleep(delay)
        
        ALL_DATA_QUERIED = False
        while not ALL_DATA_QUERIED:
            
            all_rows_, error_code, offset, ALL_DATA_QUERIED = query_all_applicable_data(data_source, key, header=header_x, PURPOSE='data',THROW_ERROR=True)
            if error_code == 'OVER_RATE_LIMIT':
                #add to delay
                delay += 60
                if delay >= MAX_DELAY:
                    print('Max delay {} reached. Progress saved.'.format(delay))
                    break
            else:
                delay = 0
                #save the data and add to completed_series_ids:
                pd.DataFrame(all_rows_).to_pickle(f'input_data/EIA/{subfolder}/series_id_data/{identifier}.pkl')
                all_rows += all_rows_ 
    return all_rows

def concat_saved_pickle_files(data_source):
    subfolder = data_source.replace("/", "_")
    directory = f'input_data/EIA/{subfolder}/series_id_data'

    all_data_frames = []

    for filename in os.listdir(directory):
        if filename.endswith('.pkl'):
            file_path = os.path.join(directory, filename)
            df = pd.read_pickle(file_path)
            all_data_frames.append(df)

    concatenated_df = pd.concat(all_data_frames, ignore_index=True)
    
    return concatenated_df

def extract_applicable_series(all_series_ids, APPLICABLE_SERIES_STRINGS):
    applicable_series = []
    for index, row in all_series_ids.iterrows():
        id = row['id']
        name = row['name']
        if (id is None) & (name is None):
            continue
        
        if 'tr' in id or any(x in id.lower() for x in APPLICABLE_SERIES_STRINGS) or 'tr' in name or any(x in name.lower() for x in APPLICABLE_SERIES_STRINGS):
            applicable_series.append([id, name])
    return applicable_series