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

import EIA_API_functions as EIA
key = 'tCnavZWYLeKCs7nKHvKVGpnAqMKXIXoXVoxa0GXF'
data_source = 'aeo/2023'
#%%

################################################################################
#START OF CODE
################################################################################
#%%

# data_source_additions = 
# data_source += data_source_additions
# all_rows_df_prev, offset = EIA.load_previous_brute_forced_series_ids(data_source)
EIA.setup_for_datasource(data_source)
#%%
all_series_ids = EIA.brute_force_scrape_all_for_data_source(data_source, key, data_source_addition='/facet/seriesId')
#%%

#now we have all the rows, we can search for the series we want:
applicable_series = []
APPLICABLE_SERIES_STRINGS=['transport','vehicle','travel', 'air', 'ship', 'road']

applicable_series = EIA.extract_applicable_series(all_series_ids, APPLICABLE_SERIES_STRINGS)

#%%
def remove_already_downloaded_series_ids_from_applicable_series(applicable_series, data_source):
    #extract series we've already got data for from the folder input_data/EIA/ {data_source.replace("/", "_")}/series_id_data. #remove these from the list of applicable series
    applicable_series_ids = [entry[0] for entry in applicable_series]
    series_ids_to_remove = []
    for file_name in os.listdir(f'input_data/EIA/{data_source.replace("/", "_")}/series_id_data'):
        series_id = file_name.strip('.pkl')
        if series_id in applicable_series_ids:
            series_ids_to_remove += [series_id]
    applicable_series = [entry for entry in applicable_series if entry[0] not in series_ids_to_remove]
    
    return applicable_series

applicable_series = remove_already_downloaded_series_ids_from_applicable_series(applicable_series, data_source)

#%%
#now we have the series we want, we can extract the data for each series:
     
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

QUERY_API_USING_SERIES_ID = True
if QUERY_API_USING_SERIES_ID:   
    bad_series_ids, completed_series_ids, all_rows = EIA.run_query_by_series_id(data_source, key, applicable_series, header)
    
    previous_data = EIA.concat_saved_pickle_files(data_source)
    #add concatenated_data to all_rows
    concatenated_data = pd.concat( [previous_data, pd.DataFrame(all_rows)] )
    
    concatenated_data.to_pickle(f'input_data/EIA/all_scraped_data_{data_source.replace("/", "_")}.pkl')
else:
    concatenated_data = pd.read_pickle(f'input_data/EIA/all_scraped_data_{data_source.replace("/", "_")}.pkl')
    # completed_series_ids = concatenated_data['seriesId'].unique().tolist()
    # bad_series_ids = []
#%%