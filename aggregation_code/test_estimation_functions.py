#load transport_data_system/intermediate_data/selection_process/DATE20230328/interpolated_stocks_mileage_occupancy_efficiency_combined_data_concordance.pkl then see where we dont have data
#%%
import pandas as pd
import numpy as np
import os
import re
import data_formatting_functions
import data_estimation_functions
import utility_functions
import datetime
import analysis_and_plotting_functions
#set dir one back
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

INDEX_COLS = ['date',
 'economy',
 'measure',
 'vehicle_type',
 'unit',
 'medium',
 'transport_type',
 'drive',
 'fuel',
 'frequency',
 'scope']

EARLIEST_DATE="2010-01-01"
LATEST_DATE='2023-01-01'


previous_FILE_DATE_ID ='DATE20230329'
FILE_DATE_ID = 'DATE20230330'

paths_dict = utility_functions.setup_paths_dict(FILE_DATE_ID,INDEX_COLS, EARLIEST_DATE, LATEST_DATE,previous_FILE_DATE_ID)

unfiltered_combined_data = pd.read_pickle(paths_dict['unfiltered_combined_data'])

road_combined_data = pd.read_pickle(paths_dict['intermediate_folder']+'/road_combined_data_TEST.pkl')

non_road_energy = pd.read_pickle(paths_dict['intermediate_folder']+'/non_road_energy.pkl')
#%%
import logging
logger = logging.getLogger(__name__)

#%%

#%% 
#get energy from egeda and either:
#	1- use the values from egeda and scale them according to how large the total road energy is compared to what it is for egeda (so the same proportion of enegry is used for non road as it is in egeda)
#    2- use the values from egeda but make it the remainder of the total road energy use (so scale between non-road energy uses are the same but the proportion comapred to total road energy use is different)

#%%
#%%
