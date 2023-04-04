#use this to test new functions by loading presets that are done in main.py
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
import plotly.express as px
import plotly.graph_objects as go
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


previous_FILE_DATE_ID =None
FILE_DATE_ID = 'DATE20230403'#'DATE20230331'

paths_dict = utility_functions.setup_paths_dict(FILE_DATE_ID,INDEX_COLS, EARLIEST_DATE, LATEST_DATE,previous_FILE_DATE_ID)

unfiltered_combined_data = pd.read_pickle(paths_dict['unfiltered_combined_data'])

road_combined_data = pd.read_pickle(paths_dict['intermediate_folder']+'/road_combined_data_TEST.pkl')

all_new_combined_data = pd.read_pickle('./intermediate_data/selection_process/DATE20230331/all_new_combined_data_TEST.pkl')#paths_dict['intermediate_folder']+'/all_new_combined_data_TEST.pkl'

stocks_mileage_occupancy_efficiency_combined_data_concordance = pd.read_pickle(paths_dict['stocks_mileage_occupancy_efficiency_combined_data_concordance'])
stocks_mileage_occupancy_efficiency_combined_data_concordance_int = pd.read_pickle(paths_dict['interpolated_stocks_mileage_occupancy_efficiency_combined_data_concordance'])

stocks_mileage_occupancy_efficiency_passenger_energy_combined_data = pd.read_pickle(paths_dict['calculated_passenger_energy_combined_data'])


plotting=False
#%%
import logging
logger = logging.getLogger(__name__)

#%%s

a = pd.read_pickle(paths_dict['energy_passenger_km_combined_data_concordance'])
a = a.loc[a.measure == 'energy']
a.date.unique()

