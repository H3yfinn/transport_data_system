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
FILE_DATE_ID = 'DATE20230408'#'DATE20230331'

paths_dict = utility_functions.setup_paths_dict(FILE_DATE_ID,INDEX_COLS, EARLIEST_DATE, LATEST_DATE,previous_FILE_DATE_ID)

df = pd.read_pickle(paths_dict['final_combined_data_not_rescaled'])
plotting=False
#%%
import logging
logger = logging.getLogger(__name__)

#%%s
#USE THIS FOR TESTING DURING DEBUGGPING BY LOADING IN SAVED PICKLES
# all_combined_data.to_pickle('x.pkl')
#load x.pkl
x = pd.read_pickle('x.pkl')

#find na's
x.isna().sum()
#find nones
x.isnull().sum()



#UNIT COL IS NA. SEE WHAT IS GOING ON
x[x['unit'].isna()]
# %%

combined_data_concordance = pd.read_pickle(paths_dict['previous_combined_data_concordance'])
combined_data = pd.read_pickle(paths_dict['previous_combined_data'])
