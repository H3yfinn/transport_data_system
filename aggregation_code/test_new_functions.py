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


previous_FILE_DATE_ID ='DATE20230410'#None
FILE_DATE_ID = 'DATE20230410'#'DATE20230331'

paths_dict = utility_functions.setup_paths_dict(FILE_DATE_ID,INDEX_COLS, EARLIEST_DATE, LATEST_DATE,previous_FILE_DATE_ID)

# df = pd.read_pickle(paths_dict['final_combined_data_not_rescaled'])
plotting=True
#%%
import logging
logger = logging.getLogger(__name__)


combined_data_concordance = pd.read_pickle(paths_dict['final_data_csv'])
# #%%
# #find duplicates when you subset for 'date', 'economy', 'measure', 'vehicle_type', 'unit', 'medium',
# #    'transport_type', 'drive', 'fuel', 'frequency', 'scope'
# duplicate_rows_df = combined_data_concordance[combined_data_concordance.duplicated(subset=['date', 'economy', 'measure', 'vehicle_type', 'unit', 'medium', 'transport_type', 'drive', 'fuel', 'frequency', 'scope'], keep=False)]
# #%%
# #and order so we can see the duplicates
# duplicate_rows_df = duplicate_rows_df.sort_values(by=['date', 'economy', 'measure', 'vehicle_type', 'unit', 'medium', 'transport_type', 'drive', 'fuel', 'frequency', 'scope'])
# # %%
# if duplicate_rows_df.shape[0] > 0:
#     print('There are {} duplicate rows in the combined data concordance'.format(duplicate_rows_df.shape[0]))
#     print(duplicate_rows_df)
# # %%

#sav as csv
combined_data_concordance.to_csv(paths_dict['final_data_csv'])