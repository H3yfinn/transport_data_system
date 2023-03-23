
#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import sys
from PIL import Image


#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

import data_formatting_functions as data_formatting_functions
import utility_functions as utility_functions
import data_selection_functions_test as data_selection_functions

create_9th_model_dataset = True

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
#%%
paths_dict = data_formatting_functions.setup_dataselection_process(FILE_DATE_ID,INDEX_COLS, EARLIEST_DATE, LATEST_DATE)

datasets_transport, datasets_other = data_formatting_functions.extract_latest_groomed_data()
#for now wont do anmything with datasets_other

#%%

import new_selection_functions


# def user_input_remove_value_from_selections_permanently(selected_dataset, data_to_select_from, data_to_select_from_for_this_year, paths_dict):
#     pass



# combined_data_concordance.to_pickle('combined_data_concordance.pkl')
# combined_data.to_pickle('combined_data.pkl')
#%%
#laod data from pickle
combined_data_concordance = pd.read_pickle('combined_data_concordance.pkl')
combined_data = pd.read_pickle('combined_data.pkl')  
grouping_cols = ['economy','vehicle_type','drive']
combined_data_concordance = new_selection_functions.manual_data_selection_handler(grouping_cols, combined_data_concordance, combined_data, paths_dict)
################################################
################################################

