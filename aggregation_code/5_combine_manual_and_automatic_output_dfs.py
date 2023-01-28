#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import time
import sys
import matplotlib.pyplot as plt

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

import data_selection_functions as data_selection_functions
PRINT_GRAPHS_AND_STATS = False


#%%
INDEX_COLS = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type','Drive','Fuel_Type','Frequency','Scope']
#Remove year from the current cols without removing it from original list, and set it as a new list
INDEX_COLS_no_year = INDEX_COLS.copy()
INDEX_COLS_no_year.remove('Date')

#%%
#load in the manual data selection df and automatic data selection df
file_date = datetime.datetime.now().strftime("%Y%m%d")
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/data_selection/', '_data_selection_manual.csv')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data_concordance_manual = pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID))#, index_col=INDEX_COLS)

file_date = datetime.datetime.now().strftime("%Y%m%d")
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/data_selection/', '_data_selection_automatic.csv')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data_concordance_automatic = pd.read_csv('intermediate_data/data_selection/{}_data_selection_automatic.csv'.format(FILE_DATE_ID), index_col=INDEX_COLS)
#%%
#COMBINE MANUAL AND AUTOMATIC DATA SELECTION OUTPUT DFs
#join the automatic and manual datasets so we can compare the dataset  columns from both the manual dataset automatic dataset
#create a final_dataset column. This will be filled with the dataset in the automatic column, except where the values in the manual and automatic dataset columns are different, for which we will use the value in the manual dataset.

#%%

#reset and set index of both dfs to INDEX_COLS
combined_data_concordance_manual = combined_data_concordance_manual.set_index(INDEX_COLS)
combined_data_concordance_automatic = combined_data_concordance_automatic.reset_index().set_index(INDEX_COLS)

#join manual and automatic data selection dfs
final_combined_data_concordance = combined_data_concordance_manual.merge(combined_data_concordance_automatic, how='outer', left_index=True, right_index=True, suffixes=('_manual', '_auto'))

#we will either have dataset names or nan values in the manual and automatic dataset columns. We want to use the manual dataset column if it is not nan, otherwise use the automatic dataset column:
#first set the dataset_selection_method column based on that criteria, and then use that to set other columns
final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_auto'].notnull(), 'Final_dataset_selection_method'] = 'Automatic'
#if the manual dataset column is not nan then use that instead
final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_manual'].notnull(), 'Final_dataset_selection_method'] = 'Manual'

#Now depending on the value of the final_dataset_selection_method column, we can set final_value and final_dataset columns
#if the final_dataset_selection_method is manual then use the manual dataset column
final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Manual', 'Value'] = final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Manual','Value_manual']
final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Manual', 'Dataset'] = final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Manual','Dataset_manual']
#if the final_dataset_selection_method is automatic then use the automatic dataset column
final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Automatic', 'Dataset'] = final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Automatic','Dataset_auto']
final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Automatic', 'Value'] = final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Automatic','Value_auto']

#%%
#drop cols ending in _manual and _auto
final_combined_data_concordance.drop(columns=[col for col in final_combined_data_concordance.columns if col.endswith('_manual') or col.endswith('_auto')], inplace=True)
#%%
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

#save the final_combined_data_concordance df to a csv
final_combined_data_concordance.to_csv('./output_data/{}_final_combined_data_concordance.csv'.format(FILE_DATE_ID), index=True)


#%%
