#%%

import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import sys
import matplotlib.pyplot as plt

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')
sys.path.append('./aggregation_code')
# sys.path.append('../../')

import data_selection_functions as data_selection_functions
import data_formatting_functions as data_formatting_functions
import utility_functions as utility_functions

#%%
#create code to run the baove functions. If you add more columns to the data then you need to make sure they are added to the list below
INDEX_COLS = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium','Transport Type','Drive','Fuel_Type','Frequency', 'Scope']

#MANUAL DATA SELECTION VARIABLES
pick_up_where_left_off = True
import_previous_selection = True
run_only_on_rows_to_select_manually = False

#load data
# FILE_DATE_ID = 'DATE20221205'
use_all_data = False#False
use_9th_edition_set =True#False#True 
#%%
if use_all_data:
    #run aggreagtion code file
    exec(open("./aggregation_code/1_aggregate_cleaned_datasets.py").read())
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    combined_dataset = pd.read_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))
    combined_data_concordance= pd.read_csv('intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
elif use_9th_edition_set:
    exec(open("./aggregation_code/1_aggregate_cleaned_dataset_9th_edition.py").read())
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/9th_dataset/', 'combined_dataset_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    combined_dataset = pd.read_csv('intermediate_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID))
    combined_data_concordance= pd.read_csv('intermediate_data/9th_dataset/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
else:
    print('Please set use_all_data or use_9th_edition_set to True')

##############################################################################
#%%
FILE_DATE_ID = 'DATE' + str(datetime.datetime.now().strftime('%Y%m%d'))


#%%
# #save datafiles from above in pickle so we can use them later
# import pickle
# combined_dataset.to_pickle('intermediate_data/testing/combined_dataset_{}.pkl'.format(FILE_DATE_ID))
# combined_data_concordance.to_pickle('intermediate_data/testing/combined_dataset_concordance_{}.pkl'.format(FILE_DATE_ID))
# # #laod them
# combined_dataset = pd.read_pickle('intermediate_data/testing/combined_dataset_{}.pkl'.format(FILE_DATE_ID))
# combined_data_concordance = pd.read_pickle('intermediate_data/testing/combined_dataset_concordance_{}.pkl'.format(FILE_DATE_ID))
#%%

#%%
EARLIEST_YEAR = "2010-01-01"
LATEST_YEAR = '2023-01-01'
#%%
combined_data_concordance_automatic,combined_data_concordance_manual,combined_data_concordance_iterator,duplicates_auto,duplicates_auto_with_year_index,combined_data_automatic,duplicates_manual,combined_data = data_formatting_functions.prepare_data_for_selection(combined_data_concordance,combined_dataset,INDEX_COLS,EARLIEST_YEAR, LATEST_YEAR)
#%%
run_automatic =False
if run_automatic:
    #i think that if we update this repo so that the manual data selection process is smoother then we can ignore the automatic selection process. So for now we will turn it off.
    combined_data_concordance_automatic, rows_to_select_manually_df = data_selection_functions.automatic_selection(combined_data_concordance_automatic,combined_data_automatic,duplicates_auto,duplicates_auto_with_year_index,INDEX_COLS, datasets_to_always_choose=[])
else:
    rows_to_select_manually_df = None

#%%
duplicates_manual = duplicates_manual.reset_index()
combined_data_concordance_manual = combined_data_concordance_manual.reset_index()
#%%

#########################SET ME TO SET VARIABLES FOR FUNCTION
pick_up_where_left_off=False#this is for if there is an error while running. If you exit out properly then you should use the import_previous_selection variable
#please note i havent tested yet because i ahvent had the chance
import_previous_manual_concordance=True

run_only_on_rows_to_select_manually=True
manually_chosen_rows_to_select=None
user_edited_combined_data_concordance_iterator=None
update_skipped_rows =False# True
consider_value_in_duplicates = True
#LEAVE THESE AS N0NE, THEY WILL GET SET BASED ON THE ABOVE VARIABLES
previous_combined_data_concordance_manual=None
previous_duplicates_manual=None
progress_csv=None

if import_previous_manual_concordance:
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/data_selection/', '_data_selection_manual')
    FILE_DATE_ID2 = 'DATE{}'.format(file_date)
    #WARNING THERES POTENTIALLY AN ISSUE WHEN YOU UPDATE THE INPUT DATA SO IT INCLUDES ANOTHER DATAPOINT AND YOU LOAD THIS IN, THE MANUAL CONCORDANCE WILL END UP AHVING TWO ROWS FOR THE SAME DATAPOINT? #cHECK IT LATER
    previous_selections = pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID2))
    # previous_selections = pd.read_csv('intermediate_data/data_selection/DATE20230214_data_selection_manual - Copy (2).csv')
elif pick_up_where_left_off:
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/data_selection/', '_duplicates_manual')
    FILE_DATE_ID2 = 'DATE{}'.format(file_date)

    previous_selections = pd.read_csv('intermediate_data/data_selection/{}_progress.csv'.format(FILE_DATE_ID2))
else:
    previous_selections = None
if pick_up_where_left_off or import_previous_selection:
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/data_selection/', '_duplicates_manual')
    FILE_DATE_ID2 = 'DATE{}'.format(file_date)
    #load duplicates_manual now so we can use it later if we need to
    previous_duplicates_manual = pd.read_csv('intermediate_data/data_selection/{}_duplicates_manual.csv'.format(FILE_DATE_ID2))

    if not consider_value_in_duplicates:
        #if consider_value_in_duplicates is set to True here then keep the value col in the duplicates_manual df, else drop it
        previous_duplicates_manual = previous_duplicates_manual.drop(columns=['Value'])
        duplicates_manual = duplicates_manual.drop(columns=['Value'])

#%%
iterator, combined_data_concordance_manual = data_formatting_functions.create_manual_data_iterator(
combined_data_concordance_iterator,
INDEX_COLS,
combined_data_concordance_manual,
duplicates_manual,
rows_to_select_manually_df,
run_only_on_rows_to_select_manually,
manually_chosen_rows_to_select,
user_edited_combined_data_concordance_iterator,
previous_selections,
previous_duplicates_manual,
update_skipped_rows)#

print('There are {} rows in the iterator'.format(len(iterator)))

#%%
combined_data_concordance_manual, duplicates_manual, bad_index_rows, num_bad_index_rows = data_selection_functions.select_best_data_manual(combined_data_concordance_iterator,iterator,combined_data_concordance_manual,combined_data,duplicates_manual,INDEX_COLS,FILE_DATE_ID=FILE_DATE_ID)#
#%%
final_combined_data_concordance = data_formatting_functions.combine_manual_and_automatic_output(combined_data_concordance_automatic,combined_data_concordance_manual,INDEX_COLS)

#%%
#do interpolation:
# FILE_DATE_ID = FILE_DATE_ID
load_progress = False
automatic_interpolation = True
automatic_interpolation_method = 'linear'
percent_of_values_needed_to_interpolate=0.7
load_progress=load_progress
INTERPOLATION_LIMIT=3

#%%
new_final_combined_data,final_combined_data_concordance = data_selection_functions.interpolate_missing_values(final_combined_data_concordance,INDEX_COLS,automatic_interpolation_method, automatic_interpolation,FILE_DATE_ID,percent_of_values_needed_to_interpolate, INTERPOLATION_LIMIT,load_progress)

if use_all_data:
    final_combined_data_concordance.to_csv('output_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID), index=False)
    new_final_combined_data.to_csv('o1utput_data/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)
elif use_9th_edition_set:
    final_combined_data_concordance.to_csv('output_data/9th_dataset/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID), index=False)
    new_final_combined_data.to_csv('output_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)
#%%


