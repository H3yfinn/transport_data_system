#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import sys
import re
import data_selection_functions as data_selection_functions
import utility_functions as utility_functions
import matplotlib.pyplot as plt

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')
sys.path.append('./aggregation_code')
#%%
#create code to run the baove functions
INDEX_COLS = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium','Transport Type','Drive','Fuel_Type','Frequency', 'Scope']

#MANUAL DATA SELECTION VARIABLES
pick_up_where_left_off = True
import_previous_selection = True
run_only_on_rows_to_select_manually = False

#load data
# FILE_DATE_ID = 'DATE20221205'
use_all_data = False
use_9th_edition_set =True 
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
FILE_DATE_ID = FILE_DATE_ID = 'DATE' + str(datetime.datetime.now().strftime('%Y%m%d'))

#%%
duplicates = data_selection_functions.identify_duplicates(combined_dataset, INDEX_COLS)

combined_data_concordance_automatic,combined_data_concordance_manual,combined_data_concordance_iterator,duplicates_auto,duplicates_auto_with_year_index,combined_data_automatic,duplicates_manual,combined_data = data_selection_functions.prepare_data_for_selection(combined_data_concordance,combined_dataset,duplicates,INDEX_COLS,EARLIEST_YEAR = "2010-01-01",    LATEST_YEAR = '2023-01-01')
#%%
run_automatic =True
if run_automatic:
    combined_data_concordance_automatic, rows_to_select_manually_df = data_selection_functions.automatic_selection(combined_data_concordance_automatic,combined_data_automatic,duplicates_auto,duplicates_auto_with_year_index,INDEX_COLS, datasets_to_always_choose=[])

#%%
#load combined_data_concordance_manual now so we can use it later if we need to
previous_combined_data_concordance_manual = pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID))
previous_duplicates_manual = pd.read_csv('intermediate_data/data_selection/{}_duplicates_manual.csv'.format(FILE_DATE_ID))
progress_csv = pd.read_csv('intermediate_data/data_selection/{}_progress.csv'.format(FILE_DATE_ID))
#%%
duplicates_manual = duplicates_manual.reset_index()
combined_data_concordance_manual = combined_data_concordance_manual.reset_index()
#%%

#########################SET ME TO SET VARIABLES FOR FUNCTION
pick_up_where_left_off=False
import_previous_selection=False
run_only_on_rows_to_select_manually=True
manually_chosen_rows_to_select=None
user_edited_combined_data_conc1ordance_iterator=None
previous_combined_data_concordance_manual= None#previous_combined_data_concordance_manual
duplicates_manual=duplicates_manual
previous_duplicates_manual=None#previous_duplicates_manual
progress_csv=None#progress_csv
#########################
#%%
iterator, combined_data_concordance_manual = data_selection_functions.create_manual_data_iterator(combined_data_concordance_iterator,
INDEX_COLS,combined_data_concordance_manual,
rows_to_select_manually_df,
pick_up_where_left_off, 
import_previous_selection,run_only_on_rows_to_select_manually,
manually_chosen_rows_to_select,
user_edited_combined_data_concordance_iterator,
previous_combined_data_concordance_manual, 
duplicates_manual,
previous_duplicates_manual, progress_csv)

#%%
combined_data_concordance_manual, duplicates_manual, bad_index_rows, num_bad_index_rows = data_selection_functions.select_best_data_manual(combined_data_concordance_iterator,iterator,combined_data_concordance_manual,combined_data,duplicates_manual,INDEX_COLS,FILE_DATE_ID=FILE_DATE_ID)
1#%%
final_combined_data_concordance = data_selection_functions.combine_manual_and_automatic_output(combined_data_concordance_automatic,combined_data_concordance_manual,INDEX_COLS)
#%%
#do interpolation:
new_final_combined_data,final_combined_data_concordance = data_selection_functions.interpolate_missing_values(final_combined_data_concordance,INDEX_COLS,automatic_interpolation_method = 'linear', automatic_interpolation = True,FILE_DATE_ID=FILE_DATE_ID,percent_of_values_needed_to_interpolate=0.7)

if use_all_data:
    final_combined_data_concordance.to_csv('output_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID), index=False)
    new_final_combined_data.to_csv('output_data/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)
elif use_9th_edition_set:
    final_combined_data_concordance.to_csv('output_data/9th_dataset/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID), index=False)
    new_final_combined_data.to_csv('output_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)
#%%

