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
import pickle
import data_selection_functions as data_selection_functions
#%%
#import data
combined_data_concordance_automatic = pd.read_pickle('intermediate_data/data_selection/combined_data_concordance_automatic.pkl')
combined_data_automatic = pd.read_pickle('intermediate_data/data_selection/combined_data_automatic.pkl')
duplicates_auto = pd.read_pickle('intermediate_data/data_selection/duplicates_auto.pkl')
duplicates_auto_with_year_index = pd.read_pickle('intermediate_data/data_selection/duplicates_auto_with_year_index.pkl')
#%%
INDEX_COLS = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type','Drive','Fuel_Type','Frequency','Scope']
#Remove year from the current cols without removing it from original list, and set it as a new list
INDEX_COLS_no_year = INDEX_COLS.copy()
INDEX_COLS_no_year.remove('Date')
#%%

#STEP 2
#AUTOMATIC METHOD

datasets_to_always_choose = []#["ATO $ Country Official Statistics"]#I DONT THINK THIS IS WORKING TBH
checkpoints_1 = []
checkpoints_2 = []

#start a timer to see how long it takes to run the automatic method
start_time = time.time()
#run the automatic method, one measure at a time
for measure in combined_data_concordance_automatic.index.get_level_values('Measure').unique():
       #filter the combined_data_concordance_automatic df to only include the current measure
       combined_data_concordance_automatic_measure = combined_data_concordance_automatic[combined_data_concordance_automatic.index.get_level_values('Measure')==measure]
       combined_data_automatic_measure = combined_data_automatic [combined_data_automatic.index.get_level_values('Measure')==measure]
       duplicates_auto_measure = duplicates_auto[duplicates_auto.index.get_level_values('Measure')==measure]
       duplicates_auto_with_year_index_measure = duplicates_auto_with_year_index[duplicates_auto_with_year_index.index.get_level_values('Measure')==measure]

       print('Measure: {}'.format(measure))
       print('Number of rows: {}'.format(len(combined_data_concordance_automatic_measure)))
       print('Time taken so far: {} seconds'.format(time.time()-start_time))
       print('\n\n')
       
       #RUN THE AUTOMATIC METHOD
       combined_data_concordance_automatic_measure, rows_to_select_manually_measure = data_selection_functions.automatic_method(combined_data_automatic_measure, combined_data_concordance_automatic_measure,duplicates_auto_measure,duplicates_auto_with_year_index_measure,datasets_to_always_choose,std_out_file = 'intermediate_data/data_selection/automatic_method.txt')
       #save the data to a csv
       filename = 'intermediate_data/data_selection/checkpoints/{}_combined_data_concordance_automatic.csv'.format(measure)
       combined_data_concordance_automatic_measure.to_csv(filename, index=True)
       checkpoints_1.append(filename)
       filename = 'intermediate_data/data_selection/checkpoints/rows_to_select_manually_{}.csv'.format(measure)
       rows_to_select_manually_measure_df = pd.DataFrame(rows_to_select_manually_measure, columns=INDEX_COLS_no_year)
       #remove duplicates from rows_to_select_manually_measure_df
       rows_to_select_manually_measure_df = rows_to_select_manually_measure_df.drop_duplicates()
       rows_to_select_manually_measure_df.to_csv(filename, index=False)
       checkpoints_2.append(filename)

#take in all the checkpoints and combine them into one df
combined_data_concordance_automatic = pd.concat([pd.read_csv(checkpoint, index_col=INDEX_COLS) for checkpoint in checkpoints_1])
rows_to_select_manually_df = pd.concat([pd.read_csv(checkpoint) for checkpoint in checkpoints_2])
#%%
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
#save
combined_data_concordance_automatic.to_csv('intermediate_data/data_selection/{}_data_selection_automatic.csv'.format(FILE_DATE_ID), index=True)
rows_to_select_manually_df.to_csv('intermediate_data/data_selection/rows_to_select_manually.csv', index=False)
       
#%%