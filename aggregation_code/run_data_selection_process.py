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
duplicates = data_formatting_functions.identify_duplicates(combined_dataset, INDEX_COLS)
#%%
EARLIEST_YEAR = "2010-01-01"
LATEST_YEAR = '2023-01-01'
#%%
combined_data_concordance_automatic,combined_data_concordance_manual,combined_data_concordance_iterator,duplicates_auto,duplicates_auto_with_year_index,combined_data_automatic,duplicates_manual,combined_data = data_formatting_functions.prepare_data_for_selection(combined_data_concordance,combined_dataset,duplicates,INDEX_COLS,EARLIEST_YEAR, LATEST_YEAR)
#%%
run_automatic =True
if run_automatic:
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






























# ##################################

# #START MANUAL DATA SELECTION

# ##################################
# INDEX_COLS_no_year = INDEX_COLS.copy()
# INDEX_COLS_no_year.remove('Date')
# #loop through the unique combinations, plot a timeseries for each one and then ask the user what dataset they want to choose for that instance and then update the dataset column in combined_data for that row to be the chosen dataset
# # %matplotlib qt 
# import matplotlib
# matplotlib.use('TkAgg')

# combined_data_concordance_manual.set_index(INDEX_COLS_no_year, inplace=True)
# duplicates_manual.set_index(INDEX_COLS_no_year, inplace=True)

# # %matplotlib qt
# #TEMP FIX START
# #NOTE MAKING THIS ONLY WORK FOR YEARLY DATA, AS IT WOULD BE COMPLICATED TO DO IT OTEHRWISE. lATER ON WE CAN TO IT COMPLETELY BY CHANGING LINES THAT ADD 1 TO THE DATE IN THE DATA SLECTION FUNCTIONS TO ADD ONE UNIT OF WHATEVER THE FREQUENCY IS. 
# #change dataframes date col to be just the year in that date eg. 2022-12-31 = 2022 by using string slicing
# combined_data_concordance_manual.Date = combined_data_concordance_manual.Date.apply(lambda x: x[:4]).astype(int)
# combined_data.Date = combined_data.Date.apply(lambda x: x[:4]).astype(int)
# duplicates_manual.Date = duplicates_manual.Date.apply(lambda x: x[:4]).astype(int)#todo CHECK WHY WE ARE GETTING FLOAT YEARS

# #order data by date
# combined_data_concordance_manual = combined_data_concordance_manual.sort_values(by='Date')
# combined_data = combined_data.sort_values(by='Date')
# duplicates_manual = duplicates_manual.sort_values(by='Date')
# #TEMP FIX END
# #Create bad_index_rows as a empty df with the same columns as index_rows
# bad_index_rows = pd.DataFrame(columns=combined_data_concordance_iterator.columns)
# num_bad_index_rows = 0
# #create progresss csv so we can add lines to it as we go
# progress_csv = 'intermediate_data/data_selection/{}_progress.csv'.format(FILE_DATE_ID)
# #%%
# for index_row in iterator.index:
#         print('x')
#         if 'Yearly' not in index_row:
#                 print('Skipping row {} as it is not yearly data'.format(index_row))
#                 continue



# #%%




































# copy_x = final_combined_data_concordance.copy()
# duplicates = copy_x[copy_x.duplicated(keep=False)]
# #%%





# #%%
# # a = combined_data_concordance_automatic.copy()
# # b = combined_data_concordance_manual.copy()
# combined_data_concordance_automatic = a.copy()
# combined_data_concordance_manual = b.copy()

# INDEX_COLS_no_year = INDEX_COLS.copy()
# INDEX_COLS_no_year.remove('Date')

# #COMBINE MANUAL AND AUTOMATIC DATA SELECTION OUTPUT DFs
# #join the automatic and manual datasets so we can compare the dataset  columns from both the manual dataset automatic dataset
# #create a final_dataset column. This will be filled with the dataset in the automatic column, except where the values in the manual and automatic dataset columns are different, for which we will use the value in the manual dataset.

# #reset and set index of both dfs to INDEX_COLS
# combined_data_concordance_manual = combined_data_concordance_manual.set_index(INDEX_COLS)
# combined_data_concordance_automatic = combined_data_concordance_automatic.reset_index().set_index(INDEX_COLS)

# #remove the Datasets and Dataset_selection_method columns from the manual df
# combined_data_concordance_manual.drop(columns=['Datasets','Dataset_selection_method'], inplace=True)
# #join manual and automatic data selection dfs
# final_combined_data_concordance = combined_data_concordance_manual.merge(combined_data_concordance_automatic, how='outer', left_index=True, right_index=True, suffixes=('_manual', '_auto'))
# #%%
# #we will either have dataset names or nan values in the manual and automatic dataset columns. We want to use the manual dataset column if it is not nan, otherwise use the automatic dataset column:
# #first set the dataset_selection_method column based on that criteria, and then use that to set other columns
# final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_auto'].notnull(), 'Dataset_selection_method'] = 'Automatic'
# #if the manual dataset column is not nan then use that instead
# final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_manual'].notnull(), 'Dataset_selection_method'] = 'Manual'
# #%%
# #filter for only where Dataset_selection_method is manual
# manual_selections = final_combined_data_concordance[final_combined_data_concordance['Dataset_selection_method'] == 'Manual']
# manual_selections.reset_index(inplace=True)

# combined_data_concordance_manual = combined_data_concordance_manual.reset_index()
# #%%
# #Now depending on the value of the Dataset_selection_method column, we can set final_value and final_dataset columns
# #if the Dataset_selection_method is manual then use the manual dataset column
# final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Manual', 'Value'] = final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Manual','Value_manual']

# final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Manual', 'Dataset'] = final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Manual','Dataset_manual']
# #if the Dataset_selection_method is automatic then use the automatic dataset column
# #%%
# final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Automatic', 'Dataset'] = final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Automatic','Dataset_auto']

# final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Automatic', 'Value'] = final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Automatic','Value_auto']
# #%%
# #drop cols ending in _manual and _auto
# final_combined_data_concordance.drop(columns=[col for col in final_combined_data_concordance.columns if col.endswith('_manual') or col.endswith('_auto')], inplace=True)

# c = final_combined_data_concordance.reset_index()
# #%%






# """
# manually_chosen_rows_to_select: set to true if you want to manually choose the rows to select using user_edited_combined_data_concordance_iterator
# user_edited_combined_data_concordance_iterator: a manually chosen dataframe with the rows to select. This allows user to define what they want to select manually (eg. all stocks)

# duplicates_manual & previous_duplicates_manual need to be available if you want to use either pick_up_where_left_off or import_previous_selection. progress_csv should also be available if you want to use pick_up_where_left_off
# """
# #Remove year from the current cols without removing it from original list, and set it as a new list
# INDEX_COLS_no_year = INDEX_COLS.copy()
# INDEX_COLS_no_year.remove('Date')

# #CREATE ITERATOR 
# #if we want to add to the rows_to_select_manually_df to check specific rows then set the below to True
# if run_only_on_rows_to_select_manually:
#     #if this, only run the manual process on index rows where we couldnt find a dataset to use automatically for some year
#     #since the automatic method is relatively strict there should be a large amount of rows to select manually
#     #note that if any one year cannot be chosen automatically then we will have to choose the dataset manually for all years of that row
#     iterator = rows_to_select_manually_df.copy()
#     iterator.set_index(INDEX_COLS_no_year, inplace=True)
#     iterator.drop_duplicates(inplace=True)#TEMP get rid of this later
# elif manually_chosen_rows_to_select:
#     #we can add rows form the combined_data_concordance_iterator as edited by the user themselves. 
#     iterator = user_edited_combined_data_concordance_iterator.copy()
#     #since user changed the data we will jsut reset index and set again
#     iterator.reset_index(inplace=True)
#     iterator.set_index(INDEX_COLS_no_year, inplace=True)

#     #for this example we will add all Stocks data (for the purpoose of betterunderstanding our stocks data!) and remove all the other data. But this is just an example of what the user could do to select specific rows
#     use_example = False
#     if use_example:
#         iterator.reset_index(inplace=True)
#         iterator = iterator[iterator['Measure']=='Stocks']
#         #set the index to the index cols
#         iterator.set_index(INDEX_COLS_no_year, inplace=True)
# else:
#     iterator = combined_data_concordance_iterator.copy()
# #%%
# #now determine whether we want to import previous progress or not:
# if previous_selections is not None:
#     # combined_data_concordance_manual,iterator = import_previous_selections(previous_selections, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS,update_skipped_rows)
    
#     #IMPORT PREVIOUS SELECTIONS
#     #Previous_selections can be the previous_combined_data_concordance_manual or the progress_csv depending on if the user wants to import completed manual selections or the progress of some manual selections
   
#     #This allows the user to import manual data selections from perveious runs to avoid having to do it again (can replace any rows where the Dataset_selection_method is na with where they are Manual in the imported csv)
#     INDEX_COLS_no_year = INDEX_COLS.copy()
#     INDEX_COLS_no_year.remove('Date')
#     ##########################################################
#     #We will make sure there are no index rows in the previous dataframes that are not in the current dataframes
#     #first the duplicates
#     previous_duplicates_manual.set_index(INDEX_COLS, inplace=True)
#     duplicates_manual.set_index(INDEX_COLS, inplace=True)
#     #remove the rows that are in the previous duplicates but not in the current duplicates
#     index_diff = previous_duplicates_manual.index.difference(duplicates_manual.index)
#     previous_duplicates_manual.drop(index_diff, inplace=True)
#     #reset the index
#     previous_duplicates_manual.reset_index(inplace=True)
#     duplicates_manual.reset_index(inplace=True)

#     #now for previous_selections and combined_data_concordance_manual
#     previous_selections.set_index(INDEX_COLS, inplace=True)
#     combined_data_concordance_manual.set_index(INDEX_COLS, inplace=True)
#     #remove the rows that are in the previous duplicates but not in the current duplicates
#     index_diff = previous_selections.index.difference(combined_data_concordance_manual.index)
#     previous_selections.drop(index_diff, inplace=True)
#     #reset the index
#     previous_selections.reset_index(inplace=True)
#     combined_data_concordance_manual.reset_index(inplace=True)
#     ##########################################################

#     ##There is a chance that some index_rows have had new data added to them, so we will compare the previous duplicates index_rows to the current duplicates and see where thier values are different, make sure that we iterate over them in the manual data selection process
#     #so find different rows in the duplicates:
#     #first make the Datasets col a string so it can be compared
#     a = previous_duplicates_manual.copy()
#     a.Datasets = a.Datasets.astype(str)
#     #set Vlaue to int, because we want to see if the value is the same or not and if float there might be floating point errors (computer science thing)
#     a.Value = a.Value.astype(int)
#     b = duplicates_manual.copy()
#     b.Datasets = b.Datasets.astype(str)
#     b.Value = b.Value.astype(int)
#     # a.set_index(INDEX_COLS_no_year,inplace=True)
#     # b.set_index(INDEX_COLS_no_year,inplace=True)
#     duplicates_diff = pd.concat([b, a])
#     #drop duplicates so we onlyu have the rows that are different, which we will want the suer tyo select for
#     duplicates_diff = duplicates_diff.drop_duplicates(keep=False)#'last')
#     #now we may have some rows with just one different column. Because we will be adding these to the combined data concordance we will remove the rows that are the same as the rows in previous_duplicates_manual, leaving only the new rows.
#     duplicates_diff = duplicates_diff[~duplicates_diff.index.isin(a.index)]
#     # ##########################################################

#     # previous_selections_old = previous_selections[~previous_selections.duplicated(subset=['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
#     #    'Transport Type', 'Drive', 'Fuel_Type', 'Frequency', 'Scope', 'Fuel',
#     #    'Dataset', 'Value', 'Dataset_selection_method', 'Comments'], keep=False)]
    
#     # aa = previous_selections[previous_selections.duplicated(subset=['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
#     #    'Transport Type', 'Drive', 'Fuel_Type', 'Frequency', 'Scope', 'Fuel',
#     #    'Dataset', 'Value', 'Dataset_selection_method', 'Comments'],keep=False)]
#     # aaa = aa[aa.Num_datapoints!=1.0]

#     # previous_selections_new = pd.concat([previous_selections_old,aaa])
#     #%%
#     ##Update the iterator: We will remove rows where the user doesnt need to select a dataset. This will be done using the previous combined_data_concordance_manual.
#     rows_to_skip = previous_selections.copy()
#     rows_to_skip.set_index(INDEX_COLS_no_year, inplace=True)
#     #we have to state wat rows we want to remove rather than keep because there will be some in the iterator that are not in the previous_selections df, and we will want to keep them.

#     #First remove rows that are in duplicates diff as we want to make sure the user selects for them since they have some detail which is different to what it was before. We do this using index_no_year to cover all the rows that have the same index but different years
#     duplicates_diff.set_index(INDEX_COLS_no_year, inplace=True)
#     rows_to_skip = rows_to_skip[~rows_to_skip.index.isin(duplicates_diff.index)]

#     #In case we are using the progress csv we will only remove rows where all years data has been selected for that index row. This will cover the way prvious_combined_data_concordance_manual df is formatted as well 
#     #first remove where num_datapoints is na or 0. 
#     rows_to_skip = rows_to_skip[~((rows_to_skip.Num_datapoints.isna()) | (rows_to_skip.Num_datapoints==0))]
#     #now find rows where there is data but the user hasnt selected a dataset for it. We will remove these rows from rows_to_skip, as we want to make sure the user selects for them (in the future this may cuase issues if we add more selection methods to the previous selections) #note this currently only occurs in the case where an error occurs during sleection
#     rows_to_remove_from_rows_to_skip = rows_to_skip[rows_to_skip.Dataset_selection_method.isna()]

#     if update_skipped_rows:
#         #if we are updating the skipped rows then we will also remove from rows to skip the rows where the Value and Dataset is NaN where the selection method is manual. This will be the case where the user has skipped selection so the selection emthod is manual but there are no values or datasets selected
#         rows_to_remove_from_rows_to_skip2 = rows_to_skip[(rows_to_skip.Value.isna()) & (rows_to_skip.Dataset.isna()) & (rows_to_skip.Dataset_selection_method=='Manual')]

#     #now remove those rows from rows_to_skip using index_no_year. This will remove any cases where an index row has one or two datapoints that werent chosen, but the rest were.
#     rows_to_skip = rows_to_skip[~rows_to_skip.index.isin(rows_to_remove_from_rows_to_skip.index)]

#     if update_skipped_rows:
#         rows_to_skip = rows_to_skip[~rows_to_skip.index.isin(rows_to_remove_from_rows_to_skip2.index)]

#     #KEEP only rows we dont want to skip in the iterator by finding the index rows in both dfs 
#     iterator = iterator[~iterator.index.isin(rows_to_skip.index)]
#     #%%
#     #copy data
#     duplicates_diff_copy = duplicates_diff.copy()
#     rows_to_skip_copy = rows_to_skip.copy()
#     combined_data_concordance_manual_copy = combined_data_concordance_manual.copy()
#     ###############
#     ##And now update the combined_data_concordance_manual:
#     #we can just add in rows from rows_to_skip to combined_data_concordance_manual, as well as the rows from duplicates_diff, as these are the rows that are different between old and new. But make sure to remove those rows from combined_data_concordance_manual first, as we dont want to have duplicates.
#     #first set indexes to Index_col (with year)
#     rows_to_skip.reset_index(inplace=True)
#     rows_to_skip.set_index(INDEX_COLS, inplace=True)
#     combined_data_concordance_manual.set_index(INDEX_COLS, inplace=True)
#     duplicates_diff.reset_index(inplace=True)
#     duplicates_diff.set_index(INDEX_COLS, inplace=True)

#     #remove the rows from combined_data_concordance_manual
#     combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(rows_to_skip.index)]
#     combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(duplicates_diff.index)]

#     #now add in the rows from rows_to_skip and duplicates_diff
#     combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, rows_to_skip, duplicates_diff])
#     #remove Date from index
#     combined_data_concordance_manual.reset_index(inplace=True)
#     #%%
#     #filter for Measure = 'Occupancy', Date = 2017-12-31 and sort by economy, vtype, drive, ttype
#     a = combined_data_concordance_manual[combined_data_concordance_manual.Measure=='Stocks']
#     a = a[a.Date=='2017-12-31']
#     a.sort_values(by=['Economy', 'Vehicle Type', 'Drive', 'Transport Type'], inplace=True)
#     #combined_data_concordance_manual[combined_data_concordance_manual.Measure=='Occupancy'].duplicated(subset=INDEX_COLS_no_year, keep=False).sum()
  

        