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
FILE_DATE_ID = ''

duplicates = data_selection_functions.identify_duplicates(combined_dataset, INDEX_COLS)

combined_data_concordance_automatic,combined_data_concordance_manual,combined_data_concordance_iterator,duplicates_auto,duplicates_auto_with_year_index,combined_data_automatic,duplicates_manual,combined_data = data_selection_functions.prepare_data_for_selection(combined_data_concordance,combined_dataset,duplicates,INDEX_COLS,EARLIEST_YEAR = "2010-01-01",    LATEST_YEAR = '2020-01-01')
#%%
run_automatic =True
if run_automatic:
    combined_data_concordance_automatic, rows_to_select_manually_df = data_selection_functions.automatic_selection(combined_data_concordance_automatic,combined_data_automatic,duplicates_auto,duplicates_auto_with_year_index,INDEX_COLS, datasets_to_always_choose=[])
#     a = rows_to_select_manually_df.copy()#TODO REMOVE THIS
# rows_to_select_manually_df = a.copy()#TODO REMOVE THIS

#load combined_data_concordance_manual now so we can use it later if we need to
previous_combined_data_concordance_manual = pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID))
previous_duplicates_manual = pd.read_csv('intermediate_data/data_selection/{}_duplicates_manual.csv'.format(FILE_DATE_ID))
progress_csv = pd.read_csv('intermediate_data/data_selection/{}_progress.csv'.format(FILE_DATE_ID))
# previous_duplicates_manual = duplicates_manual.copy()#TODO REMOVE THIS
#reset duplicates_manual, combined_data_concordance_manual index
duplicates_manual = duplicates_manual.reset_index()
combined_data_concordance_manual = combined_data_concordance_manual.reset_index()

#########################SET ME TO SET VARIABLES FOR FUNCTION
pick_up_where_left_off=True
import_previous_selection=False
run_only_on_rows_to_select_manually=True
manually_chosen_rows_to_select=None
user_edited_combined_data_concordance_iterator=None
previous_combined_data_concordance_manual= previous_combined_data_concordance_manual
duplicates_manual=duplicates_manual
previous_duplicates_manual=previous_duplicates_manual
progress_csv=progress_csv
#########################

#create_manual_data_iterator
#Remove year from the current cols without removing it from original list, and set it as a new list
INDEX_COLS_no_year = INDEX_COLS.copy()
INDEX_COLS_no_year.remove('Date')

#CREATE ITERATOR 
#if we want to add to the rows_to_select_manually_df to check specific rows then set the below to True
if run_only_on_rows_to_select_manually:
    #if this, only run the manual process on index rows where we couldnt find a dataset to use automatically for some year
    #since the automatic method is relatively strict there should be a large amount of rows to select manually
    #note that if any one year cannot be chosen automatically then we will have to choose the dataset manually for all years of that row
    iterator = rows_to_select_manually_df.copy()
    iterator.set_index(INDEX_COLS_no_year, inplace=True)
    iterator.drop_duplicates(inplace=True)#TEMP get rid of this later
elif manually_chosen_rows_to_select:
    #we can add rows form the combined_data_concordance_iterator as edited by the user themselves. 
    iterator = user_edited_combined_data_concordance_iterator.copy()
    #since user changed the data we will jsut reset index and set again
    iterator.reset_index(inplace=True)
    iterator.set_index(INDEX_COLS_no_year, inplace=True)

    #for this example we will add all Stocks data (for the purpoose of betterunderstanding our stocks data!) and remove all the other data. But this is just an example of what the user could do to select specific rows
    use_example = False
    if use_example:
        iterator.reset_index(inplace=True)
        iterator = iterator[iterator['Measure']=='Stocks']
        #set the index to the index cols
        iterator.set_index(INDEX_COLS_no_year, inplace=True)
else:
    iterator = combined_data_concordance_iterator.copy()
#%%
#now determine whether we want to import previous progress or not:
if import_previous_selection:
    # iterator, combined_data_concordance_manual = import_previous_runs_progress_to_manual(previous_combined_data_concordance_manual, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS)
        #IMPORT PREVIOUS RUNS PROGRESS
    #create option to import manual data selection from perveious runs to avoid having to do it again (can replace any rows where the Final_dataset_selection_method is na with where they are Manual in the imported csv)
    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('Date')
    ##########################################################
    #We will make sure there are no index rows in the previous dataframes that are not in the current dataframes
    #first the duplicates
    previous_duplicates_manual.set_index(INDEX_COLS, inplace=True)
    duplicates_manual.set_index(INDEX_COLS, inplace=True)
    #remove the rows that are in the previous duplicates but not in the current duplicates
    index_diff = previous_duplicates_manual.index.difference(duplicates_manual.index)
    previous_duplicates_manual.drop(index_diff, inplace=True)
    #reset the index
    previous_duplicates_manual.reset_index(inplace=True)
    duplicates_manual.reset_index(inplace=True)

    #now for previous_combined_data_concordance_manual and combined_data_concordance_manual
    previous_combined_data_concordance_manual.set_index(INDEX_COLS, inplace=True)
    combined_data_concordance_manual.set_index(INDEX_COLS, inplace=True)
    #remove the rows that are in the previous duplicates but not in the current duplicates
    index_diff = previous_combined_data_concordance_manual.index.difference(combined_data_concordance_manual.index)
    previous_combined_data_concordance_manual.drop(index_diff, inplace=True)
    #reset the index
    previous_combined_data_concordance_manual.reset_index(inplace=True)
    combined_data_concordance_manual.reset_index(inplace=True)
    ##########################################################

    ##There is a chance that some index_rows have had new data added to them, so we will compare the previous duplicates index_rows to the current duplicates and see where thier values are different, make sure that we iterate over them in the manual data selection process
    #so find different rows in the duplicates:
    #first make the Datasets col a string so it can be compared
    a = previous_duplicates_manual.copy()
    a.Datasets = a.Datasets.astype(str)
    b = duplicates_manual.copy()
    b.Datasets = b.Datasets.astype(str)
    a.set_index(INDEX_COLS_no_year,inplace=True)
    b.set_index(INDEX_COLS_no_year,inplace=True)
    duplicates_diff = pd.concat([b, a]).drop_duplicates(keep=False)

    ##First update the iterator:
    #get the rows where the Dataselection method is manual
    manual_index_rows = previous_combined_data_concordance_manual.copy()

    #create a version where we rmeove Date
    manual_index_rows_no_date = manual_index_rows.copy()
    manual_index_rows_no_date.drop('Date', axis=1, inplace=True)
    #remove duplicates
    manual_index_rows_no_date.drop_duplicates(inplace=True)
    #now we want to remove any rows where the Dataselection method is manual so we dont overwrite them in selection process
    manual_index_rows_no_date_no_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method!='Manual']
    #but note that there are some rows where because we are missing any data for certain years then their index will be added to the iterator as well, so we need to remove these rows by searching for them:
    manual_index_rows_no_date_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method=='Manual']
    #now set index to same as iterator, so there is no Date col. 
    manual_index_rows_no_date_manual.set_index(INDEX_COLS_no_year, inplace=True)
    manual_index_rows_no_date_no_manual.set_index(INDEX_COLS_no_year, inplace=True)

    #remove rows that have changed in teh duplcuicates dfs from manual_index_rows_no_date_manual so they dont get removed from the iterator:
    manual_index_rows_no_date_manual = manual_index_rows_no_date_manual[~manual_index_rows_no_date_manual.index.isin(duplicates_diff.index)]

    #make sure theres no rows in no_manual that are in manual (this will remove all rows, regardless of date where one of teh rows has been selected manually)
    manual_index_rows_no_date_no_manual = manual_index_rows_no_date_no_manual[~manual_index_rows_no_date_no_manual.index.isin(manual_index_rows_no_date_manual.index)]

    #KEEP only these rows in the iterator by finding the index rows in both dfs 
    iterator = iterator[iterator.index.isin(manual_index_rows_no_date_no_manual.index)]

    ##And now update the combined_data_concordance_manual that we were orignially using:
    #find the index_rows that we have already set in previous_combined_data_concordance_manual and remove them from combined_data_concordance_manual, then replace them with the rows from previous_combined_data_concordance_manual.
    previous_combined_data_concordance_manual.set_index(INDEX_COLS_no_year,inplace=True)
    combined_data_concordance_manual.set_index(INDEX_COLS_no_year,inplace=True)

    #remove the different rows in the duplicates from the index_rows we are about to remove from combined_data_concordance_manual, so we dont miss them and instead go over any index_rows we have new data for
    previous_combined_data_concordance_manual = previous_combined_data_concordance_manual[~previous_combined_data_concordance_manual.index.isin(duplicates_diff.index)]

    #now remove these index_rows from combined_data_concordance_manual
    combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(previous_combined_data_concordance_manual.index)]

    #replace these rows in combined_data_concordance_manual by using concat
    combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, previous_combined_data_concordance_manual])

    #reset index
    combined_data_concordance_manual.reset_index(inplace=True)
#%%
pick_up_where_left_off = True
if pick_up_where_left_off:
    # iterator, combined_data_concordance_manual = pickup_incomplete_manual_progress(progress_csv, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS)
    ##PICKUP LATEST PROGRESS
    #we want to save the state the user was last at so they can pick up on where they last left off. So load in the data from progress_csv, see what values have had their Dataselection method set to manual and remove them from the iterator.
    #we will then replace those rows in combined_data_concordance_manual
    #there is one subtle part to this, in that an index row will only be removed from the iterator if all the years of that index row have been set to manual. So if the user has set some years to manual but not all, for example by quitting halfway through choosing all the values for a chart, then we will not remove that index row from the iterator and the user should redo it. BUT if during the selection process the user skips rows then this will save that (they can be identified as rows where the dataselection method is manual but the value and num datapoints are NaN - they will be interpolated later)
    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('Date')
    #make the date column a datetime object
    progress_csv.Date = progress_csv.Date.apply(lambda x: str(x) + '-12-31')
    
    ##########################################################
    #We will make sure there are no index rows in the previous dataframes that are not in the current dataframes
    #first the duplicates
    previous_duplicates_manual.set_index(INDEX_COLS, inplace=True)
    duplicates_manual.set_index(INDEX_COLS, inplace=True)
    #remove the rows that are in the previous duplicates but not in the current duplicates
    index_diff = previous_duplicates_manual.index.difference(duplicates_manual.index)
    previous_duplicates_manual.drop(index_diff, inplace=True)

    #reset the index
    previous_duplicates_manual.reset_index(inplace=True)
    duplicates_manual.reset_index(inplace=True)

    #now for previous_combined_data_concordance_manual and combined_data_concordance_manual
    progress_csv.set_index(INDEX_COLS, inplace=True)
    combined_data_concordance_manual.set_index(INDEX_COLS, inplace=True)
    #remove the rows that are in the previous duplicates but not in the current duplicates
    index_diff = progress_csv.index.difference(combined_data_concordance_manual.index)
    progress_csv.drop(index_diff, inplace=True)
    
    #reset the index
    progress_csv.reset_index(inplace=True)
    combined_data_concordance_manual.reset_index(inplace=True)
    ##########################################################

    progress_csv.set_index(INDEX_COLS, inplace=True)

    ##There is a chance that some index_rows have had new data added to them, so we will compare the previous duplicates index_rows to the current duplicates and see where thier values are different, make sure that we iterate over them in the manual data selection process
    #so find different rows in the duplicates:
    #first make the Datasets col a string so it can be compared
    a = previous_duplicates_manual.copy()
    a.Datasets = a.Datasets.astype(str)
    b = duplicates_manual.copy()
    b.Datasets = b.Datasets.astype(str)
    a.set_index(INDEX_COLS_no_year,inplace=True)
    b.set_index(INDEX_COLS_no_year,inplace=True)
    duplicates_diff = pd.concat([b, a]).drop_duplicates(keep=False)
    
    #First update the iterator:
    #get the rows where the Dataselection method is manual
    manual_index_rows = progress_csv.copy()
    #create a version where we rmeove Date
    manual_index_rows_no_date = manual_index_rows.copy()
    manual_index_rows_no_date.reset_index(inplace=True)
    manual_index_rows_no_date.drop('Date', axis=1, inplace=True)
    #remove duplicates
    manual_index_rows_no_date.drop_duplicates(inplace=True)
    #now we want to remove any rows where the Dataselection method is manual 
    manual_index_rows_no_date_no_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method!='Manual']
    #but note that there are some rows where because we are missing any data for certain years then their index will be added to the iterator as well, so we need to remove these rows by searching for them:
    manual_index_rows_no_date_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method=='Manual']
    #now set index to same as iterator
    manual_index_rows_no_date_manual.set_index(INDEX_COLS_no_year, inplace=True)
    manual_index_rows_no_date_no_manual.set_index(INDEX_COLS_no_year, inplace=True)
    
    #remove rows that have changed in teh duplcuicates dfs from manual_index_rows_no_date_manual so they dont get removed from the iterator:
    manual_index_rows_no_date_manual = manual_index_rows_no_date_manual[~manual_index_rows_no_date_manual.index.isin(duplicates_diff.index)]

    #make sure theres no rows in no_manual that are in manual
    manual_index_rows_no_date_no_manual = manual_index_rows_no_date_no_manual[~manual_index_rows_no_date_no_manual.index.isin(manual_index_rows_no_date_manual.index)]
    #KEEP only these rows in the iterator by finding the index rows in both dfs 
    iterator = iterator[iterator.index.isin(manual_index_rows_no_date_no_manual.index)]

    ##And now update the combined_data_concordance_manual:
    #find the rows that we have already set in combined_data_concordance_manual and remove them, then replace them with the new rows
    manual_index_rows = manual_index_rows[manual_index_rows.Dataset_selection_method=='Manual']

    #remove the different rows in the duplicates from the index_rows we are about to remove from combined_data_concordance_manual, so we dont miss them and instead go over any index_rows we have new data for
    #set index to index_cols_no_year
    manual_index_rows.reset_index(inplace=True)
    manual_index_rows.set_index(INDEX_COLS_no_year, inplace=True)
    manual_index_rows = manual_index_rows[~manual_index_rows.index.isin(duplicates_diff.index)]
    manual_index_rows.reset_index(inplace=True)
    #make date a part of the index in combined_data_concordance_manual
    combined_data_concordance_manual.set_index(INDEX_COLS,inplace=True)
    manual_index_rows.set_index(INDEX_COLS,inplace=True)

    #now remove these rows from combined_data_concordance_manual
    combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(manual_index_rows.index)]
    #replace these rows in combined_data_concordance_manual by using concat
    combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, manual_index_rows])
    #remove Date from index
    combined_data_concordance_manual.reset_index(inplace=True)

#%%
































# %%
