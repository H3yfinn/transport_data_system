import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import time
import matplotlib.pyplot as plt
import warnings
import sys

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False

#ignore by message
warnings.filterwarnings("ignore", message="indexing past lexsort depth may impact performance")



def identify_duplicates(combined_dataset, INDEX_COLS):
    """ This function will take in the combined dataset and create a df which essentially summarises the set of datapoints we ahve for each unique index row (including the date). It will create a list of the datasets for which data is available, a count of those datasets as well as the option to conisder the sum of the vlaue, which allows the user to accruately understand if any values for the index row have changed, since the sum of vlaues would have to. This is utilised in the import_previous_runs_progress_to_manual() and pickup_incomplete_manual_progress() functions, if the user includes that column during that part of the process."""
        
    #first chekc for duplicated rows when we ignore the vlaue column
    duplicates = combined_dataset.copy()
    duplicates = duplicates.drop(columns=['Value'])
    duplicates = duplicates[duplicates.duplicated(keep=False)]
    if len(duplicates) > 0:
        print('There are duplicate rows in the dataset with different Values. Please fix them before continuing. You will probably want to split them into different datasets. The duplicates are: ')
        print(duplicates)

        #extrasct the rows with duplicates and sabve them to a csv so we can import them into a spreadsheet to look at them
        duplicates = combined_dataset.copy()
        col_no_value = [col for col in duplicates.columns if col != 'Value']
        duplicates = duplicates[duplicates.duplicated(subset=col_no_value,keep=False)]
        duplicates.to_csv('intermediate_data/testing/erroneus_duplicates.csv', index=False)

        raise Exception('There are duplicate rows in the dataset. Please fix them before continuing')

    ###########################################################
    #now recreate duplicates wiuth list of datasets and count
    duplicates = combined_dataset.copy()
    duplicates =  duplicates.groupby(INDEX_COLS,dropna=False).agg({'Dataset': lambda x: list(x), 'Value': lambda x: sum(x.dropna())}).reset_index()

    #make sure the lists are sorted so that the order is consistent
    duplicates['Dataset'] = duplicates['Dataset'].apply(lambda x: sorted(x))
    #create count column
    duplicates['Num_datapoints'] = duplicates['Dataset'].apply(lambda x: len(x))
    #rename dataset to datasets
    duplicates.rename(columns={'Dataset':'Datasets'}, inplace=True)

    return duplicates




##############################################################################




def prepare_data_for_selection(combined_data_concordance,combined_dataset,duplicates,INDEX_COLS,EARLIEST_YEAR = "2010-01-01", LATEST_YEAR = '2020-01-01'):
    """This function will take in the combined data and combined data concordance dataframes and prepare them for the manual selection process. It will filter the data to only include data from the years we are interested in, and remove any duplicate data for the 8th edition transport model carbon neutrality scenario. It will also create a dataframe which replicates the final dataframe but with no values in the dataset, value and duplicate columns (This dataframe is created in aggregation_code\1_aggregate_cleaned_datasets.py so we can just import that as combined_data_concordance)"""#TODO double check that that is true, it came from ai
    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('Date')
    
    #filter data where year is less than our earliest year
    combined_dataset = combined_dataset[combined_dataset['Date'] >= EARLIEST_YEAR]
    combined_data_concordance = combined_data_concordance[combined_data_concordance['Date'] >= EARLIEST_YEAR]
    duplicates = duplicates[duplicates['Date'] >= EARLIEST_YEAR]
    #and also only data where year is less than the latest year
    combined_dataset = combined_dataset[combined_dataset['Date'] < LATEST_YEAR]
    combined_data_concordance = combined_data_concordance[combined_data_concordance['Date'] < LATEST_YEAR]
    duplicates = duplicates[duplicates['Date'] < LATEST_YEAR]

    #we need a dataframe which replicates the final dataframe but with no values in the dataset, value and duplicate columns (This dataframe is created in aggregation_code\1_aggregate_cleaned_datasets.py so we can just import that as combined_data_concordance)
    #In the folowing scripts we will fill that df with the dataset that we choose to use for each row. Any rows where we dont have the dataset to use we will leave blank and that will end up as an NA
    combined_data_concordance['Dataset'] = None
    # combined_data_concordance['Num_datapoints'] = None
    # combined_data_concordance['Datasets'] = None
    combined_data_concordance['Value'] = None
    combined_data_concordance['Dataset_selection_method'] = None
    combined_data_concordance['Comments'] = None

    #add Datasets and Num_datapoints columns from duplicates_manual to combined_data for use in setting values, as well as to combined_data_concordance_manual for use in setting the dataset
    combined_dataset = combined_dataset.merge(duplicates.reset_index().set_index(INDEX_COLS)[['Datasets', 'Num_datapoints']], how='left', left_on=INDEX_COLS, right_on=INDEX_COLS)

    combined_data_concordance_manual = combined_data_concordance.copy()
    combined_data_concordance_manual = combined_data_concordance_manual.set_index(INDEX_COLS_no_year)
    combined_data_concordance_manual = combined_data_concordance_manual.merge(duplicates.reset_index().set_index(INDEX_COLS)[['Datasets', 'Num_datapoints']], how='left', left_on=INDEX_COLS, right_on=INDEX_COLS)

    #AUTOMATIC data prep
    combined_data_concordance_automatic = combined_data_concordance.set_index(INDEX_COLS)
    duplicates_auto = duplicates.set_index(INDEX_COLS_no_year)
    duplicates_auto_with_year_index = duplicates.set_index(INDEX_COLS)
    combined_data_automatic = combined_dataset.set_index(INDEX_COLS+['Dataset'])

    #MANUAL data prep
    combined_data_concordance_iterator = combined_data_concordance[INDEX_COLS_no_year].drop_duplicates().set_index(INDEX_COLS_no_year)
    duplicates_manual = duplicates.set_index(INDEX_COLS_no_year)
    combined_dataset = combined_dataset.set_index(INDEX_COLS_no_year)

    return combined_data_concordance_automatic,combined_data_concordance_manual,combined_data_concordance_iterator,duplicates_auto,duplicates_auto_with_year_index,combined_data_automatic,duplicates_manual,combined_dataset




#%%
##############################################################################


# def create_manual_data_iterator(combined_data_concordance_iterator,INDEX_COLS,combined_data_concordance_manual,duplicates_manual,rows_to_select_manually_df=None, pick_up_where_left_off=False, import_previous_selection=False,run_only_on_rows_to_select_manually=False,manually_chosen_rows_to_select=None,user_edited_combined_data_concordance_iterator=None,previous_combined_data_concordance_manual=None, previous_duplicates_manual=None, progress_csv=None, update_skipped_rows=False):
#     """
#     manually_chosen_rows_to_select: set to true if you want to manually choose the rows to select using user_edited_combined_data_concordance_iterator
#     user_edited_combined_data_concordance_iterator: a manually chosen dataframe with the rows to select. This allows user to define what they want to select manually (eg. all stocks)
    
#     duplicates_manual & previous_duplicates_manual need to be available if you want to use either pick_up_where_left_off or import_previous_selection. progress_csv should also be available if you want to use pick_up_where_left_off
#     """
#     #Remove year from the current cols without removing it from original list, and set it as a new list
#     INDEX_COLS_no_year = INDEX_COLS.copy()
#     INDEX_COLS_no_year.remove('Date')

#     #CREATE ITERATOR 
#     #if we want to add to the rows_to_select_manually_df to check specific rows then set the below to True
#     if run_only_on_rows_to_select_manually:
#         #if this, only run the manual process on index rows where we couldnt find a dataset to use automatically for some year
#         #since the automatic method is relatively strict there should be a large amount of rows to select manually
#         #note that if any one year cannot be chosen automatically then we will have to choose the dataset manually for all years of that row
#         iterator = rows_to_select_manually_df.copy()
#         iterator.set_index(INDEX_COLS_no_year, inplace=True)
#         iterator.drop_duplicates(inplace=True)#TEMP get rid of this later
#     elif manually_chosen_rows_to_select:
#         #we can add rows form the combined_data_concordance_iterator as edited by the user themselves. 
#         iterator = user_edited_combined_data_concordance_iterator.copy()
#         #since user changed the data we will jsut reset index and set again
#         iterator.reset_index(inplace=True)
#         iterator.set_index(INDEX_COLS_no_year, inplace=True)

#         #for this example we will add all Stocks data (for the purpoose of betterunderstanding our stocks data!) and remove all the other data. But this is just an example of what the user could do to select specific rows
#         use_example = False
#         if use_example:
#             iterator.reset_index(inplace=True)
#             iterator = iterator[iterator['Measure']=='Stocks']
#             #set the index to the index cols
#             iterator.set_index(INDEX_COLS_no_year, inplace=True)
#     else:
#         iterator = combined_data_concordance_iterator.copy()

#     #now determine whether we want to import previous progress or not:
#     if import_previous_selection:
#         iterator, combined_data_concordance_manual = import_previous_runs_progress_to_manual(previous_combined_data_concordance_manual, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS,update_skipped_rows)
    
#     if pick_up_where_left_off:
#         iterator, combined_data_concordance_manual = pickup_incomplete_manual_progress(progress_csv, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS,update_skipped_rows)

#     return iterator, combined_data_concordance_manual



##############################################################################

def create_manual_data_iterator(
combined_data_concordance_iterator,
INDEX_COLS,
combined_data_concordance_manual,
duplicates_manual,
rows_to_select_manually_df=None,
run_only_on_rows_to_select_manually=False,
manually_chosen_rows_to_select=None,
user_edited_combined_data_concordance_iterator=None,
previous_selections=None,
previous_duplicates_manual=None,
update_skipped_rows=False):
    
    """
    manually_chosen_rows_to_select: set to true if you want to manually choose the rows to select using user_edited_combined_data_concordance_iterator
    user_edited_combined_data_concordance_iterator: a manually chosen dataframe with the rows to select. This allows user to define what they want to select manually (eg. all stocks)

    duplicates_manual & previous_duplicates_manual need to be available if you want to use either pick_up_where_left_off or import_previous_selection. progress_csv should also be available if you want to use pick_up_where_left_off
    """
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

    #now determine whether we want to import previous progress or not:
    if previous_selections is not None:
        combined_data_concordance_manual,iterator = import_previous_selections(previous_selections, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS,update_skipped_rows)
        
    return iterator, combined_data_concordance_manual



##############################################################################




#%%
def import_previous_selections(previous_selections, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS,update_skipped_rows=False):
    #IMPORT PREVIOUS SELECTIONS
    #Previous_selections can be the previous_combined_data_concordance_manual or the progress_csv depending on if the user wants to import completed manual selections or the progress of some manual selections
   
    #This allows the user to import manual data selections from perveious runs to avoid having to do it again (can replace any rows where the Dataset_selection_method is na with where they are Manual in the imported csv)
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

    #now for previous_selections and combined_data_concordance_manual
    previous_selections.set_index(INDEX_COLS, inplace=True)
    combined_data_concordance_manual.set_index(INDEX_COLS, inplace=True)
    #remove the rows that are in the previous duplicates but not in the current duplicates
    index_diff = previous_selections.index.difference(combined_data_concordance_manual.index)
    previous_selections.drop(index_diff, inplace=True)
    #reset the index
    previous_selections.reset_index(inplace=True)
    combined_data_concordance_manual.reset_index(inplace=True)
    ##########################################################

    ##There is a chance that some index_rows have had new data added to them, so we will compare the previous duplicates index_rows to the current duplicates and see where thier values are different, make sure that we iterate over them in the manual data selection process
    #so find different rows in the duplicates:
    #first make the Datasets col a string so it can be compared
    a = previous_duplicates_manual.copy()
    a.Datasets = a.Datasets.astype(str)
    b = duplicates_manual.copy()
    b.Datasets = b.Datasets.astype(str)
    # a.set_index(INDEX_COLS_no_year,inplace=True)
    # b.set_index(INDEX_COLS_no_year,inplace=True)
    duplicates_diff = pd.concat([b, a])
    #set Vlaue to int, because we want to see if the value is the same or not and if float there might be floating point errors (computer science thing)
    duplicates_diff.Value = duplicates_diff.Value.astype(int)
    #drop duplicates
    duplicates_diff = duplicates_diff.drop_duplicates(keep=False)
    ##########################################################

    ##Update the iterator: We will remove rows where the user doesnt need to select a dataset. This will be done using the previous combined_data_concordance_manual.
    rows_to_skip = previous_selections.copy()
    rows_to_skip.set_index(INDEX_COLS_no_year, inplace=True)
    #we have to state wat rows we want to remove rather than keep because there will be some in the iterator that are not in the previous_selections df, and we will want to keep them.

    #First remove rows that are in duplicates diff as we want to make sure the user selects for them since they have some detail which is different to what it was before. We do this using index_no_year to cover all the rows that have the same index but different years
    duplicates_diff.set_index(INDEX_COLS_no_year, inplace=True)
    rows_to_skip = rows_to_skip[~rows_to_skip.index.isin(duplicates_diff.index)]

    #In case we are using the progress csv we will only remove rows where all years data has been selected for that index row. This will cover the way prvious_combined_data_concordance_manual df is formatted as well 
    #first remove where num_datapoints is na or 0. 
    rows_to_skip = rows_to_skip[~((rows_to_skip.Num_datapoints.isna()) | (rows_to_skip.Num_datapoints==0))]
    #now find rows where there is data but the user hasnt selected a dataset for it. We will remove these rows from rows_to_skip, as we want to make sure the user selects for them (in the future this may cuase issues if we add more selection methods to the previous selections) #note this currently only occurs in the case where an error occurs during sleection
    rows_to_remove_from_rows_to_skip = rows_to_skip[rows_to_skip.Dataset_selection_method.isna()]

    if update_skipped_rows:
        #if we are updating the skipped rows then we will also remove from rows to skip the rows where the Value and Dataset is NaN where the selection method is manual. This will be the case where the user has skipped selection so the selection emthod is manual but there are no values or datasets selected
        rows_to_remove_from_rows_to_skip2 = rows_to_skip[(rows_to_skip.Value.isna()) & (rows_to_skip.Dataset.isna()) & (rows_to_skip.Dataset_selection_method=='Manual')]

    #now remove those rows from rows_to_skip using index_no_year. This will remove any cases where an index row has one or two datapoints that werent chosen, but the rest were.
    rows_to_skip = rows_to_skip[~rows_to_skip.index.isin(rows_to_remove_from_rows_to_skip.index)]

    if update_skipped_rows:
        rows_to_skip = rows_to_skip[~rows_to_skip.index.isin(rows_to_remove_from_rows_to_skip2.index)]

    #KEEP only rows we dont want to skip in the iterator by finding the index rows in both dfs 
    iterator = iterator[~iterator.index.isin(rows_to_skip.index)]

    ###############
    ##And now update the combined_data_concordance_manual:
    #we can just add in rows from rows_to_skip to combined_data_concordance_manual, as well as the rows from duplicates_diff, as these are the rows that are different between old and new. But make sure to remove those rows from combined_data_concordance_manual first, as we dont want to have duplicates.
    #first set indexes to Index_col (with year)
    rows_to_skip.reset_index(inplace=True)
    rows_to_skip.set_index(INDEX_COLS, inplace=True)
    combined_data_concordance_manual.set_index(INDEX_COLS, inplace=True)
    duplicates_diff.reset_index(inplace=True)
    duplicates_diff.set_index(INDEX_COLS, inplace=True)

    #remove the rows from combined_data_concordance_manual
    combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(rows_to_skip.index)]
    combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(duplicates_diff.index)]

    #now add in the rows from rows_to_skip and duplicates_diff
    combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, rows_to_skip, duplicates_diff])

    #remove Date from index
    combined_data_concordance_manual.reset_index(inplace=True)

    return combined_data_concordance_manual, iterator



##############################################################################


def combine_manual_and_automatic_output(combined_data_concordance_automatic,combined_data_concordance_manual,INDEX_COLS):

    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('Date')

    #COMBINE MANUAL AND AUTOMATIC DATA SELECTION OUTPUT DFs
    #join the automatic and manual datasets so we can compare the dataset  columns from both the manual dataset automatic dataset
    #create a final_dataset column. This will be filled with the dataset in the automatic column, except where the values in the manual and automatic dataset columns are different, for which we will use the value in the manual dataset.

    #reset and set index of both dfs to INDEX_COLS
    combined_data_concordance_manual = combined_data_concordance_manual.set_index(INDEX_COLS)
    combined_data_concordance_automatic = combined_data_concordance_automatic.reset_index().set_index(INDEX_COLS)

    #remove the Datasets and Dataset_selection_method columns from the manual df
    combined_data_concordance_manual.drop(columns=['Datasets','Dataset_selection_method'], inplace=True)
    #join manual and automatic data selection dfs
    final_combined_data_concordance = combined_data_concordance_manual.merge(combined_data_concordance_automatic, how='outer', left_index=True, right_index=True, suffixes=('_manual', '_auto'))

    #we will either have dataset names or nan values in the manual and automatic dataset columns. We want to use the manual dataset column if it is not nan, otherwise use the automatic dataset column:
    #first set the dataset_selection_method column based on that criteria, and then use that to set other columns
    final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_auto'].notnull(), 'Dataset_selection_method'] = 'Automatic'
    #if the manual dataset column is not nan then use that instead
    final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_manual'].notnull(), 'Dataset_selection_method'] = 'Manual'

    #Now depending on the value of the Dataset_selection_method column, we can set final_value and final_dataset columns
    #if the Dataset_selection_method is manual then use the manual dataset column
    final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Manual', 'Value'] = final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Manual','Value_manual']
    final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Manual', 'Dataset'] = final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Manual','Dataset_manual']
    #if the Dataset_selection_method is automatic then use the automatic dataset column
    final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Automatic', 'Dataset'] = final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Automatic','Dataset_auto']
    final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Automatic', 'Value'] = final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_selection_method'] == 'Automatic','Value_auto']

    #drop cols ending in _manual and _auto
    final_combined_data_concordance.drop(columns=[col for col in final_combined_data_concordance.columns if col.endswith('_manual') or col.endswith('_auto')], inplace=True)

    return final_combined_data_concordance


# #TODO
# def import_previous_runs_progress_to_manual(previous_combined_data_concordance_manual, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS,update_skipped_rows=False):
#     #IMPORT PREVIOUS RUNS PROGRESS
#     #create option to import manual data selection from perveious runs to avoid having to do it again (can replace any rows where the Dataset_selection_method is na with where they are Manual in the imported csv)
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

#     #now for previous_combined_data_concordance_manual and combined_data_concordance_manual
#     previous_combined_data_concordance_manual.set_index(INDEX_COLS, inplace=True)
#     combined_data_concordance_manual.set_index(INDEX_COLS, inplace=True)
#     #remove the rows that are in the previous duplicates but not in the current duplicates
#     index_diff = previous_combined_data_concordance_manual.index.difference(combined_data_concordance_manual.index)
#     previous_combined_data_concordance_manual.drop(index_diff, inplace=True)
#     #reset the index
#     previous_combined_data_concordance_manual.reset_index(inplace=True)
#     combined_data_concordance_manual.reset_index(inplace=True)
#     ##########################################################

#     ##There is a chance that some index_rows have had new data added to them, so we will compare the previous duplicates index_rows to the current duplicates and see where thier values are different, make sure that we iterate over them in the manual data selection process
#     #so find different rows in the duplicates:
#     #first make the Datasets col a string so it can be compared
#     a = previous_duplicates_manual.copy()
#     a.Datasets = a.Datasets.astype(str)
#     b = duplicates_manual.copy()
#     b.Datasets = b.Datasets.astype(str)
#     a.set_index(INDEX_COLS_no_year,inplace=True)
#     b.set_index(INDEX_COLS_no_year,inplace=True)
#     duplicates_diff = pd.concat([b, a])
#     #set Vlaue to int, because we want to see if the value is the same or not and if float there might be floating point errors (computer science thing)
#     duplicates_diff.Value = duplicates_diff.Value.astype(int)
#     duplicates_diff = duplicates_diff[~duplicates_diff.index.duplicated(keep=False)]

#     ##First update the iterator:
#     #get the rows where the Dataselection method is manual
#     manual_index_rows = previous_combined_data_concordance_manual.copy()

#     #create a version where we rmeove Date
#     manual_index_rows_no_date = manual_index_rows.copy()
#     manual_index_rows_no_date.drop('Date', axis=1, inplace=True)
#     #remove duplicates
#     manual_index_rows_no_date.drop_duplicates(inplace=True)
#     #now we want to remove any rows where the Dataselection method is manual so we dont overwrite them in selection process
#     manual_index_rows_no_date_no_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method!='Manual']
#     #but note that there are some rows where because we are missing any data for certain years then their index will be added to the iterator as well, so we need to remove these rows by searching for them:
#     manual_index_rows_no_date_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method=='Manual']
#     #now set index to same as iterator, so there is no Date col. 
#     manual_index_rows_no_date_manual.set_index(INDEX_COLS_no_year, inplace=True)
#     manual_index_rows_no_date_no_manual.set_index(INDEX_COLS_no_year, inplace=True)

#     #remove rows that have changed in teh duplcuicates dfs from manual_index_rows_no_date_manual so they dont get removed from the iterator:
#     manual_index_rows_no_date_manual = manual_index_rows_no_date_manual[~manual_index_rows_no_date_manual.index.isin(duplicates_diff.index)]

#     #also if update_skipped_rows is True then we need to remove any rows that have been skipped from the iterator. You can find these by finding rows where Dataset_selection_method is Manual but the Dataseta and Value are NA
#     if update_skipped_rows:
#         #find the rows where the Dataset_selection_method is Manual but the Dataset and Value are NA
#         manual_index_rows_no_date_manual_skipped = manual_index_rows_no_date_manual[(manual_index_rows_no_date_manual.Dataset_selection_method=='Manual') & (manual_index_rows_no_date_manual.Dataset.isna()) & (manual_index_rows_no_date_manual.Value.isna())]
#         #remove these rows from the manual_index_rows_no_date_manual so they dont get removed from the iterator
#         manual_index_rows_no_date_manual = manual_index_rows_no_date_manual[~manual_index_rows_no_date_manual.index.isin(manual_index_rows_no_date_manual_skipped.index)]

#     #make sure theres no rows in no_manual that are in manual (this will remove all rows, regardless of date where one of teh rows has been selected manually)
#     manual_index_rows_no_date_no_manual = manual_index_rows_no_date_no_manual[~manual_index_rows_no_date_no_manual.index.isin(manual_index_rows_no_date_manual.index)]

#     #KEEP only these rows in the iterator by finding the index rows in both dfs 
#     iterator = iterator[iterator.index.isin(manual_index_rows_no_date_no_manual.index)]

#     ##And now update the combined_data_concordance_manual that we were orignially using:
#     #find the index_rows that we have already set in previous_combined_data_concordance_manual and remove them from combined_data_concordance_manual, then replace them with the rows from previous_combined_data_concordance_manual.
#     previous_combined_data_concordance_manual.set_index(INDEX_COLS_no_year,inplace=True)
#     combined_data_concordance_manual.set_index(INDEX_COLS_no_year,inplace=True)

#     #remove the different rows in the duplicates from the index_rows we are about to remove from combined_data_concordance_manual, so we dont miss them and instead go over any index_rows we have new data for
#     previous_combined_data_concordance_manual = previous_combined_data_concordance_manual[~previous_combined_data_concordance_manual.index.isin(duplicates_diff.index)]

#     #now remove these index_rows from combined_data_concordance_manual
#     combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(previous_combined_data_concordance_manual.index)]

#     #replace these rows in combined_data_concordance_manual by using concat
#     combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, previous_combined_data_concordance_manual])

#     #reset index
#     combined_data_concordance_manual.reset_index(inplace=True)

#     return iterator, combined_data_concordance_manual









# ##########################################################################################






# def pickup_incomplete_manual_progress(progress_csv, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS, update_skipped_rows=False):
#     """ this is very similar to import_previous_runs_progress_to_manual() except it is used if there was an error duiring the manual data selection process. This is useful because there is a chance the user accidentally exits the program by exiting a matplotlib graph, which will cause an error. """
#     ##PICKUP LATEST PROGRESS
#     #we want to save the state the user was last at so they can pick up on where they last left off. So load in the data from progress_csv, see what values have had their Dataselection method set to manual and remove them from the iterator.
#     #we will then replace those rows in combined_data_concordance_manual
#     #there is one subtle part to this, in that an index row will only be removed from the iterator if all the years of that index row have been set to manual. So if the user has set some years to manual but not all, for example by quitting halfway through choosing all the values for a chart, then we will not remove that index row from the iterator and the user should redo it. BUT if during the selection process the user skips rows then this will save that (they can be identified as rows where the dataselection method is manual but the value and num datapoints are NaN - they will be interpolated later)
#     INDEX_COLS_no_year = INDEX_COLS.copy()
#     INDEX_COLS_no_year.remove('Date')
#     #make the date column a datetime object
#     progress_csv.Date = progress_csv.Date.apply(lambda x: str(x) + '-12-31')
    
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

#     #now for previous_combined_data_concordance_manual and combined_data_concordance_manual
#     progress_csv.set_index(INDEX_COLS, inplace=True)
#     combined_data_concordance_manual.set_index(INDEX_COLS, inplace=True)
#     #remove the rows that are in the previous duplicates but not in the current duplicates
#     index_diff = progress_csv.index.difference(combined_data_concordance_manual.index)
#     progress_csv.drop(index_diff, inplace=True)
#     #reset the index
#     progress_csv.reset_index(inplace=True)
#     combined_data_concordance_manual.reset_index(inplace=True)
#     ##########################################################

#     progress_csv.set_index(INDEX_COLS, inplace=True)

#     ##There is a chance that some index_rows have had new data added to them, so we will compare the previous duplicates index_rows to the current duplicates and see where thier values are different, make sure that we iterate over them in the manual data selection process
#     #so find different rows in the duplicates:
#     #first make the Datasets col a string so it can be compared
#     a = previous_duplicates_manual.copy()
#     a.Datasets = a.Datasets.astype(str)
#     b = duplicates_manual.copy()
#     b.Datasets = b.Datasets.astype(str)
#     a.set_index(INDEX_COLS_no_year,inplace=True)
#     b.set_index(INDEX_COLS_no_year,inplace=True)
#     duplicates_diff = pd.concat([b, a])
#     #set Vlaue to int, because we want to see if the value is the same or not and if float there might be floating point errors (computer science thing)
#     duplicates_diff.Value = duplicates_diff.Value.astype(int)
#     duplicates_diff = duplicates_diff[~duplicates_diff.index.duplicated(keep=False)]

#     #First update the iterator:
#     #get the rows where the Dataselection method is manual
#     manual_index_rows = progress_csv.copy()
#     #create a version where we rmeove Date
#     manual_index_rows_no_date = manual_index_rows.copy()
#     manual_index_rows_no_date.reset_index(inplace=True)
#     manual_index_rows_no_date.drop('Date', axis=1, inplace=True)
#     #remove duplicates
#     manual_index_rows_no_date.drop_duplicates(inplace=True)
#     #now we want to remove any rows where the Dataselection method is manual 
#     manual_index_rows_no_date_no_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method!='Manual']
#     #but note that there are some rows where because we are missing any data for certain years then their index will be added to the iterator as well, so we need to remove these rows by searching for them:
#     manual_index_rows_no_date_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method=='Manual']
#     #now set index to same as iterator
#     manual_index_rows_no_date_manual.set_index(INDEX_COLS_no_year, inplace=True)
#     manual_index_rows_no_date_no_manual.set_index(INDEX_COLS_no_year, inplace=True)
    
#     #remove rows that have changed in teh duplcuicates dfs from manual_index_rows_no_date_manual so they dont get removed from the iterator:
#     manual_index_rows_no_date_manual = manual_index_rows_no_date_manual[~manual_index_rows_no_date_manual.index.isin(duplicates_diff.index)]


#     #also if update_skipped_rows is True then we need to remove any rows that have been skipped from the iterator. You can find these by finding rows where Dataset_selection_method is Manual but the Dataseta and Value are NA
#     if update_skipped_rows:
#         #find the rows where the Dataset_selection_method is Manual but the Dataset and Value are NA
#         manual_index_rows_no_date_manual_skipped = manual_index_rows_no_date_manual[(manual_index_rows_no_date_manual.Dataset_selection_method=='Manual') & (manual_index_rows_no_date_manual.Dataset.isna()) & (manual_index_rows_no_date_manual.Value.isna())]
#         #remove these rows from the manual_index_rows_no_date_manual so they dont get removed from the iterator
#         manual_index_rows_no_date_manual = manual_index_rows_no_date_manual[~manual_index_rows_no_date_manual.index.isin(manual_index_rows_no_date_manual_skipped.index)]

#     #make sure theres no rows in no_manual that are in manual
#     manual_index_rows_no_date_no_manual = manual_index_rows_no_date_no_manual[~manual_index_rows_no_date_no_manual.index.isin(manual_index_rows_no_date_manual.index)]
#     #KEEP only these rows in the iterator by finding the index rows in both dfs 
#     iterator = iterator[iterator.index.isin(manual_index_rows_no_date_no_manual.index)]

#     ##And now update the combined_data_concordance_manual:
#     #find the rows that we have already set in combined_data_concordance_manual and remove them, then replace them with the new rows
#     manual_index_rows = manual_index_rows[manual_index_rows.Dataset_selection_method=='Manual']

#     #remove the different rows in the duplicates from the index_rows we are about to remove from combined_data_concordance_manual, so we dont miss them and instead go over any index_rows we have new data for
#     #set index to index_cols_no_year
#     manual_index_rows.reset_index(inplace=True)
#     manual_index_rows.set_index(INDEX_COLS_no_year, inplace=True)
#     manual_index_rows = manual_index_rows[~manual_index_rows.index.isin(duplicates_diff.index)]
#     manual_index_rows.reset_index(inplace=True)
#     #make date a part of the index in combined_data_concordance_manual
#     combined_data_concordance_manual.set_index(INDEX_COLS,inplace=True)
#     manual_index_rows.set_index(INDEX_COLS,inplace=True)

#     #now remove these rows from combined_data_concordance_manual
#     combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(manual_index_rows.index)]
#     #replace these rows in combined_data_concordance_manual by using concat
#     combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, manual_index_rows])
#     #remove Date from index
#     combined_data_concordance_manual.reset_index(inplace=True)

#     return iterator, combined_data_concordance_manual

##########################################################################################
