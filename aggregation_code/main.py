
#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re


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
combined_data = data_formatting_functions.combine_datasets(datasets_transport, FILE_DATE_ID,paths_dict)
    
#%%
combined_data_concordance = data_formatting_functions.create_concordance(combined_data, frequency = 'Yearly')
# combined_data_concordance, combined_data = data_formatting_functions.change_column_names(combined_data_concordance, combined_data)

#%%
if create_9th_model_dataset:
    #import snapshot of 9th concordance
    model_concordances_base_year_measures_file_name = './intermediate_data/9th_dataset/{}'.format('model_concordances_measures.csv')
    data_formatting_functions.filter_for_9th_edition_data(combined_data, model_concordances_base_year_measures_file_name, paths_dict)


#since we dont expect to run the data selection process that often we will just save the data in a dated folder in intermediate_data/data_selection_process/FILE_DATE_ID/


#%%
combined_data, combined_data_concordance = data_formatting_functions.prepare_data_for_selection(combined_data_concordance,combined_data,paths_dict)#todo reaplce everything that is combined_dataset with combined_data

#%%

#todo, since we are importing deleted datasets later, we should consider wehther that will rem,ove any dupclaites?
###########################################################
#create dataframe of duplicates with list of datasets and count of datasets
duplicates = combined_data.copy()
#make Potential_datapoints a copy of the dataset column
duplicates['Potential_datapoints'] = duplicates['Dataset']
# add source and dataset to the INDEX_COLS
INDEX_COLS_source_dataset = INDEX_COLS + ['Dataset', 'Source']
#group by the index columns and sum the values
duplicates =  duplicates.groupby(INDEX_COLS_source_dataset,dropna=False).agg({'Potential_datapoints': lambda x: list(x), 'Value': lambda x: sum(x.dropna())}).reset_index()

#make sure the lists are sorted so that the order is consistent
duplicates['Potential_datapoints'] = duplicates['Potential_datapoints'].apply(lambda x: sorted(x))
#create count column
duplicates['Num_datapoints'] = duplicates['Potential_datapoints'].apply(lambda x: len(x))

#join onto combined_data_concordance
new_combined_data_concordance = duplicates.merge(combined_data_concordance, on=INDEX_COLS_source_dataset, how='left')#todo does this result in what we'd like? are there any issues with not using .copy)( on anythiing
new_combined_data = duplicates.merge(combined_data, on=INDEX_COLS_source_dataset, how='left')#todo, do we need to use [['Datasets', 'Num_datapoints']] here?

# test_identify_duplicated_datapoints_to_select_for(combined_dataset,combined_data_concordance,new_combined_data_concordance,INDEX_COLS)


#%%
##############################################################################

#todo do these
def import_previous_data_concordance():
    #load in concordance and then use merge_previous_data_concordance() to change any rows that the same.
    #todo, take a look at the previous selctions fucntion and see if we can just use a merge to udpate it. I guess it is jsut the concordance that needs updating?
    def merge_previous_data_concordance():
    pass
    pass
def import_previous_combined_dataset():
    #whats the point in this one? todo
    pass
def merge_previous_combined_dataset():
    pass

#%%

#create list of tuples of these datasets where the first element is the Fuel_Typefolder of the dataset (eg. 'intermediate_data/Macro/') and the second element is the filename to identify the dataset (eg. 'all_macro_data_') and the third element is the full path to the dataset (eg. 'intermediate_data/Macro/all_macro_data_{}.csv'.format(FILE_DATE_ID))









# %%


def import_previous_selections(previous_selections, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,paths_dict,update_skipped_rows=False):
    """
    #todo we should make this way way more ismple by changing the inhernet way we do it
    Please note this is quite a complicated process. 
    #WARNING THERES POTENTIALLY AN ISSUE WHEN YOU UPDATE THE INPUT DATA SO IT INCLUDES ANOTHER DATAPOINT AND YOU LOAD THIS IN, THE MANUAL CONCORDANCE WILL END UP AHVING TWO ROWS FOR THE SAME DATAPOINT? #cHECK IT LATER
    """
    #IMPORT PREVIOUS SELECTIONS
    #Previous_selections can be the previous_combined_data_concordance_manual or the progress_csv depending on if the user wants to import completed manual selections or the progress of some manual selections
   
    #This allows the user to import manual data selections from perveious runs to avoid having to do it again (can replace any rows where the Dataset_selection_method is na with where they are Manual in the imported csv)
    ##########################################################
    #We will make sure there are no index rows in the previous dataframes that are not in the current dataframes
    #first the duplicates
    previous_duplicates_manual.set_index(paths_dict['INDEX_COLS'], inplace=True)
    duplicates_manual.set_index(paths_dict['INDEX_COLS'], inplace=True)
    #remove the rows that are in the previous duplicates but not in the current duplicates
    index_diff = previous_duplicates_manual.index.difference(duplicates_manual.index)
    previous_duplicates_manual.drop(index_diff, inplace=True)
    #reset the index
    previous_duplicates_manual.reset_index(inplace=True)
    duplicates_manual.reset_index(inplace=True)

    #now for previous_selections and combined_data_concordance_manual
    previous_selections.set_index(paths_dict['INDEX_COLS'], inplace=True)
    combined_data_concordance_manual.set_index(paths_dict['INDEX_COLS'], inplace=True)
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
    rows_to_skip.set_index(paths_dict['INDEX_COLS_no_year'], inplace=True)
    #we have to state wat rows we want to remove rather than keep because there will be some in the iterator that are not in the previous_selections df, and we will want to keep them.

    #First remove rows that are in duplicates diff as we want to make sure the user selects for them since they have some detail which is different to what it was before. We do this using index_no_year to cover all the rows that have the same index but different years
    duplicates_diff.set_index(paths_dict['INDEX_COLS_no_year'], inplace=True)
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
    rows_to_skip.set_index(paths_dict['INDEX_COLS'], inplace=True)
    combined_data_concordance_manual.set_index(paths_dict['INDEX_COLS'], inplace=True)
    duplicates_diff.reset_index(inplace=True)
    duplicates_diff.set_index(paths_dict['INDEX_COLS'], inplace=True)

    #remove the rows from combined_data_concordance_manual
    combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(rows_to_skip.index)]
    combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(duplicates_diff.index)]

    #now add in the rows from rows_to_skip and duplicates_diff
    combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, rows_to_skip, duplicates_diff])

    #remove Date from index
    combined_data_concordance_manual.reset_index(inplace=True)

    return combined_data_concordance_manual, iterator


