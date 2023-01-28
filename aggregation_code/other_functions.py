#turn the scripts we have into funcitons
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False
def identify_duplicates(combined_dataset, INDEX_COLS):
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
    #now recreate duplicates but this time with the value column
    duplicates = combined_dataset.copy()
    duplicates =  duplicates.groupby(INDEX_COLS,dropna=False).agg({'Dataset': lambda x: list(x)}).reset_index()
    #make sure the lists are sorted so that the order is consistent
    duplicates['Dataset'] = duplicates['Dataset'].apply(lambda x: sorted(x))
    #create count column
    duplicates['Count'] = duplicates['Dataset'].apply(lambda x: len(x))
    #rename dataset to datasets
    duplicates.rename(columns={'Dataset':'Datasets'}, inplace=True)

    return duplicates

import data_selection_functions as data_selection_functions


def prepare_data_for_selection(combined_data_concordance,combined_data,INDEX_COLS,PRINT_GRAPHS_AND_STATS = False,EARLIEST_YEAR = "2010-01-01",    LATEST_YEAR = '2020-01-01'):
    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('Date')
    
    #filter data where year is less than our earliest year
    combined_data = combined_data[combined_data['Date'] >= EARLIEST_YEAR]
    combined_data_concordance = combined_data_concordance[combined_data_concordance['Date'] >= EARLIEST_YEAR]
    duplicates = duplicates[duplicates['Date'] >= EARLIEST_YEAR]
    #and also only data where year is less than the latest year
    combined_data = combined_data[combined_data['Date'] < LATEST_YEAR]
    combined_data_concordance = combined_data_concordance[combined_data_concordance['Date'] < LATEST_YEAR]
    duplicates = duplicates[duplicates['Date'] < LATEST_YEAR]

    #######################################################################
    #STARTING SPECIFIC FIX, FEEL FREE TO IGNORE IT
    #To make things faster in the manual dataseelection process, for any rows in the eighth edition dataset where the data for both the carbon neutral and reference scenarios (in source column) is the same, we will remove the carbon neutral scenario data, as we would always choose the reference data anyways.
    #Unfortunately the code to do this is a bit long and messy so i removed it from this function and put it in data_selection_functions.py
    combined_data, duplicates = data_selection_functions.remove_duplicate_transport_8th_data(combined_data, duplicates)
    #######################################################################
    
#%%
#######################################################################
#STARTING SPECIFIC FIX, FEEL FREE TO IGNORE IT
#I have made this into a function to reduce the code in this aprticular file. it is in data_selection_functions.py and reduces the code in this file by about 40 lines
combined_data, duplicates = data_selection_functions.remove_duplicate_transport_8th_data(combined_data, duplicates)
#######################################################################
#but actually temporarily we are going to just remove any data for the Carbon neutrality scenario since it is just another scenario and we dont want to have to deal with it
combined_data = combined_data[combined_data['Dataset'] != '8th edition transport model $ Carbon neutrality']
#to remove it fromduplicates we need to remove it from the datasets lists column and reduce count by 1
duplicates['Datasets'] = duplicates['Datasets'].apply(lambda x: [i for i in x if i != '8th edition transport model $ Carbon neutrality'])
#double check count is correct
duplicates['Count'] = duplicates['Datasets'].apply(lambda x: len(x))
#%%
#we need a dataframe which replicates the final dataframe but with no values in the dataset, value and duplicate columns (This dataframe is created in aggregation_code\1_aggregate_cleaned_datasets.py so we can just import that as combined_data_concordance)
#In the folowing scripts we will fill that df with the dataset that we choose to use for each row. Any rows where we dont have the dataset to use we will leave blank and that will end up as an NA

#we will also create a column to indicate if the value was manually selected or automatically selected and a column to indicate how many datapoints there were for that row
#later on we can fill the value column with the value from the dataset that we choose to use for each row, but for now we will leave it blank
combined_data_concordance['Dataset'] = None
combined_data_concordance['Num_datapoints'] = None
combined_data_concordance['Value'] = None
combined_data_concordance['Dataset_selection_method'] = None
combined_data_concordance['Comments'] = None

#add Datasets and Count columns from duplicates_manual to combined_data for use in setting values
combined_data = combined_data.merge(duplicates.reset_index().set_index(INDEX_COLS)[['Datasets', 'Count']], how='left', left_on=INDEX_COLS, right_on=INDEX_COLS)

#set index of all the dfs we will use MANUAL METHODS, using the INDEX_COLs:

#AUTOMATIC data prep
combined_data_concordance_automatic = combined_data_concordance.set_index(INDEX_COLS)
duplicates_auto = duplicates.set_index(INDEX_COLS_no_year)
duplicates_auto_with_year_index = duplicates.set_index(INDEX_COLS)
combined_data_automatic = combined_data.set_index(INDEX_COLS+['Dataset'])

#MANUAL data prep
combined_data_concordance_iterator = combined_data_concordance[INDEX_COLS_no_year].drop_duplicates().set_index(INDEX_COLS_no_year)#TODO i think this is correct
combined_data_concordance_manual = combined_data_concordance.set_index(INDEX_COLS_no_year)
duplicates_manual = duplicates.set_index(INDEX_COLS_no_year)
combined_data = combined_data.set_index(INDEX_COLS_no_year)

#%%
#save data to be imported in data selection script. Dont bother with date ids as there are enough other checkpoints
combined_data_concordance_automatic.to_pickle('intermediate_data/data_selection/combined_data_concordance_automatic.pkl')
combined_data_concordance_manual.to_pickle('intermediate_data/data_selection/combined_data_concordance_manual.pkl')
combined_data_concordance_iterator.to_pickle('intermediate_data/data_selection/combined_data_concordance_iterator.pkl')
duplicates_auto.to_pickle('intermediate_data/data_selection/duplicates_auto.pkl')
duplicates_auto_with_year_index.to_pickle('intermediate_data/data_selection/duplicates_auto_with_year_index.pkl')
duplicates_manual.to_pickle('intermediate_data/data_selection/duplicates_manual.pkl')
combined_data_automatic.to_pickle('intermediate_data/data_selection/combined_data_automatic.pkl')
combined_data.to_pickle('intermediate_data/data_selection/combined_data.pkl')

#%%

