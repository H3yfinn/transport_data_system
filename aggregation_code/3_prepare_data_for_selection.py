#The intention is to guide the user through picking the best dataset to use for each datapoint where there are multiple. To speed things up it also uses an automatic method if the user is happy with it. If this is done then the user will only end up having to use the manual method for perhaps a few if no datapoints that were too tricky for the automatic method. The manual method will use the python prompt method to ask what datapoint the user wants to sue, as well as showing a graph of the different datapoints in the whole time series.
#The automatic method currently has these steps:
#in the automatic method we will use the following rules in an order of priority from 1 being the highest priority to n being the lowest priority:
#1. if there are two or more datapoints for a given row and one is from the same dataset as the previous year for that same unique row then use that one
#2. if there are two or more datapoints for a given row and all but one dataset is missing in the next year for that same unique row then use the one that is not missing
#3. if there are two or more datapoints for a given row and one is closer and within 25% of the previous year, then use that one
#4 if none of the above apply then ask the user to manually select the best datapoint for each row, if it hasn't been done already in the past
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

#if using jupyter notebook then set the backend to inline so that the graphs are displayed in the notebook instead of in a new window

# %matplotlib inline

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

import data_selection_functions as data_selection_functions
PRINT_GRAPHS_AND_STATS = False

#%%
#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
# FILE_DATE_ID = 'DATE20221206'

#%%
#this is for determining what are the best datpoints to use when there are two or more datapoints for a given time period

#load data
# FILE_DATE_ID = 'DATE20221205'
combined_data_concordance= pd.read_csv('intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
combined_data = pd.read_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))

#%%
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'duplicates')
FILE_DATE_ID = 'DATE{}'.format(file_date)

#open the csv file with duplicates in it
duplicates = pd.read_csv('intermediate_data/duplicates{}.csv'.format(FILE_DATE_ID))

#datasets column is being converted to a string for some reason, we will convert it back to a list
duplicates['Datasets'] = duplicates['Datasets'].apply(lambda x: eval(x))

#%%
INDEX_COLS = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type','Drive','Fuel_Type','Frequency','Scope']
#Remove year from the current cols without removing it from original list, and set it as a new list
INDEX_COLS_no_year = INDEX_COLS.copy()
INDEX_COLS_no_year.remove('Date')


#%%
#filter for the years we want to bother selecting data for
EARLIEST_YEAR = "2010-01-01"
LATEST_YEAR = '2020-01-01'#datetime.datetime.now().year
#filter data where year is less than 2010
combined_data = combined_data[combined_data['Date'] >= EARLIEST_YEAR]
combined_data_concordance = combined_data_concordance[combined_data_concordance['Date'] >= EARLIEST_YEAR]
duplicates = duplicates[duplicates['Date'] >= EARLIEST_YEAR]
#and also only data where year is less than the current year
combined_data = combined_data[combined_data['Date'] < LATEST_YEAR]
combined_data_concordance = combined_data_concordance[combined_data_concordance['Date'] < LATEST_YEAR]
duplicates = duplicates[duplicates['Date'] < LATEST_YEAR]

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












