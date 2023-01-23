#communicate_whole_dataset_details.py
#%%
#This script will go through the dataset we have aggregated in 1_aggregate_cleaned_datasets.py and communicate the details of the dataset and datasets within it to the user.
#It will use the categories in the columns, like the datset column, to specify useful information so that the user can quickly identify what is in the dataset and what is not. This is in a style that can also be repurposed to be used in the documentation of the dataset.
#We can also use this script to identify any problems with the dataset, like missing data, or data that is in a category that we werent expecting to see in the dataset. This would be done with a sense check of the outputs by the user.

############################################################################
#%%
#load in the dataset
#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import plotly.express as px
pd.options.plotting.backend = "plotly"#set pandas backend to plotly plotting instead of matplotlib
import plotly.io as pio
pio.renderers.default = "browser"#allow plotting of graphs in the interactive notebook in vscode #or set to notebook
import plotly.graph_objects as go
import plotly

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False

#%%
#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
# FILE_DATE_ID = ''

#%%
#this is for determining what are the best datpoints to use when there are two or more datapoints for a given time period

#load data
combined_dataset = pd.read_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))

#%%
############################################################################

#NOW FOR EACH DATASET WITHIN THE COMBINED DATASET WE WANT TO COMMUNICATE THE DETAILS OF THE DATASET TO THE USER:
#Note that we will format a kind of bullet point system using two spaces and a hyphen for each bullet point. This will be used to communicate the details of the dataset to the user more clearly. But more specific bullet points will be indented with four spaces and a hyphen, and then 6 spaces and a hyphen for more specific bullet points. This will be used to communicate the details of the dataset to the user more clearly.

#So for each unique dataset in the dataset column:
#print the name of the dataset
#Then for each column, then that will have more indented bullet points
#and we will print the following details about the data in that column for the dataset:

#1 If the columnn is a characters column, We will print each unique value in the column separated by a comma
#2 if the column is the Value column then for each unique value in the measure column we will print the min, max, mean, median, and standard deviation of the vlaues in the Value column for the unqiuie value in the Measure column
# 3. if it is a Year or Date col,  you will get the min and max dates. 
#BUT note that if there is a nan value in the column then it will cause errors so we will need to remove the nan values before we do this and print them as a separate bullet point

na_dataset = combined_dataset[combined_dataset.isna().any(axis=1)]
combined_dataset = combined_dataset.dropna()
dataset_details= ''
for dataset in combined_dataset.Dataset.unique():
    #get the dataset
    dataset_df = combined_dataset[combined_dataset.Dataset == dataset]
    #get the categories
    categories = dataset_df.columns
    #create a string to print out the details of the dataset
    dataset_details += '\n\nDataset: {}\r\n'.format(dataset)
    #for each column, get the details of the column
    for column in categories:
        #get the details of the column
        dataset_details += '  - Category: {}\r\n'.format(column)
        #if the column is the Value column then for each unique value in the measure column we will print the min, max, mean, median, and standard deviation of the vlaues in the Value column for the unqiuie value in the Measure column
        if column == 'Value':
            for measure in dataset_df.Measure.unique():
                dataset_details += '    - Measure: {}\r\n'.format(measure)
                dataset_details += '      - Min: {}\r\n'.format(dataset_df[dataset_df.Measure == measure].Value.min())
                dataset_details += '      - Max: {}\r\n'.format(dataset_df[dataset_df.Measure == measure].Value.max())
                dataset_details += '      - Mean: {}\r\n'.format(dataset_df[dataset_df.Measure == measure].Value.mean())
                dataset_details += '      - Median: {}\r\n'.format(dataset_df[dataset_df.Measure == measure].Value.median())
                dataset_details += '      - Standard Deviation: {}\r\n'.format(dataset_df[dataset_df.Measure == measure].Value.std())
        #if the column is a characters column, We will print each unique value in the column separated by a comma
        elif dataset_df[column].dtype == 'object':
            dataset_details += '    - Values: {}\r\n'.format(','.join(dataset_df[column].unique()))
        #except if it is a Year or Date, in which case you will get the min and max dates. 
        elif column == 'Year' or column == 'Date':
            dataset_details += '    - Min: {}\r\n'.format(dataset_df[column].min())
            dataset_details += '    - Max: {}\r\n'.format(dataset_df[column].max())
        
    else:
        #if the column is not a number or a character, then we will just print the type of the column
        dataset_details += '    - Type: {}\r\n'.format(dataset_df[column].dtype)
    dataset_details += '##############################################\n\n'

############################################################################
#%%
#now print details about the na values in the dataset
#its important that we understand what columns they occured in and for what values in those columns they occured in other rows for, and how many times. This will help us to understand what the problem is and how to fix it, but also not provide too much information that it is overwhelming.
#so we will print the following details about the na values in the dataset, for each dAtaset in the dataset column:
#1 The number of na values in the df  
#2 The number of na values in each column (so ignore this if it is the Value column, and altogether ignore if this is the na_value_column_dataset)
#3 For each non na value in each column, how many rows are there of that unqique value as that will help us to understand what the cause of NA's is and how to fix it
na_value_column_dataset = na_dataset[na_dataset.Value.isna()]
not_na_value_column_dataset = na_dataset[na_dataset.Value.notna()]
na_dataset_details = '##############################################\n\n'
na_dataset_details += 'Details about the na values in the dataset:\r'
for dataset in na_dataset.Dataset.unique():
    #get the dataset
    na_dataset_df = not_na_value_column_dataset[not_na_value_column_dataset.Dataset == dataset]
    na_dataset_df_value_col = na_value_column_dataset[na_value_column_dataset.Dataset == dataset]

    #create a new string to print out the details of the na values in the dataset
    na_dataset_details += '\n\nDataset: {}\r\n'.format(dataset)
    #1 The number of na values in the df
    na_dataset_details += '  - Number of NA Values: {}\r\n'.format(len(na_dataset_df))
    #2 The number of na values in each column (so ignore this if it is the Value column, and altogether ignore if this is the na_value_column_dataset)

    for column in na_dataset_df.columns:
        if column != 'Value':
            na_dataset_details += '    - Number of NA Values in {}: {}\r\n'.format(column, len(na_dataset_df[na_dataset_df[column].isna()]))
    #add a new line
    na_dataset_details += '\r\n'

    #3 For each non na value in each column, how many rows are there of that unqique value as that will help us to understand what the cause of NA's is and how to fix it
    for column in na_dataset_df.columns:
        if column != 'Value':
            if len(na_dataset_df[column].value_counts()) > 0:
                na_dataset_details += '    - Number of Rows for each Unique Value in {}: {}\r\n'.format(column, na_dataset_df[column].value_counts())
    #add a new line
    na_dataset_details += '\r\n'
    
    #now print the details of the not_na_value_column_dataset
    #1 The number of na values in the df
    na_dataset_details += '  - Number of NA Values in Value Column: {}\r\n'.format(len(na_dataset_df_value_col))
    
    #add a new line
    na_dataset_details += '\r\n'
    
#Now we will print the details of the dataset
print(dataset_details)
#Now we will print the details of the na values in the dataset
print(na_dataset_details)

#Done!
#%%
#save dataset details to a new text file in the plotting_output/text_content folder: dataset_details{}.tx
with open('plotting_output/text_content/dataset_details{}.txt'.format(FILE_DATE_ID), 'w') as f:
    f.write(dataset_details)
    f.write(na_dataset_details)

    

# %%

