
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
FILE_DATE_ID = 'DATE{}'.format(file_date)
# FILE_DATE_ID = ''
 
#%%
#this is for determining what are the best datpoints to use when there are two or more datapoints for a given time period

#load data
# FILE_DATE_ID = 'DATE20221205'
combined_dataset = pd.read_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))
combined_data_concordance= pd.read_csv('intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
#%%
#visualise what data points we have multiple data for by creating a table that shows what rows have duplicates when we ignore the value and dataset columns:

#But first things first we need to make sure there are no unexpected values in the dataset, otehrwsie they could be values which are miscategorised and therefore we might not realise when we have more than one of a given datapoint. We will do this by printing a strongly worded statement to the user to double check that there are no unexpected values in the dataset!
print('Dear User this is a strongly worded statement to check that there are no unexpected values in the dataset. Thank you for your time. ')

#%%
#Now we can find the rows with duplicates (but make sure to show what dataset the duplicates are in)).
#we will track how many of each row there are and for what datasets they are in.

#first chekc for duplicated rows when we ignore the vlaue column
duplicates = combined_dataset.copy()
duplicates = duplicates.drop(columns=['Value'])
duplicates = duplicates[duplicates.duplicated(keep=False)]
#If there are any duplicates then we will print them out to the user and tell the user to fix them before continuing
if len(duplicates) > 0:
       print('There are duplicate rows in the dataset with different Values. Please fix them before continuing. You will probably want to split them into different datasets. The duplicates are: ')
       print(duplicates)
       raise Exception('There are duplicate rows in the dataset. Please fix them before continuing')
        

#%%
INDEX_COLS = ['Year', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type','Drive']#these are the cols which are tracked in the dataset as being essential to identify a datapoint

duplicates = combined_dataset.copy()

#create a list of the datasets for each unique row in a new column called datasets and then count how many of each there are in that list and put it in a new column called count
duplicates = duplicates.groupby(INDEX_COLS).agg({'Dataset': lambda x: list(x)}).reset_index()
#make sure the lists are sorted so that the order is consistent
duplicates['Dataset'] = duplicates['Dataset'].apply(lambda x: sorted(x))
#create count column
duplicates['Count'] = duplicates['Dataset'].apply(lambda x: len(x))
#rename dataset to datasets
duplicates.rename(columns={'Dataset':'Datasets'}, inplace=True)

#%%
#put it into a csv file for the viewer to look at in a spreadsheet (this seems like the best way to do it)
duplicates.to_csv('intermediate_data/duplicates{}.csv'.format(FILE_DATE_ID), index=False)

# #open the csv file for the user
# os.startfile('intermediate_data/duplicates{}.csv'.format(FILE_DATE_ID))
#%%
