
#%%
import datetime
import pandas as pd
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
combined_dataset = pd.read_csv('output_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))
combined_data_concordance= pd.read_csv('output_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
#%%
#visualise what data points we have multiple data for by creating a table that shows what rows have duplicates when we ignore the value and dataset columns:

#But first things first we need to make sure there are no unexpected values in the dataset, otehrwsie they could be values which are miscategorised and therefore we might not realise when we have more than one of a given datapoint. We will do this by printing a strongly worded statement to the user to double check that there are no unexpected values in the dataset!
print('Dear User this is a strongly worded statement to check that there are no unexpected values in the dataset. Thank you for your time. ')

#%%
#Now we can find the rows with duplicates (but make sure to show what dataset the duplicates are in)).
#we will track how many of each row there are and for what datasets they are in.

current_cols = ['Year', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type','Drive']#these are the cols which are tracked in the dataset as being essential to identify a datapoint

duplicates = combined_dataset.copy()

#create a list of the datasets for each unique row in a new column called datasets and then count how many of each there are in that list and put it in a new column called count
duplicates = duplicates.groupby(current_cols).agg({'Dataset': lambda x: list(x)}).reset_index()
#create count column
duplicates['Count'] = duplicates['Dataset'].apply(lambda x: len(x))
#rename dataset to datasets
duplicates.rename(columns={'Dataset':'Datasets'}, inplace=True)
# #separate the dataset column into multiple columns so that it is easier to read. so this will find the comma separated list of datasets and then split it into a list of datasets and then create a new column for each dataset
# # duplicates = duplicates.join(duplicates['Datasets'].str.split(', ', expand=True).add_prefix('Dataset_'))#I BELIEVE THIS SHOULD WORK BUT NEVER GOT TO TRY IT WHEN THERE WAS MORE THAN ONE DUPLICATE
# #drop the old datasets column
# duplicates = duplicates.drop(columns=['Datasets'])

#%%
#put it into a csv file for the viewer to look at in a spreadsheet (this seems like the best way to do it)
duplicates.to_csv('output_data/duplicates{}.csv'.format(FILE_DATE_ID))

# #open the csv file for the user
# os.startfile('output_data/duplicates{}.csv'.format(FILE_DATE_ID))
#%%
