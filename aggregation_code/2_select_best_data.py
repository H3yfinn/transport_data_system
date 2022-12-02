
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
FILE_DATE_ID = '_DATE{}'.format(file_date)
# FILE_DATE_ID = ''

#%%
#this is for determining what are the best datpoints to use when there are two or more datapoints for a given time period

#load data
combined_dataset = pd.read_csv('output_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))

#%%
#visualise what data points we have multiple data for by creating a table that shows what rows have duplicates when we ignore the value and dataset columns:

#But first things first we need to make sure there are no unexpected values in the dataset, otehrwsie they could be values which are miscategorised and therefore we might not realise when we have more than one of a given datapoint. We will do this by printing a strongly worded statement to the user to double check that there are no unexpected values in the dataset!
print('Dear User this is a strongly worded statement to check that there are no unexpected values in the dataset. Thank you for your time. ')

#%%
#Now we can find the rows with duplicates (but make sure to show what dataset the duplicates are in)).
#we will print how many of each row there are and for what datasets they are in.
#but make sure that it is in a way that is easy to read and understand, not a dataframe

current_cols = ['Year', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type','Drive']

#get a dataset of all the rows that have duplicates
duplicates = combined_dataset[combined_dataset.duplicated(subset=current_cols, keep=False)]
#now we need to group by the current_cols and then count how many of each there are for each dataset
duplicates = duplicates.groupby(current_cols).agg({'Dataset': ['count', lambda x: ', '.join(x)]})
#change the column names to be more readable
duplicates.columns = ['Count', 'Datasets']
#sort by count
duplicates = duplicates.sort_values(by='Count', ascending=False)
#reset the index
duplicates = duplicates.reset_index()
#separate the dataset column into multiple columns so that it is easier to read. so this will find the comma separated list of datasets and then split it into a list of datasets and then create a new column for each dataset
duplicates = duplicates.join(duplicates['Datasets'].str.split(', ', expand=True).add_prefix('Dataset_'))#I BELIEVE THIS SHOULD WORK BUT NEVER GOT TO TRY IT WHEN THERE WAS MORE THAN ONE DUPLICATE
#drop the old datasets column
duplicates = duplicates.drop(columns=['Datasets'])
#put it into a csv file for the viewer to look at in a spreadsheet (this seems like the best way to do it)
duplicates.to_csv('output_data/duplicates{}.csv'.format(FILE_DATE_ID))

# #open the csv file for the user
# os.startfile('output_data/duplicates{}.csv'.format(FILE_DATE_ID))
#%%

#now we need to find the rows that have duplicates and then find the best datapoint to use for each row
#we will provide an automatic way of doing this and then also allow the user to manually select the best datapoint for each row
#in the automatic way we will use the following rules in an order of priority from 1 being the highest priority to n being the lowest priority:
#1. if there are two or more datapoints for a given row and one is from the same dataset as the previous year for that same unique row then use that one
#2. if there are two or more datapoints for a given row and one is from the same dataset as the next year for that same unique row then use that one
#3. if there are two or more datapoints for a given row and one is 25% closer to the previous year for that same unique row then use that one
#4. if there are two or more datapoints for a given row and one is 25% closer to the next year for that same unique row then use that one
#5 if none of the above apply then raise an error and ask the user to manually select the best datapoint for each row

#%%
#this can probably be done using github copilot ina really cool way so I will try that


