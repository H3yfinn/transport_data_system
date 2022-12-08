#use this to gather together data that is already cleaned and put it into the same dataset. Once thats been done we can pass it to the next script to select the best data for each time period

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

#load in the cleaned datasets here and then deal with them in a cell each
ATO_dataset_clean = pd.read_csv('output_data/ATO_data/ATO_dataset_clean_{}.csv'.format(FILE_DATE_ID))

eigth_edition_transport_data = pd.read_csv('output_data/8th_edition_transport_model/eigth_edition_transport_data_{}.csv'.format(FILE_DATE_ID))

#%%
#handle ATO dataset
ATO_dataset_clean['Dataset'] = 'ATO'
#make Vehicle Type column instead of Vehicle type with no capital
ATO_dataset_clean.rename(columns={'Vehicle type':'Vehicle Type'}, inplace=True)
#same for transport type
ATO_dataset_clean.rename(columns={'Transport type':'Transport Type'}, inplace=True)

#%%
#handle transport model dataset

#%%
#join data together
combined_data = ATO_dataset_clean.append(eigth_edition_transport_data, ignore_index=True)


#%%
# #create a concordance which contains all the unique rows in the combined data df, when you remove the Dataset and value columns.
# combined_data_concordance = combined_data.drop(columns=['Dataset', 'Value']).drop_duplicates()
# #we will also find the max and min Year value in the whole dataset. Then remove the year column
# MAX = combined_data['Year'].max()
# MIN = combined_data['Year'].min()
# combined_data_concordance.drop(columns=['Year'], inplace=True)
# # Then for every unique row, create a df with a row for each year between the min and max year. Then join all the rows together to create a concordance
# #create empty df to append to
# combined_data_concordance_new = pd.DataFrame()
# for index, row in combined_data_concordance.iterrows():
#     for year in range(MIN, MAX):
#         #set the year value as an int (not a float)
#         row['Year'] = int(year)
#         combined_data_concordance_new = combined_data_concordance_new.append(row, ignore_index=True)
        
#create a concordance which contains all the unique rows in the combined data df, when you remove the Dataset and value columns.
combined_data_concordance = combined_data.drop(columns=['Dataset', 'Value']).drop_duplicates()
#we will also find the max and min Year value in the whole dataset. Then remove the year column
MAX = combined_data['Year'].max()
MIN = combined_data['Year'].min()
combined_data_concordance.drop(columns=['Year'], inplace=True)

#create empty df to append to
combined_data_concordance_new = pd.DataFrame()
#create an array with the years in the range MIN to MAX
years = np.arange(MIN, MAX)

#calculate the number of rows in the concordance so we can track how long the script might run for
num_rows = len(combined_data_concordance.index)
#split num_rows number into 10 equally spaced numbers so we can print out progress when we are 10% done, 20% done etc
progress_markers = np.linspace(0, num_rows, 10, dtype=int)
#start timer
start = datetime.datetime.now()

run_this = False
if run_this:
    #iterate through the unique rows in the concordance
    for index, row in combined_data_concordance.iterrows():
        #use the repeat method to create a new row for each year in the range
        repeated_row = pd.DataFrame(np.repeat([row], len(years), axis=0))
        #set the year column in the repeated row to the array of years
        repeated_row['Year'] = years
        #append the repeated row to the concordance
        combined_data_concordance_new = combined_data_concordance_new.append(repeated_row, ignore_index=True)
        #print progress if we are 10% done, 20% done etc
        if index in progress_markers:
            print('{}% done'.format(int(index/num_rows*100)))
            print('Time elapsed: {}'.format(datetime.datetime.now() - start))#FYI this took me 2 hrs to do. it needs to be much much faster if we are going to scale to 10x data


    #set all column names except the Year to be the same as the combined data concordance
    #set Year col as an index 
    combined_data_concordance_new.set_index('Year', inplace=True)
    combined_data_concordance_new.columns = combined_data_concordance.columns
    #reset the index so that the Year column is a column again
    combined_data_concordance_new.reset_index(inplace=True)
    combined_data_concordance_new.to_csv('output_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID), index=False)
else:
    #for now rename the file with the current date id using shutil
    import shutil
    shutil.copy('output_data/combined_dataset_concordance_DATE20221205.csv', 'output_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
    #delete old file
    os.remove('output_data/combined_dataset_concordance_DATE20221205.csv')
#%%

#save
combined_data.to_csv('output_data/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)

#%%