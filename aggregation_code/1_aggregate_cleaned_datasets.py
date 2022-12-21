#use this to gather together data that is already cleaned and put it into the same dataset. Once thats been done we can pass it to the next script to select the best data for each time period

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
# FILE_DATE_ID = 'DATE20221212'
#%%

#load in the cleaned datasets here and then deal with them in a cell each
ATO_dataset_clean = pd.read_csv('intermediate_data/ATO_data/ATO_dataset_clean_{}.csv'.format(FILE_DATE_ID))

eigth_edition_transport_data = pd.read_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_{}.csv'.format(FILE_DATE_ID))
#remove all 8th edition data that is from the reference and carbon neutrality scenarios
eigth_edition_transport_data = eigth_edition_transport_data[eigth_edition_transport_data['Dataset'] == '8th edition transport model']

#%%
#handle ATO dataset
ATO_dataset_clean['Dataset'] = 'ATO'
#make Vehicle Type column instead of Vehicle type with no capital
ATO_dataset_clean.rename(columns={'Vehicle type':'Vehicle Type'}, inplace=True)
#same for transport type
ATO_dataset_clean.rename(columns={'Transport type':'Transport Type'}, inplace=True)

#change the unit called number of vehicles on road to stocks
ATO_dataset_clean['Unit'] = ATO_dataset_clean['Unit'].replace('number of vehicles on road', 'Stocks')

#change transport type values from ['Freight', 'Passenger', 'Combined'] to ['freight', 'passenger', 'combined'] (lowercase)
ATO_dataset_clean['Transport Type'] = ATO_dataset_clean['Transport Type'].str.lower()
#also make all Medium values lowercase
ATO_dataset_clean['Medium'] = ATO_dataset_clean['Medium'].str.lower()
#remove nan values in vlaue column
ATO_dataset_clean = ATO_dataset_clean[ATO_dataset_clean['Value'].notna()]
#%%
#handle transport model dataset

#%%
#join data together
combined_data = ATO_dataset_clean.append(eigth_edition_transport_data, ignore_index=True)

#filter data where year is less than 2010
combined_data = combined_data[combined_data['Year'] >= 2010]

#%%
#Important step: make sure that units are the same for each measure so that they can be compared. If they are not then the measure should be different.
#For example, if one measure is in tonnes and another is in kg then they should just be converted. But if one is in tonnes and another is in number of vehicles then they should be different measures.
for measure in combined_data['Measure'].unique():
    if len(combined_data[combined_data['Measure'] == measure]['Unit'].unique()) > 1:
        print(measure)
        print(combined_data[combined_data['Measure'] == measure]['Unit'].unique())
        raise Exception('There are multiple units for this measure. This is not allowed. Please fix this before continuing.')
#%%
#CREATE CONCORDANCE
#create a concordance which contains all the unique rows in the combined data df, when you remove the Dataset and value columns.
combined_data_concordance = combined_data.drop(columns=['Dataset', 'Value']).drop_duplicates()
#we will also find the max and min Year value in the whole dataset. Then remove the year column
MAX = combined_data['Year'].max()
MIN = combined_data['Year'].min()
combined_data_concordance = combined_data_concordance.drop(columns=['Year']).drop_duplicates()

#create empty df to append to
combined_data_concordance_new = pd.DataFrame()
#create an array with the years in the range MIN to MAX
years = np.arange(MIN, MAX+1)

#calculate the number of rows in the concordance so we can track how long the script might run for
num_rows = len(combined_data_concordance.index)
#split num_rows number into 10 equally spaced numbers so we can print out progress when we are 10% done, 20% done etc
progress_markers = np.linspace(0, num_rows, 10, dtype=int)
#start timer so we can give an estimate of how long the script will take to run
start = datetime.datetime.now()

#%%
run_this = True
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
            print('Time elapsed: {}'.format(datetime.datetime.now() - start))

    #set all column names except the Year to be the same as the combined data concordance
    #set Year col as an index 
    combined_data_concordance_new.set_index('Year', inplace=True)
    combined_data_concordance_new.columns = combined_data_concordance.columns
    #reset the index so that the Year column is a column again
    combined_data_concordance_new.reset_index(inplace=True)

    combined_data_concordance_new.to_csv('intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID), index=False)

#%%

#save
combined_data.to_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)

#%%