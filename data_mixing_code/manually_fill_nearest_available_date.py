
#intention is to build a quick to use function that will fill in missing dates with the nearest available data if that is available. the hoep is that this will inspire building something simple that can be used to fill in missing data in a more automated way... automated because its liekly this will have to be done over an over again for different data sets.

#%%

#we will use the data from 1_aggregate...
import pandas as pd
import numpy as np
import os
import datetime
import re

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

#%%

import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_concordance_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
concord = pd.read_csv('./intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))

#@and the 9th dsata inn case we need it
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/9th_dataset/', 'combined_dataset_concordance')
FILE_DATE_ID = 'DATE{}'.format(file_date)
concord_9th = pd.read_csv('./intermediate_data/9th_dataset/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))

#and load in the same data for combined data
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data = pd.read_csv('./intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/9th_dataset/', 'combined_dataset')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data_9th = pd.read_csv('./intermediate_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID))

#%%

#we will just go through what is missing, and then if that has a vlaue in the date before or after it. if so put it in a new df. We can load that df in separately to aggregate.

#first identify rows in the base year that are missing values
base_year = '2017-12-31'

#to find where base year data is missing search for differences between the concordance and the combined data. Do this using index rows
INDEX_COLS = ['Medium', 'Transport Type', 'Vehicle Type', 'Drive',  'Economy','Frequency', 'Measure', 'Unit', 'Scope','Fuel_Type','Date']

concord1 = concord[concord['Date'] == base_year]
combined_data1 = combined_data[combined_data['Date'] == base_year]

#set the index to the index cols
concord1 = concord1.set_index(INDEX_COLS)
combined_data1 = combined_data1.set_index(INDEX_COLS)
concord = concord.set_index(INDEX_COLS)
combined_data = combined_data.set_index(INDEX_COLS)
#%%

#now we can find the rows that are missing in the combined data comapred to the concordance
missing_rows = concord1.index.difference(combined_data1.index)
missing_rows = list(missing_rows)
missing_rows_with_nearest_date = []

#create empty version of combined data
combined_data_filled = combined_data.copy()
combined_data_filled = combined_data_filled.iloc[0:0]
for additional_year in [1,2,3,4]:
        
    #now for those rows, loop through the them in combined data and find if they have the same index but for a date + or - 1 year from there. if so, then we can use that value to fill in the missing data.
    upper_date = str(int(base_year[:4])+additional_year) + '-12-31'
    lower_date = str(int(base_year[:4])-additional_year) + '-12-31'

    #now we can loop through the missing rows and find the nearest available date
    for row in missing_rows:

        #now we can check if the row exists in the combined data for the upper and lower dates by replacing the date in the row with the upper and lower dates
        row_upper = row[:-1] + (upper_date,)
        row_lower = row[:-1] + (lower_date,)
        
        #now we can check if the row exists in the combined data for the upper and lower dates
        if row_upper in combined_data.index:
            #if so, then we can add that row and its vlaues from combined data to the list
            #grab row then change the date (which is in the index) to the original date
            new_row = combined_data.loc[row_upper]
            new_row = new_row.reset_index()
            new_row['Date'] = base_year
            new_row = new_row.set_index(INDEX_COLS)

            #create 'Source' column, makie it the Dataset
            new_row['Source'] = new_row['Dataset']
            #change dataset to 'Nearest available date'
            new_row['Dataset'] = 'Nearest_year_{}'.format(str(int(base_year[:4])+additional_year))

            missing_rows_with_nearest_date.append(new_row)
            #drop row from missing rows
            missing_rows.remove(row)
        elif row_lower in combined_data.index:
            #if so, then we can add that row and its vlaues from combined data to the list
            #grab row then change the date to the original date
            new_row = combined_data.loc[row_lower]
            new_row = new_row.reset_index()
            new_row['Date'] = base_year
            new_row = new_row.set_index(INDEX_COLS)

            #create 'Source' column, makie it the Dataset
            new_row['Source'] = new_row['Dataset']
            #change dataset to 'Nearest available date'
            new_row['Dataset'] = 'Nearest_year_{}'.format(str(int(base_year[:4])-additional_year))

            missing_rows_with_nearest_date.append(new_row)
            missing_rows.remove(row)

#%%
#create df from the list of rows, assuming that there may be some entries trhat have double ups (so the shape of list is 3d)
#put the rows into the empty df
for row in missing_rows_with_nearest_date:
    #append row to df
    combined_data_filled = combined_data_filled.append(row)
#reset index
combined_data_filled = combined_data_filled.reset_index()

#%%
#save as csv
combined_data_filled.to_csv('./intermediate_data/estimated/nearest_available_date{}.csv'.format(FILE_DATE_ID), index=False)

#%%
a= combined_data.reset_index()
# %%
