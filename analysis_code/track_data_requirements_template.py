#intention is to build a system that makes it easy to track what data is required for a given analysis and what data is available
#this is a work in progress

#so it should take in a dataframe with cols for economy, year, medium, transport type, vehicle type, drive, and MOST IMPORTANTLY, the measure that is required. 
#these input data files should ideally be the same files that are used for the analysis, so that the data is already in the correct format and there is no doubling up of work for either task.
#the output should then be:
# 1. the input data file with a new column that indicates whether the data is available or not
# 2. a list of the data that is missing, with the economy, year, medium, transport type, vehicle type, drive, and measure that is missing
# 3. a list of the data that is available, with the economy, year, medium, transport type, vehicle type, drive, and measure that is available
# 4. most importantly, well designed graphs to quickly convery the information in 2 and 3

#take in the concordances table from the transport model:

#check what data in the concordances we ahve in the transport database

#%%
#this will probably6 end up being a file thast is used in other systems to see wat data in the transport data sytstem is available.

#%%
#load in the concordacne for data we need for the application
import pandas as pd
import numpy as np 

#load in the concordance (in this case for the transport model)
model_concordances_version = '_v1.0'
model_concordances_all_file_name = 'model_concordances_all{}.csv'.format(model_concordances_version)
concordance = pd.read_csv('./input_data/concordances/{}'.format(model_concordances_all_file_name))