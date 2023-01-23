#want to look at vehicle turnover rates for data where we have both the vehiclle sales and vehicle registrations data. From that we can tell the amount of vehicles lost from the fleet each year (make sure to consider sales), and then we can calculate the turnover rate as a percentage of the fleet

#%%
#import libraries
import pandas as pd
import numpy as np
import os
import sys
import matplotlib.pyplot as plt
import re
import datetime
# %matplotlib inline

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False
USE_INTERPOLATED_DATA = True
#%%
#laod in data that has been interpolated
file_date = datetime.datetime.now().strftime("%Y%m%d")
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./output_data/', '_interpolated_combined_data_condordance.csv')
FILE_DATE_ID = 'DATE{}'.format(file_date)
interpolated_data = pd.read_csv('output_data/{}_interpolated_combined_data_condordance.csv'.format(FILE_DATE_ID))

#%%
INDEX_COLS = ['Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type', 'Drive', 'Fuel_Type', 'Frequency', 'Date',
       'Dataset']
#Remove year from the current cols without removing it from original list, and set it as a new list
INDEX_COLS_no_year = INDEX_COLS.copy()
INDEX_COLS_no_year.remove('Date')

#%%
#extract measures that are vehicle sales
vehicle_sales = interpolated_data[interpolated_data['Measure'].isin(['vehicle_sales', 'Stocks'])]

#also take a look at new vehicle efficiency
new_vehicle_efficiency = interpolated_data[interpolated_data['Measure'].isin(['new_vehicle_efficiency'])]

#%%