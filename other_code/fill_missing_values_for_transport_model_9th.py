#intention is to build this so that it can be used to fill missing vlaues before the output form this system is imported into the model. This is done here, where it is not a part of the main transport data system because the vlaues that arte filled will be non legitamate vlaues. for example, estimating passenger air activity based on revenue passenger km. These are things that shouldnt be done in the model either because it will make the model too cluttered for anyone who wants to run it independent of this transport datas ystem.

#first import data from the output for transport data system and also the missing vlaues as defined within the transpoort model. 
#then we will go thorugh the missing vlaues and fill them where we can (it might also instigate filling the missing vlauesa with actual vlaues.)

#%%
import pandas as pd
import numpy as np
import os
import datetime
import re
#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')


#%%
# missing values:
# E:/APERC/transport_model_9th_edition/output_data/for_other_modellers/missing_values.csv
missing_values = pd.read_csv('../transport_model_9th_edition/output_data/for_other_modellers/missing_values/_missing_input_values.csv')

#now lets plot those missing values in a way that is easy to visualise. This means:
#since most missing data points are missing for every economy, dont need to plot them
#data is only for one year but perhaps search for the same data point in other years
#drive and vehicle type can probably be in the same col
#there are some measures for which we have no data at all. just separate them as its clear we dont have data for them already eg. New_vehicle_efficiency,Occupancy_or_load,Turnover_rate (we can also print the number of missing values for each measure to understand this)
#Stocks needs some looking at 
# %%
#STOCKS
#load data
file_date = datetime.datetime.now().strftime("%Y%m%d")
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_dataset = pd.read_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))

#filter for stocks only
#%%
#now lets plot those missing values in a way that is easy to visualise. This means: