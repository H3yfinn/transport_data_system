#since some economies are very unlikely to have the more recent drive types like fcevs or even just data on things like phevd, phevg, we will load in a sheet which shows the data we are missing for these drive types and just set them to 0. This was created using the transport models function for spotting whether there is data missing by comparing against a concordance. 

#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import pickle
import matplotlib.pyplot as plt
import warnings
import sys

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

FILE_DATE_ID = 'DATE' + datetime.datetime.now().strftime('%Y%m%d')
#%%
#load in the data
missing_drive_values = pd.read_csv('input_data/9th_transport_model/missing_drive_values.csv')#, index=False)

# %%
#set value to 0
missing_drive_values['Value'] = 0
#set dataset to 'missing_drive_values'
missing_drive_values['Dataset'] = 'missing_drive_values'
#set source to '9th_transport_model'
missing_drive_values['Source'] = '9th_transport_model'
#set date to the Date plus '-12-31'
missing_drive_values['Date'] = missing_drive_values['Date'].astype(str)+'-12-31'
missing_drive_values['Fuel'] = 'all'
missing_drive_values['Scope'] = 'National'
# %%
#save to csv
missing_drive_values.to_csv(f'intermediate_data/estimated/filled_missing_values/missing_drive_values_{FILE_DATE_ID}.csv', index=False)
# %%


