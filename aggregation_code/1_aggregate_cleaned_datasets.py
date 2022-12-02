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
FILE_DATE_ID = '_DATE{}'.format(file_date)
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
# %%

#save
combined_data.to_csv('output_data/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)

#%%