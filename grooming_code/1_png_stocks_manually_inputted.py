#%%
#take in data files. they all have around about the same structure:
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import re
import datetime
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')

#please note that the code here was developed using this: https://chatgpt.com/share/67bfc901-6dac-8000-bb26-71fe6b3267dd
#%%

# Load raw data and templates
png_data_path = 'input_data/png/png_stocks_manually_inputted.xlsx'

png_data = pd.read_excel(png_data_path)
#%%
#drop png_manual_stocks_not_for_use from dataset col
png_data = png_data.loc[~png_data['dataset'].isin(['png_manual_stocks_not_for_use'])]
###################################
#%%
# date id to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

png_data.to_csv('intermediate_data/png/{}_png_stocks_manually_inputted.csv'.format(FILE_DATE_ID), index=False) 
#%%