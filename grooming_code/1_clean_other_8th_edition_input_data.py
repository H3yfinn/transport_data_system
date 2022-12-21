#this is intended to be where all data that is used in the model is cleaned before being adjusted to be used in the model.

#CLEANING IS anything that involves changing the format of the data. The next step is filling in missing values. 

#NOTE this data only needs to be available for the base year, as it is then changed by the growth rate that is part of the user input. 
#%%
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
pd.options.plotting.backend = "plotly"#set pandas backend to plotly plotting instead of matplotlib
import plotly.io as pio
pio.renderers.default = "browser"#allow plotting of graphs in the interactive notebook in vscode #or set to notebook
import datetime

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False

#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
#%%
#adjustments
turnover_rate = pd.read_excel('input_data/8th_edition_transport_model/adjustments_spreadsheet.xlsx', sheet_name='Turnover_Rate')

occupance_load = pd.read_excel('input_data/8th_edition_transport_model/adjustments_spreadsheet.xlsx', sheet_name='OccupanceAndLoad')

new_vehicle_efficiency = pd.read_excel('input_data/8th_edition_transport_model/adjustments_spreadsheet.xlsx', sheet_name='new_vehicle_efficiency')

#%%
#adjust adjustments data
#make Vehicle Type and Drive cols lowercase 
occupance_load['Vehicle Type'] = occupance_load['Vehicle Type'].str.lower()

new_vehicle_efficiency['Vehicle Type'] = new_vehicle_efficiency['Vehicle Type'].str.lower()
new_vehicle_efficiency['Drive'] = new_vehicle_efficiency['Drive'].str.lower()

turnover_rate['Vehicle Type'] = turnover_rate['Vehicle Type'].str.lower()


#%%
#replicate data so we have data for each scneario in the adjustments data. We can dcide later if we want to explicitly create diff data for the scenrios later or always replicate,, or even rpovdie a switch

occupance_load_CN = occupance_load.copy()
occupance_load_CN['Scenario'] = 'Carbon Neutral'
occupance_load['Scenario'] = 'Reference'
occupance_load = pd.concat([occupance_load, occupance_load_CN])

turnover_rate_CN = turnover_rate.copy()
turnover_rate_CN['Scenario'] = 'Carbon Neutral'
turnover_rate['Scenario'] = 'Reference'
turnover_rate = pd.concat([turnover_rate, turnover_rate_CN])

#create data for 2017 using the data from 2018
turnover_rate_2017 = turnover_rate[turnover_rate['Year']==2018].copy()
turnover_rate_2017['Year'] = 2017
turnover_rate = pd.concat([turnover_rate, turnover_rate_2017])

#%%
#rename cols
occupance_load.rename(columns={"Value": "Occupancy_or_load"}, inplace=True)
turnover_rate.rename(columns={"Value": "Turnover_rate"}, inplace=True)
new_vehicle_efficiency.rename(columns={"Value": "New_vehicle_efficiency"}, inplace=True)

#ideal to change spreadhseet than renae here

#%%
#SAVE
turnover_rate.to_csv('intermediate_data/8th_edition_transport_model/turnover_rate.csv', index=False)
occupance_load.to_csv('intermediate_data/8th_edition_transport_model/occupance_load.csv', index=False)

new_vehicle_efficiency.to_csv('intermediate_data/8th_edition_transport_model/new_vehicle_efficiency.csv', index=False)
#%%