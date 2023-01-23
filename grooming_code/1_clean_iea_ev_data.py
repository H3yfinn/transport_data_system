#import data from iea's evs dataset here:
#and convert it to a format suitable for the transport data system
#this data will be especail;ly useful for filling in the holes we have for stocks of drives. I imagine thaT we will jsut fulfill the activity per drive by timesing drive_share_of_stock by activity_per_vehicle_type or summin.
#also this dataset contians projections to 2030 (useful for estimating good forecasting amounts and testing accuracy of the model)
#also this dataset contains data on charging points (useful for estimating charging infrastructure requirements)
#also this dataset contains data on oil displacement (useful for estimating oil demand decrease)
#also this dataset contains data on electricity demand (useful for estimating electricity demand increase and energy demand inncrease. Perhaps we can try estiamte activity from this and some efficiency data somewhere too?)

#%%
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
import datetime

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False

#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

evs = pd.read_csv('input_data/IEA/IEA-EV-data.csv')
#%%
#convert region to economy code where possible
#first import the economy code to region mapping
economy_name_to_code = pd.read_csv('config/economy_code_to_name.csv')

#stack the Economy name, alt_name and alt_name2 columns into a single column
economy_name_to_code = economy_name_to_code.melt(id_vars=['Economy'], value_vars=['Economy_name', 'Alt_name', 'Alt_name2'], var_name='column_name', value_name='Economy name').drop(columns=['column_name'])

#now join the economy name to code mapping to the evs data
evs = evs.merge(economy_name_to_code, how='left', left_on='region', right_on='Economy name')
#see where it didn't work
evs[evs['Economy'].isna()].region.unique()
#'Belgium', 'Brazil', 'Denmark', 'Europe', 'Finland', 'France',
    #    'Germany', 'Greece', 'Iceland', 'India', 'Italy', 'Netherlands',
    #    'Norway', 'Other Europe', 'Poland', 'Portugal',
    #    'Rest of the world', 'South Africa', 'Spain', 'Sweden',
    #    'Switzerland', 'United Kingdom',
#checked the above m,anually and they are all correct
#so remove them all
evs = evs[~evs['Economy'].isna()]
#%%
#we will change 'category' to 'source', 'parameter' to 'measure', 'mode' to 'vehicle type', powertrain to 'drive'
evs.rename(columns={'category':'source', 'parameter':'measure', 'mode':'vehicle type', 'powertrain': 'drive'}, inplace=True)
#drop region and economy name
evs.drop(columns=['region', 'Economy name'], inplace=True)
#%%
#now within the measure names, rename them to be consistent with the other data sources. First print the unique values
evs.measure.unique()
# 'EV sales', 'EV stock', 'EV sales share', 'EV stock share',
#        'EV charging points', 'Oil displacement Mbd',
#        'Oil displacement Mlge', 'Electricity demand'
#rename the above to:
# 'Sales', 'Stocks', 'Sales share', 'Stock share', 'EV Charging points','Oil displacement Mbd', 'Oil displacement Mlge', 'Energy'
evs['measure'] = evs['measure'].replace({'EV sales':'Sales', 'EV stock':'Stocks', 'EV sales share':'Sales share', 'EV stock share':'Stock share', 'EV charging points':'EV Charging points', 'Oil displacement Mbd':'Oil displacement Mbd', 'Oil displacement Mlge':'Oil displacement Mlge', 'Electricity demand':'Energy'})
#and change v type
evs['vehicle type'].unique()#'Cars', 'EV', 'Vans', 'Buses', 'Trucks'
 #Bit confused about 'EV' so take a look:
evs[evs['vehicle type']=='EV']#it's for charging points so make it NA
# so change all to: ldv, np.nan, ldv, bus, ht
evs['vehicle type'] = evs['vehicle type'].replace({'Cars':'ldv', 'EV':np.nan, 'Vans':'ldv', 'Buses':'bus', 'Trucks':'ht'})
#and change drive
evs['drive'].unique()
# 'BEV', 'EV', 'PHEV', 'Publicly available fast',
#        'Publicly available slow'
#check out EV
evs[evs['drive']=='EV']#it looks like its sales for 'bevs' specicifically but it would be good to double chekc online TODO
#change to 'bev', 'EV' (for now), 'phev', np.nan, np.nan
evs['drive'] = evs['drive'].replace({'BEV':'bev', 'EV':'EV', 'PHEV':'phev', 'Publicly available fast':np.nan, 'Publicly available slow':np.nan})
evs['unit'].unique()#'sales', 'stock', 'percent', 'charging points',
    #    'Milion barrels per day', 'Milion litres gasoline equivalent',
    #    'GWh']
#keep all the same but we are going to change gwh to pj
evs['unit'] = evs['unit'].replace({'GWh':'PJ'})
#and convert the values to PJ
evs.loc[evs['unit']=='PJ', 'value'] = evs.loc[evs['unit']=='PJ', 'value'] * 3.6#TODO check conversion works ok

#%%
#great looks pretty clean form here!

#saeve the data
evs.to_csv('intermediate_data/iea/{}_evs.csv'.format(FILE_DATE_ID), index=False)
# %%
