#import data from iea's evs dataset here: https://www.iea.org/data-and-statistics/data-tools/global-ev-data-explorer
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
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')

PRINT_GRAPHS_AND_STATS = False

#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

evs = pd.read_csv('input_data/IEA/IEA GEVO 2023 data.csv')
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
evs.rename(columns={'category':'source', 'parameter':'measure', 'mode':'vehicle type', 'powertrain': 'drive', 'year':'date'}, inplace=True)
#drop region and economy name
evs.drop(columns=['region', 'Economy name'], inplace=True)
#%%
#now within the measure names, rename them to be consistent with the other data sources. First print the unique values
evs.measure.unique()
#2022 data measures:
# 'EV sales', 'EV stock', 'EV sales share', 'EV stock share',
#        'EV charging points', 'Oil displacement Mbd',
#        'Oil displacement Mlge', 'Electricity demand'
#2023 data measures:
# ['EV sales', 'EV stock', 'EV sales share', 'EV stock share',
#        'EV charging points', 'Electricity demand', 'Oil displacement Mbd',
#        'Oil displacement, million lge']
#rename the above to:
# 'Sales', 'Stocks', 'Sales share', 'Stock share', 'EV Charging points','Oil displacement Mbd', 'Oil displacement Mlge', 'Energy'
evs['measure'] = evs['measure'].replace({'EV sales':'Sales', 'EV stock':'Stocks', 'EV sales share':'Sales share', 'EV stock share':'Stock share', 'EV charging points':'EV Charging points', 'Oil displacement Mbd':'Oil displacement Mbd', 'Oil displacement, million lge':'Oil displacement Mlge', 'Electricity demand':'Energy'})

#%%
#and change v type
evs['vehicle type'].unique()#'Cars', 'EV', 'Trucks', 'Vans', 'Buses'
 #Bit confused about 'EV' so take a look:
evs[evs['vehicle type']=='EV']#it's for charging points so make it NA
# so change all to: ldv, np.nan, van, bus, ht
evs['vehicle type'] = evs['vehicle type'].replace({'Cars':'lpv', 'EV':np.nan, 'Vans':'lcv', 'Buses':'bus', 'Trucks':'ht'})
#set ldv's to have transport type = 'passenger'
evs.loc[evs['vehicle type']=='lpv', 'transport type'] = 'passenger'
#set lcv's to have transport type = 'combined'

evs.loc[evs['vehicle type']=='lcv', 'transport type'] = 'freight'

#set bus's to have transport type = 'passenger'
evs.loc[evs['vehicle type']=='bus', 'transport type'] = 'passenger'
#set ht's to have transport type = 'freight'
evs.loc[evs['vehicle type']=='ht', 'transport type'] = 'freight'
#%%
#and change drive
evs['drive'].unique()
# 'BEV', 'EV', 'PHEV', 'Publicly available fast',
#        'Publicly available slow'
#check out EV
evs[evs['drive']=='EV']#sales share and oil displacement is for 'EV's whjich refers to bev and phev so change to bev and phev

# #create a comment column where we set it to drive where drive ='Publicly available fast', 'Publicly available slow'
# evs['comment'] = np.nan
# evs.loc[evs['drive'].isin(['Publicly available fast', 'Publicly available slow']), 'comment'] = evs.loc[evs['drive'].isin(['Publicly available fast', 'Publicly available slow']), 'drive']

#change to 'bev', 'EV' (for now), 'phev', np.nan, np.nan
evs['drive'] = evs['drive'].replace({'BEV':'bev', 'EV':'bev and phev', 'PHEV':'phev'})
evs['unit'].unique()#array(['Vehicles', 'percent', 'charging points', 'PJ',
    #    'Milion barrels per day', 'Oil displacement, million lge'],
    #   dtype=object)
#keep all the same but we are going to change gwh to pj
evs['unit'] = evs['unit'].replace({'GWh':'PJ', 'Vehicles':'Stocks'})
#and convert the values to PJ
evs.loc[evs['unit']=='PJ', 'value'] = evs.loc[evs['unit']=='PJ', 'value'] * 0.0036
#%%


#%%
evs['Medium'] = 'road' 

#where measure is EV Charging points then set transport type to 'combined'
evs.loc[evs['measure']=='EV Charging points', 'transport type'] = 'combined'

#where vehicle type is np.nan then set 'vehicle type' to 'combined'
evs.loc[evs['vehicle type'].isna(), 'vehicle type'] = 'combined'
#%%
#set dataset to 'IEA_ev_explorer'
evs['dataset'] = 'IEA_ev_explorer'

#make date have the format YYYY-MM-DD, with mm=12, dd=31
evs['date'] = evs['date'].astype(str) + '-12-31'

evs['frequency'] = 'Yearly'

evs['fuel'] = 'all'

evs['scope'] = 'National'
#%%
#great looks pretty clean from here!

#saeve the dat
#we will ncorporate the ev and phev stock shares for ldv (passenger), bus (passenger), hdv and ht (freight) from iea and calculate the bev and phev stock shares for ldv (passenger), bus (passenger), hdv (combined transport type) and ht (freight) from iea. This si because stock shares are for 'bev and phev' in iea data explorer

#grab stock share data
evs_stock_share = evs[evs['measure'] == 'Stock share']
#convert stock share to a proportion by dividing by 100
evs_stock_share['value'] = evs_stock_share['value']/100
#grab stock data
evs_stock = evs[evs['measure'] == 'Stocks']
cols = evs.columns.tolist()
cols.remove('value')
cols.remove('drive')
#pivot so we have bev and phev in a column
evs_stock_wide = evs_stock.pivot(index=cols, columns='drive', values='value').reset_index()
#%%
#set nas to 0 in bev and phev cols
evs_stock_wide['bev'] = evs_stock_wide['bev'].fillna(0)
evs_stock_wide['phev'] = evs_stock_wide['phev'].fillna(0)
#claculate shares of total stocks of phev to bev
evs_stock_wide['phev_share'] = evs_stock_wide['phev']/(evs_stock_wide['bev']+evs_stock_wide['phev'])
evs_stock_wide['bev_share'] = evs_stock_wide['bev']/(evs_stock_wide['bev']+evs_stock_wide['phev'])
#drop bev and phev cols
evs_stock_wide = evs_stock_wide.drop(columns=['bev', 'phev'])
#%%
#merge with stock share data
cols.remove('measure')
cols.remove('unit')
evs_stock_share = evs_stock_share.merge(evs_stock_wide, on = cols, how='left', suffixes=('', '_y'))

#times Value by shares to get new stock share for phev and bev
evs_stock_share['phev'] = evs_stock_share['value']*evs_stock_share['phev_share']
evs_stock_share['bev'] = evs_stock_share['value']*evs_stock_share['bev_share']

#drop cols
evs_stock_share_new = evs_stock_share.drop(columns=['phev_share','bev_share', 'value',  'unit_y', 'measure_y', 'drive'])
#%%
#melt so we have bev and phev in a column called drive 
cols = evs_stock_share_new.columns.tolist()
cols.remove('bev')
cols.remove('phev')
evs_stock_share_new = evs_stock_share_new.melt(id_vars=cols, value_vars=['bev', 'phev'], var_name='Drive', value_name='value')

#%%
# concat to ev new
#first set all cols to lower case in btoh dfs
evs_stock_share_new.columns = evs_stock_share_new.columns.str.lower()
evs.columns = evs.columns.str.lower()

evs_new = pd.concat([evs, evs_stock_share_new], axis=0, ignore_index=True)

#%%
#save the data
evs_new.to_csv('intermediate_data/IEA/{}_evs.csv'.format(FILE_DATE_ID), index=False)
# %%


#lpot USA sales share, with facet by vehicle type and transport type and color by source

