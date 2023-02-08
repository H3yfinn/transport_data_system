# \transport_data_system\input_data\EGEDA\00APEC_FUELSUMSREMOVED

#%%
#set working directory as one folder back
import os
import re
import pandas as pd
import numpy as np
import datetime
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')


#%%
# #read in the workbook then iterate through every sheet and concat togehter using name of sheet as a column
# egeda = pd.read_excel('input_data/EGEDA/00APEC_FUELSUMSREMOVED.xlsx', sheet_name=None)
# egeda = pd.concat(egeda, names=['sheet_name'])

# #save the concatenated data as a csv
# egeda.to_csv('input_data/EGEDA/00APEC_FUELSUMSREMOVED.csv')
#%%
#load the csv
FILE_DATE_ID = 'DATE' + str(datetime.datetime.now().strftime('%Y%m%d'))
egeda = pd.read_csv('input_data/EGEDA/00APEC_FUELSUMSREMOVED.csv')
#%%
egeda.reset_index(inplace=True)

#remove the following sheets:
# '00_APEC', '22_SEA', '23_NEA', '23_bONEA',
#        '24_OAM', '24_bOOAM', '25_OCE'
egeda = egeda[~egeda['sheet_name'].isin(['00_APEC', '22_SEA', '23_NEA', '23_bONEA', '24_OAM', '24_bOOAM', '25_OCE'])]

#take a look at cols
# egeda.columns #'sheet_name',     'level_1',        'Fuel',      'Sector','Unnamed: 42'
#remove 'level_1' column and then make tall
egeda.drop(columns=['Unnamed: 1','index','Unnamed: 42'], inplace=True)
egeda = pd.melt(egeda, id_vars= ['sheet_name', 'Fuel', 'Sector'], var_name='year', value_name='value')

# %%
#now find the unique fuel names and sector names
fuel_names = egeda['Fuel'].unique()
sector_names = egeda['Sector'].unique()

#%%
#extract energy that is used for transport: 
# '15. Transport sector', '15.1 Domestic air transport', '15.2 Road',
#        '15.3 Rail', '15.4 Domestic navigation', '15.5 Pipeline transport',
#        '15.6 Non-specified transport',
sectors = ['15. Transport sector', '15.1 Domestic air transport', '15.2 Road',
         '15.3 Rail', '15.4 Domestic navigation', '15.5 Pipeline transport',
            '15.6 Non-specified transport']
#extract transport
egeda_transport = egeda[egeda['Sector'].isin(sectors)]

#create mediums to match the sector: eg: '15.1 Domestic air transport' = 'air'
mediums_to_sectors = dict()
mediums_to_sectors['air'] = ['15.1 Domestic air transport']
mediums_to_sectors['road'] = ['15.2 Road']
mediums_to_sectors['rail'] = ['15.3 Rail']
mediums_to_sectors['ship'] = ['15.4 Domestic navigation']
mediums_to_sectors['pipeline'] = ['15.5 Pipeline transport']
mediums_to_sectors['non_specified'] = ['15.6 Non-specified transport']

#%%
#now insert columns for transport type, frequency and so on
egeda_transport['Transport Type'] = np.nan
egeda_transport['Frequency'] ='Yearly'   
egeda_transport['Unit'] = 'PJ'
egeda_transport['Source'] = 'EGEDA'
egeda_transport['Dataset'] = 'EGEDA'
egeda_transport['Measure'] = 'Energy'
egeda_transport['Vehicle Type'] = np.nan
egeda_transport['Drive'] = np.nan
#rename sheet name to economy
egeda_transport.rename(columns={'sheet_name':'Economy', 'year':'Date','value':'Value'}, inplace=True)

#replace sector with Medium using the dictionary
for medium, sectors in mediums_to_sectors.items():
    egeda_transport.loc[egeda_transport['Sector'].isin(sectors), 'Medium'] = medium

#create yyyy-mm-dd date so the date s the 31 of december of the year
egeda_transport['Date'] = egeda_transport['Date'] +'-12-31'

#%%
# Remove the Sector column
egeda_transport.drop(columns=['Sector'], inplace=True)

#save the data
egeda_transport.to_csv('intermediate_data/EGEDA/EGEDA_transport_output{}.csv'.format(FILE_DATE_ID), index=False)

# # %%
# #now extract certain fuel uses if they are only used for one thing: 
# air = ['7.02 Aviation gasoline','7.04 Gasoline type jet fuel', '7.05 Kerosene type jet fuel']
# #extract air
# egeda_air = egeda[egeda['Fuel'].isin(air)]


#%%


