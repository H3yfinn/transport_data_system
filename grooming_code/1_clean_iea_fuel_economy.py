#this dataset has potential; for being useful for vehiclke efficiency data. We will only load the sheet called 'Edited_average_fuel_consumption' which has been cleaned to be easier to use. 

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
#input_data\IEA\Global_fuel_economy.xlsx
eff = pd.read_excel('input_data/IEA/Global_fuel_economy.xlsx', sheet_name='Edited_average_fuel_consumption')

#%%
#convert region to economy code where possible
#first import the economy code to region mapping
economy_name_to_code = pd.read_csv('config/economy_code_to_name.csv')

#stack the Economy name, alt_name and alt_name2 columns into a single column
economy_name_to_code = economy_name_to_code.melt(id_vars=['Economy'], value_vars=['Economy_name', 'Alt_name', 'Alt_name2'], var_name='column_name', value_name='Economy name').drop(columns=['column_name'])

#now join the economy name to code mapping to the evs data
eff = eff.merge(economy_name_to_code, how='left', left_on='Economy', right_on='Economy name')

#rename Economy_y to Economy
eff = eff.rename(columns={'Economy_y':'Economy'})

#see where it didn't work
eff[eff['Economy'].isna()].Economy_x.unique()
# ['Argentina', 'Austria', 'Belgium', 'Brazil', 'Bulgaria', 'Croatia',
#        'Cyprus', 'Czech Republic', 'Denmark', 'Egypt', 'Estonia',
#        'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Iceland',
#        'India', 'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg',
#        'Macedonia', 'Malta', 'Netherlands', 'Norway', 'Poland',
#        'Portugal', 'Romania', 'Russian Federation', 'Slovakia',
#        'Slovenia', 'South Africa', 'Spain', 'Sweden', 'Switzerland',
#        'Turkey', 'Ukraine', 'United Kingdom']
#checked the above m,anually and they are all correct
#so remove them all
eff = eff[~eff['Economy'].isna()]

#and drop Economy_x and Economy name
eff = eff.drop(columns=['Economy_x', 'Economy name'])
#%%

#now we can make it tall so we have a Years col and a Value col
eff = eff.melt(id_vars='Economy', var_name='Year', value_name='Value')

#now convert from litres of gasoline equivalent per 100km to PJ per 100km and then to PJ per km
#from a quick look it seems that 1 litre of gasoline is equivalent to 3.42e-8 PJ #from here https://hextobinary.com/unit/energy/from/gasoline/to/joule
eff['Value'] = eff['Value'] * 3.42e-8

#now convert from PJ per 100km to PJ per km
eff['Value'] = eff['Value'] / 100
#%%
eff['Measure'] ='Efficiency'
eff['Unit'] = 'PJ per km'
eff['Source'] = 'GFEI'
eff['Vehicle Type'] = 'ldv'
eff['Transport Type'] = 'combined'
eff['Drive'] = 'ice'
eff['Medium'] = 'road'

#%%

eff['Dataset'] = 'IEA Fuel Economy'
#make the first letter of words in columns uppercase
eff.columns = eff.columns.str.title()
#remove na values in value column
eff = eff[eff['Value'].notna()]
#create a date column with month and day set to 12-31
eff['Date'] = eff['Year'].astype(str) + '-12-31'
#make frequency column and set to yearly
eff['Frequency'] = 'Yearly'
#remove Year column
eff = eff.drop(columns=['Year'])
#%%
#now we can save the data
eff.to_csv('intermediate_data/IEA/{}_iea_fuel_economy.csv'.format(FILE_DATE_ID), index=False)
#%%