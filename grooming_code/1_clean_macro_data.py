#we will import data from imf un and oecd for gdp and population. 
#%%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
import re
import os
import common_functions as cf
#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

#import data
#input_data\Macro\OECD
#input_data\Macro\IMF
#input_data\Macro\UN
#%%
#imf data
#Please note that you will need to manually chagne the file to csv from xls version. Could not find how to load in xls file without errors using pd.read_excel('input_data\\Macro\\IMF\\gdp.xls', engine='xlrd')
PPP_IMF = pd.read_csv('input_data\\Macro\\IMF\\ppp.csv')
GDP_IMF = pd.read_csv('input_data\\Macro\\IMF\\gdp.csv')

#OECD data
PPP_OECD = pd.read_csv('input_data\\Macro\\OECD\\PPP.csv')
GDP_forecast_OECD = pd.read_csv('input_data\\Macro\\OECD\\GDPLTFORECAST.csv')

#UN data
POP_UN = pd.read_excel('input_data\\Macro\\UN\\UN_PPP2022_Output_PopTot.xlsx')

#load regional mapping 
economy_code_to_name = pd.read_csv('config\\economy_code_to_name.csv')
#%%
#clean data
#stack all then melt imf data together:

#rename 'Implied PPP conversion rate (National currency per international dollar)' to a new economy column
PPP_IMF.rename(columns={'Implied PPP conversion rate (National currency per international dollar)':'Economy name'}, inplace=True)
#rename 'GDP (current US$)' to a new economy column
GDP_IMF.rename(columns={'GDP, current prices (Billions of U.S. dollars)':'Economy name'}, inplace=True)

#create cols which specifiy measure
PPP_IMF['Measure'] = 'PPP'
GDP_IMF['Measure'] = 'GDP_billions_USD_current'

#stack the data
imf_data= pd.concat([PPP_IMF, GDP_IMF], axis=0)

#melt the data
imf_data = imf_data.melt(id_vars=['Economy name', 'Measure'], var_name='Year', value_name='Value')

#%%
#join on the economy code to name 
economy_code_to_name_tall = cf.make_economy_code_to_name_tall(economy_code_to_name)
imf_data = imf_data.merge(economy_code_to_name_tall, how='left', on='Economy name')

#%%
#separate nas in economy name column and check that ehy dont contain any aperc economys
imf_data_nas = imf_data[imf_data['Economy'].isna()]
imf_data_nas['Economy name'].unique()
##to include in economy name to code csv: 'Hong Kong SAR'#'Russian Federation'#'Taiwan Province of China'#China, People's Republic of #'Korea, Republic of' #done
#Interesting: '©IMF, 2022'#'Asia and Pacific', 'Australia and New Zealand'#'North America',#'Southeast Asia',#'World'
#%%
imf_data.dropna(subset=['Economy name'], inplace=True)
#%%
#now join the economy name to code 
eff = eff.merge(economy_name_to_code, how='left', left_on='Economy', right_on='Economy name')




#merge imf data with economy code to name





#%%