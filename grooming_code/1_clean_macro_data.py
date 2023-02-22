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
GDP_current_IMF = pd.read_csv('input_data\\Macro\\IMF\\gdp_current.csv')
GDP_PPP_IMF = pd.read_csv('input_data\\Macro\\IMF\\gdp_ppp.csv')

#OECD data
PPP_OECD = pd.read_csv('input_data\\Macro\\OECD\\PPP.csv')
GDP_forecast_OECD = pd.read_csv('input_data\\Macro\\OECD\\GDPLTFORECAST.csv')
real_gdp_growth_short_term_forecast = pd.read_csv('input_data\\Macro\\OECD\\real_gdp_growth_short_term_forecast.csv')
nominal_gdp_growth_short_term_forecast = pd.read_csv('input_data\\Macro\\OECD\\nominal_gdp_growth_short_term_forecast.csv')
#UN data
POP_UN = pd.read_excel('input_data\\Macro\\UN\\UN_PPP2022_Output_PopTot.xlsx')

# World Bank data
dec_conversion_rate = pd.read_csv('input_data\\Macro\\WORLD_BANK\\dec_alternative_LCU_to_USD_conversion_factor.csv')
exchange_rate = pd.read_csv('input_data\\Macro\\WORLD_BANK\\LCU_to_USD_exchange_rate.csv')

#load regional mapping 
economy_code_to_name = pd.read_csv('config\\economy_code_to_name.csv')

#%%
##############################################################################
#IMF DATA
##############################################################################
#clean data
#stack all then melt imf data together:

#rename 'Implied PPP conversion rate (National currency per international dollar)' to a new economy column
PPP_IMF.rename(columns={'Implied PPP conversion rate (National currency per international dollar)':'IMF Economy name'}, inplace=True)
#rename 'GDP (current US$)' to a new economy column
GDP_current_IMF.rename(columns={'GDP, current prices (Billions of U.S. dollars)':'IMF Economy name'}, inplace=True)
GDP_PPP_IMF.rename(columns={'GDP, current prices (Purchasing power parity; billions of international dollars)':'IMF Economy name'}, inplace=True)

#create cols which specifiy measure
PPP_IMF['Measure'] = 'PPP'
GDP_current_IMF['Measure'] = 'GDP_billions_USD_current'
GDP_PPP_IMF['Measure'] = 'GDP_billions_international_dollars_PPP'

#stack the data
imf_data= pd.concat([PPP_IMF, GDP_current_IMF, GDP_PPP_IMF], axis=0)

#melt the data
imf_data = imf_data.melt(id_vars=['IMF Economy name', 'Measure'], var_name='Year', value_name='Value')

#there are some cases where 'IMF Economy name' is nan. We will drop these rows
imf_data.dropna(subset=['IMF Economy name'], inplace=True)
#%%
#join on the economy code to name using outer so we can check where we are missing data
economy_code_to_name_tall = cf.make_economy_code_to_name_tall(economy_code_to_name)
imf_data = imf_data.merge(economy_code_to_name_tall, how='outer', left_on='IMF Economy name', right_on='Economy name')

#%%
updated_imf_data = imf_data.copy()
updated_imf_data.dropna(subset=['Economy'], inplace=True)

#%%
#DOUBLE CHECK
#take a look at imf data where we are missing economy codes:
#first extract economys where 'IMF Economy name' is nan
x = imf_data[imf_data['IMF Economy name'].isna()]['Economy'].unique().tolist()
#remove those that already have an associated 'IMF Economy name' as these are where we had multiple possible economy names in economy_code_to_name.csv
x = [i for i in x if i not in updated_imf_data['Economy'].unique()]
if len(x) == 0:
    print('Good. All economy codes in imf data are in economy_code_to_name.csv')
else:
    print('Bad. The following economy codes are in APERC but not included in the IMF data: {}'.format(x))

#take a look at imf data where we have economy codes but no economy name just in case we are missing something
y = imf_data[imf_data['Economy'].isna()]['IMF Economy name'].unique()


#%%
#now clean and format the imf data to go into the transport data system
#where Value is 'no data' then drop the row
updated_imf_data = updated_imf_data[updated_imf_data['Value'] != 'no data']
#where Date is nan then drop the row
updated_imf_data.dropna(subset=['Year'], inplace=True)
#drop LOCATION and iso_code cols
updated_imf_data.drop(columns=['IMF Economy name','Economy name'], inplace=True)
#rename cols
updated_imf_data.rename(columns={'Measure':'Unit','Year':'Date'}, inplace=True)
#if Unit is 'PPP' then make Measure = 'PPP'
updated_imf_data.loc[updated_imf_data['Unit'] == 'PPP', 'Measure'] = 'PPP'
#if Unit is 'GDP_billions_USD_current' then make Measure = 'GDP'
updated_imf_data.loc[updated_imf_data['Unit'] == 'GDP_billions_USD_current', 'Measure'] = 'GDP_current'
updated_imf_data.loc[updated_imf_data['Unit'] == 'GDP_billions_international_dollars_PPP', 'Measure'] = 'GDP_PPP'

#make Date col into yyyy-mm-dd format
updated_imf_data['Date'] = updated_imf_data['Date'].apply(lambda x: str(int(x))+'-12-31')
#Make Frequency into 'Yearly'
updated_imf_data['Frequency'] = 'Yearly'

#make dataset and source cols. We will name the source after the Unit since that specifies where the data came from
updated_imf_data['Dataset'] = 'IMF'
updated_imf_data['Source'] = updated_imf_data['Unit']
#%%

##############################################################################
#OECD DATA
##############################################################################
#%%
oecd = pd.concat([PPP_OECD, GDP_forecast_OECD,real_gdp_growth_short_term_forecast,nominal_gdp_growth_short_term_forecast], axis=0)
#merge on economy code to name's iso code col
oecd = oecd.merge(economy_code_to_name[['iso_code', 'Economy']], how='outer', left_on='LOCATION', right_on='iso_code')
#%%
#take a look at where we are missing economy codes
x = oecd[oecd['iso_code'].isna()]['LOCATION'].unique()#this is only na, so we have all the aperc economys in the imf data#TODO i dont think this has worked right

#take a look at imf data where we have economy codes but no economy name just in case we are missing something
y = oecd[oecd['LOCATION'].isna()]['iso_code'].unique()

if len(y) == 0:
    print('Good. All economy codes in oecd data are in economy_code_to_name.csv')
else:
    print('Bad. The following economy codes are in APERC but not included in the OECD data: {}'.format(y))

#drop nas from oecd data
oecd.dropna(subset=['iso_code'], inplace=True)
oecd.dropna(subset=['LOCATION'], inplace=True)

#%%
#now clean and format the oecd data to incorporate into transport system
#drop LOCATION and iso_code cols
oecd.drop(columns=['LOCATION', 'iso_code','SUBJECT','Flag Codes'], inplace=True)
#rename cols
oecd.rename(columns={'MEASURE':'Unit','TIME':'Date','INDICATOR':'Measure','FREQUENCY':'Frequency'}, inplace=True)
#Make Frequency into 'Yearly' if it is 'A' 
oecd.loc[oecd['Frequency'] == 'A', 'Frequency'] = 'Yearly'
#if 'Q' then make it 'Quarterly'
oecd.loc[oecd['Frequency'] == 'Q', 'Frequency'] = 'Quarterly'
#If Yearly then make Date col into yyyy-mm-dd format
oecd['Date'] = oecd.apply(lambda x: str(int(x['Date']))+'-12-31' if x['Frequency'] == 'Yearly' else x['Date'], axis=1)
#If Quarterly then make Date col into yyyy-mm-dd format from yyyy-qQ format where Q is 1,2,3,4, eg 2019-Q1
oecd['Date'] = oecd.apply(lambda x: str(int(x['Date'].split('-')[0]))+'-'+str(int(x['Date'].split('-')[1].replace('Q',''))*3)+'-31' if x['Frequency'] == 'Quarterly' else x['Date'], axis=1)
#make dataset and source cols
oecd['Dataset'] = 'OECD'
#make source col the same as the measure as this is where the data came from
oecd['Source'] = oecd['Measure']
#%%

##############################################################################
#UN DATA
##############################################################################

#go to the first non nan row in first col, if any values are 4 digit year values then set it to the header
POP_UN_edit = POP_UN.iloc[POP_UN.iloc[:,0].first_valid_index():]
header_found = False
for col in POP_UN_edit.iloc[0,:]:
    if str(col).isnumeric() and len(str(col)) == 4:
        #we ahve found the header row so set it to the header
        POP_UN_edit.columns = POP_UN_edit.iloc[0,:]
        #drop the header row
        POP_UN_edit.drop(POP_UN_edit.index[0], inplace=True)
        header_found = True
        break
if header_found == False:
    print('Could not find header row in UN data, please check it manually')

#%%
# POP_UN_edit.columns#Index([                               'Index',
#                                 'Variant',
#    'Region, subregion, country or area *',
#                                   'Notes',
#                           'Location code',
#                         'ISO3 Alpha-code',
#                         'ISO2 Alpha-code',
#                             'SDMX code**',
#                                    'Type',
#                             'Parent code',

#connect the economy code to name to the UN data using the ISO3 Alpha-code and iso_code cols
POP_UN_edit = POP_UN_edit.merge(economy_code_to_name[['iso_code', 'Economy']], how='outer', left_on='ISO3 Alpha-code', right_on='iso_code')
#we will get some rows where both the iso_code and ISO3 Alpha-code are na. THese are expeted to be regions and not countries so we will drop them
z = POP_UN_edit[POP_UN_edit['iso_code'].isna() & POP_UN_edit['ISO3 Alpha-code'].isna()]
POP_UN_edit.dropna(subset=['iso_code', 'ISO3 Alpha-code'], how='all', inplace=True)

x = POP_UN_edit[POP_UN_edit['ISO3 Alpha-code'].isna()]['iso_code'].unique()
if len(x) == 0:
    print('Good. All economy codes in UN data are in economy_code_to_name.csv')
else:
    print('Bad. The following economy codes are in APERC but not included in the UN data: {}'.format(x))
y = POP_UN_edit[POP_UN_edit['iso_code'].isna()]['Region, subregion, country or area *'].unique()

#drop nas
POP_UN_edit.dropna(subset=['iso_code'], inplace=True)
POP_UN_edit.dropna(subset=['ISO3 Alpha-code'], inplace=True)

#%%
#Make variant into 'Source' col since this is best way to specify where the data came from
POP_UN_edit.rename(columns={'Variant':'Source'}, inplace=True)
#add 'Population forecast ' to the start of the source col
POP_UN_edit['Source'] = 'Population forecast ' + POP_UN_edit['Source']
#make Unit col as Thousands
POP_UN_edit['Unit'] = 'Thousands'
#make Frequency col as Yearly
POP_UN_edit['Frequency'] = 'Yearly'
#make Measure col as Population
POP_UN_edit['Measure'] = 'Population'
#drop cols that are not needed
POP_UN_edit.drop(columns=['iso_code','Region, subregion, country or area *','Notes','Location code','ISO3 Alpha-code','ISO2 Alpha-code','SDMX code**','Type','Parent code','Index'], inplace=True)
#make dataset col
POP_UN_edit['Dataset'] = 'UN'

#%%
#melt the year cols into a single col
POP_UN_edit = pd.melt(POP_UN_edit, id_vars=['Economy','Source','Unit','Frequency','Measure','Dataset'], var_name='Date', value_name='Value')
#make the date col into yyyy-mm-dd format
POP_UN_edit['Date'] = POP_UN_edit['Date'].apply(lambda x: str(int(x))+'-12-31')
#%%

##############################################################################
# World Bank Data
##############################################################################
def find_and_set_first_row_with_header(df, first_col_name):
    #Drop the first rows until you reach the col called 'Country Name' in the first col
    header_found = False
    for row in range(0, len(df)):
        if df.iloc[row,0] == first_col_name:
            df = df.iloc[row:]
            ##set header
            df.columns = df.iloc[0,:]
            #drop header row
            df.drop(df.index[0], inplace=True)
            header_found = True
            return df
    if header_found == False:
        print('Could not find header row in World Bank data, please check it manually')
        return df

dec_conversion_rate = find_and_set_first_row_with_header(dec_conversion_rate, 'Country Name')
exchange_rate = find_and_set_first_row_with_header(exchange_rate, 'Country Name')

#concat the two dataframes
wb_data = pd.concat([dec_conversion_rate, exchange_rate], axis=0)

#%%
#connect the economy code to name to the WB data using the 	Country Code and iso_code cols
wb_data = wb_data.merge(economy_code_to_name[['iso_code', 'Economy']], how='outer', left_on='Country Code', right_on='iso_code')

x = wb_data[wb_data['Country Code'].isna()]['iso_code'].unique()#we are missing Taiwan in here. This is expected since the WB data does not have Taiwan
y = wb_data[wb_data['iso_code'].isna()]['Country Name'].unique()

#drop nas
wb_data.dropna(subset=['iso_code'], inplace=True)
wb_data.dropna(subset=['Country Code'], inplace=True)

#we can insert taiwan later somehow. #TODO
#%%
# wb_data.columns#Index([  'Country Name',   'Country Code', 'Indicator Name', 'Indicator Code'
#drop cols that are not needed
wb_data.drop(columns=['Country Name','Country Code','iso_code'], inplace=True)
#rename Indicator Code to Source
wb_data.rename(columns={'Indicator Code':'Source'}, inplace=True)
#set unit to the part of source thats in brackets (e.g. 'DEC alternative conversion factor (LCU per US$)')
wb_data['Unit'] = wb_data['Indicator Name'].apply(lambda x: x[x.find('(')+1:x.find(')')])
#set measure to the part of source thats before the brackets (e.g. 'DEC alternative conversion factor')
wb_data['Measure'] = wb_data['Indicator Name'].apply(lambda x: x[:x.find('(')-1])
#drop Indicator Name col
wb_data.drop(columns=['Indicator Name'], inplace=True)
#set frequency to Yearly
wb_data['Frequency'] = 'Yearly'
#set dataset to World Bank
wb_data['Dataset'] = 'World Bank'
#melt the year cols into a single col
wb_data = pd.melt(wb_data, id_vars=['Economy','Source','Unit','Frequency','Measure','Dataset'], var_name='Date', value_name='Value')
#make the date col into yyyy-mm-dd format
wb_data['Date'] = wb_data['Date'].apply(lambda x: str(int(x))+'-12-31')

#%%

##############################################################################
#MERGE ALL DATA
##############################################################################
#%%
#concat all data
all_data = pd.concat([updated_imf_data, oecd, POP_UN_edit,wb_data], axis=0)
#convert all cols to str
all_data = all_data.astype(str)
#convert values to numeric
all_data['Value'] = pd.to_numeric(all_data['Value'], errors='coerce')

# %%
#save to csv
all_data.to_csv('intermediate_data/Macro/all_macro_data_{}.csv'.format(FILE_DATE_ID), index = False)






#first grab only the IMF current and PPP data then calculate gdp_current * ppp and plot it against gdp_ppp
imf_ppp_gdp = all_data[(all_data['Dataset'] == 'IMF')]
# pivot the data so that the measure values are sep cols
imf_ppp_gdp = imf_ppp_gdp.pivot(index=['Economy','Date'], columns='Measure', values='Value').reset_index()
#convert to 
#calc 'estimated gdp ppp' col
imf_ppp_gdp['estimated_gdp_ppp'] = imf_ppp_gdp['GDP_current'] * imf_ppp_gdp['PPP']
#plot using plotly with a facet col for each economy and a line for each measure
#melt the data so we have one value col of GDP_PPP and estimated_gdp_ppp
imf_ppp_gdp = pd.melt(imf_ppp_gdp[['Economy','Date','GDP_PPP','estimated_gdp_ppp']], id_vars=['Economy','Date'], var_name='Measure', value_name='Value')
#%%
import plotly.express as px
fig = px.line(imf_ppp_gdp, x="Date", y='Value',color='Measure', facet_col="Economy", facet_col_wrap=7)
fig.show()



# %%
