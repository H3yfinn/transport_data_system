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
egeda_no_fuel_sums = pd.read_csv('input_data/EGEDA/00APEC_FUELSUMSREMOVED.csv')
egeda_with_fuel_sums_latest = pd.read_csv('input_data/EGEDA/00APEC_December2023.csv')

#%%
#replace Unnamed: 0 with Fuel and Unnamed: 0 with Sector
egeda_with_fuel_sums_latest.rename(columns={'Unnamed: 0':'Fuel_Type', 'Unnamed: 1':'Sector'}, inplace=True)
#%%
#find the unique values in Fuel and Sector in egeda_no_fuel_sums
#change Fuel to Fuel_Type 
egeda_no_fuel_sums.rename(columns={'Fuel':'Fuel_Type'}, inplace=True)
fuel_names = egeda_no_fuel_sums['Fuel_Type'].unique()
sector_names = egeda_no_fuel_sums['Sector'].unique()
fuel_names2 = egeda_with_fuel_sums_latest['Fuel_Type'].unique()
sector_names2 = egeda_with_fuel_sums_latest['Sector'].unique()
#%%
#find what vlaues are in egeda_with_fuel_sums_latest['Fuel_Type'] but not in egeda_no_fuel_sums['Fuel_Type'], and also for Sector
fuel_names_diff = [x for x in fuel_names2 if x not in fuel_names]
sector_names_diff = [x for x in sector_names2 if x not in sector_names]
#%%
#for testing, extract the rows where Fuel or Sector are in fuel_names_diff or sector_names_diff
problem_fuels = [np.nan,
 'TFEC',
 'changes',
 'share',
 'industry',
 'Services',
 'residential',
 'transport']
problem_sectors = [np.nan, 'coal', 'oil', 'gas', 'renewables', 'electricity', 'others', 'Total']
a = egeda_with_fuel_sums_latest.copy()
a= egeda_with_fuel_sums_latest[egeda_with_fuel_sums_latest['Fuel_Type'].isin(problem_fuels) | egeda_with_fuel_sums_latest['Sector'].isin(problem_sectors)]
a.to_csv('input_data/EGEDA/bad_rows.csv')
#%%
#CREATE ANNOYING INPUT TO MAKE SURE USER IS HAPPY THAT WE ARE MISSING THE VLAUES IN fuel_names_diff AND sector_names_diff:
annoying_input = False#True
if annoying_input:
    annoying_input = input('Are you sure you want to continue? The following values are in egeda_with_fuel_sums_latest but not in egeda_no_fuel_sums: \n fuel_names_diff: ' + str(fuel_names_diff) + '\n sector_names_diff: ' + str(sector_names_diff) + '\n\n If you are sure you want to continue, type "yes" and press enter: ')
    if annoying_input != 'yes':
        raise ValueError('You did not type "yes" so the script will stop running')

#%%
#then filter for only the rows where Fuel and Sector are in fuel_names and sector_names
egeda_with_fuel_sums = egeda_with_fuel_sums_latest[egeda_with_fuel_sums_latest['Fuel_Type'].isin(fuel_names)]
egeda_with_fuel_sums = egeda_with_fuel_sums[egeda_with_fuel_sums['Sector'].isin(sector_names)]
#%%
#in 'sheet_name', if there is no _ in the 3rd position, then we can add a _ to the 3rd position and shift everything else along by one
egeda_with_fuel_sums['sheet_name'] = egeda_with_fuel_sums['sheet_name'].apply(lambda x: x[:2] + '_' + x[2:] if '_' not in x[2] else x)
#%%
egeda=egeda_with_fuel_sums.copy()
#%%
#FIX INCORRECT ECONOMY VLAUES
#it seems that some economys have different codes so we will load in the economy codes and double check what is diff eg. 15_rp == 15_phl
economy_codes = pd.read_csv('config/economy_code_to_name.csv')

missing_economies = pd.merge(egeda, economy_codes[['alt_aperc_name', 'Economy']], left_on='sheet_name', right_on='Economy', how='left')

#%%
#missing economies are where the Economy is nan
missing_economies = missing_economies[missing_economies['Economy'].isna()]['sheet_name'].unique()#'15_PHL', '17_SGP', '00_APEC', '22_SEA', '23_NEA', '23_bONEA',
    #    '24_OAM', '24_bOOAM', '25_OCE'

#show the user what entres in missing_economies arent in economy_codes.alt_aperc_name
missing_economies_from_concordance = economy_codes.alt_aperc_name.unique()
missing_economies_from_concordance = [x for x in missing_economies if x not in missing_economies_from_concordance]
print('Please check none of the following economies need to be in the alt_aperc_name column of the economy_code_to_name.csv file: ' + str(missing_economies_from_concordance) + '\n\n')
#'15_PHL', '17_SGP', '00_APEC', '22_SEA', '23_NEA', '23_bONEA',

#%%
annoying_input = False#True
if annoying_input:
    #ask for user input and if they say yes, remove the missing_economies_from_concordance from the egeda dataframe
    annoying_input = input('Do you want to remove the following Economy entries from the EGEDA dataset: ' + str(missing_economies_from_concordance) + '\n\n If you are sure you want to continue, type "yes" and press enter: ')

    if annoying_input == 'yes':
        pass
        #remove the missing_economies_from_concordance from the egeda dataframe
egeda = egeda[~egeda['sheet_name'].isin(missing_economies_from_concordance)]
#%%
#to repalce vlaues that are missing from economy_codes['Economy'], do a left join on economy_codes['alt_aperc_name']. Then where alt_aperc_name is nan, then we can just use original vlaue sheet_name, else we can use the economy_codes['Economy'] value
egeda = pd.merge(egeda, economy_codes[['alt_aperc_name', 'Economy']], left_on='sheet_name', right_on='alt_aperc_name', how='left')
#where alt_aperc_name is nan, then we can just use the sheet_name. If not na then we can use the oroiginal economy_codes['Economy'] value
egeda.loc[egeda['alt_aperc_name'].isna(), 'Economy'] = egeda.loc[egeda['alt_aperc_name'].isna(), 'sheet_name']
egeda['sheet_name'] = egeda['Economy']
#drop the alt_aperc_name column and the economy column
egeda.drop(columns=['alt_aperc_name', 'Economy'], inplace=True)
#%%

#NOW Extract the data we want and make it tidy:

#%%
egeda.reset_index(inplace=True)

# #remove the following sheets: #PLEASE ADD MORE IF YOU NEED TO
# # '00_APEC', '22_SEA', '23_NEA', '23_bONEA',
# #        '24_OAM', '24_bOOAM', '25_OCE'
# egeda = egeda[~egeda['sheet_name'].isin(['00_APEC', '22_SEA', '23_NEA', '23_bONEA', '24_OAM', '24_bOOAM', '25_OCE'])]

#take a look at cols
# egeda.columns #'sheet_name',     'level_1',        'Fuel_Type',      'Sector','Unnamed: 42'
#remove any cols with Unnamed or index in the name
egeda = egeda[[x for x in egeda.columns if 'Unnamed' not in x and 'index' not in x]]
#%%
egeda = pd.melt(egeda, id_vars= ['sheet_name', 'Fuel_Type', 'Sector'], var_name='year', value_name='value')

# %%
#now find the unique fuel names and sector names
fuel_names = egeda['Fuel_Type'].unique()
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
mediums_to_sectors['nonspecified'] = ['15.6 Non-specified transport']
mediums_to_sectors[np.nan] = ['15. Transport sector']#if the sector is 15. Transport sector then the medium is nan, and the value is the total
#%%
a = egeda_transport.dropna(subset=['value'])


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
egeda_transport.rename(columns={'sheet_name':'Economy', 'year':'Date','value':'Value', 'Fuel_Type': 'Fuel_Type'}, inplace=True)

#replace sector with Medium using the dictionary
for medium, sectors in mediums_to_sectors.items():
    egeda_transport.loc[egeda_transport['Sector'].isin(sectors), 'Medium'] = medium

#create yyyy-mm-dd date so the date s the 31 of december of the year
egeda_transport['Date'] = egeda_transport['Date'] +'-12-31'

#%%
#there are a lot of nan in the value col so we will drop them
egeda_transport.dropna(subset=['Value'], inplace=True)
#and remove 0's
egeda_transport = egeda_transport[egeda_transport['Value'] != 0]
#%%
# Remove the Sector column
egeda_transport.drop(columns=['Sector'], inplace=True)

#save the data
egeda_transport.to_csv('intermediate_data/EGEDA/EGEDA_transport_output{}.csv'.format(FILE_DATE_ID), index=False)

# # %%
# #now extract certain fuel uses if they are only used for one thing: 
# air = ['7.02 Aviation gasoline','7.04 Gasoline type jet fuel', '7.05 Kerosene type jet fuel']
# #extract air
# egeda_air = egeda[egeda['Fuel_Type'].isin(air)]


#%%
#SEE WHERE WE ARE MISSING DATA

#analysis:
#show where economys dont have data for a medium:
#first filter for Fuel_Type == '19 Total'
egeda_transport_total = egeda_transport[egeda_transport['Fuel_Type'] == '19 Total']

#then drop all cols except economy and medium
egeda_transport_total = egeda_transport_total[['Economy', 'Medium']]
egeda_transport_total.drop_duplicates(inplace=True)
#now make wide
egeda_transport_total = egeda_transport_total.pivot(index='Economy', columns='Medium', values='Medium').reset_index()
#drop nan col
egeda_transport_total.drop(columns=[np.nan], inplace=True)
#keep only
#%%
#melt
egeda_transport_total = pd.melt(egeda_transport_total, id_vars=['Economy'], var_name='Medium')

#where value is nan set to False, if not True
egeda_transport_total['value'].fillna(False, inplace=True)
egeda_transport_total.loc[egeda_transport_total['value'] != False, 'value'] = True
#make wide again then plot ass a table
egeda_transport_total = egeda_transport_total.pivot(index='Economy', columns='Medium', values='value')
#plot as table with False values highlighted
egeda_transport_total.style.applymap(lambda x: 'background-color: red' if x == False else '')
#%%




#now plot bar chart with economy on x, grouped by medium, and value on y


# #now group by economy and medium and count the number of rows and put
# egeda_transport_total = egeda_transport_total.groupby(['Economy', 'Medium']).count()
# #now reset the index
# egeda_transport_total.reset_index(inplace=True)
# #replace medium na with 0
# egeda_transport_total['Medium'].fillna(0, inplace=True)
# #now pivot the table so that the mediums are the columns
# egeda_transport_total = egeda_transport_total.pivot(index='Economy', columns='Medium', values='Fuel_Type')
# #now fill the nan with 0
# egeda_transport_total.fillna(0, inplace=True)
# #now plot bar chart with economy on x, grouped by medium, and value on why

# %%

#Because we are interested in how transport and industry interact we will also extract industry energy use:
egeda_industry = egeda[egeda['Sector'] == '14. Industry sector']

a = egeda_industry.dropna(subset=['value'])

#rename sheet name to economy
egeda_industry.rename(columns={'sheet_name':'Economy', 'year':'Date','value':'Value', 'Fuel_Type': 'Fuel_Type'}, inplace=True)

#create yyyy-mm-dd date so the date s the 31 of december of the year
egeda_industry['Date'] = egeda_industry['Date'] +'-12-31'

#%%
#there are a lot of nan in the value col so we will drop them
egeda_industry.dropna(subset=['Value'], inplace=True)
#and remove 0's
egeda_industry = egeda_industry[egeda_industry['Value'] != 0]
#%%
# Remove the Sector column
egeda_industry.drop(columns=['Sector'], inplace=True)

#save the data
egeda_industry.to_csv('intermediate_data/EGEDA/egeda_industry_output{}.csv'.format(FILE_DATE_ID), index=False)

# # %%
# #now extract certain fuel uses if they are only used for one thing: 
# air = ['7.02 Aviation gasoline','7.04 Gasoline type jet fuel', '7.05 Kerosene type jet fuel']
# #extract air
# egeda_air = egeda[egeda['Fuel_Type'].isin(air)]

#%%