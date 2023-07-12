#pull data thats been cleansed for the 9th edition mdoel




#NOTE, MAKE SURE TO RUN split_egeda_cleansed_into_transport_types.py after this because the output from this is not by transport type and oit isnt used in the data system


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
model_input_9th = pd.read_csv('input_data/EGEDA/model_df_wide_ref_20230630.csv')
# egeda_with_fuel_sums_latest = pd.read_csv('input_data/EGEDA/00APEC.csv')

#make sure scenario col only contains reference
model_input_9th = model_input_9th[model_input_9th['scenarios'] == 'reference']
#then drop the col
model_input_9th.drop(columns=['scenarios'], inplace=True)

#changfe Economy = 17_SGP to 17_SIN, and 15_PHL to 15_RP 
model_input_9th['economy'] = model_input_9th['economy'].replace({'17_SGP': '17_SIN', '15_PHL': '15_RP'})
# model_input_9th.columns: Index(['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors',
#        'sub3sectors', 'sub4sectors', 'fuels', 'subfuels', '1980', '1981',
#        '1982', '1983', '1984', '1985', '1986', '1987', '1988', '1989', '1990',
#        '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999',
#        '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008',
#        '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017',
#        '2018', '2019', '2020', '2021', '2022', '2023', '2024', '2025', '2026',
#        '2027', '2028', '2029', '2030', '2031', '2032', '2033', '2034', '2035',
#        '2036', '2037', '2038', '2039', '2040', '2041', '2042', '2043', '2044',
#        '2045', '2046', '2047', '2048', '2049', '2050', '2051', '2052', '2053',
#        '2054', '2055', '2056', '2057', '2058', '2059', '2060', '2061', '2062',
#        '2063', '2064', '2065', '2066', '2067', '2068', '2069', '2070'],
#       dtype='object')

#grasb data for the sectors col
# 15_transport_sector
# 17_nonenergy_use
model_input_9th = model_input_9th[(model_input_9th['sectors'] == '15_transport_sector') | (model_input_9th['sectors'] == '17_nonenergy_use')]

#anbd finally data for these sub1sectors col:
#sub1sectors (matches to medium)
# 15_01_domestic_air_transport
# 15_02_road
# 15_03_rail
# 15_04_domestic_navigation
# 15_05_pipeline_transport
# 15_06_nonspecified_transport
# 17_03_transport_sector
model_input_9th = model_input_9th[(model_input_9th['sub1sectors'].isin(['15_01_domestic_air_transport', '15_02_road', '15_03_rail', '15_04_domestic_navigation', '15_05_pipeline_transport', '15_06_nonspecified_transport', '17_03_transport_sector']))]

#map medium basded on sub1sectors:
medium_mapping = {'air': '15_01_domestic_air_transport', 'road': '15_02_road', 'rail': '15_03_rail', 'ship': '15_04_domestic_navigation', 'nonspecified': '15_06_nonspecified_transport', 'pipeline': '15_05_pipeline_transport', '17_03_transport_sector': 'nonenergy_use'}
inverse_medium_mapping = {v: k for k, v in medium_mapping.items()}
model_input_9th['medium'] = model_input_9th['sub1sectors'].map(inverse_medium_mapping)

#keep only the cols we need (i.e. drop the cols we dont)
model_input_9th.drop(columns=['sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors'], inplace=True)

#%%
#now we need to grab the most specific fuel type for each row.
#the fuels cols and subfuels cols work like so: there is always a value in fuels cols. If theres an x in subfuels then the fuels col contains the most specific fuel, but if theres a value in subfuels then use that as most specific fuel.
model_input_9th['fuel_type'] = np.where(model_input_9th['subfuels']=='x', model_input_9th['fuels'], model_input_9th['subfuels'])
#%%
#however, we will now remove aggreagtes. Do this by iterating through groups of ''economy', medium and identifying if fuels where subfuels is x are aggregates or not. they are aggregates if that fuel has subfuels in other rows.
for row in model_input_9th.groupby(['economy', 'medium']).groups:
    #grab the rows for this group
    rows = model_input_9th.loc[model_input_9th.groupby(['economy', 'medium']).groups[row]]
    #now loop throug the fuels in this group and identify if they are aggregates or not
    for fuel in rows['fuel_type'].unique():
        #if there is more than one subfuel for this fuel, then it is an aggregate
        if len(rows[rows['fuel_type']==fuel]['subfuels'].unique()) > 1:
            #now remove all rows that have this fuel and subfuel = x
            model_input_9th = model_input_9th[~((model_input_9th['fuel_type']==fuel) & (model_input_9th['subfuels']=='x'))]
    
#%%
#drop the subfuels col and fuels
model_input_9th.drop(columns=['subfuels', 'fuels'], inplace=True)
#melt the years cols
model_input_9th = model_input_9th.melt(id_vars=['economy', 'medium', 'fuel_type'], var_name='date', value_name='value')

#%%
#make all columns start with capital letter
model_input_9th.columns = [col.capitalize() for col in model_input_9th.columns]
#replace Fuel_type with Fuel_Type
model_input_9th.rename(columns={'Fuel_type': 'Fuel_Type'}, inplace=True)
#now insert columns for transport type, frequency and so on
model_input_9th['Transport Type'] = np.nan
model_input_9th['Frequency'] ='Yearly'   
model_input_9th['Unit'] = 'PJ'
model_input_9th['Source'] = '9th_cleansed'
model_input_9th['Dataset'] = 'EGEDA'
model_input_9th['Measure'] = 'Energy'
model_input_9th['Vehicle Type'] = np.nan
model_input_9th['Drive'] = np.nan

#create yyyy-mm-dd date so the date s the 31 of december of the year
model_input_9th['Date'] = model_input_9th['Date'] +'-12-31'

#%%
#there are a lot of nan in the value col so we will drop them
model_input_9th.dropna(subset=['Value'], inplace=True)
#and remove 0's
model_input_9th = model_input_9th[model_input_9th['Value'] != 0]
#%%

#save the data
model_input_9th.to_csv('intermediate_data/EGEDA/model_input_9th_cleaned{}.csv'.format(FILE_DATE_ID), index=False)

#%%
# # #now extract certain fuel uses if they are only used for one thing: 
# # air = ['7.02 Aviation gasoline','7.04 Gasoline type jet fuel', '7.05 Kerosene type jet fuel']
# # #extract air
# # egeda_air = egeda[egeda['Fuel_Type'].isin(air)]


# #%%
# #SEE WHERE WE ARE MISSING DATA

# #analysis:
# #show where economys dont have data for a medium:
# #first filter for Fuel_Type == '19 Total'
# model_input_9th_total = model_input_9th[model_input_9th['Fuel_Type'] == '19 Total']

# #then drop all cols except economy and medium
# model_input_9th_total = model_input_9th_total[['Economy', 'Medium']]
# model_input_9th_total.drop_duplicates(inplace=True)
# #now make wide
# model_input_9th_total = model_input_9th_total.pivot(index='Economy', columns='Medium', values='Medium').reset_index()
# #drop nan col
# model_input_9th_total.drop(columns=[np.nan], inplace=True)
# #keep only
# #%%
# #melt
# model_input_9th_total = pd.melt(model_input_9th_total, id_vars=['Economy'], var_name='Medium')

# #where value is nan set to False, if not True
# model_input_9th_total['value'].fillna(False, inplace=True)
# model_input_9th_total.loc[model_input_9th_total['value'] != False, 'value'] = True
# #make wide again then plot ass a table
# model_input_9th_total = model_input_9th_total.pivot(index='Economy', columns='Medium', values='value')
# #plot as table with False values highlighted
# model_input_9th_total.style.applymap(lambda x: 'background-color: red' if x == False else '')
# #%%




# #now plot bar chart with economy on x, grouped by medium, and value on y


# # #now group by economy and medium and count the number of rows and put
# # model_input_9th_total = model_input_9th_total.groupby(['Economy', 'Medium']).count()
# # #now reset the index
# # model_input_9th_total.reset_index(inplace=True)
# # #replace medium na with 0
# # model_input_9th_total['Medium'].fillna(0, inplace=True)
# # #now pivot the table so that the mediums are the columns
# # model_input_9th_total = model_input_9th_total.pivot(index='Economy', columns='Medium', values='Fuel_Type')
# # #now fill the nan with 0
# # model_input_9th_total.fillna(0, inplace=True)
# # #now plot bar chart with economy on x, grouped by medium, and value on why

# # %%

# #Because we are interested in how transport and industry interact we will also extract industry energy use:
# egeda_industry = egeda[egeda['Sector'] == '14. Industry sector']

# a = egeda_industry.dropna(subset=['value'])

# #rename sheet name to economy
# egeda_industry.rename(columns={'sheet_name':'Economy', 'year':'Date','value':'Value', 'Fuel_Type': 'Fuel_Type'}, inplace=True)

# #create yyyy-mm-dd date so the date s the 31 of december of the year
# egeda_industry['Date'] = egeda_industry['Date'] +'-12-31'

# #%%
# #there are a lot of nan in the value col so we will drop them
# egeda_industry.dropna(subset=['Value'], inplace=True)
# #and remove 0's
# egeda_industry = egeda_industry[egeda_industry['Value'] != 0]
# #%%
# # Remove the Sector column
# egeda_industry.drop(columns=['Sector'], inplace=True)

# #save the data
# egeda_industry.to_csv('intermediate_data/EGEDA/egeda_industry_output{}.csv'.format(FILE_DATE_ID), index=False)

# # # %%
# # #now extract certain fuel uses if they are only used for one thing: 
# # air = ['7.02 Aviation gasoline','7.04 Gasoline type jet fuel', '7.05 Kerosene type jet fuel']
# # #extract air
# # egeda_air = egeda[egeda['Fuel_Type'].isin(air)]

# #%%
# %%
