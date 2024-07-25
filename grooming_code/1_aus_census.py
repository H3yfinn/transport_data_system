#take in data from aus census. This data has good information on stocks for a variety of sub types and even fuels. However no actual df_final data so might have to deal with that by importing IEA stock shares.


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
#%%
#load simplifeid aus vehicle censusx data. 
df = pd.read_excel(r'.\input_data\Australia\aus_vehicle_census_simplified.xlsx',sheet_name='vehicle_census_simplified')
# cols Date	Vehicle Type	Transport Type	Value	Vehicle_sub_type	Measure	Drive	Weight	Comment

#now do some manipulation on the data. 

# we will be calcualting the avg age, ice_g, ice_d splits and splitting vehicle sub types into the vehicle types we use.

#%%
CAR_TO_SUV_RATIO = 0.15#TODO MAKE THIS LESS ARBITRARY

#where Vehicle Type is car, we wil times it by x TO GET THE NUMBER OF SUV'S (ESTIAMTE, COULD CHANGE) THEN SUBTRACT THAT FROM THE CAR VALUE TO GET THE NUMBER OF CARS
cars = df[(df['Vehicle Type']=='car') & (df['Measure']=='stocks')]
suvs = cars.copy()
suvs['Vehicle Type'] = 'suv'
suvs['Value'] = suvs['Value']*CAR_TO_SUV_RATIO
cars['Value'] = cars['Value'] - suvs['Value']
df = pd.concat([df,cars,suvs])
#then make suv have same age as car
cars = df[(df['Vehicle Type']=='car') & (df['Measure']=='average_age')]
suvs = cars.copy()
suvs['Vehicle Type'] = 'suv'
df = pd.concat([df,suvs])

#%%
#first grab the data with Weights in it (data that doesnt have nas in that col) The weights represent the weight of vehicles in that vehicle sub type. THeir value is the stocks of that vehicle sub type.
df_weights_stocks = df[(df['Weight'].notna()) & (df['Measure']=='stocks')]
#calcualte percent of vehicles in each vehicle type for each Vehicle sub type, for each Date
df_weights_stocks['percent_of_vehicle_type'] = df_weights_stocks.groupby(['Date','Vehicle_sub_type'])['Value'].transform(lambda x: x/x.sum())
#sum up by Vehicle sub type and Vehicle Type and date so we know the total percent of each vehicle sub type for each Date
df_weights_stocks_percents = df_weights_stocks.groupby(['Date','Vehicle Type','Vehicle_sub_type'])['percent_of_vehicle_type'].sum().reset_index()

#and create a sum of the non percentage data too, since ti will be used later
df_weights_stocks = df_weights_stocks.groupby(['Date','Vehicle Type'])['Value'].sum().reset_index()
#set some cols
df_weights_stocks['Drive'] = 'all'
df_weights_stocks['Transport Type'] = 'freight'
df_weights_stocks['Measure'] = 'stocks'
breakpoint()#im not sure about this. need to mkae sure its working for average age.
#%%
#use percents to calcualte the number of ice_g and ice_d vehicles in each vehicle type for each Date:
#first grab data where Drive isnt all
df_weights_stocks_fuel = df[(df['Drive']!='all') & (df['Measure']=='stocks')]
#drop vehicle type
df_weights_stocks_fuel = df_weights_stocks_fuel.drop(columns=['Vehicle Type'])
#join the weights on date and vehicle sub type using an innner, then calcualte the number of ice_g and ice_d vehicles in each vehicle type for each Date
df_weights_stocks_fuel = df_weights_stocks_fuel.merge(df_weights_stocks_percents,on=['Date','Vehicle_sub_type'],how='inner')

#now calcualte the number of ice_g and ice_d vehicles in each vehicle type for each Date
df_weights_stocks_fuel['Value'] = df_weights_stocks_fuel['Value']*df_weights_stocks_fuel['percent_of_vehicle_type']

#now sum value by date, vehicle type transport type, drive
df_weights_stocks_fuel = df_weights_stocks_fuel.groupby(['Date','Vehicle Type','Transport Type','Drive'])['Value'].sum().reset_index()

#set measre to stocks
df_weights_stocks_fuel['Measure'] = 'stocks'
#%%
# do the same thing for vehicle age using the percent of vehicles that are each vehicle sub type to find the average age of the vehicles by vehicle type but use it to find the average age when summing ages of similar vehicle types.
df_weights_age = df[(df['Measure'] == 'average_age')]

# drop vehicle type
df_weights_age = df_weights_age.drop(columns=['Vehicle Type'])

# join the weights on date and vehicle sub type using an inner, then calculate weighted avg age
df_weights_age = df_weights_age.merge(df_weights_stocks_percents, on=['Date', 'Vehicle_sub_type'], how='inner')
df_weights_age['Weighted_age'] = df_weights_age['Value'] * df_weights_age['percent_of_vehicle_type']

# now sum value by date, vehicle type transport type, drive
df_weights_age = df_weights_age.groupby(['Date', 'Vehicle Type', 'Transport Type', 'Drive']).sum().reset_index()

# calculate the weighted average age
df_weights_age['Value'] = df_weights_age['Weighted_age'] / df_weights_age['percent_of_vehicle_type']
df_weights_age.drop(columns=['Weighted_age', 'percent_of_vehicle_type', 'Weight'], inplace=True)

#%%
#currently average age is set for 'all' drive types. we want to set it based on the drive.
#we want average age we have in the data to be used for 'old drive types' and set to 1 for 'new drive types'. so we need to find the average age of the old drive types and the new drive types. So grab the drives form teh concordance, chekc they can be split into the new and old drive types i expectm and then set theoir average age to 1 or the average age we have in the data.
#load in the concordance

import yaml
with open('config/selection_config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
concordances = pd.read_csv(cfg['concordances_file_path'])

#grab the drive types for road:
drive_types = concordances[(concordances['Medium']=='road')].Drive.unique()#array(['bev', 'ice_d', 'ice_g', 'cng', 'fcev', 'lpg', 'phev_d', 'phev_g'],
new_drives = ['bev','phev_d','phev_g','fcev']
old_drives = ['ice_d','ice_g','cng','lpg']
#check that the drive types are in the data
if set(drive_types) != set(new_drives+old_drives):
    print('ERROR: drive types dont match')
    breakpoint()
    
old_drive_concordance = concordances[(concordances['Medium']=='road') & (concordances['Drive'].isin(old_drives))][['Drive','Vehicle Type', 'Transport Type']].drop_duplicates()
new_drive_concordance = concordances[(concordances['Medium']=='road') & (concordances['Drive'].isin(new_drives))][['Drive','Vehicle Type', 'Transport Type']].drop_duplicates()

#join the concordance on the data so we can replicate the average age for the new drive types
df_weights_age=df_weights_age.drop(columns=['Drive'])
df_weights_age_old = df_weights_age.merge(old_drive_concordance,on=['Vehicle Type','Transport Type'],how='inner').copy()
df_weights_age_new = df_weights_age.merge(new_drive_concordance,on=['Vehicle Type','Transport Type'],how='inner').copy()
df_weights_age_new['Value'] = 1

#concat the two dataframes
df_age = pd.concat([df_weights_age_old,df_weights_age_new])

df_age = df_age[['Date','Vehicle Type','Transport Type','Drive','Measure','Value']].drop_duplicates()
#%%
#now get the rest of the stocks. do this by grabbing data where drive is all and measur eis stocks
df_stocks = df[(df['Drive']=='all' )& (df['Measure']=='stocks')& (df['Weight'].isna())]
#drop the vtypes with double_up in their name as they are just duplicates
df_stocks = df_stocks[~df_stocks['Vehicle Type'].str.contains('double_up')]
#concat with df_weights_stocks to add on the data for rigid trucks
df_stocks = pd.concat([df_stocks,df_weights_stocks])
#concat with the fuel data to add on the data for ice_g and ice_d
df_stocks = pd.concat([df_stocks,df_weights_stocks_fuel])
#drop the vehicle sub type column
df_stocks = df_stocks.drop(columns=['Vehicle_sub_type', 'Weight'])

#sum by date, vehicle type, transport type, drive, measure
df_stocks = df_stocks.groupby(['Date','Vehicle Type','Transport Type','Drive','Measure'])['Value'].sum().reset_index()

#%%
df_final = pd.concat([df_stocks, df_age])
#set comment to 
df_final['Comment'] = 'no_comment' 
#set dataset to 'AUS_census'
df_final['Dataset'] = 'AUS_census'
#make date have the format YYYY-MM-DD, with mm=12, dd=31
df_final['Date'] = df_final['Date'].astype(str) + '-12-31'
df_final['Frequency'] = 'Yearly'
df_final['Fuel'] = 'all'
df_final['Scope'] = 'National'
df_final['Economy'] = '01_AUS'
df_final['Medium'] = 'road'
df_final['Unit'] = np.where(df_final['Measure']=='average_age','age','stocks')
#%%
###########################
#POSTHOC UPADTE. MOVE HALF OF LCV'S INTO LT'S SINCE WE BELIEVE THEY ARE BEING MISCATEGORISED BECAUSE OF THEIR SIZE. (the proprotion of LCV's is higher than it is in most otnher economies which is leading them to dominate the EV transition, since the assumption is that lcv's are easier to electrify than lt's - but in aussie case these vehicles are just big trucks being driven round the countryside to get people from a to b with little to no related freight.)
lcvs = df_final[(df_final['Vehicle Type']=='lcv') & (df_final['Measure']=='stocks')]
lts = lcvs.copy()
lts['Vehicle Type'] = 'lt'
lts['Transport Type'] = 'passenger'
lts['Value'] = lts['Value']*0.5
lcvs['Value'] = lcvs['Value']*0.5
df_final = df_final[~((df_final['Vehicle Type']=='lcv') & (df_final['Measure']=='stocks'))]
df_final = pd.concat([df_final,lcvs,lts])
#sum
df_final = df_final.groupby(['Date','Vehicle Type','Transport Type','Drive','Measure','Comment','Dataset','Frequency','Fuel','Scope','Economy','Medium','Unit'])['Value'].sum().reset_index()

###########################
#%%
#save the data

#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
df_final.to_csv('intermediate_data/AUS/{}_aus_census.csv'.format(FILE_DATE_ID), index=False)


#%%






















#OLD ATTEMPT TO LOAD DATA DIRECTLY. NOW USING SIMPLIFIED VERSION.
#load in the data
# os.listdir(r'E:\APERC\transport_data_system\input_data\USA\alternative_fuels_data_center')
# ['10302_buses_9-7-21.xlsx',
#  '10305_ldv_efficiency_power_11-8-22.xlsx',
#  '10309_vmt_by_vehicle_type_3-26-20.xlsx',
#  '10310_fuel_economy_by_vehicle_type_3-26-20.xlsx',
#  '10311_passenger_mpg_5-18-23.xlsx',
#  '10661_Fuel_Use_by_Mode_and_Fuel_Type_5-17-23.xlsx',
#  '10963_EV_range_efficiency_4-4-23.xlsx']

# default_beginning_row = 2
# column_names_row = 1
# file_title_row = 0
# dfs_dict = {}
# #load in excel file and iterate through sheets. put them into a dicitonary. \transport_data_system\input_data\Australia
# file = pd.read_excel(r'.\input_data\Australia\aus vehicle census.xls',sheet_name=None)
# #name sheets manually:
# sheets = ['contents','registered_vehicles','stocks_per_capita','average_vehicle_age', 'registered_vehicles_fuel_type', 'fuel_type_by_state','vehicle_makes', 'rigid_trucks_registration', 'articulated_trucks_registration', 'year_of_manfacture']
# # for i, s in enumerate(sheets):
# for i, items, in enumerate(file.items()):
#     dfs_dict[sheets[i]] = items[1]

# #now clean the data.
    
# # #%%
# # for k,v in dfs_dict.items():
# #     #make a measure col with the key name
# #     measure = k
# #     # dfs_dict[k]['measure'] = measure

# #%%
# #it will be a bit difficult with this because we have a weirdly formatted dataset. Genreally, on the 7th row, the actual data starts. but every 4th row is a new category, and on the first row is just region names. We really only want data for the region Australia which is on the 10th colunm )last col). first col contains the years, which are not consecutively going up by one, but isntead split like [2016, 2020, 2021].
# # The sheets for this are:
# # 1,2,3, 9 - where the numbers indicate the index of the sheet in the list of sheet names above.
# #then 4,5,7,8 have their data starting on 8th row.. but 4,5 have fuels on the 6th row - ignore row after that. 
# # and 6 contains years on the 5th row - ignore row after that. 
# # 7,8 contains truck tonnes )GVM and GCM on row 6. Im not sure about what these refer to yet. 

# #%%
# #registered_vehicles:
# #drop the first 6 rows, keep the 1st and last rows
# dfs_dict['registered_vehicles'].drop([0,1,2,3,4,5],axis=0,inplace=True)












# #%%




#     #make the column names
#     #drop theat file title row and column_names_row
#     dfs_dict[k].drop([file_title_row,column_names_row],axis=0,inplace=True)
#     print(dfs_dict[k].head())
#     print(dfs_dict[k].columns)
# #find first na row and drop all rows after that
# for f in dfs_dict.keys():
#     first_na_row = dfs_dict[f].isna().sum(axis=1).values.argmax()
#     dfs_dict[f].drop(dfs_dict[f].index[first_na_row:],axis=0,inplace=True)
# #drop empty columns too
# for f in dfs_dict.keys():
#     dfs_dict[f].dropna(axis=1,how='all',inplace=True)
#     print(dfs_dict[f].head())
#     print(dfs_dict[f].columns)
# #%%
# #drop any with unnamed in the column name
# for f in dfs_dict.keys():
#     for c in dfs_dict[f].columns:
#         if 'unnamed' in str(c).lower():
#             #check if col contains nans
#             if dfs_dict[f][c].isna().sum() == dfs_dict[f].shape[0]:
#                 dfs_dict[f].drop(c,axis=1,inplace=True)
#             else:
#                 print('column {} for file {} needs a proper name:'.format(c,f))
#                 print('It has data in rows: {}'.format(dfs_dict[f][c].dropna().index))
#                 #print teh data in those rows
#                 print(dfs_dict[f].loc[dfs_dict[f][c].dropna().index,:])
#                 #ask user if they want to delete or rename. if rename, ask for new name
#                 user_input = input('Do you want to delete this column? (y/n)')
#                 if user_input.lower() == 'y':
#                     dfs_dict[f].drop(c,axis=1,inplace=True)
#                 else:
#                     new_name = input('What is the new name?')
#                     dfs_dict[f].rename(columns={c:new_name},inplace=True)

# #somehow we have the second to last col as a NaN col. drop it if it is all NaNs
# for f in dfs_dict.keys():
#     if dfs_dict[f].iloc[:,-2].isna().sum() == dfs_dict[f].shape[0]:
#         dfs_dict[f].drop(dfs_dict[f].columns[-2],axis=1,inplace=True)
#     print(f)
#     print(dfs_dict[f].columns)
#     print(dfs_dict[f]['measure'].unique())
