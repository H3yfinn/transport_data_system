#take in data from aus census. This data has good information on stocks for a variety of sub types and even fuels. However no actual EVs data so might have to deal with that by importing IEA stock shares.


#%%
#take in data files. they all have around about the same structure:
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import re
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
#%%
#load simplifeid aus vehicle censusx data. 
file = pd.read_excel(r'.\input_data\Australia\aus_vehicle_census_simplified.xlsx',sheet_name='vehicle_census_simplified')
# cols Date	Vehicle Type	Transport Type	Value	Vehicle_sub_type	Measure	Drive	Weight	Comment

#now do some manipulation on the data. 

# we will be calcualting the avg age, ice_g, ice_d splits and splitting vehicle sub types into the vehicle types we use.


#first grab the data with Weights in it (data that doesnt have nas in that col)



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
