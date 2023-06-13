#we will load in the ddata from E:\APERC\transport_data_system\input_data\USA\alternative_fuels_data_center and then incorporate the afdc factors. They seem like useful default values we can use for the USA and other economys for things like efficiency, mileage and fuel prices. 
#note that theyh separate their data into vehicle sub types. we want to introduce those to the data system later but for now (using an if statement) we will jsut avg by ve3hicel typE to find the value. We will then format the data so it can be incroporated like the other data. We will also create an extra datasset that has this data for all other economys so we can easily incorpoate it as default values for them.
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
#load in the data
# os.listdir(r'E:\APERC\transport_data_system\input_data\USA\alternative_fuels_data_center')
# ['10302_buses_9-7-21.xlsx',
#  '10305_ldv_efficiency_power_11-8-22.xlsx',
#  '10309_vmt_by_vehicle_type_3-26-20.xlsx',
#  '10310_fuel_economy_by_vehicle_type_3-26-20.xlsx',
#  '10311_passenger_mpg_5-18-23.xlsx',
#  '10661_Fuel_Use_by_Mode_and_Fuel_Type_5-17-23.xlsx',
#  '10963_EV_range_efficiency_4-4-23.xlsx']

default_beginning_row = 2
column_names_row = 1
file_title_row = 0
dfs_dict = {}
#load in those files and print their structures:
for f in  [
 '10309_vmt_by_vehicle_type_3-26-20.xlsx',
 '10310_fuel_economy_by_vehicle_type_3-26-20.xlsx',
 '10311_passenger_mpg_5-18-23.xlsx']:
    #  '10661_Fuel_Use_by_Mode_and_Fuel_Type_5-17-23.xlsx',
    #  '10963_EV_range_efficiency_4-4-23.xlsx']:
    print(f)
    dfs_dict[f] = pd.read_excel(r'.\input_data\USA\alternative_fuels_data_center\{}'.format(f))
    #make a measure col with the non na value in file title row
    measure = dfs_dict[f].iloc[file_title_row,:].dropna().values[0]
    #now make next row the column names
    dfs_dict[f].columns = dfs_dict[f].iloc[column_names_row,:].values
    dfs_dict[f]['measure'] = measure
    #drop theat file title row and column_names_row
    dfs_dict[f].drop([file_title_row,column_names_row],axis=0,inplace=True)
    print(dfs_dict[f].head())
    print(dfs_dict[f].columns)
#find first na row and drop all rows after that
for f in dfs_dict.keys():
    first_na_row = dfs_dict[f].isna().sum(axis=1).values.argmax()
    dfs_dict[f].drop(dfs_dict[f].index[first_na_row:],axis=0,inplace=True)
#drop empty columns too
for f in dfs_dict.keys():
    dfs_dict[f].dropna(axis=1,how='all',inplace=True)
    print(dfs_dict[f].head())
    print(dfs_dict[f].columns)
#%%
#drop any with unnamed in the column name
for f in dfs_dict.keys():
    for c in dfs_dict[f].columns:
        if 'unnamed' in str(c).lower():
            #check if col contains nans
            if dfs_dict[f][c].isna().sum() == dfs_dict[f].shape[0]:
                dfs_dict[f].drop(c,axis=1,inplace=True)
            else:
                print('column {} for file {} needs a proper name:'.format(c,f))
                print('It has data in rows: {}'.format(dfs_dict[f][c].dropna().index))
                #print teh data in those rows
                print(dfs_dict[f].loc[dfs_dict[f][c].dropna().index,:])
                #ask user if they want to delete or rename. if rename, ask for new name
                user_input = input('Do you want to delete this column? (y/n)')
                if user_input.lower() == 'y':
                    dfs_dict[f].drop(c,axis=1,inplace=True)
                else:
                    new_name = input('What is the new name?')
                    dfs_dict[f].rename(columns={c:new_name},inplace=True)

#somehow we have the second to last col as a NaN col. drop it if it is all NaNs
for f in dfs_dict.keys():
    if dfs_dict[f].iloc[:,-2].isna().sum() == dfs_dict[f].shape[0]:
        dfs_dict[f].drop(dfs_dict[f].columns[-2],axis=1,inplace=True)
    print(f)
    print(dfs_dict[f].columns)
    print(dfs_dict[f]['measure'].unique())

#%%
#now we need to format the data so it can be incorporated into the data system.. 
# 10302_buses_9-7-21.xlsx
# Index(['Year', 'Diesel', 'Gasoline', 'Natural Gas', 'Hybrid', 'Biodiesel*',
#        'Other', 'Total', 'measure'],
#       dtype='object')
# ['Transit Buses by Fuel Type']
# 10309_vmt_by_vehicle_type_3-26-20.xlsx
# Index(['Vehicle Type', 'VMT per Vehicle', 'Source', 'measure'], dtype='object')
# ['Average Annual Vehicle Miles Traveled per Vehicle by Major Vehicle Category']
# 10310_fuel_economy_by_vehicle_type_3-26-20.xlsx
# Index(['Vehicle Type', 'MPG Gasoline', 'MPG Diesel', 'Source', 'measure'], dtype='object')
# ['Average Fuel Economy by Major Vehicle Category']
# 10311_passenger_mpg_5-18-23.xlsx
# Index(['Vehicle Type', 'Vehicle MPGGE*', 'Avg. Load Factor (Persons/Vehicle)',
#        'pmpGGE', 'measure'],
#       dtype='object')
# ['Average Per-Passenger Fuel Economy by Travel Mode']
#so, for the index cols, we should choose what these values match and whther they need to be melted:
INDEX_COLS = ['date',
 'economy',
 'measure',
 'vehicle_type',
 'vehicle_sub_type',
 'unit',
 'medium',
 'transport_type',
 'drive',
 'fuel',
 'frequency',
 'scope']
#for example, for the first file, we have: 
# 10302_buses_9-7-21.xlsx
# Index(['Year', 'Diesel', 'Gasoline', 'Natural Gas', 'Hybrid', 'Biodiesel*',
#    'Other', 'Total', 'measure'],
#the measure is 'Transit Buses by Fuel Type'
#so the values seem to represent Fuels. We can melt these into a single column, fuel
#the index cols are: ['date','measure','fuel', 'vehicle_type', 'transport_type', vehicle_sub_type, 'drive']
#where we make vehicle_type = bus, transport_type = passenger,  vehicle_sub_type = 'transit', drive will be different bassed on fuel. so it will be ice if fuel is diesel, gasoline, or biodiesel, and phev if fuel hybrid and cng if fuel is natural gas and although electric is not in this file, it will be bev if fuel is electric
# buses_fuels = dfs_dict['10302_buses_9-7-21.xlsx']
# #drop Total col
# buses_fuels.drop('Total',axis=1,inplace=True)
# #melt the fuels
# buses_fuels = pd.melt(buses_fuels,id_vars=['Year','measure'],var_name='fuel',value_name='value')
# buses_fuels['measure'] = 'stocks'
# buses_fuels['vehicle_type'] = 'bus'
# buses_fuels['transport_type'] = 'passenger'
# buses_fuels['vehicle_sub_type'] = 'transit'
# fuel_to_drive = {'Diesel':'ice_d','Gasoline':'ice_g','Biodiesel*':'ice_d','Natural Gas':'cng','Hybrid':'phev','Other':'other'}
# buses_fuels['drive'] = buses_fuels['fuel'].map(fuel_to_drive)
# buses_fuels['unit'] = 'stocks'
# buses_fuels['medium'] = 'road'
# buses_fuels['scope'] = 'national'
# buses_fuels['date'] = buses_fuels['Year'].astype(str) + '-12-31'
# buses_fuels.drop(['Year'],axis=1,inplace=True)
# buses_fuels['economy'] = '20_USA'
#ended up deciding that this wasnt sueful because it only provides info on transit buses. we would be missing many other types of buses and it would just get more confusing than useful. we therefore cant use it very well for the data system. we will use the other bus file instead.
#then we will do the rest ismilarly.

#%%
#10309_vmt_by_vehicle_type_3-26-20
# Index(['Vehicle Type', 'VMT per Vehicle', 'Source', 'measure'], dtype='object')
vmt_by_vehicle_type = dfs_dict['10309_vmt_by_vehicle_type_3-26-20.xlsx']

#lsit vehicle types
# vmt_by_vehicle_type['Vehicle Type'].unique()
#'Class 8 Truck', 'Transit Bus', 'Paratransit Shuttle',
#    'Refuse Truck', 'Delivery Truck', 'School Bus', 'Light Truck/Van',
#    'Car', 'Motorcycle'
#set the measure to vmt
vmt_by_vehicle_type['measure'] = 'Mileage'
#set the transport type to passenger for car, motorcycle, light truck/van, school bus, paratransit shuttle, and transit bus, and freight for class 8 truck, refuse truck, and delivery truck
vmt_by_vehicle_type['transport_type'] = vmt_by_vehicle_type['Vehicle Type'].map({'Car': 'passenger', 'Motorcycle': 'passenger', 'Light Truck/Van': 'passenger', 'School Bus': 'passenger', 'Paratransit Shuttle': 'passenger', 'Transit Bus': 'passenger', 'Class 8 Truck': 'freight', 'Refuse Truck': 'freight', 'Delivery Truck': 'freight'})
#set the vehicle sub type to transit for transit bus, school bus, and paratransit shuttle, medium for class 8 truck, refuse truck, and delivery truck, light for light truck/van, and car for car and motorcycle
vmt_by_vehicle_type['vehicle_sub_type'] = vmt_by_vehicle_type['Vehicle Type'].map({'Car': 'car', 'Motorcycle': '2w', 'Light Truck/Van': 'light truck', 'School Bus': 'urban', 'Paratransit Shuttle': 'transit', 'Transit Bus': 'transit', 'Class 8 Truck': 'heavy truck', 'Refuse Truck': 'urban', 'Delivery Truck': 'urban'})
#copy the row for light truck and change the vehicle type to van
new_row = vmt_by_vehicle_type.loc[vmt_by_vehicle_type['Vehicle Type'] == 'Light Truck/Van'].copy()
new_row['vehicle_sub_type'] = 'Van'
#make transport type freight
new_row['transport_type'] = 'freight'
#append the new row
vmt_by_vehicle_type = vmt_by_vehicle_type.append(new_row)
#now change vehicled types. we wnat to have ldv, 2w, ht, bus
vmt_by_vehicle_type['vehicle_type'] = vmt_by_vehicle_type['Vehicle Type'].map({'Car': 'ldv', 'Motorcycle': '2w', 'Light Truck/Van': 'ldv', 'School Bus': 'bus', 'Paratransit Shuttle': 'bus', 'Transit Bus': 'bus', 'Class 8 Truck': 'ht', 'Refuse Truck': 'ht', 'Delivery Truck': 'ht'})
vmt_by_vehicle_type['drive'] = 'all'
#set the unit to vmt
vmt_by_vehicle_type['unit'] = 'mileage'
#set the medium to road
vmt_by_vehicle_type['medium'] = 'road'
#set the scope to national
vmt_by_vehicle_type['scope'] = 'national'
#set the date to 2019-12-31
vmt_by_vehicle_type['date'] = '2019-12-31'
#set the economy to 20_USA
vmt_by_vehicle_type['economy'] = '20_USA'
#drop the vehicle type col
vmt_by_vehicle_type.drop('Vehicle Type',axis=1,inplace=True)
#doro[p the source col
vmt_by_vehicle_type.drop('Source',axis=1,inplace=True)
#rename vmt per vehicle to value
vmt_by_vehicle_type.rename({'VMT per Vehicle':'value'},axis=1,inplace=True)

#average out any duplicated rowsl. for example transit shuttles/buses are the same thing essentially.. or at least with how we are CURRENTLY breaking our data into vehicle sub types. we migth change this later.
vmt_by_vehicle_type = vmt_by_vehicle_type.groupby(['transport_type','vehicle_sub_type','vehicle_type','drive','unit','medium','scope','date','economy','measure']).mean().reset_index()

#%%

#  10311_passenger_mpg_5-18-23.xlsx
# Index(['Vehicle Type', 'Vehicle MPGGE*', 'Avg. Load Factor (Persons/Vehicle)',
#        'pmpGGE', 'measure'],
#       dtype='object')
# ['Average Per-Passenger Fuel Economy by Travel Mode']
passenger_mpg = dfs_dict['10311_passenger_mpg_5-18-23.xlsx']
#we only want the avg load factor and we will use it as occupancy
passenger_mpg = passenger_mpg[['Vehicle Type','Avg. Load Factor (Persons/Vehicle)']]
#rename the avg load factor to occupancy
passenger_mpg.rename({'Avg. Load Factor (Persons/Vehicle)':'value'},axis=1,inplace=True)
#set the measure to occupancy
passenger_mpg['measure'] = 'Occupancy'
#unit to Passengers
passenger_mpg['unit'] = 'passengers'
#set the medium to road
passenger_mpg['medium'] = 'road'
#set the scope to national
passenger_mpg['scope'] = 'national'
#set the date to 2019-12-31
passenger_mpg['date'] = '2019-12-31'
#set the economy to 20_USA
passenger_mpg['economy'] = '20_USA'
#set the transport type to passenger
passenger_mpg['transport_type'] = 'passenger'
#set the vehicle sub type 
# passenger_mpg['Vehicle Type'].unique()
#array(['Transit Rail', 'Intercity Rail', 'Commuter Rail', 'Airlines**',
#    'Motorcycles', 'Cars', 'Light Trucks', 'Transit Buses',
#    'Demand Response***']
#drop Demand Response***
passenger_mpg = passenger_mpg[passenger_mpg['Vehicle Type'] != 'Demand Response***']
passenger_mpg['vehicle_sub_type'] = passenger_mpg['Vehicle Type'].map({'Cars': 'car', 'Motorcycles': '2w', 'Light Trucks': 'light truck', 'Transit Buses': 'transit', 'Transit Rail': 'transit', 'Intercity Rail': 'intercity', 'Commuter Rail': 'commuter', 'Airlines**': 'air'})
#map vehicle type to better vehicle type
passenger_mpg['vehicle_type'] = passenger_mpg['Vehicle Type'].map({'Cars': 'ldv', 'Motorcycles': '2w', 'Light Trucks': 'ldv', 'Transit Buses': 'bus', 'Transit Rail': 'rail', 'Intercity Rail': 'rail', 'Commuter Rail': 'rail', 'Airlines**': 'air'})
#where vehicle type is iar change medium to air, then vehicle type, vehicle sub type and drive to all
passenger_mpg.loc[passenger_mpg['Vehicle Type'] == 'Airlines**','medium'] = 'air'
passenger_mpg.loc[passenger_mpg['Vehicle Type'] == 'Airlines**','vehicle_type'] = 'all'
passenger_mpg.loc[passenger_mpg['Vehicle Type'] == 'Airlines**','vehicle_sub_type'] = 'all'
passenger_mpg.loc[passenger_mpg['Vehicle Type'] == 'Airlines**','drive'] = 'all'
#cahnge medium to rail for rail
passenger_mpg.loc[passenger_mpg['Vehicle Type'].isin(['Transit Rail','Intercity Rail','Commuter Rail']),'medium'] = 'rail'
passenger_mpg.loc[passenger_mpg['Vehicle Type'].isin(['Transit Rail','Intercity Rail','Commuter Rail']),'drive'] = 'all'
passenger_mpg.loc[passenger_mpg['Vehicle Type'].isin(['Transit Rail','Intercity Rail','Commuter Rail']),'vehicle_type'] = 'all'

#set drive to all for now but later on it would be good to set this for drive types indivudally
passenger_mpg['drive'] = 'all'

#drop the vehicle type col
passenger_mpg.drop('Vehicle Type',axis=1,inplace=True)

#avg
passenger_mpg = passenger_mpg.groupby(['transport_type','vehicle_sub_type','vehicle_type','drive','unit','medium','scope','date','economy','measure']).mean().reset_index()


#%%
# 10310_fuel_economy_by_vehicle_type_3-26-20.xlsx
# Index(['Vehicle Type', 'MPG Gasoline', 'MPG Diesel', 'Source', 'measure'], dtype='object')
# ['Average Fuel Economy by Major Vehicle Category']

fuel_economy_by_vehicle_type = dfs_dict['10310_fuel_economy_by_vehicle_type_3-26-20.xlsx']
#this is an interesing dastaset. We dont currently split ice in to g and d but we probably should eventually. perhaps we can name them ice_g and ice_d, as g and d are a bit ambiguous. Probably will be one of those cases like with vehicle sub type where we know the data for more developed countries but have to guess it ffor less developed countries.
#lets name them ice_g and ice_d and then also create an average of teh two that is just ice
fuel_economy_by_vehicle_type.rename({'MPG Gasoline':'ice_g','MPG Diesel':'ice_d'},axis=1,inplace=True)
#calc avg
fuel_economy_by_vehicle_type['ice'] = fuel_economy_by_vehicle_type[['ice_g','ice_d']].mean(axis=1)
#%%
#melt
fuel_economy_by_vehicle_type = fuel_economy_by_vehicle_type.melt(id_vars=['Vehicle Type','Source','measure'],value_vars=['ice_g','ice_d','ice'],var_name='drive',value_name='value').reset_index(drop=True)
#set the measure to fuel economy
fuel_economy_by_vehicle_type['measure'] = 'Efficiency'
#set the unit to mpg
fuel_economy_by_vehicle_type['unit'] = 'Km_per_PJ'#'Km_per_MJ'
#we want our uhnit to be in terms of 'PJ per km' so do the conversion from Avg. Fuel Economy (mpg) to PJ per km: https://chat.openai.com/share/c81f01e1-8192-4681-8e8d-d2e1db9032c0 #note that it hallucinated a little bit in htere.
# # The energy per distance for a vehicle with a fuel economy of 1 mile per gallon (mpg) is approximately 
# #7.4088*10^-8 petajoules per kilometer (PJ/km).
# # Please note that this conversion assumes the energy content of gasoline to be approximately 31.5 MJ/L. The actual energy content can vary depending on the specific formulation of the gasoline.
#%%
#since the vlaues will be so small, convert to scientific notation
mpg_per_km_to_km_per_PJ =1.3497462477054316*10**7
fuel_economy_by_vehicle_type['value'] = fuel_economy_by_vehicle_type['value']* mpg_per_km_to_km_per_PJ
# hmm it might be good to convert values to a larger vlaue so its easier to remember.. eg.  74.088 MJ/km to one mile per gallon (mpg) .. just thinking about how to make this easier to remember\

#set the medium to road
fuel_economy_by_vehicle_type['medium'] = 'road'
#set the scope to national
fuel_economy_by_vehicle_type['scope'] = 'national'
#set the date to 2019-12-31
fuel_economy_by_vehicle_type['date'] = '2019-12-31'
#set the economy to 20_USA
fuel_economy_by_vehicle_type['economy'] = '20_USA'
#set the vehicle sub type:
fuel_economy_by_vehicle_type['Vehicle Type'].unique()
#array(['Refuse Truck', 'Transit Bus', 'Class 8 Truck', 'School Bus',
#    'Delivery Truck', 'Paratransit Shuttle', 'Light Truck/Van', 'Car',
#    'Ridesourcing Vehicle', 'Motorcycle'], dtype=object)
fuel_economy_by_vehicle_type['vehicle_sub_type'] = fuel_economy_by_vehicle_type['Vehicle Type'].map({'Car':'car','Motorcycle':'2w','Light Truck/Van':'light truck','Transit Bus':'transit','Refuse Truck':'urban','Class 8 Truck':'Class 8','Delivery Truck':'urban','School Bus':'urban','Paratransit Shuttle':'transit','Ridesourcing Vehicle':'ridesource'})
#map vehicle type to better vehicle type
fuel_economy_by_vehicle_type['vehicle_type'] = fuel_economy_by_vehicle_type['Vehicle Type'].map({'Car':'ldv','Motorcycle':'2w','Light Truck/Van':'ldv','Transit Bus':'bus','Refuse Truck':'ht','Class 8 Truck':'ht','Delivery Truck':'ht','School Bus':'bus','Paratransit Shuttle':'bus','Ridesourcing Vehicle':'ridesource'})
#drop the vehicle type col
fuel_economy_by_vehicle_type.drop('Vehicle Type',axis=1,inplace=True)

#now depdning on the vehicle sub type, map the  transport type to passenger or freight
fuel_economy_by_vehicle_type['transport_type'] = fuel_economy_by_vehicle_type['vehicle_sub_type'].map({
    'car': 'passenger',
    '2w': 'passenger',
    'light truck': 'freight',
    'transit': 'passenger',
    'urban': 'freight',
    'Class 8': 'freight',
    'ridesource': 'passenger'
})
#avg
fuel_economy_by_vehicle_type = fuel_economy_by_vehicle_type.groupby(['transport_type','vehicle_sub_type','vehicle_type','drive','unit','medium','scope','date','economy','measure']).mean().reset_index()

#%%

#save each df to a csv individually
#save the data
# evs_new.to_csv('intermediate_data/IEA/{}_evs_2022.csv'.format(FILE_DATE_ID), index=False)
#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
import datetime
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
#save the data
fuel_economy_by_vehicle_type.to_csv('intermediate_data/USA/{}_fuel_economy_by_vehicle_type.csv'.format(FILE_DATE_ID), index=False)
passenger_mpg.to_csv('intermediate_data/USA/{}_passenger_mpg.csv'.format(FILE_DATE_ID), index=False)
vmt_by_vehicle_type.to_csv('intermediate_data/USA/{}_vmt_by_vehicle_type.csv'.format(FILE_DATE_ID), index=False)
# buses_fuels.to_csv('intermediate_data/USA/{}_buses_fuels.csv'.format(FILE_DATE_ID), index=False)
# %%

#%%

#plot all the above data using plotly. 
#we will plot the values using bars. they will be faceted by transport type, x will be the medium+vehicletype, color will b subvehicletype+drive, y will be the value
fuel_economy_by_vehicle_type['medium+vehicle_type'] = fuel_economy_by_vehicle_type['medium'] + ' ' + fuel_economy_by_vehicle_type['vehicle_type']
fuel_economy_by_vehicle_type['sub_vehicle_type+drive'] = fuel_economy_by_vehicle_type['vehicle_sub_type'] + ' ' + fuel_economy_by_vehicle_type['drive']
title = 'Fuel economy by vehicle type'
fig = px.bar(fuel_economy_by_vehicle_type, x="medium+vehicle_type", y="value", color="sub_vehicle_type+drive", facet_col="transport_type", facet_col_wrap=2, labels={'value':'Fuel economy (MJ/km)','medium+vehicle_type':'Medium + Vehicle type','sub_vehicle_type+drive':'Sub vehicle type + drive'}, title=title)
#save to plotting_output\analysis as html
fig.write_html("plotting_output/analysis/usa/{}.html".format(title))

vmt_by_vehicle_type['medium+vehicle_type'] = vmt_by_vehicle_type['medium'] + ' ' + vmt_by_vehicle_type['vehicle_type']
vmt_by_vehicle_type['sub_vehicle_type+drive'] = vmt_by_vehicle_type['vehicle_sub_type'] + ' ' + vmt_by_vehicle_type['drive']
title = 'VMT by vehicle type'
fig = px.bar(vmt_by_vehicle_type, x="medium+vehicle_type", y="value", color="sub_vehicle_type+drive", facet_col="transport_type", facet_col_wrap=2,  labels={'value':'VMT (km)','medium+vehicle_type':'Medium + Vehicle type','sub_vehicle_type+drive':'Sub vehicle type + drive'}, title=title)
#save to plotting_output\analysis as html
fig.write_html("plotting_output/analysis/usa/{}.html".format(title))

passenger_mpg['medium+vehicle_type'] = passenger_mpg['medium'] + ' ' + passenger_mpg['vehicle_type']
passenger_mpg['sub_vehicle_type+drive'] = passenger_mpg['vehicle_sub_type'] + ' ' + passenger_mpg['drive']
title = 'Passenger occupancy by vehicle type'
fig = px.bar(passenger_mpg, x="medium+vehicle_type", y="value", color="sub_vehicle_type+drive", facet_col="transport_type", facet_col_wrap=2,  labels={'value':'Passenger occupancy','medium+vehicle_type':'Medium + Vehicle type','sub_vehicle_type+drive':'Sub vehicle type + drive'}, title=title)
#save to plotting_output\analysis as html
fig.write_html("plotting_output/analysis/usa/{}.html".format(title))

# buses_fuels['medium+vehicle_type'] = buses_fuels['medium'] + ' ' + buses_fuels['vehicle_type']
# buses_fuels['sub_vehicle_type+drive'] = buses_fuels['vehicle_sub_type'] + ' ' + buses_fuels['drive']
# title = 'Buses fuels by vehicle type'
# fig = px.line(buses_fuels, x="date", y="value", color="sub_vehicle_type+drive", title=title)
# #save to plotting_output\analysis as html
# fig.write_html("plotting_output/analysis/usa/{}.html".format(title))
# #%%

#%%
#drop medium+vehicle_type and sub_vehicle_type+drive
fuel_economy_by_vehicle_type.drop(['medium+vehicle_type','sub_vehicle_type+drive'],axis=1,inplace=True)
vmt_by_vehicle_type.drop(['medium+vehicle_type','sub_vehicle_type+drive'],axis=1,inplace=True)
passenger_mpg.drop(['medium+vehicle_type','sub_vehicle_type+drive'],axis=1,inplace=True)
#%%
#we will now make the factors available for use for other economys. just do this by appending a copy of the data for each eocnomy.
# #also could incorporate all drive types here. to do that we can load in the concordances file from E:\APERC\transport_data_system\input_data\concordances\9th\model_concordances_measures.csv

concordances = pd.read_csv('input_data/concordances/9th/model_concordances_measures.csv')
#make all cols snake case
concordances.columns = concordances.columns.str.lower().str.replace(' ','_')

#drop date, unit
concordances.drop(['date', 'unit'],axis=1,inplace=True)
#where measure is occupancy or laod and transport type is freight, set it to load, then set it to occ where ttype is passenger
concordances.loc[(concordances['measure']=='Occupancy_or_load') & (concordances['transport_type']=='freight'),'measure'] = 'Load'
concordances.loc[(concordances['measure']=='Occupancy_or_load') & (concordances['transport_type']=='passenger'),'measure'] = 'Occupancy'
#join each df to the concordances file by dropping their economy col and joining on the other cols (except vehicle_sub_type)
#to get the extra drives we will drop drive from the files except fuel_economy_by_vehicle_type since it has the correct drives already
vmt_by_vehicle_type_new = vmt_by_vehicle_type.drop(['drive', 'economy'],axis=1).drop_duplicates().merge(concordances,how='inner',on=['transport_type','vehicle_type','medium','measure'])

fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type.drop(['economy'],axis=1).merge(concordances,how='inner',on=['transport_type','drive','vehicle_type','medium','measure'])

passenger_mpg_new = passenger_mpg.drop(['drive', 'economy'],axis=1).drop_duplicates().merge(concordances,how='inner',on=['transport_type','vehicle_type','medium','measure'])
#%%
#GREAT!
#save these in same folder as the other files
vmt_by_vehicle_type_new.to_csv('intermediate_data/USA/all_economys_vmt_by_vehicle_type_{}.csv'.format(FILE_DATE_ID),index=False)
fuel_economy_by_vehicle_type_new.to_csv('intermediate_data/USA/all_economys_fuel_economy_by_vehicle_type_{}.csv'.format(FILE_DATE_ID),index=False)
passenger_mpg_new.to_csv('intermediate_data/USA/all_economys_passenger_mpg_{}.csv'.format(FILE_DATE_ID),index=False)


# %%
