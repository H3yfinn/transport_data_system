
#%%
#repalcement for estimate avg efficiency data because that wasnt resulting in very trsutworthy valeus. Instead we will use this to retreive and estimat better valus.
import pandas as pd
import numpy as np
import os
import datetime
import re
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
import sys
folder_path = './aggregation_code'  # Replace with the actual path of the folder you want to add
sys.path.append(folder_path)
import utility_functions 
#first, take in data from 
# fuel_economy_by_vehicle_type_new.to_csv('intermediate_data/USA/all_economys_fuel_economy_by_vehicle_type_{}.csv'.format(FILE_DATE_ID),index=False)
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/USA/', 'all_economys_fuel_economy_by_vehicle_type_')
fuel_economy_by_vehicle_type_new = pd.read_csv('intermediate_data/USA/all_economys_fuel_economy_by_vehicle_type_{}.csv'.format('DATE'+file_date))

#note that this data is really only for the US. but we will just assume its the same for all countries for now. Then we will times the ice value by 3.5 to find the efficiency of BEV's. And we will times the fcev value by 1.8 to find the efficiency of fcevs.
#this is based on the following:
# The figures can vary, but for a ballpark:

# Fuel Cell Electric Vehicles (FCEVs): Around 40-60% efficient tank-to-wheel
# Battery Electric Vehicles (BEVs): Around 85-90% efficient tank-to-wheel
# Gasoline Vehicles: Approximately 20-30% efficient tank-to-wheel
# Diesel Vehicles: Around 30-45% efficient tank-to-wheel
# CNG and LPG are about 40-45% efficient tank-to-wheel
#so if we assume everything in terms of the efficiency of an ev then to convert from bev to x drive type we would do the following:
#bev to ice_g: 87.5/25 times more efficient
#bev to ice_d (87.5/37.5): 2.33 times more efficient
#bev to cng: 42.5/87.5 times more efficient
#bev to lpg: 42.5/87.5 times more efficient
#bev to phev_g: (87.5/25)/2 times more efficient
#bev to phev_d: (87.5/37.5)/2  times more efficient
#bev to fcev: 50/87.5 times more efficient
#bev to bev: 1 times more efficient
#%%
# Define average efficiency values in percentage terms
# avg_efficiency = {
#     'bev': 87.5,
#     'ice_g': 25,
#     'ice_d': 37.5,
#     'cng': 42.5,
#     'lpg': 42.5,
#     'phev_g': 25,  # Assuming the ICE part of PHEV is as efficient as a normal gasoline engine
#     'phev_d': 37.5,  # Assuming the ICE part of PHEV is as efficient as a normal diesel engine
#     'fcev': 50
# }
#the above was swapped for index average efficiency of each drive type in the USA's EIA outlook data. this essentially allows us to copy their efficiency data and therefore avoid having to estimate it ourselves. This is because the data is not very reliable when we estimate it ourselves, wehreas the EIA data is very reliable!
#%%
efficiency_eia = pd.read_csv('intermediate_data/EIA/eia_2023_weo_fuel_economy_original.csv')
#drop all cols except vehicle type, drive and value, then index the vlaes in terms of the ice_g efficiency
#grab year == 2022 only
efficiency_eia = efficiency_eia[efficiency_eia['year']==2022]

efficiency_eia = efficiency_eia[['original_vehicle_type', 'drive','Value']]
efficiency_eia = efficiency_eia.groupby(['original_vehicle_type','drive']).mean().reset_index()
#index the values in terms of the ice_g efficiency within each vehicle type
#this helps to avoid where some vehicle types will not have all teh drives and so averaging at the first step will sway the values
for vehicle_type in efficiency_eia['original_vehicle_type'].unique():
    v_type_eff = efficiency_eia[efficiency_eia['original_vehicle_type']==vehicle_type]
    v_type_eff['Value'] = v_type_eff['Value']/v_type_eff[v_type_eff['drive']=='ice_g']['Value'].values[0]
    efficiency_eia.loc[efficiency_eia['original_vehicle_type']==vehicle_type,'Value'] = v_type_eff['Value']

efficiency_eia.reset_index(inplace=True, drop=True)
#%%
#drop vehicle type
efficiency_eia = efficiency_eia.drop(columns=['original_vehicle_type'])
#now we can get the average, indexed, efficiency of each drive type
avg_efficiency = efficiency_eia.groupby(['drive']).mean().reset_index()
#convert to dictionary
avg_efficiency = avg_efficiency.set_index('drive').to_dict()['Value']
#Interesting that cng ends up at 0.9 whereas lpg is 1.4. i guess we jsut stick with the EIA data for now. Also notethat this change decreases the ratios from:
# {'bev': 3.5,
#  'ice_g': 1.0,
#  'ice_d': 1.5,
#  'cng': 1.7,
#  'lpg': 1.7,
#  'phev_g': 2.25,
#  'phev_d': 2.5,
#  'fcev': 2.0}
# to:
#   {'bev': 2.6005059601244254,
#  'cng': 0.9036279850081149,
#  'fcev': 1.2344189390821694,
#  'ice_d': 1.0516428159749103,
#  'ice_g': 1.0,
#  'lpg': 1.4333634668259467,
#  'phev_g': 1.5731773927778308,
#  'phev_d': 1.5731773927778308}
#add phev_d as the same value as phev_g (it might be sloghtly more efficient but we will assume its the same for now since sample size is small)
avg_efficiency['phev_d'] = avg_efficiency['phev_g']
efficiency_ratios = avg_efficiency
#%%

# # Calculate efficiency ratios with respect to ice_g
# efficiency_ratios = {key: value / avg_efficiency['ice_g'] for key, value in avg_efficiency.items()}

# # For PHEVs, we divide by 2 to account for the part-time electric operation
# efficiency_ratios['phev_g'] = (efficiency_ratios['phev_g'] + efficiency_ratios['bev'])/2
# efficiency_ratios['phev_d'] = (efficiency_ratios['phev_d'] + efficiency_ratios['bev'])/2

# # Print the efficiency ratios
# for vehicle, ratio in efficiency_ratios.items():
#     print(f"{vehicle} is {ratio:.2f} times more efficient than ice_g.")
#%%

#we will also assume that cng and lpg powered vehicles are the same efficiency as ice ones. THis is because there is not much knowledge aobut these, but it is assumed that if they are more efficient its only by a few percent, and they would also lose out on the relative efficiency gains from learning.

#drop the vehicle sub type ans then average the values
# fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type_new.drop(columns=['vehicle_sub_type'])
index_cols = fuel_economy_by_vehicle_type_new.columns.tolist()
index_cols.remove('value')
fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type_new.groupby(index_cols).mean().reset_index()

#now drop vehicle_type = ridesource
fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type_new[~fuel_economy_by_vehicle_type_new['vehicle_type'].isin(['ridesource'])]

#get ice_g and ice_d data:
fuel_economy_by_vehicle_type_new_ice_g = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['drive']=='ice_g']
fuel_economy_by_vehicle_type_new_ice_d = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['drive']=='ice_d']

# #now filter for only the ice_g vehicles
fuel_economy_by_vehicle_type_new_ice = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['drive']=='ice_g']

#and now for drive in cng, lpg, create a cpoy of the df
fuel_economy_by_vehicle_type_new_cng = fuel_economy_by_vehicle_type_new_ice.copy()
fuel_economy_by_vehicle_type_new_lpg = fuel_economy_by_vehicle_type_new_ice.copy()
fuel_economy_by_vehicle_type_new_bev = fuel_economy_by_vehicle_type_new_ice.copy()
fuel_economy_by_vehicle_type_new_phev_g = fuel_economy_by_vehicle_type_new_ice_g.copy()
fuel_economy_by_vehicle_type_new_phev_d = fuel_economy_by_vehicle_type_new_ice_d.copy()
fuel_economy_by_vehicle_type_new_phev = fuel_economy_by_vehicle_type_new_ice.copy()
fuel_economy_by_vehicle_type_new_fcev = fuel_economy_by_vehicle_type_new_ice.copy()
#and then change the drive type to cng and lpg respectively
fuel_economy_by_vehicle_type_new_cng['drive'] = 'cng'
fuel_economy_by_vehicle_type_new_lpg['drive'] = 'lpg'

fuel_economy_by_vehicle_type_new_bev['drive'] = 'bev'
fuel_economy_by_vehicle_type_new_fcev['drive'] = 'fcev'

# fuel_economy_by_vehicle_type_new_phev['drive'] = 'phev'
fuel_economy_by_vehicle_type_new_phev_g['drive'] = 'phev_g'
fuel_economy_by_vehicle_type_new_phev_d['drive'] = 'phev_d'

#and now calcualte the efficiency all of these vehicles in terms of ice_g
fuel_economy_by_vehicle_type_new_bev['value'] = fuel_economy_by_vehicle_type_new_bev['value']*efficiency_ratios['bev']
fuel_economy_by_vehicle_type_new_fcev['value'] = fuel_economy_by_vehicle_type_new_fcev['value']*efficiency_ratios['fcev']
fuel_economy_by_vehicle_type_new_phev_g['value'] = fuel_economy_by_vehicle_type_new_phev_g['value']*efficiency_ratios['phev_g']
fuel_economy_by_vehicle_type_new_phev_d['value'] = fuel_economy_by_vehicle_type_new_phev_d['value']*efficiency_ratios['phev_d']
fuel_economy_by_vehicle_type_new_cng['value'] = fuel_economy_by_vehicle_type_new_cng['value']*efficiency_ratios['cng']
fuel_economy_by_vehicle_type_new_lpg['value'] = fuel_economy_by_vehicle_type_new_lpg['value']*efficiency_ratios['lpg']


#and then concat them to the original df
fuel_economy_by_vehicle_type_new = pd.concat([fuel_economy_by_vehicle_type_new_ice,fuel_economy_by_vehicle_type_new_cng,fuel_economy_by_vehicle_type_new_lpg, fuel_economy_by_vehicle_type_new_bev, fuel_economy_by_vehicle_type_new_fcev, 
 fuel_economy_by_vehicle_type_new_phev_g, fuel_economy_by_vehicle_type_new_phev_d, fuel_economy_by_vehicle_type_new_ice_g, fuel_economy_by_vehicle_type_new_ice_d])
# fuel_economy_by_vehicle_type_new_phev,
#and then drop the duplicates
fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type_new.drop_duplicates()

#%%
#ADJUST VEHICLE TYPE
#now adjust efficiency for cars vs suvs and ht vs mt, since they are all based on the same data
car = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['vehicle_type']=='car']
suv = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['vehicle_type']=='suv']
ht = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['vehicle_type']=='ht']
mt = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['vehicle_type']=='mt']
lcv = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['vehicle_type']=='lcv']
lt = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['vehicle_type']=='lt']
fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type_new[~fuel_economy_by_vehicle_type_new['vehicle_type'].isin(['car','suv','ht','mt', 'lcv', 'lt'])]
car['value'] = car['value']
suv['value'] = suv['value']*0.8
lt['value'] = lt['value']*0.8#came from its own data
ht['value'] = ht['value']
mt['value'] = mt['value']
lcv['value'] = lcv['value']*2
#get the lcvs and make them a little more efficient. They represent quite a range of vehicles and currently they relfect a delivery truck which si quite inefficient. So we will make them 2 times more efficient (might be an exaggeration but we will see how it goes)
#%%
#extract 2w vehicle type and replicate it for freight
two_wheeler = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['vehicle_type']=='2w']
two_wheeler['transport_type'] = 'freight'
fuel_economy_by_vehicle_type_new = pd.concat([fuel_economy_by_vehicle_type_new,two_wheeler])

#put thme back in:
fuel_economy_by_vehicle_type_new = pd.concat([fuel_economy_by_vehicle_type_new,car,suv,ht,mt,lcv,lt])
#%%
#now replicate the df for every year between 2010 and 2025
fuel_economy_by_vehicle_type_new['date'] = '2010-12-31'
fuel_economy_by_vehicle_type_new_2010 = fuel_economy_by_vehicle_type_new.copy()
for year in range(2010+1, 2025):
    fuel_economy_by_vehicle_type_new_2010['date'] = str(year)+'-12-31'
    fuel_economy_by_vehicle_type_new = pd.concat([fuel_economy_by_vehicle_type_new,fuel_economy_by_vehicle_type_new_2010])

#and replicate the data again so we ahve a new df for new_vehicle_efficiency
new_fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type_new.copy()
new_fuel_economy_by_vehicle_type_new['measure']='new_vehicle_efficiency'

#concatenate the two dataframes
fuel_economy_by_vehicle_type_new = pd.concat([fuel_economy_by_vehicle_type_new,new_fuel_economy_by_vehicle_type_new])
#%%
#where the economy is 08_JPN, make the car efficiency 1.2 times more efficient, because of the smaller size of cars in japan
fuel_economy_by_vehicle_type_new.loc[(fuel_economy_by_vehicle_type_new['economy']=='08_JPN') & (fuel_economy_by_vehicle_type_new['vehicle_type']=='car'),'value'] = fuel_economy_by_vehicle_type_new.loc[(fuel_economy_by_vehicle_type_new['economy']=='08_JPN') & (fuel_economy_by_vehicle_type_new['vehicle_type']=='car'),'value']*1.2

#make dataset col
fuel_economy_by_vehicle_type_new['dataset'] = 'USA_alternative_fuels_data_center'

fuel_economy_by_vehicle_type_new['Fuel'] = 'all'

#%%
import datetime
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
#save it so that it can be concatenated to the transport_data_system_df_original
fuel_economy_by_vehicle_type_new.to_csv('intermediate_data/estimated/USA_based_vehicle_efficiency_estimates_{}.csv'.format(FILE_DATE_ID),index=False)

#%%

import plotly.express as px
import plotly.graph_objects as go
fuel_economy_by_vehicle_type_plot = fuel_economy_by_vehicle_type_new.copy()
fuel_economy_by_vehicle_type_plot['transport_type+vehicle_type'] = fuel_economy_by_vehicle_type_plot['transport_type'] + ' ' + fuel_economy_by_vehicle_type_plot['vehicle_type']
# fuel_economy_by_vehicle_type['drive'] = fuel_economy_by_vehicle_type['vehicle_sub_type'] + ' ' + fuel_economy_by_vehicle_type['drive']
#foilter for only usa
fuel_economy_by_vehicle_type_plot = fuel_economy_by_vehicle_type_plot[fuel_economy_by_vehicle_type_plot['economy']=='20_USA']
#extract year form date
fuel_economy_by_vehicle_type_plot['year'] = fuel_economy_by_vehicle_type_plot['date'].str[:4].astype(int)
#grab  measure = Efficiency only
fuel_economy_by_vehicle_type_plot = fuel_economy_by_vehicle_type_plot[fuel_economy_by_vehicle_type_plot['measure']=='Efficiency']
#%%
title = 'Fuel economy by vehicle type - adjusted usa only'
fig = px.bar(fuel_economy_by_vehicle_type_plot[fuel_economy_by_vehicle_type_plot['date']==fuel_economy_by_vehicle_type_plot['date'].max()], x="transport_type+vehicle_type", y="value", color="drive", facet_col="transport_type", facet_col_wrap=2, title=title)
#save to plotting_output\analysis as html
fig.write_html("plotting_output/analysis/usa/{}.html".format(title))

#do a strip chart*
fig = px.strip(fuel_economy_by_vehicle_type_plot[fuel_economy_by_vehicle_type_plot['date']==fuel_economy_by_vehicle_type_plot['date'].max()], x='transport_type+vehicle_type', y='value', color='drive', title=title)
fig.write_html("plotting_output/analysis/usa/strip - {}.html".format(title))

#do a faceted bar chart
fig = px.bar(fuel_economy_by_vehicle_type_plot[fuel_economy_by_vehicle_type_plot['date']==fuel_economy_by_vehicle_type_plot['date'].max()], x='drive', y='value', color='drive', title=title, facet_col="transport_type+vehicle_type", facet_col_wrap=4)
fig.write_html("plotting_output/analysis/usa/bar - {}.html".format(title))

#plot as line, with facets as the transport_type+vehicle_type
title = 'Line graph - Fuel economy by vehicle type - adjusted usa only'
fig = px.line(fuel_economy_by_vehicle_type_plot, x="year", y="value", color="drive", facet_col="transport_type+vehicle_type", facet_col_wrap=2, title=title)
fig.write_html("plotting_output/analysis/usa/{}.html".format(title))

#
# %%
