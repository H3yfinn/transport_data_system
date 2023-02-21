
#%%

#we will use the data from 1_aggregate...
import pandas as pd
import numpy as np
import os
import datetime
import re

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

#%%

import utility_functions as utility_functions
# file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_concordance_')
# FILE_DATE_ID = 'DATE{}'.format(file_date)
# concord = pd.read_csv('./intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('./output_data/9th_dataset/', 'combined_dataset')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data_9th = pd.read_csv('./output_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID))

#%%
analyse = True
if analyse:
    #lets also load in passengerkm, freight tonne km and stocks data from the transport data system.
    # combined_data_9th = pd.read_csv('{}/output_data/9th_dataset/combined_dataset_{}.csv'.format(transport_data_system_folder,FILE_DATE_ID2))
    combined_data_9th = combined_data_9th[combined_data_9th.Measure.isin(['passenger_km','freight_tonne_km','Stocks','Occupancy', 'Load'])]

    #filter for 2017 only
    combined_data_9th = combined_data_9th[combined_data_9th.Date == '2017-12-31']
    #keep only road
    combined_data_9th = combined_data_9th[combined_data_9th.Medium == 'road']
    #drop nonnec cols
    combined_data_9th = combined_data_9th.drop(columns=['Fuel_Type','Source','Dataset','Dataset_selection_method','Unit'])

    #and we want to calcualte a value for efficiency per km so we will convert x_km to km by dividing by occupancy or load rate. To do thsi we will sep two dfs, based on transport type then pivot the m,easure col to get energy, km and occupancy/load cols. Then we can calcualte efficiency by dividing energy by (km/occupancy or load)

    freight = combined_data_9th[combined_data_9th['Transport Type'] == 'freight']
    passenger = combined_data_9th[combined_data_9th['Transport Type'] == 'passenger']
    
    #pivot
    cols = combined_data_9th.columns.to_list()
    cols.remove('Measure')
    cols.remove('Value')
    freight = freight.pivot(index=cols,columns='Measure',values='Value').reset_index()
    passenger = passenger.pivot(index=cols,columns='Measure',values='Value').reset_index()

    #drop drive and sum freight_tonne_km, passenger_km and stocks by the index cols
    freight = freight.drop(columns=['Drive'])
    passenger = passenger.drop(columns=['Drive'])
    
    INDEX_COLS = ['Date', 'Economy', 'Medium', 'Transport Type', 'Vehicle Type', 'Frequency',
       'Scope']
    #calc sum of freight_tonne_km, passenger_km and stocks
    freight_sum = freight.groupby(INDEX_COLS).sum().reset_index()
    passenger_sum = passenger.groupby(INDEX_COLS).sum().reset_index()
    #calc avg of occupancy and load
    freight_avg = freight.groupby(INDEX_COLS).mean().reset_index()
    passenger_avg = passenger.groupby(INDEX_COLS).mean().reset_index()

    #grab occ and laod  from avg, then drop from sum
    freight_sum = freight_sum.drop(columns=['Load'])
    freight = freight_sum.merge(freight_avg[['Load']],left_index=True,right_index=True,how='left')
    passenger_sum = passenger_sum.drop(columns=['Occupancy'])
    passenger = passenger_sum.merge(passenger_avg[['Occupancy']],left_index=True,right_index=True,how='left')

    #calcualte efficiency
    freight['travel_km_per_stock'] = (freight['freight_tonne_km'] / freight['Load'])/freight['Stocks']
    passenger['travel_km_per_stock'] = (passenger['passenger_km'] / passenger['Occupancy'])/passenger['Stocks']

    #concat
    travkm_per_stock = pd.concat([freight,passenger],sort=False)

    #set dataset to 'Calculated'
    travkm_per_stock['Dataset'] = 'Calculated'


#%%
#convert vlaues to billion km  
travkm_per_stock['travel_km_per_stock_billion'] = travkm_per_stock['travel_km_per_stock']/1000000000
#convert date to just the first 4 digits so its easier to plot
travkm_per_stock['Date'] = travkm_per_stock['Date'].astype(str).str[:4]

#and make a colwhich joins 'Vehicle Type','Transport Type','Drive'
travkm_per_stock['index'] = travkm_per_stock['Vehicle Type'] + ' ' + travkm_per_stock['Transport Type']
#%%
# #now 
# #%%
# analyse = False
# if analyse:
#     #now plot violin for y=Efficiency,x=Date and label=Dataset, facet=Economy, hue=Vehicle Type
#     plot = px.bar(eff_data,x='index',y='Efficiency',color='Vehicle Type',facet_col='Economy',facet_col_wrap=3,hover_data=['Vehicle Type','Transport Type','Drive'])
#     #make each y axis independent
#     plot = plot.update_yaxes(matches=None)
#     plot.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
#     plot.write_html('plotting_output/testing/efficiency_data_bar.html', auto_open=True)

#     # print avg eff for each economy for each dataset
#     for economy in eff_data.Economy.unique():
#         print('\nXXXXX Economy: {} XXXXXXX'.format(economy))
#         for dataset in eff_data.Dataset.unique():
#             #ignore 'interpolation' dataset
#             if dataset == 'interpolation':
#                 continue
#             print('Dataset: {}'.format(dataset))
#             print(eff_data[(eff_data.Economy == economy) & (eff_data.Dataset == dataset)].Efficiency.mean())
#     #ok great it loks (jsut from a quick look no avg's) like the eff value centres around 3e-9 PJ per km for all data we plotted. If we convert that to litres of gasoline it is 3e-9/3.42e-8 = 0.087 litres per km.which is 8.7 per 100km which is just a bit higher than the average fuel economy of cars and vans in 2021 in the IEA https://www.iea.org/fuels-and-technologies/fuel-economy which is good because it measn we can feel happy using the IEA value, times the occupancy rate of 1.5 to get the eff per passenger km for ldv's!
#     #Its also good because it means that the ratio of enegry use to passenger km for ldv g or d drives is correct. 

# #ALSO PLOT EFFICIENCY IN LITRES PER KM
# analyse = False
# if analyse:
#     #now plot y=Efficiency,x=Date and label=Dataset, facet=Economy, hue=Vehicle Type
#     plot = px.bar(eff_data,x='index',y='L_per_100km',color='Vehicle Type',facet_col='Economy',facet_col_wrap=3,hover_data=['Vehicle Type','Transport Type','Drive'])
#     plot = plot.update_yaxes(matches=None)
#     plot.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))

#     plot.write_html('plotting_output/testing/efficiency_data_l_per_100km_bar.html', auto_open=True)

#     # print avg eff for each economy for each dataset
#     for economy in eff_data.Economy.unique():
#         print('\nXXXXX Economy: {} XXXXXXX'.format(economy))
#         for dataset in eff_data.Dataset.unique():
#             #ignore 'interpolation' dataset
#             if dataset == 'interpolation':
#                 continue
#             print('Dataset: {}'.format(dataset))
#             print(eff_data[(eff_data.Economy == economy) & (eff_data.Dataset == dataset)].Efficiency.mean())
#     #ok great it loks (jsut from a quick look no avg's) like the eff value centres around 3e-9 PJ per km for all data we plotted. If we convert that to litres of gasoline it is 3e-9/3.42e-8 = 0.87 litres per km.which is 8.7 per 100km which is just a bit higher than the average fuel economy of cars and vans in 2021 in the IEA https://www.iea.org/fuels-and-technologies/fuel-economy which is good because it measn we can feel happy using the IEA value, times the occupancy rate of 1.5 to get the eff per passenger km for ldv's!
#     #Its also good because it means that the ratio of enegry use to passenger km for ldv g or d drives is correct. 
#     #some others to expect:
#     #heavy truck = 30-40 l/100km
#     #bus = 20-30 l/100km
#     #freight ldv = probably 10-20 l/100km
#     #ldv = 8-10 l/100km
#     #motorcycle = 5-8 l/100km
#     #The typical hybrid offers fuel savings and CO2 reductions of 20 to 40% over gasoline-only vehicles.
#     #The typical ev efficiency of energy conversion from on-board storage to turning the wheels is nearly five times greater for electricity than gasoline ~ which would be equiv to 1-2l/100km for an ldv i think

#%%
#lets group by index, remove outliers then calculate the mean of L_per_100km and Efficiency then plot again:
travkm_per_stock = travkm_per_stock[['index','travel_km_per_stock_billion','travel_km_per_stock','Vehicle Type','Transport Type']]
#remove outliers within the group ['index','Vehicle Type']
travkm_per_stock = travkm_per_stock.groupby(['index','Vehicle Type','Transport Type']).apply(lambda x: x[(x.travel_km_per_stock-x.travel_km_per_stock.mean()).abs() < 2*x.travel_km_per_stock.std()])

#drop index and vehicle type
travkm_per_stock.reset_index(inplace=True,drop=True)

#now do again
travkm_per_stock = travkm_per_stock.groupby(['index','Vehicle Type','Transport Type']).apply(lambda x: x[(x.travel_km_per_stock-x.travel_km_per_stock.mean()).abs() < 2*x.travel_km_per_stock.std()])

#drop index and vehicle type
travkm_per_stock.reset_index(inplace=True,drop=True)
#%%
analyse = True
import plotly.express as px
if analyse:
    #plot with no outliers
    plot = px.box(travkm_per_stock,x='index',y='travel_km_per_stock_billion',color='Vehicle Type',facet_col_wrap=3)
    plot = plot.update_yaxes(matches=None)
    plot.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))

    plot.write_html('plotting_output/estimations/travel_km_per_stock_billion_box_no_outliers.html', auto_open=True)


#%%
#aagrab the median as it is less sensitive to outliers
travkm_per_stock = travkm_per_stock.groupby(['index','Vehicle Type','Transport Type']).median()
travkm_per_stock.reset_index(inplace=True)
#%%
if analyse:
    #now plot y=Efficiency,x=Date and label=Dataset, facet=Economy, hue=Vehicle Type
    plot = px.bar(travkm_per_stock,x='index',y='travel_km_per_stock_billion',color='Vehicle Type',facet_col_wrap=3)
    plot = plot.update_yaxes(matches=None)
    plot.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))

    plot.write_html('plotting_output/estimations/efficiency_data_l_per_100km_bar_no_outliers.html', auto_open=True)
#%%
#drop index col and travel_km_per_stock_billion then merge on all the otehr nec cols
travkm_per_stock = travkm_per_stock.drop(columns=['travel_km_per_stock_billion', 'index'])
travkm_per_stock = travkm_per_stock.rename(columns={'travel_km_per_stock':'Value'})
travkm_per_stock['Measure'] = 'Travel_km_per_stock'
travkm_per_stock['Unit'] = 'Travel km per stock'
travkm_per_stock['Dataset'] = 'travkm_per_stock_no_outliers'
travkm_per_stock['Date'] = '2017-12-31'
travkm_per_stock['Source'] = 'transport_data_system_9th'
travkm_per_stock['Medium'] = 'road'
travkm_per_stock['Frequency'] = 'Yearly'
travkm_per_stock['Scope'] = 'National'

travkm_per_stock['Economy'] = combined_data_9th.Economy.unique()[0]
new_df = travkm_per_stock.copy()
for economy in combined_data_9th.Economy.unique()[1:]:
    travkm_per_stock2 = travkm_per_stock.copy()
    travkm_per_stock2['Economy'] = economy
    new_df = pd.concat([new_df,travkm_per_stock2],ignore_index=True)
#%%
#we also need every unique drive. 
drive = combined_data_9th[['Vehicle Type','Transport Type','Drive']].drop_duplicates()
# So join combined_data_9th with the new_df using Transport Type and Vehicle Type as the keys
new_df = new_df.merge(drive,how='left',on=['Vehicle Type','Transport Type'])


#%%
#thats good enough. lets just save that in a separate file and incorporate it with all data.
#save it so that it can be concatenated to the transport_data_system_df_original
new_df.to_csv('intermediate_data/estimated/travel_km_per_stock_estimates_{}.csv'.format(FILE_DATE_ID),index=False)

#%%

