
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
    #lets also load in passengerkm, freight tonne km and energy use data from the transport data system.
    # combined_data_9th = pd.read_csv('{}/output_data/9th_dataset/combined_dataset_{}.csv'.format(transport_data_system_folder,FILE_DATE_ID2))
    combined_data_9th = combined_data_9th[combined_data_9th.Measure.isin(['passenger_km','freight_tonne_km','Energy','Occupancy', 'Load'])]

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

    #calcualte efficiency
    freight['Efficiency'] = freight['Energy']/(freight['freight_tonne_km']/freight['Load'])
    passenger['Efficiency'] = passenger['Energy']/(passenger['passenger_km']/passenger['Occupancy'])

    #concat
    combined_data_9th = pd.concat([freight,passenger],sort=False)

    #set dataset to 'Calculated'
    combined_data_9th['Dataset'] = 'Calculated'

    #There may be some missing values because we removed na efficiency values (0/0)
    #we will fill all fcev and phev values with an efficiency of 80% of their ice counterparts. so to do this we will extract any fcev or phevd/phevg drom drive;
    fcev = combined_data_9th[combined_data_9th.Drive.isin(['fcev'])]
    phev_d = combined_data_9th[combined_data_9th.Drive.isin(['phevd'])]
    phev_g = combined_data_9th[combined_data_9th.Drive.isin(['phevg'])]
    ice = combined_data_9th[combined_data_9th.Drive.isin(['g', 'd'])]
    #calc mean of g and d:
    ice = ice.groupby(['Economy','Vehicle Type','Transport Type']).mean().reset_index()
    combined_data_9th = combined_data_9th[~combined_data_9th.Drive.isin(['fcev','phevd','phevg'])]
    #now merge with the ice data
    fcev = fcev.merge(ice[['Economy','Vehicle Type','Transport Type','Efficiency']],on=['Economy','Vehicle Type','Transport Type'],how='left',suffixes=('','_ice'))
    phev_d = phev_d.merge(ice[['Economy','Vehicle Type','Transport Type','Efficiency']],on=['Economy','Vehicle Type','Transport Type'],how='left',suffixes=('','_ice'))
    phev_g = phev_g.merge(ice[['Economy','Vehicle Type','Transport Type','Efficiency']],on=['Economy','Vehicle Type','Transport Type'],how='left',suffixes=('','_ice'))
    #now we can fill the na values with 80% of the ice values
    fcev['Efficiency'] = fcev['Efficiency'].fillna(fcev['Efficiency_ice']*0.8)
    phev_d['Efficiency'] = phev_d['Efficiency'].fillna(phev_d['Efficiency_ice']*0.8)
    phev_g['Efficiency'] = phev_g['Efficiency'].fillna(phev_g['Efficiency_ice']*0.8)
    #now we can drop the ice values
    fcev = fcev.drop(columns=['Efficiency_ice'])
    phev_d = phev_d.drop(columns=['Efficiency_ice'])
    phev_g = phev_g.drop(columns=['Efficiency_ice'])
    #now we can concat the dfs back together
    combined_data_9th = pd.concat([fcev,phev_d,phev_g,combined_data_9th],sort=False)

    # #drop efficiency = na since itll come from0/0
    combined_data_9th = combined_data_9th.dropna(subset=['Efficiency'])

#%%
analyse = True
if analyse:
    #we will concat with the other eff data so we can plot it altogether.
    # eff_data = pd.concat([transport_data_system_df,combined_data_9th],sort=False)
    eff_data = combined_data_9th.copy()#atm we only get 0 values for new vehicle eff form the transport data system so lets just use the calculated data
    #drop na and 0 values
    eff_data = eff_data.dropna(subset=['Efficiency'])
    eff_data = eff_data[eff_data.Efficiency != 0]   
    #most of the data ranges from xe-10 to xe-8 whjere x is a number between 1 and 9. the higher numbers are making it hard to see the lower numbers. Lets try filter out bad numbers to get a better plot
    do_this = False
    if do_this:
        #to properly analyse it would be best if kept only the economys where we have data in 'IEA Fuel Economy $ GFEI'
        IEA_economys = transport_data_system_df.loc[transport_data_system_df.Dataset == 'IEA Fuel Economy $ GFEI','Economy'].unique()
        eff_data = eff_data[eff_data.Economy.isin(IEA_economys)]

    # #and keep only drive = 'g' 'd' and 'ice'
    # eff_data = eff_data[eff_data.Drive.isin(['g','d','ice'])]

    #and make a colwhich joins 'Vehicle Type','Transport Type','Drive'
    eff_data['index'] = eff_data['Vehicle Type'] + ' ' + eff_data['Transport Type'] + ' ' + eff_data['Drive']

#%%
#convert vlaues to litres per km we need to divide eff by 3.42e-8
eff_data['L_per_100km'] = (eff_data['Efficiency']/3.42e-8)*100
#convert date to just the first 4 digits so its easier to plot
eff_data['Date'] = eff_data['Date'].astype(str).str[:4]
#now 
#%%
analyse = False
if analyse:
    #now plot violin for y=Efficiency,x=Date and label=Dataset, facet=Economy, hue=Vehicle Type
    plot = px.bar(eff_data,x='index',y='Efficiency',color='Vehicle Type',facet_col='Economy',facet_col_wrap=3,hover_data=['Vehicle Type','Transport Type','Drive'])
    #make each y axis independent
    plot = plot.update_yaxes(matches=None)
    plot.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    plot.write_html('plotting_output/testing/efficiency_data_bar.html', auto_open=True)

    # print avg eff for each economy for each dataset
    for economy in eff_data.Economy.unique():
        print('\nXXXXX Economy: {} XXXXXXX'.format(economy))
        for dataset in eff_data.Dataset.unique():
            #ignore 'interpolation' dataset
            if dataset == 'interpolation':
                continue
            print('Dataset: {}'.format(dataset))
            print(eff_data[(eff_data.Economy == economy) & (eff_data.Dataset == dataset)].Efficiency.mean())
    #ok great it loks (jsut from a quick look no avg's) like the eff value centres around 3e-9 PJ per km for all data we plotted. If we convert that to litres of gasoline it is 3e-9/3.42e-8 = 0.087 litres per km.which is 8.7 per 100km which is just a bit higher than the average fuel economy of cars and vans in 2021 in the IEA https://www.iea.org/fuels-and-technologies/fuel-economy which is good because it measn we can feel happy using the IEA value, times the occupancy rate of 1.5 to get the eff per passenger km for ldv's!
    #Its also good because it means that the ratio of enegry use to passenger km for ldv g or d drives is correct. 

#ALSO PLOT EFFICIENCY IN LITRES PER KM
analyse = False
if analyse:
    #now plot y=Efficiency,x=Date and label=Dataset, facet=Economy, hue=Vehicle Type
    plot = px.bar(eff_data,x='index',y='L_per_100km',color='Vehicle Type',facet_col='Economy',facet_col_wrap=3,hover_data=['Vehicle Type','Transport Type','Drive'])
    plot = plot.update_yaxes(matches=None)
    plot.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))

    plot.write_html('plotting_output/testing/efficiency_data_l_per_100km_bar.html', auto_open=True)

    # print avg eff for each economy for each dataset
    for economy in eff_data.Economy.unique():
        print('\nXXXXX Economy: {} XXXXXXX'.format(economy))
        for dataset in eff_data.Dataset.unique():
            #ignore 'interpolation' dataset
            if dataset == 'interpolation':
                continue
            print('Dataset: {}'.format(dataset))
            print(eff_data[(eff_data.Economy == economy) & (eff_data.Dataset == dataset)].Efficiency.mean())
    #ok great it loks (jsut from a quick look no avg's) like the eff value centres around 3e-9 PJ per km for all data we plotted. If we convert that to litres of gasoline it is 3e-9/3.42e-8 = 0.87 litres per km.which is 8.7 per 100km which is just a bit higher than the average fuel economy of cars and vans in 2021 in the IEA https://www.iea.org/fuels-and-technologies/fuel-economy which is good because it measn we can feel happy using the IEA value, times the occupancy rate of 1.5 to get the eff per passenger km for ldv's!
    #Its also good because it means that the ratio of enegry use to passenger km for ldv g or d drives is correct. 
    #some others to expect:
    #heavy truck = 30-40 l/100km
    #bus = 20-30 l/100km
    #freight ldv = probably 10-20 l/100km
    #ldv = 8-10 l/100km
    #motorcycle = 5-8 l/100km
    #The typical hybrid offers fuel savings and CO2 reductions of 20 to 40% over gasoline-only vehicles.
    #The typical ev efficiency of energy conversion from on-board storage to turning the wheels is nearly five times greater for electricity than gasoline ~ which would be equiv to 1-2l/100km for an ldv i think

#%%
#lets group by index, remove outliers then calculate the mean of L_per_100km and Efficiency then plot again:
avg_eff = eff_data[['index','Efficiency','L_per_100km','Vehicle Type','Transport Type','Drive']]
#remove outliers within the group ['index','Vehicle Type']
avg_eff = avg_eff.groupby(['index','Vehicle Type','Transport Type','Drive']).apply(lambda x: x[(x.Efficiency-x.Efficiency.mean()).abs() < 2*x.Efficiency.std()])

#drop index and vehicle type
avg_eff.reset_index(inplace=True,drop=True)

#now do again
avg_eff = avg_eff.groupby(['index','Vehicle Type','Transport Type','Drive']).apply(lambda x: x[(x.Efficiency-x.Efficiency.mean()).abs() < 2*x.Efficiency.std()])

#drop index and vehicle type
avg_eff.reset_index(inplace=True,drop=True)
#%%
analyse = True
import plotly.express as px
if analyse:
    #plot with no outliers
    plot = px.box(avg_eff,x='index',y='L_per_100km',color='Vehicle Type',facet_col_wrap=3)
    plot = plot.update_yaxes(matches=None)
    plot.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))

    plot.write_html('plotting_output/estimations/efficiency_data_l_per_100km_box_no_outliers.html', auto_open=True)


#%%
#calcualte the mean
avg_eff = avg_eff.groupby(['index','Vehicle Type','Transport Type','Drive']).mean()
avg_eff.reset_index(inplace=True)
#%%
if analyse:
    #now plot y=Efficiency,x=Date and label=Dataset, facet=Economy, hue=Vehicle Type
    plot = px.bar(avg_eff,x='index',y='L_per_100km',color='Vehicle Type',facet_col_wrap=3)
    plot = plot.update_yaxes(matches=None)
    plot.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))

    plot.write_html('plotting_output/estimations/efficiency_data_l_per_100km_bar_no_outliers.html', auto_open=True)
#%%
#drop index col and L_per_100km then merge on all the otehr nec cols
avg_eff = avg_eff.drop(columns=['L_per_100km', 'index'])
avg_eff = avg_eff.rename(columns={'Efficiency':'Value'})
avg_eff['Measure'] = 'New_vehicle_efficiency'
avg_eff['Unit'] = 'PJ per km'
avg_eff['Dataset'] = 'avg_eff_no_outliers'
avg_eff['Date'] = '2017-12-31'
avg_eff['Source'] = 'transport_data_system_9th'
avg_eff['Medium'] = 'road'
avg_eff['Frequency'] = 'Yearly'
avg_eff['Scope'] = 'National'

avg_eff['Economy'] = combined_data_9th.Economy.unique()[0]
new_df = avg_eff.copy()
for economy in combined_data_9th.Economy.unique()[1:]:
    avg_eff2 = avg_eff.copy()
    avg_eff2['Economy'] = economy
    new_df = pd.concat([new_df,avg_eff2],ignore_index=True)


#%%
#thats good enough. lets just save that in a separate file and incorporate it with all data.
#save it so that it can be concatenated to the transport_data_system_df_original
new_df.to_csv('intermediate_data/estimated/new_vehicle_efficiency_estimates_{}.csv'.format(FILE_DATE_ID),index=False)

#%%

