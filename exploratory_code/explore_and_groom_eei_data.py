#import eei data from input_data/EEI/activity.xlsx
#%%
import pandas as pd
import numpy as np
import os
import re
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
# Load data
data = pd.read_excel('input_data/EEI/activity.xlsx', sheet_name='Sheet1')
data.head()
#%%
#cols:
# Country	Activity	Product	2000	2001	2002	2003	2004	2005	2006	2007	2008	2009	2010	2011	2012	2013	2014	2015	2016	2017	2018	2019	2020	2021

#Convert Product into Measure/Unit columns:

#first, we have an issue where there are a lot of trailing spaces in the Product column. so remove them then see the unique vlaues:
data['Product'] = data['Product'].str.strip()
data['Product'].unique()

#also repalce vlaues that are just '..' with np.nan
data.replace('..', np.nan, inplace=True)
data.replace('x', np.nan, inplace=True)
# array(['Population (10^6)', 'Services employment (10^6)',
#        'Occupied dwellings (10^6)', 'Residential floor area (10^9 m2)',
#        'Heating degree days (10^3)', 'Cooling degree days (10^3)',
#        'Stocks (million units)', 'Value added (10^9 USD PPP 2015)',
#        'Cement production (10^6 t)', 'Steel production (10^6 t)',
#        'Passenger-kilometres (10^9 pkm)', 'Vehicle-kilometres (10^9 vkm)',
#        'Vehicle stock (10^6)', 'Tonne-kilometres (10^9 tkm)',
#        'Occupied dwellings of which heated by oil products (%)',
#        'Occupied dwellings of which heated by gas (%)',
#        'Occupied dwellings of which heated by biofuels (%)',
#        'Occupied dwellings of which heated by district heating (%)',
#        'Occupied dwellings of which heated by electricity (%)',
#        'Services floor area (10^9 m2)', 'Peak power (MWp)'], dtype=object
product_to_measure_dict = {
    'Population (10^6)': 'Population',
    'Services employment (10^6)': 'Services employment',
    'Occupied dwellings (10^6)': 'Occupied dwellings',
    'Residential floor area (10^9 m2)': 'Residential floor area',
    'Heating degree days (10^3)': 'Heating degree days',
    'Cooling degree days (10^3)': 'Cooling degree days',
    'Stocks (million units)': 'Residential technology stocks',
    'Value added (10^9 USD PPP 2015)': 'GDP',
    'Cement production (10^6 t)': 'Cement production',
    'Steel production (10^6 t)': 'Steel production',
    'Passenger-kilometres (10^9 pkm)': 'Passenger_km',
    'Vehicle-kilometres (10^9 vkm)': 'Vehicle kilometres',
    'Vehicle stock (10^6)': 'Stocks',
    'Tonne-kilometres (10^9 tkm)': 'Freight_tonne_km',#will need to have transport type set to freight and measure set to activity
    'Occupied dwellings of which heated by oil products (%)': 'Occupied dwellings of which heated by oil products',
    'Occupied dwellings of which heated by gas': 'Occupied dwellings of which heated by gas',
    'Occupied dwellings of which heated by biofuels': 'Occupied dwellings of which heated by biofuels',
    'Occupied dwellings of which heated by district heating': 'Occupied dwellings of which heated by district heating',
    'Occupied dwellings of which heated by electricity': 'Occupied dwellings of which heated by electricity',
    'Services floor area (10^9 m2)': 'Services floor area',
    'Peak power (MWp)': 'Peak power'
}

product_to_unit_dict = {
    'Population (10^6)': '10^6',
    'Services employment (10^6)': '10^6',
    'Occupied dwellings (10^6)': '10^6',
    'Residential floor area (10^9 m2)': '10^9 m2',
    'Heating degree days (10^3)': '10^3',
    'Cooling degree days (10^3)': '10^3',
    'Stocks (million units)': 'million units',
    'Value added (10^9 USD PPP 2015)': '10^9 USD PPP 2015',
    'Cement production (10^6 t)': '10^6 t',
    'Steel production (10^6 t)': '10^6 t',
    'Passenger-kilometres (10^9 pkm)': '10^9 pkm',
    'Vehicle-kilometres (10^9 vkm)': '10^9 vkm',
    'Vehicle stock (10^6)': '10^6',
    'Tonne-kilometres (10^9 tkm)': '10^9 tkm',
    'Occupied dwellings of which heated by oil products (%)': '%',
    'Occupied dwellings of which heated by gas': '%',
    'Occupied dwellings of which heated by biofuels': '%',
    'Occupied dwellings of which heated by district heating': '%',
    'Occupied dwellings of which heated by electricity': '%',
    'Services floor area (10^9 m2)': '10^9 m2',
    'Peak power (MWp)': 'MWp'
}
#%%
# We also have Activity column:
# array(['General Activity', 'Freezers',
#        'Refrigerator/Freezer combinations', 'Dish washers',
#        'Clothes washers', 'Clothes dryers', 'Televisions',
#        'Personal computers', 'Manufacturing [ISIC 10-18; 20-32]',
#        'Food and tobacco [ISIC 10-12]',
#        'Textiles and leather [ISIC 13-15]',
#        'Wood and wood products [ISIC 16]',
#        'Paper pulp and printing [ISIC 17-18]',
#        'Chemicals and chemical products [ISIC 20-21]',
#        'Rubber and plastic [ISIC 22]', 'Non-metallic minerals [ISIC 23]',
#        'Basic metals [ISIC 24]', 'Machinery [ISIC 25-28]',
#        'Other manufacturing [ISIC 31-32]',
#        'Agriculture forestry and fishing [ISIC 01-03]',
#        'Mining [ISIC 05-09]', 'Construction [ISIC 41-43]',
#        'Total Services', 'Commodities', 'Cars/light trucks',
#        'Motorcycles', 'Buses', 'Passenger trains',
#        'Domestic passenger airplanes', 'Domestic passenger ships',
#        'Freight trucks', 'Freight trains', 'Domestic freight airplanes',
#        'Domestic freight ships', 'Total passenger transport',
#        'Total freight transport', 'Transport equipment [ISIC 29-30]',
#        'Coke and refined petroleum products [ISIC 19]',
#        'Of which: Metro and light rail', 'Of which: Conventional rail',
#        'Of which: Light commercial vehicle', 'Refrigerators',
#        'Air conditioners', 'Of which: Cars', 'Heat pump',
#        'Solar thermal panels', 'Solar photovoltaic panels',
#        'Of which: High-speed rail'], dtype=object)
data['Activity'].unique()

#this is kind of the sector. Its probably good if we keep that as a column but rename it to detailed_sector. then we can also create a sector column that would be one of transport, residential, services, industry, agriculture, macro or other.
#%%
#rename columns:
data['detailed_sector'] = data['Activity']
sector_dict = {
    'General Activity': 'macro',
    'Freezers': 'residential',
    'Refrigerator/Freezer combinations': 'residential',
    'Dish washers': 'residential',
    'Clothes washers': 'residential',
    'Clothes dryers': 'residential',
    'Televisions': 'residential',
    'Personal computers': 'residential',
    'Manufacturing [ISIC 10-18; 20-32]': 'industry',
    'Food and tobacco [ISIC 10-12]': 'industry',
    'Textiles and leather [ISIC 13-15]': 'industry',
    'Wood and wood products [ISIC 16]': 'industry',
    'Paper pulp and printing [ISIC 17-18]': 'industry',
    'Chemicals and chemical products [ISIC 20-21]': 'industry',
    'Rubber and plastic [ISIC 22]': 'industry',
    'Non-metallic minerals [ISIC 23]': 'industry',
    'Basic metals [ISIC 24]': 'industry',
    'Machinery [ISIC 25-28]': 'industry',
    'Other manufacturing [ISIC 31-32]': 'industry',
    'Agriculture forestry and fishing [ISIC 01-03]': 'agriculture',
    'Mining [ISIC 05-09]': 'industry',
    'Construction [ISIC 41-43]': 'industry',
    'Total Services': 'services',
    'Commodities': 'industry',
    'Cars/light trucks': 'transport',
    'Motorcycles': 'transport',
    'Buses': 'transport',
    'Passenger trains': 'transport',
    'Domestic passenger airplanes': 'transport',
    'Domestic passenger ships': 'transport',
    'Freight trucks': 'transport',
    'Freight trains': 'transport',
    'Domestic freight airplanes': 'transport',
    'Domestic freight ships': 'transport',
    'Total passenger transport': 'transport',
    'Total freight transport': 'transport',
    'Transport equipment [ISIC 29-30]': 'industry',
    'Coke and refined petroleum products [ISIC 19]': 'industry',
    'Of which: Metro and light rail': 'transport',
    'Of which: Conventional rail': 'transport',
    'Of which: Light commercial vehicle': 'transport',
    'Refrigerators': 'residential',
    'Air conditioners': 'residential',
    'Of which: Cars': 'transport',
    'Heat pump': 'residential',
    'Solar thermal panels': 'residential',
    'Solar photovoltaic panels': 'residential',
    'Of which: High-speed rail': 'transport'
}

###################################################
#%%
#ok now do the mappings:
data['sector'] = data['detailed_sector'].map(sector_dict)
data['measure'] = data['Product'].map(product_to_measure_dict)
data['unit'] = data['Product'].map(product_to_unit_dict)
data.drop(columns=['Product', 'Activity'], inplace=True)
#and convert any economies we know to their iso codes
country_codes = pd.read_csv('config/economy_code_to_name.csv')
#stack the following cols: Economy_name	Alt_name	Alt_name2	Alt_name3

country_codes = country_codes.melt(id_vars='Economy', value_vars=['Economy_name', 'Alt_name', 'Alt_name2', 'Alt_name3'], value_name='Country')
#and drop nas
country_codes.dropna(inplace=True)


data = data.merge(country_codes[['Country', 'Economy']], on='Country', how='left')

#dont need to worry if any are missing, this is jsut to identify apec eocnomeis.

#double check these: data[['detailed_sector',
#                 'sector',         'measure',            'unit']].drop_duplicates().to_clipboard()
# detailed_sector	sector	measure	unit
# General Activity	macro	Population	10^6
# General Activity	macro	Services employment	10^6
# General Activity	macro	Occupied dwellings	10^6
# General Activity	macro	Residential floor area	10^9 m2
# General Activity	macro	Heating degree days	10^3
# General Activity	macro	Cooling degree days	10^3
# Freezers	residential	Residential technology stocks	million units
# Refrigerator/Freezer combinations	residential	Residential technology stocks	million units
# Dish washers	residential	Residential technology stocks	million units
# Clothes washers	residential	Residential technology stocks	million units
# Clothes dryers	residential	Residential technology stocks	million units
# Televisions	residential	Residential technology stocks	million units
# Personal computers	residential	Residential technology stocks	million units
# Manufacturing [ISIC 10-18; 20-32]	industry	GDP	10^9 USD PPP 2015
# Food and tobacco [ISIC 10-12]	industry	GDP	10^9 USD PPP 2015
# Textiles and leather [ISIC 13-15]	industry	GDP	10^9 USD PPP 2015
# Wood and wood products [ISIC 16]	industry	GDP	10^9 USD PPP 2015
# Paper pulp and printing [ISIC 17-18]	industry	GDP	10^9 USD PPP 2015
# Chemicals and chemical products [ISIC 20-21]	industry	GDP	10^9 USD PPP 2015
# Rubber and plastic [ISIC 22]	industry	GDP	10^9 USD PPP 2015
# Non-metallic minerals [ISIC 23]	industry	GDP	10^9 USD PPP 2015
# Basic metals [ISIC 24]	industry	GDP	10^9 USD PPP 2015
# Machinery [ISIC 25-28]	industry	GDP	10^9 USD PPP 2015
# Other manufacturing [ISIC 31-32]	industry	GDP	10^9 USD PPP 2015
# Agriculture forestry and fishing [ISIC 01-03]	agriculture	GDP	10^9 USD PPP 2015
# Mining [ISIC 05-09]	industry	GDP	10^9 USD PPP 2015
# Construction [ISIC 41-43]	industry	GDP	10^9 USD PPP 2015
# Total Services	services	GDP	10^9 USD PPP 2015
# Commodities	industry	Cement production	10^6 t
# Commodities	industry	Steel production	10^6 t
# Cars/light trucks	transport	Passenger_km	10^9 pkm
# Cars/light trucks	transport	Vehicle kilometres	10^9 vkm
# Cars/light trucks	transport	Stock	10^6
# Motorcycles	transport	Passenger_km	10^9 pkm
# Motorcycles	transport	Vehicle kilometres	10^9 vkm
# Motorcycles	transport	Stock	10^6
# Buses	transport	Passenger_km	10^9 pkm
# Buses	transport	Vehicle kilometres	10^9 vkm
# Buses	transport	Stock	10^6
# Passenger trains	transport	Passenger_km	10^9 pkm
# Domestic passenger airplanes	transport	Passenger_km	10^9 pkm
# Domestic passenger ships	transport	Passenger_km	10^9 pkm
# Freight trucks	transport	Freight_tonne_km	10^9 tkm
# Freight trucks	transport	Vehicle kilometres	10^9 vkm
# Freight trucks	transport	Stock	10^6
# Freight trains	transport	Freight_tonne_km	10^9 tkm
# Domestic freight airplanes	transport	Freight_tonne_km	10^9 tkm
# Domestic freight ships	transport	Freight_tonne_km	10^9 tkm
# Total passenger transport	transport	Passenger_km	10^9 pkm
# Total freight transport	transport	Freight_tonne_km	10^9 tkm
# General Activity	macro	Occupied dwellings of which heated by oil products	%
# General Activity	macro		
# Transport equipment [ISIC 29-30]	transport	GDP	10^9 USD PPP 2015
# Coke and refined petroleum products [ISIC 19]	industry	GDP	10^9 USD PPP 2015
# Of which: Metro and light rail	transport	Passenger_km	10^9 pkm
# Of which: Conventional rail	transport	Passenger_km	10^9 pkm
# Of which: Light commercial vehicle	transport	Stock	10^6
# Refrigerators	residential	Residential technology stocks	million units
# Air conditioners	residential	Residential technology stocks	million units
# General Activity	macro	Services floor area	10^9 m2
# Of which: Cars	transport	Stock	10^6
# Passenger trains	transport	Vehicle kilometres	10^9 vkm
# Passenger trains	transport	Stock	10^6
# Freight trains	transport	Vehicle kilometres	10^9 vkm
# Freight trains	transport	Stock	10^6
# Heat pump	residential	Residential technology stocks	million units
# Solar thermal panels	residential	Residential technology stocks	million units
# Solar photovoltaic panels	residential	Peak power	MWp
# Of which: High-speed rail	transport	Passenger_km	10^9 pkm

#%%

transport_data = data[data['sector'] == 'transport']
transport_data['measure'].unique()
#'Passenger_km', 'Vehicle kilometres', 'Stocks', 'Freight_tonne_km', 'GDP'
#we should separate transport sector and give it the following columns:
# Date	Vehicle Type	Transport Type	Drive	Measure	Comment	Dataset	Frequency	Fuel	Scope	Economy	Medium	Unit	Value
#where vehicle type is one of 2w
# bus
# car (or lpv)
# lt (or lpv)  
# suv (or lpv)
# mt (or ht)
# ht (or ht)
# lcv
# all - for non road , e.g. rail, ship, air

#and transport type is one of passenger or freight

#and drive is one of bev, fcev, phev (or phev_g, phev_d), ice (or ice_g, ice_d), lpg, cng

#%%
#and medium is one of road, rail, air, ship

transport_df = data[data['sector'] == 'transport']
transport_df['Vehicle Type'] = transport_df['detailed_sector'].map({
    'Cars/light trucks': 'lpv',
    'Motorcycles': '2w',
    'Buses': 'bus',
    'Freight trucks': 'ht',
    'Freight all': 'ht',
    'Freight trains': 'all',
    'Domestic freight airplanes': 'all',
    'Domestic freight ships': 'all',
    'Domestic passenger airplanes': 'all',
    'Domestic passenger ships': 'all',
    'Passenger trains': 'all',
    'Total passenger transport': 'all',
    'Total freight transport': 'all',
    'Of which: Metro and light rail': 'all',
    'Of which: Conventional rail': 'all',
    'Of which: Cars': 'car',
    'Of which: Light commercial vehicle': 'lcv',
    'Of which: High-speed rail': 'all'})
    

transport_df['Transport Type'] = transport_df['detailed_sector'].map({
    'Cars/light trucks': 'passenger',
    'Motorcycles': 'passenger',
    'Buses': 'passenger',
    'Freight trucks': 'freight',
    'Freight trains': 'freight',
    'Domestic freight airplanes': 'freight',
    'Domestic freight ships': 'freight',
    'Domestic passenger airplanes': 'passenger',
    'Domestic passenger ships': 'passenger',
    'Passenger trains': 'passenger',
    'Total passenger transport': 'passenger',
    'Total freight transport': 'freight',
    'Of which: Metro and light rail': np.nan,#have to determine what the measure is to determine if its passenger or freight
    'Of which: Conventional rail': np.nan,#have to determine what the measure is to determine if its passenger or freight
    'Of which: Cars': 'passenger',
    'Of which: Light commercial vehicle': 'freight',
    'Of which: High-speed rail': np.nan#have to determine what the measure is to determine if its passenger or freight
    })

transport_df['Drive'] = 'all'
transport_df['Fuel'] = 'all'

transport_df['Medium'] = transport_df['detailed_sector'].map({
    'Cars/light trucks': 'road',
    'Motorcycles': 'road',
    'Buses': 'road',
    'Freight trucks': 'road',
    'Freight trains': 'rail',
    'Domestic freight airplanes': 'air',
    'Domestic freight ships': 'ship',
    'Domestic passenger airplanes': 'air',
    'Domestic passenger ships': 'ship',
    'Passenger trains': 'rail',
    'Total passenger transport': 'all',
    'Total freight transport': 'all',
    'Of which: Metro and light rail': 'rail',
    'Of which: Conventional rail': 'rail',
    'Of which: Cars': 'road',
    'Of which: Light commercial vehicle': 'road',
    'Of which: High-speed rail': 'rail'})


#and lastly based on if the measure is Passenger_km or Freight_tonne_km we can determine if its passenger or freight
transport_df['Transport Type'] = np.where(transport_df['measure'] == 'Passenger_km', 'passenger', transport_df['Transport Type'])
transport_df['Transport Type'] = np.where(transport_df['measure'] == 'Freight_tonne_km', 'freight', transport_df['Transport Type'])

#sadly the 'Of which: Cars': 'road', and 'Of which: Light commercial vehicle': 'freight', values are too sparse to sue so just drop them:
transport_df = transport_df[~((transport_df['detailed_sector'] == 'Of which: Cars') | (transport_df['detailed_sector'] == 'Of which: Light commercial vehicle'))]
#%%



#melt so the 4 digit years columns are now rows
transport_df = transport_df.melt(id_vars=['detailed_sector',
                'sector',         'measure',            'unit',
               'Economy',    'Vehicle Type',  'Transport Type',
                 'Drive',            'Fuel',           
                'Medium', 'Country'], var_name='Date', value_name='Value').reset_index(drop=True)

# transport_df.to_clipboard()

#now we can calcualte miuleage and if we grab the energy sue for transport sector we might be able to calcualte the efficiency:
#%%
#first calc mileage:
#to do this we will pivot the meausre col
#gfirst check for  duplicates when we exclude value
dupes= transport_df[['Date', 'detailed_sector',
                'sector',         'measure',            'unit',
               'Economy',    'Vehicle Type',  'Transport Type',
                 'Drive',            'Fuel',
                'Medium', 'Country']].duplicated()
if dupes.any():
    print(transport_df[dupes])
    print('duplicates found')
#%%
#before our calculations we need to make sure the units are the same magnitudes so they can be used togehter:
#first check the unique units:
transport_df[['unit', 'measure']].drop_duplicates()#unit	measure
# 0	10^9 pkm	Passenger_km
# 1	10^9 vkm	Vehicle kilometres
# 2	10^6	Stock
# 12	10^9 tkm	Freight_tonne_km

#magnitude to whole number:
magnitude_to_whole_number = {
    '10^9 pkm': 1e9,
    '10^9 vkm': 1e9,
    '10^6': 1e6,
    '10^9 tkm': 1e9
}
transport_df['Value'] = transport_df['Value'] * transport_df['unit'].replace(magnitude_to_whole_number)

#%%  
transport_df_wide = transport_df.drop(columns='unit')
transport_df_wide =  transport_df_wide.pivot(index=['Date', 'detailed_sector',
                'sector',
               'Economy',    'Vehicle Type',  'Transport Type',
                 'Drive',            'Fuel',
                'Medium', 'Country'], columns='measure', values='Value').reset_index()

transport_df_wide['occupancy'] = transport_df_wide['Passenger_km'] / transport_df_wide['Vehicle kilometres']
transport_df_wide['load'] = transport_df_wide['Freight_tonne_km'] / transport_df_wide['Vehicle kilometres']
transport_df_wide['mileage'] = (transport_df_wide['Vehicle kilometres'] / transport_df_wide['Stocks'])
#also lets rename Vehicle kilometres to travel_km
transport_df_wide.rename(columns={'Vehicle kilometres': 'travel_km'}, inplace=True)
#%%
transport_df_tall = transport_df_wide.melt(id_vars=['Date', 'detailed_sector',
                'sector',         'Economy',    'Vehicle Type',  'Transport Type',
                 'Drive',         'Fuel',
                'Medium', 'Country'], var_name='measure', value_name='Value').reset_index(drop=True)

#drop any infs
transport_df_tall = transport_df_tall.replace([np.inf, -np.inf], np.nan)

#also set any 0's to nan cause they are obv wrong
transport_df_tall['Value'] = transport_df_tall['Value'].replace(0, np.nan)

#and drop where transport type is apssenger and measure is freight_tonne_km or measure is load, and likewise for freight transport type and passenger_km or occupancy. Then convert the measure to activity or occupancy_or_load
transport_df_tall = transport_df_tall[~((transport_df_tall['Transport Type'] == 'passenger') & ((transport_df_tall['measure'] == 'Freight_tonne_km') | (transport_df_tall['measure'] == 'load')))]
transport_df_tall = transport_df_tall[~((transport_df_tall['Transport Type'] == 'freight') & ((transport_df_tall['measure'] == 'Passenger_km') | (transport_df_tall['measure'] == 'occupancy')))]
transport_df_tall['measure'] = transport_df_tall['measure'].replace({'Freight_tonne_km': 'activity', 'Passenger_km': 'activity', 'load': 'occupancy_or_load', 'occupancy': 'occupancy_or_load'})

#fix units:
# 'Activity' 'Stocks', 'travel_km',
#    'occupancy_or_load', 'mileage'
measure_to_unit_dict = {
    'mileage': 'Km_per_stock',
    'occupancy_or_load': 'passengers_or_tonnes',
    'activity': 'passenger_km_or_freight_tonne_km',
    'Stocks': 'Stocks',
    'travel_km': 'Km'}

transport_df_tall['unit'] = transport_df_tall['measure'].replace(measure_to_unit_dict)  

#Find these and rename their measures to indicate they are a bit different. Just drop Of which and tehn add as - ###
#e.g. 'Of which: Metro and light rail' -> 'Passenger_km - Metro and light rail'
#but first create a filter:
of_which_filter = transport_df_tall['detailed_sector'].str.contains('Of which: ')
transport_df_tall.loc[of_which_filter, 'measure'] = transport_df_tall.loc[of_which_filter, 'measure'].str.replace('Of which: ', '') + ' - ' + transport_df_tall.loc[of_which_filter, 'detailed_sector'].str.replace('Of which: ', '')
#%%


##########################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################
#NOW ENERGY DATA
#%%
import pandas as pd
import numpy as np

# Assuming 'energy' DataFrame is already loaded
energy = pd.read_excel('input_data/EEI/energy.xlsx', sheet_name='Sheet1')

# Drop trailing spaces
energy['Product'] = energy['Product'].str.strip()

# Replace values that are just '..' with np.nan
energy.replace('..', np.nan, inplace=True)
#also any 'x' with np.nan
energy.replace('x', np.nan, inplace=True)

# Mapping for Vehicle Type
energy['Vehicle Type'] = energy['Mode/vehicle type'].map({
    'Total passenger and freight transport': 'all',
    'Total passenger transport': 'all',
    'Cars/light trucks': 'lpv',
    'Motorcycles': '2w',
    'Buses': 'bus',
    'Passenger trains': 'all',
    'Domestic passenger airplanes': 'all',
    'Domestic passenger ships': 'all',
    'Total freight transport': 'all',
    'Freight trucks': 'ht',
    'Freight trains': 'all',
    'Domestic freight airplanes': 'all',
    'Domestic freight ships': 'all',
    'Total road': 'road',
    'Total trains': 'all',
    'Total airplanes': 'all',
    'Total ships': 'all',
    'Of which: Metro and light rail': 'all',
    'Of which: Conventional rail': 'all'
})

# Mapping for Transport Type
energy['Transport Type'] = energy['Mode/vehicle type'].map({
    'Total passenger and freight transport': 'combined',
    'Total passenger transport': 'passenger',
    'Cars/light trucks': 'passenger',
    'Motorcycles': 'passenger',
    'Buses': 'passenger',
    'Passenger trains': 'passenger',
    'Domestic passenger airplanes': 'passenger',
    'Domestic passenger ships': 'passenger',
    'Total freight transport': 'freight',
    'Freight trucks': 'freight',
    'Freight trains': 'freight',
    'Domestic freight airplanes': 'freight',
    'Domestic freight ships': 'freight',
    'Total road': 'combined',
    'Total trains': 'combined',
    'Total airplanes': 'combined',
    'Total ships': 'combined',
    'Of which: Metro and light rail': 'combined',
    'Of which: Conventional rail': 'combined'
})

# Mapping for Medium
energy['Medium'] = energy['Mode/vehicle type'].map({
    'Total passenger and freight transport': 'all',
    'Total passenger transport': 'all',
    'Cars/light trucks': 'road',
    'Motorcycles': 'road',
    'Buses': 'road',
    'Passenger trains': 'rail',
    'Domestic passenger airplanes': 'air',
    'Domestic passenger ships': 'ship',
    'Total freight transport': 'all',
    'Freight trucks': 'road',
    'Freight trains': 'rail',
    'Domestic freight airplanes': 'air',
    'Domestic freight ships': 'ship',
    'Total road': 'road',
    'Total trains': 'rail',
    'Total airplanes': 'air',
    'Total ships': 'ship',
    'Of which: Metro and light rail': 'rail',
    'Of which: Conventional rail': 'rail'
})

#%%
#now we can create drive mappings mbased on the mix of Product and medium
# energy[['Product', 'Medium']].drop_duplicates()
# Product	Medium
# 0	Motor gasoline (PJ)	all
# 1	Diesel and light fuel oil (PJ)	all
# 2	LPG (PJ)	all
# 3	Heavy fuel oil (PJ)	all
# 4	Jet fuel and aviation gasoline (PJ)	all
# 5	Gas (PJ)	all
# 6	Electricity (PJ)	all
# 7	Coal and coal products (PJ)	all
# 8	Other sources (PJ)	all
# 9	Total final energy use (PJ)	all
# 20	Motor gasoline (PJ)	road
# 21	Diesel and light fuel oil (PJ)	road
# 22	LPG (PJ)	road
# 23	Gas (PJ)	road
# 24	Electricity (PJ)	road
# 25	Other sources (PJ)	road
# 26	Total final energy use (PJ)	road
# 39	Diesel and light fuel oil (PJ)	rail
# 40	Heavy fuel oil (PJ)	rail
# 41	Gas (PJ)	rail
# 42	Electricity (PJ)	rail
# 43	Coal and coal products (PJ)	rail
# 44	Other sources (PJ)	rail
# 45	Total final energy use (PJ)	rail
# 46	Jet fuel and aviation gasoline (PJ)	air
# 47	Other sources (PJ)	air
# 48	Total final energy use (PJ)	air
# 49	Motor gasoline (PJ)	ship
# 50	Diesel and light fuel oil (PJ)	ship
# 51	Heavy fuel oil (PJ)	ship
# 52	Gas (PJ)	ship
# 53	Coal and coal products (PJ)	ship
# 54	Other sources (PJ)	ship
# 55	Total final energy use (PJ)	ship

#basically, in our modelling we have the ollowig drive-fuel combinations. So we can map the above to these (from \transport_model_9th_edition\config\concordances_and_config_data\drive_type_to_fuel.csv):
# Drive	Fuel
# ship_electric	17_electricity
# rail_electricity	17_electricity
# phev_g	17_electricity
# phev_d	17_electricity
# bev	17_electricity
# air_electric	17_electricity
# fcev	16_x_hydrogen
# air_hydrogen	16_x_hydrogen
# ship_kerosene	16_x_efuel
# ship_gasoline	16_x_efuel
# ship_diesel	16_x_efuel
# rail_kerosene	16_x_efuel
# rail_gasoline	16_x_efuel
# rail_diesel	16_x_efuel
# phev_g	16_x_efuel
# phev_d	16_x_efuel
# ice_g	16_x_efuel
# ice_d	16_x_efuel
# air_kerosene	16_x_efuel
# air_jet_fuel	16_x_efuel
# air_gasoline	16_x_efuel
# air_diesel	16_x_efuel
# air_av_gas	16_x_efuel
# ship_ammonia	16_x_ammonia
# ship_kerosene	16_07_bio_jet_kerosene
# rail_kerosene	16_07_bio_jet_kerosene
# air_kerosene	16_07_bio_jet_kerosene
# air_jet_fuel	16_07_bio_jet_kerosene
# air_av_gas	16_07_bio_jet_kerosene
# ship_diesel	16_06_biodiesel
# rail_diesel	16_06_biodiesel
# phev_d	16_06_biodiesel
# ice_d	16_06_biodiesel
# air_diesel	16_06_biodiesel
# ship_gasoline	16_05_biogasoline
# rail_gasoline	16_05_biogasoline
# phev_g	16_05_biogasoline
# ice_g	16_05_biogasoline
# air_gasoline	16_05_biogasoline
# ship_lpg	16_01_biogas
# rail_natural_gas	16_01_biogas
# rail_lpg	16_01_biogas
# lpg	16_01_biogas
# cng	16_01_biogas
# air_lpg	16_01_biogas
# rail_natural_gas	08_01_natural_gas
# cng	08_01_natural_gas
# air_jet_fuel	07_x_jet_fuel
# ship_lpg	07_09_lpg
# rail_lpg	07_09_lpg
# lpg	07_09_lpg
# air_lpg	07_09_lpg
# ship_fuel_oil	07_08_fuel_oil
# rail_fuel_oil	07_08_fuel_oil
# air_fuel_oil	07_08_fuel_oil
# ship_diesel	07_07_gas_diesel_oil
# rail_diesel	07_07_gas_diesel_oil
# phev_d	07_07_gas_diesel_oil
# ice_d	07_07_gas_diesel_oil
# air_diesel	07_07_gas_diesel_oil
# ship_kerosene	07_06_kerosene
# rail_kerosene	07_06_kerosene
# air_kerosene	07_06_kerosene
# air_av_gas	07_02_aviation_gasoline
# ship_gasoline	07_01_motor_gasoline
# rail_gasoline	07_01_motor_gasoline
# phev_g	07_01_motor_gasoline
# ice_g	07_01_motor_gasoline
# air_gasoline	07_01_motor_gasoline
# rail_coal	01_x_thermal_coal
# ship_fuel_oil	16_x_efuel
# ship_fuel_oil	16_06_biodiesel
# ship_lng	08_02_lng
# ship_lng	16_01_biogas
# ship_hydrogen	16_x_hydrogen
 
energy['Product_Medium'] = energy['Product'] + energy['Medium'] 
product_medium_to_drive = {
    'Motor gasoline (PJ)road': 'ice_g',
    'Diesel and light fuel oil (PJ)road': 'ice_d',
    'LPG (PJ)road': 'lpg',
    'Gas (PJ)road': 'cng',
    'Electricity (PJ)road': 'bev',
    'Other sources (PJ)road': np.nan, 
    'Total final energy use (PJ)road': 'all',
    'Diesel and light fuel oil (PJ)rail': 'rail_diesel',
    'Heavy fuel oil (PJ)rail': 'rail_fuel_oil',
    'Gas (PJ)rail': 'rail_natural_gas',
    'Electricity (PJ)rail': 'rail_electricity',
    'Coal and coal products (PJ)rail': 'rail_coal',
    'Other sources (PJ)rail': np.nan,
    'Total final energy use (PJ)rail': 'all',
    'Jet fuel and aviation gasoline (PJ)air': 'air_jet_fuel',
    'Other sources (PJ)air': np.nan, 
    'Total final energy use (PJ)air': 'all',
    'Motor gasoline (PJ)ship': 'ship_gasoline',
    'Diesel and light fuel oil (PJ)ship': 'ship_diesel',
    'Heavy fuel oil (PJ)ship': 'ship_fuel_oil',
    'Gas (PJ)ship': 'ship_lpg',
    'Coal and coal products (PJ)ship': 'ship_coal',
    'Other sources (PJ)ship': np.nan, 
    'Total final energy use (PJ)ship': 'all',
    'Motor gasoline (PJ)all': 'all',
    'Diesel and light fuel oil (PJ)all': 'all',
    'LPG (PJ)all': 'all',
    'Heavy fuel oil (PJ)all': 'all',
    'Jet fuel and aviation gasoline (PJ)all': 'all',
    'Gas (PJ)all': 'all',
    'Electricity (PJ)all': 'all',
    'Coal and coal products (PJ)all': 'all',
    'Other sources (PJ)all': np.nan,
    'Total final energy use (PJ)all': 'all'
}
    
energy['Drive'] = energy['Product_Medium'].replace(product_medium_to_drive)
energy.drop(columns='Product_Medium', inplace=True)

#drop ' (PJ)' from Product and then rename to fuel
energy['Fuel'] = energy['Product'].str.replace(' (PJ)', '')
#and replace Total final energy use wth all
energy['Fuel'] = energy['Fuel'].replace('Total final energy use', 'all')
energy.drop(columns='Product', inplace=True)
    
#map the Country to Economy
energy = energy.merge(country_codes[['Country', 'Economy']], on='Country', how='left')

#melt so the 4 digit years columns are now rows
#'Vehicle Type',    'Transport Type',            'Medium',
#    'Drive',              'Fuel'],   'Country', 'Mode/vehicle type', 'Economy'

energy = energy.melt(id_vars=['Mode/vehicle type', 'Country', 'Economy', 'Vehicle Type', 'Transport Type', 'Medium', 'Drive', 'Fuel'], var_name='Date', value_name='Value').reset_index(drop=True)

#lastly make measure = Energy and unit = PJ
energy['measure'] = 'Energy'
energy['unit'] = 'PJ'

#Find these and rename their measure to be Energy - Metro and light rail or Energy - Conventional rail. jsut so they dont get mixed in with the other transport data
# 'Of which: Metro and light rail'
# 'Of which: Conventional rail'
energy.loc[energy['Mode/vehicle type'] == 'Of which: Metro and light rail', 'measure'] = 'Energy - Metro and light rail'
energy.loc[energy['Mode/vehicle type'] == 'Of which: Conventional rail', 'measure'] = 'Energy - Conventional rail'


#%%



#%%
#########################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################

#concat energy and transport activity data
energy_activity_concat = pd.concat([energy, transport_df_tall], ignore_index=True)
#delete uncesary cols:
# Index(['Mode/vehicle type', 'Country', 'Economy', 'Vehicle Type',
#        'Transport Type', 'Medium', 'Drive', 'Fuel', 'Date', 'Value', 'measure',
#        'unit', 'detailed_sector', 'sector', 'Comment', 'Dataset', 'Frequency',
#        'Scope'],
energy_activity= energy_activity_concat[['Date', 'Vehicle Type', 'Transport Type', 'Drive', 'measure', 'Fuel', 'Economy', 'Medium', 'Country', 'Value']]
#check for dupes
dupes = energy_activity.duplicated()
if dupes.any():
    print(energy_activity[dupes])
    print('duplicates found')

#%%

#now do opereations on the merged data:

#set 0's to nan to avoid infinities and divide by 0 errors
energy_activity['Value'] = energy_activity['Value'].replace(0, np.nan)
#first create efficiency where we can:

energy_activity_wide = energy_activity.pivot(index=['Date', 'Vehicle Type', 'Transport Type', 'Drive', 'Fuel', 'Economy', 'Medium', 'Country'], columns='measure', values='Value').reset_index()
energy_activity_wide['efficiency'] = energy_activity_wide['travel_km'] / energy_activity_wide['Energy']
energy_activity_wide['intensity'] = energy_activity_wide['Energy'] / energy_activity_wide['activity']
#%%
#and then melt back to tall
energy_activity_tall = energy_activity_wide.melt(id_vars=['Date', 'Vehicle Type', 'Transport Type', 'Drive', 'Fuel', 'Economy', 'Medium', 'Country'], var_name='measure', value_name='Value').reset_index(drop=True)
#set any infs and 0's to nan
energy_activity_tall = energy_activity_tall.replace([np.inf, -np.inf], np.nan)
energy_activity_tall['Value'] = energy_activity_tall['Value'].replace(0, np.nan)

#foramt value as float
energy_activity_tall['Value'] = energy_activity_tall['Value'].astype(float)

#%%

#and also, create averages of the mileage and occupancy/load for each vehicle type, transport type, medium combination and fill in missing values with those:
energy_activity_tall_avgs = energy_activity_tall.groupby(['Vehicle Type', 'Transport Type', 'Medium', 'Drive', 'measure', 'Date'])['Value'].mean(numeric_only=True).reset_index().dropna()
#keep  only the factors: efficiency, intensity, mileage, occupancy_or_load
energy_activity_tall_avgs = energy_activity_tall_avgs[energy_activity_tall_avgs['measure'].isin(['efficiency', 'intensity', 'mileage', 'occupancy_or_load'])]
energy_activity_tall = energy_activity_tall.merge(energy_activity_tall_avgs, on=['Vehicle Type', 'Transport Type', 'Medium', 'Drive',  'measure', 'Date'], how='left', suffixes=('', '_avg'))
#first, in comments, where we would fill a missing value with a non nan value, add a comment to indicate this
energy_activity_tall['Comment'] = np.where((energy_activity_tall['Value'].isna()) & (energy_activity_tall['Value_avg'].notna()), 'Average of EEI', '')
energy_activity_tall['Value'] = energy_activity_tall['Value'].fillna(energy_activity_tall['Value_avg'])

#%%
energy_activity_tall.drop(columns='Value_avg', inplace=True)

#and then drop nas in valu col to
energy_activity_tall = energy_activity_tall[energy_activity_tall['Value'].notna()]

#add the energy_activity_tall_avgs to the energy_activity_tall df
energy_activity_tall_avgs['Economy'] = 'all'
energy_activity_tall_avgs['Country'] = 'all'
# energy_activity_tall_avgs['Date'] = 'all'
energy_activity_tall_avgs['Fuel'] = 'all'
energy_activity_tall_avgs['Comment'] = 'average of EEI'
energy_activity_tall = pd.concat([energy_activity_tall, energy_activity_tall_avgs], ignore_index=True)

#fi

# %%

energy_activity_tall['Scope'] = 'national'
energy_activity_tall['Comment'] = energy_activity_tall['Comment'].replace('', 'no_comment')
energy_activity_tall['Dataset'] = 'EEI'
energy_activity_tall['Frequency'] = 'annual'
#%%
# #create a mpaping between unit and measure
# 'Energy', 'Energy - Conventional rail',
#        'Energy - Metro and light rail', 'Stocks', 'activity',
#        'activity - Conventional rail', 'activity - High-speed rail',
#        'activity - Metro and light rail', 'mileage', 'occupancy_or_load',
#        'travel_km', 'efficiency', 'intensity'
measure_to_unit_dict = {
    'mileage': 'Km_per_stock',
    'occupancy_or_load': 'passengers_or_tonnes',
    'activity': 'passenger_km_or_freight_tonne_km',
    'Energy': 'PJ',
    'Energy - Conventional rail': 'PJ',
    'Energy - Metro and light rail': 'PJ',
    'Stocks': 'Stocks',
    'travel_km': 'Km',
    'efficiency': 'Km_per_PJ',
    'intensity': 'pj_per_passenger_or_freight_tonne_km'
}
energy_activity_tall['Unit'] = energy_activity_tall['measure'].replace(measure_to_unit_dict)

#and finally convert the magnitudes to how they are in the other data:
# Unit	Measure	Magnitude_adjustment	Magnitude_adjusted_unit
# Pj	Energy	1	Pj
# Stocks	Stocks	0.000001	Million_stocks
# Km_per_pj	New_vehicle_efficiency	0.000000001	Billion_km_per_pj
# Km_per_pj	Efficiency	0.000000001	Billion_km_per_pj
# %	Turnover_rate	1	%
# %	Supply_side_fuel_share 	1	%
# %	Demand_side_fuel_share 	1	%
# Passengers_or_tonnes	Occupancy_or_load	1	Passengers_or_tonnes
# Passenger_km_or_freight_tonne_km	Activity	1.00E-09	Billion_passenger_km_or_freight_tonne_km
# Km_per_stock	Mileage	1.00E-03	Thousand_km_per_stock
# %	Non_road_efficiency_growth	1	%
# %	Vehicle_sales_share	1	%
# %	New_vehicle_efficiency_growth	1	%
# %	Turnover_rate_growth	1	%
# %	Occupancy_or_load_growth	1	%
# %	Activity_growth	1	%
# Km	Travel_km	1.00E-09	Billion_km
# Pj_per_passenger_or_freight_tonne_km	Intensity	1.00E+09	Pj_per_billion_passenger_or_freight_tonne_km
# Real_gdp	Gdp	0.000001	Real_gdp_millions
# Population	Population	0.001	Population_thousands
# Gdp_per_capita	Gdp_per_capita	1.00E-03	Thousand_Gdp_per_capita
# Stocks_per_thousand_capita	Stocks_per_thousand_capita	1	Stocks_per_thousand_capita
# Passengers	Load	1	Passengers
# Tonnes	Occpancy	1	Tonnes
# Passenger_km	Passenger_km	1.00E-09	Billion_passenger_km
# Tonne_km	Freight_tonne_km	1.00E-09	Billion_tonne_km
# Stocks_per_thousand_capita	Gompertz_gamma	1	Stocks_per_thousand_capita
# Age	Average_age	1	Age
#%%

#convert the unit to pj_per_passenger_or_freight_tonne_km
energy_activity_tall['Unit'] = energy_activity_tall['Unit'].replace('pj_per_billion_activity', 'pj_per_passenger_or_freight_tonne_km')
# #'PJ', 'Stocks', 'passenger_km_or_freight_tonne_km',
# #    'activity - Conventional rail', 'activity - High-speed rail',
# #    'activity - Metro and light rail', 'Km_per_stock',
# #    'passengers_or_tonnes', 'Km', 'Km_per_PJ', 'pj_per_passenger_or_freight_tonne_km'
# magnitude_conversions = {
#     'Stocks': 1,#/1e6,
#     'Km_per_PJ': 1,#/1e9,
#     'Km_per_stock': 1,#/1e3,
#     'passengers_or_tonnes': 1,  
#     'passenger_km_or_freight_tonne_km': 1,#/1e9,
#     'pj_per_passenger_or_freight_tonne_km': 1,#/1e9,   
#     'Km': 1,#/1e9,
#     'activity - Conventional rail': 1,#/1e9,
#     'activity - High-speed rail': 1,#/1e9,
#     'activity - Metro and light rail': 1,#/1e9,
#     'PJ': 1
# }
    
# energy_activity_tall['Value'] = energy_activity_tall['Value'] * energy_activity_tall['Unit'].replace(magnitude_conversions)

#%%
#save this to input_data\EEI\energy_activity_cleaned.csv adn then drop economy == np.nan 
energy_activity_tall.to_csv('input_data/EEI/energy_activity_cleaned.csv', index=False)
# %%
#now create a version that we can be happy to use in our final dataset:
#firstly remove economy == na.
energy_activity_for_system = energy_activity_tall.copy()
energy_activity_for_data_system = energy_activity_for_system.copy()
energy_activity_for_data_system = energy_activity_for_data_system[energy_activity_for_data_system['Economy'].notna()]
#and remove Ecnomy =='all'
energy_activity_for_data_system = energy_activity_for_data_system[energy_activity_for_data_system['Economy'] != 'all']
#drop country col
energy_activity_for_data_system = energy_activity_for_data_system.drop(columns='Country')
#then remove anything to do with medium=road transprot type =freight since we dont know the ratios of lcvs within that. 
energy_activity_for_data_system = energy_activity_for_data_system[~((energy_activity_for_data_system['Medium'] == 'road') & (energy_activity_for_data_system['Transport Type'] == 'freight'))]
#also remove anythign with transport type =combined
energy_activity_for_data_system = energy_activity_for_data_system[energy_activity_for_data_system['Transport Type'] != 'combined']
#and wehre FUel != all
energy_activity_for_data_system = energy_activity_for_data_system[energy_activity_for_data_system['Fuel'] == 'all']
#and tehse measures: Energy - Conventional rail
# Energy - Conventional rail
# Energy - Metro and light rail
# Energy - Metro and light rail
# activity - High-speed rail
energy_activity_for_data_system = energy_activity_for_data_system[~energy_activity_for_data_system['measure'].isin(['Energy - Conventional rail', 'Energy - Metro and light rail', 'activity - High-speed rail'])]

import datetime
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
energy_activity_for_data_system.to_csv(f'intermediate_data/estimated/eei_processed_from_energy_activity_{FILE_DATE_ID}.csv', index=False)#was prbiously named eei_FILE_DATE_ID.csv but to make it more unique we added the _processed_from_energy_activity_ to the name

#i think the factors might have the wrong magnitudes. I think they should be in the same units as the measures. So lets convert them to the same units as the measures

#%%

##########################################################################################################################################################################################

#                              ANALYSIS 
# ###############################################################################################################################################################################################################################################################################################################################################################################

def convert_string_to_snake_case(string):
    """
    Converts a string to snake case
    """
    # Convert anything to snake case, inclkuding a string with spaces
    string = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    string = re.sub('([a-z0-9])([A-Z])', r'\1_\2', string).lower()
    #repalce spaces with underscores
    string = string.replace(' ', '_')
    #replace any double underscores with single underscores
    string = string.replace('__', '_')
    return string

def convert_all_cols_to_snake_case(df):
    #will convert all vlaues in cols to snake case
    for col in df.columns:
        if col not in ['economy', 'value', 'date']:
            #if type of col is not string then tell the user
            #make any nan values into strings. 
            df[col] = df[col].fillna('nan')
            try:
                df[col] = df[col].apply(convert_string_to_snake_case)
            except:
                breakpoint()
                raise ValueError(f'Could not convert {col} to snake case')
            #reutrn nas to nan
            df[col] = df[col].replace('nan', np.nan)
    return df

def replace_bad_col_names(col):
    col = convert_string_to_snake_case(col)
    if col == 'fuel_type':
        col = 'fuel'  
    if col == 'comments':
        col = 'comment'
    if col == 'units':
        col = 'unit'
    return col
#finally lets do some quick analysis to compared values to what we are currently using. We will jsut compare economys we have vs those in our modelling output and the transport data system, vs what is here. and then compare the averages too:
#first take in teh most recent data:
# \transport_data_system\output_datacombined_data_DATE20240612.csv
# ../transport_model_9th_edition\output_data\model_output_detailed\all_economies_NON_ROAD_DETAILED_20240709_model_output20240709.csv
transport_data_system = pd.read_csv('output_data/combined_data_DATE20240612.csv')
transport_model = pd.read_csv('../transport_model_9th_edition/output_data/model_output_detailed/all_economies_NON_ROAD_DETAILED_20240709_model_output20240709.csv')
#first tranpsort model needs to be melted:
transport_model = transport_model.melt(id_vars=['Economy', 'Date', 'Medium', 'Vehicle Type', 'Transport Type', 'Drive',
       'Scenario', 'Unit'], var_name='measure', value_name='Value').reset_index(drop=True)
#fitler so scenario only == 'Reference'
transport_model = transport_model[transport_model['Scenario'] == 'Reference']
energy_activity_for_system_plot = energy_activity_for_system.copy()
#tehse will be the sets of data we will compare: (using a scatter plot of the values with x = dataset, y = value and color = Drive). w will only plot rows where dates match and there will be a dashboard for each measure-medium combo, with a different graph for each vehicle type, transport type combo
road_filter = energy_activity_for_system_plot.loc[(energy_activity_for_system_plot['Vehicle Type']!='all')&(energy_activity_for_system_plot['Transport Type']!='combined')][['Transport Type','measure']].drop_duplicates()
non_road_filter = energy_activity_for_system_plot.loc[(energy_activity_for_system_plot['Medium']!='road')&(energy_activity_for_system_plot['Medium']!='all')&(~energy_activity_for_system_plot['measure'].isin(['Energy - Conventional rail', 'Energy - Metro and light rail', 'activity - High-speed rail']))&(energy_activity_for_system_plot['Transport Type']!='combined')&(~energy_activity_for_system_plot['Drive'].isna())][['Vehicle Type','Medium', 'Transport Type','measure']].drop_duplicates()
all_all_all_filter = energy_activity_for_system_plot.loc[(energy_activity_for_system_plot['Medium']=='all')&(energy_activity_for_system_plot['Vehicle Type']=='all')&(energy_activity_for_system_plot['Fuel']=='all')][['Transport Type', 'measure']].drop_duplicates()

#%%
#make all column names snake case as well as values inside the columns
transport_data_system.columns = [replace_bad_col_names(col) for col in transport_data_system.columns]
transport_data_system = convert_all_cols_to_snake_case(transport_data_system)

transport_model.columns = [replace_bad_col_names(col) for col in transport_model.columns]
transport_model = convert_all_cols_to_snake_case(transport_model)

energy_activity_for_system_plot.columns = [replace_bad_col_names(col) for col in energy_activity_for_system_plot.columns]
energy_activity_for_system_plot = convert_all_cols_to_snake_case(energy_activity_for_system_plot)

road_filter.columns = [replace_bad_col_names(col) for col in road_filter.columns]
road_filter = convert_all_cols_to_snake_case(road_filter)

non_road_filter.columns = [replace_bad_col_names(col) for col in non_road_filter.columns]
non_road_filter = convert_all_cols_to_snake_case(non_road_filter)

all_all_all_filter.columns = [replace_bad_col_names(col) for col in all_all_all_filter.columns]
all_all_all_filter = convert_all_cols_to_snake_case(all_all_all_filter)

#%%
#and do the magnitude conversions for all the dfs to make them easier to read as they have less extreme magnitudes
magnitude_conversions_from_unit_data_system = {
    'stocks': 1/1e6,
    'km_per_pj': 1/1e9,
    'km_per_stock': 1/1e3,
    'passengers_or_tonnes': 1,  
    'passenger_km_or_freight_tonne_km': 1/1e9,
    'pj_per_passenger_or_freight_tonne_km': 1e9,   
    'km': 1/1e9,
    'activity_conventional_rail': 1/1e9,
    'activity_high_speed_rail': 1/1e9,
    'activity_metro_and_light_rail': 1/1e9,
    'pj': 1
}

magnitude_conversions_from_unit_model = {
    'stocks': 1,#1/1e6,
    'km_per_pj': 1,#? its so low even after making this adjustment of 1/1e9
    'km_per_stock': 1,#/1e3,
    'passengers_or_tonnes': 1,  
    'passenger_km_or_freight_tonne_km': 1,#/1e9,
    'pj_per_passenger_or_freight_tonne_km': 1,#/1e9,   
    'km': 1,#/1e9,
    'activity_conventional_rail': 1,#/1e9,
    'activity_high_speed_rail': 1,#/1e9,
    'activity_metro_and_light_rail': 1,#/1e9,
    'pj': 1
}

magnitude_conversions_from_unit_eei = {
    'stocks': 1/1e6,
    'km_per_pj': 1/1e9,
    'km_per_stock': 1/1e3,
    'passengers_or_tonnes': 1,  
    'passenger_km_or_freight_tonne_km': 1/1e9,
    'pj_per_passenger_or_freight_tonne_km': 1e9, 
    'km': 1/1e9,
    'activity_conventional_rail': 1/1e9,
    'activity_high_speed_rail': 1/1e9,
    'activity_metro_and_light_rail': 1/1e9,
    'pj': 1
}
    
    
measure_to_unit_dict = {
    'mileage': 'km_per_stock',
    'occupancy_or_load': 'passengers_or_tonnes',
    'activity': 'passenger_km_or_freight_tonne_km',
    'energy': 'pj',
    'stocks': 'stocks',
    'travel_km': 'km',
    'efficiency': 'km_per_pj',
    'intensity': 'pj_per_passenger_or_freight_tonne_km',
}

#%%
transport_model['unit'] = transport_model['measure'].map(measure_to_unit_dict)
#drop nas in unit
transport_model = transport_model[transport_model['unit'].notna()]
transport_model['magnitude'] = transport_model['unit'].map(magnitude_conversions_from_unit_model).fillna(1)
transport_model['value'] = transport_model['value'] * transport_model['magnitude']

# transport_data_system['unit'] = transport_data_system['measure'].replace(measure_to_unit_dict)
transport_data_system['magnitude'] = transport_data_system['unit'].map(magnitude_conversions_from_unit_data_system).fillna(1)
transport_data_system['value'] = transport_data_system['value'] * transport_data_system['magnitude']

#only change magnitude for the eei data
energy_activity_for_system_plot['magnitude'] = energy_activity_for_system_plot['unit'].map(magnitude_conversions_from_unit_eei).fillna(1)
energy_activity_for_system_plot['value'] = energy_activity_for_system_plot['value'] * energy_activity_for_system_plot['magnitude']

#%%
#drop unit
transport_data_system.drop(columns=['unit','magnitude'], inplace=True)
#drop magnitude
energy_activity_for_system_plot.drop(columns=['magnitude'], inplace=True)
#drop unit
transport_model.drop(columns=['unit','magnitude'], inplace=True)
    
    
#%%    

#Now filter on the remainign datasets: #we'll manage all_all_all separately
transport_data_system_road = transport_data_system.merge(road_filter, on=['transport_type', 'measure'], how='inner')
transport_data_system_non_road = transport_data_system.merge(non_road_filter, on=['vehicle_type', 'transport_type', 'medium', 'measure'], how='inner')
transport_data_system_all = transport_data_system.merge(all_all_all_filter, on=['transport_type', 'measure'], how='inner')

transport_model_road = transport_model.merge(road_filter, on=['transport_type', 'measure'], how='inner')
transport_model_non_road = transport_model.merge(non_road_filter, on=['vehicle_type', 'transport_type', 'medium', 'measure'], how='inner')
transport_model_all = transport_model.merge(all_all_all_filter, on=['transport_type', 'measure'], how='inner')

energy_activity_for_system_road = energy_activity_for_system_plot.merge(road_filter, on=['transport_type', 'measure'], how='inner').copy()
energy_activity_for_system_non_road = energy_activity_for_system_plot.merge(non_road_filter, on=[ 'transport_type', 'medium', 'measure'], how='inner').copy()
energy_activity_for_system_all =  energy_activity_for_system_plot.loc[(energy_activity_for_system_plot['medium']=='all')&(energy_activity_for_system_plot['vehicle_type']=='all')&(energy_activity_for_system_plot['fuel']=='all')]

#now concat all the dataframes after creating a origin column for each
energy_activity_for_system_road['origin'] = 'EEI'
energy_activity_for_system_non_road['origin'] = 'EEI'
energy_activity_for_system_all['origin'] = 'EEI'
transport_data_system_road['origin'] = 'data_system'
transport_data_system_non_road['origin'] = 'data_system'
transport_data_system_all['origin'] = 'data_system'
transport_model_road['origin'] = 'model'
transport_model_non_road['origin'] = 'model'
transport_model_all['origin'] = 'model'

#grab dates that match across all dataframes
dates_in_all = set(energy_activity_for_system_road['date']).intersection(set(energy_activity_for_system_non_road['date'])).intersection(set(transport_data_system_road['date'])).intersection(set(transport_data_system_non_road['date'])).intersection(set(transport_model_road['date'])).intersection(set(transport_model_non_road['date']))
#%%
#lastly, where the economy is not  in energy_activity_for_system_road or energy_activity_for_system_non_road, remove them. But also get the sum of the values for each measure-medium-vehicle type-transport type-drive combo and add that to the dataframe with economy = all -  except for factors, where they will be weigthed by their activity
economies = energy_activity_for_system_road['economy'].drop_duplicates()
all_economies_df = pd.DataFrame()
for df in [transport_data_system_road, transport_data_system_non_road, transport_model_road, transport_model_non_road]:
    new_df = pd.DataFrame()
    for economy in economies:
        if economy!='all':#if its one of those economes we will be averaging out factors and summing up non factors by removing drive type for that economy only, but if its not then we will be averaging out factors by removing drive type and economy for all economies
            breakpoint()
            df_economy = df.loc[df['economy'] == economy].copy()
        else:
            df_economy = df.copy()
            
        factors= ['efficiency', 'intensity', 'mileage', 'occupancy_or_load']
        non_factors = ['activity', 'energy', 'stocks', 'travel_km']
        activity = df_economy.loc[df_economy['measure'] == 'activity']
        if economy!='all':
            new_df_economy = df_economy.loc[df_economy['measure'].isin(non_factors)].copy()
            #group car, lt and suv into lpv and ht, mt, lcv into ht
            new_df_economy['vehicle_type'] = new_df_economy['vehicle_type'].replace({'car': 'lpv', 'lt': 'lpv', 'suv': 'lpv', 'ht': 'ht', 'mt': 'ht', 'lcv': 'ht'})
            new_df_economy = new_df_economy.groupby(['date', 'vehicle_type', 'transport_type', 'medium',  'measure', 'origin'])[['value']].sum().reset_index()
            new_df_economy['economy'] = economy
            new_df_economy['drive'] = 'all'
        else:
            new_df_economy = pd.DataFrame()#we dont really want a sum of the non factors for all economies, its not really useful
            
        for measure in factors:
            df_measure = df_economy.loc[df_economy['measure'] == measure].copy()
            df_measure = df_measure.merge(activity, on=['economy', 'date', 'vehicle_type', 'transport_type', 'medium', 'drive'], how='inner', suffixes=('', '_activity'))
            
            df_measure['value'] = df_measure['value'] * df_measure['value_activity']
            
            #group car, lt and suv into lpv and ht, mt, lcv into ht
            df_measure['vehicle_type'] = df_measure['vehicle_type'].replace({'car': 'lpv', 'lt': 'lpv', 'suv': 'lpv', 'ht': 'ht', 'mt': 'ht', 'lcv': 'ht'})
            #now sum the values for each measure-medium-vehicle type-transport type-drive combo and divide by the activity sum to get the weighted average
            df_measure = df_measure.groupby(['date', 'vehicle_type', 'transport_type', 'medium', 'measure', 'origin'])[['value', 'value_activity']].sum().reset_index()
            #if value_activity is 0, then set value to nan
            # Replace 0 with NaN in 'value_activity' to avoid division by zero
            df_measure['value_activity'] = df_measure['value_activity'].replace(0, np.nan)
            df_measure['value'] = df_measure['value'] / df_measure['value_activity']
            df_measure = df_measure.drop(columns='value_activity')
            df_measure['economy'] = economy
            df_measure['drive'] = 'all'
            new_df_economy = pd.concat([new_df_economy, df_measure], ignore_index=True)
        new_df = pd.concat([new_df, new_df_economy], ignore_index=True)
        
    all_economies_df = pd.concat([all_economies_df, new_df], ignore_index=True)
            
            
#now do all_all_all: this is pretty similar but we will be dropping out the vehicle type and medium cols, plus doing an extra step to create an aggregate without the transport type col and setting it to 'combined'
all_all_all = pd.DataFrame()
for df in [transport_data_system_all, transport_model_all]:
    new_df = pd.DataFrame()
    for economy in economies:
        if economy!='all':#if its one of those economes we will be averaging out factors and summing up non factors by removing drive type for that economy only, but if its not then we will be averaging out factors by removing drive type and economy for all economies
            df_economy = df.loc[df['economy'] == economy].copy()
        else:
            df_economy = df.copy()
            
        factors= ['efficiency', 'intensity', 'mileage', 'occupancy_or_load']
        non_factors = ['activity', 'energy', 'stocks', 'travel_km']
        activity = df_economy.loc[df_economy['measure'] == 'activity']
        if economy!='all':
            new_df_economy = df_economy.loc[df_economy['measure'].isin(non_factors)].copy()
            ##
            new_df_economy_combined_transport = new_df_economy.copy()
            new_df_economy_combined_transport['transport_type'] = 'combined'
            new_df_economy = pd.concat([new_df_economy, new_df_economy_combined_transport], ignore_index=True)
            ##
            new_df_economy = new_df_economy.groupby(['date', 'transport_type', 'measure', 'origin'])[['value']].sum().reset_index()
            
            new_df_economy['economy'] = economy
            new_df_economy['drive'] = 'all'
            new_df_economy['vehicle_type'] = 'all'
            new_df_economy['medium'] = 'all'
        else:
            new_df_economy = pd.DataFrame()#we dont really want a sum of the non factors for all economies, its not really useful
            
        for measure in factors:
            df_measure = df_economy.loc[df_economy['measure'] == measure].copy()
            df_measure = df_measure.merge(activity, on=['economy', 'date', 'transport_type', 'medium', 'drive', 'vehicle_type'], how='inner', suffixes=('', '_activity'))
            
            df_measure['value'] = df_measure['value'] * df_measure['value_activity']
            ##
            df_measure_combined_transport = df_measure.copy()
            df_measure_combined_transport['transport_type'] = 'combined'
            df_measure = pd.concat([df_measure, df_measure_combined_transport], ignore_index=True)
            ##
            #now sum the values for each measure-medium-vehicle type-transport type-drive combo and divide by the activity sum to get the weighted average
            df_measure = df_measure.groupby(['date', 'transport_type', 'measure', 'origin'])[['value', 'value_activity']].sum().reset_index()
            #if value_activity is 0, then set value to nan
            # Replace 0 with NaN in 'value_activity' to avoid division by zero
            df_measure['value_activity'] = df_measure['value_activity'].replace(0, np.nan)
            df_measure['value'] = df_measure['value'] / df_measure['value_activity']
            df_measure = df_measure.drop(columns='value_activity')
            df_measure['economy'] = economy
            df_measure['drive'] = 'all'
            df_measure['vehicle_type'] = 'all'
            df_measure['medium'] = 'all'
            new_df_economy = pd.concat([new_df_economy, df_measure], ignore_index=True)
        new_df = pd.concat([new_df, new_df_economy], ignore_index=True)
        
    all_all_all = pd.concat([all_all_all, new_df], ignore_index=True)
    
#%%
all_economies_df = pd.concat([all_economies_df, all_all_all], ignore_index=True)


#%%
#concat all the dataframes
all_data = pd.concat([energy_activity_for_system_road, energy_activity_for_system_non_road, all_economies_df], ignore_index=True)

all_data = all_data[all_data['date'].isin(dates_in_all)]
#now by measure-medium combo, create a dashboard with a scatter plot for each vehicle type-transport type-economy combo. we can craerte dashboard using lolty exp[ress and teh facet cols/rows params
import plotly.express as px

for measure_medium in all_data[['measure', 'medium']].drop_duplicates().itertuples():
    measure = measure_medium.measure
    medium = measure_medium.medium
    #filter the data
    data = all_data[(all_data['measure'] == measure) & (all_data['medium'] == medium)]
    #create a scatter plot for each vehicle type-transport-economy type combo
    #make origin also contain drive
    data['origin'] = data['origin'] #+ ' - ' + data['drive']
    data['vehicle_type'] = data['vehicle_type'] + ' - ' + data['transport_type']
    fig = px.scatter(data, x='origin', y='value', color='economy', facet_col='vehicle_type', symbol='drive')
    
    fig.update_yaxes(matches=None)
        
    # Show the tick marks and numbers for each y-axis
    fig.update_yaxes(showticklabels=True)
    #show the 

    #MAKE THE title of the graph the medium-measure
    fig.update_layout(title_text='{} - {}'.format(measure, medium))
    #put in html
    fig.write_html('plotting_output/analysis/EEI_comparison_{}_{}.html'.format(measure, medium))
    

#%%

#findings:
# -ht efficiency is almost 3x too low (might include lcvs though)
# -2w efficiency is almost 2x too low
# - intensity is super wacky. probably wrong calculation.
# - rok bus stocks (41x) way too low in model and freight stocks about 3x too high
# -aus ht stocks 2x too low. this is even when considering high lcvs in aus. ??
# - bus stocks about 4x too low in aus
# - 2w stocks also about 2x too low in aus
# - but with rok and aus, we have 3x their bus occupancy. 
# - in rok, cda, aus and jp we have almost 2x their load for hts. 


# so overall, it seems i should try to fix these thigns but also be happy with where thigns are at also - a lot of these issues (maybe besides korea bus stocks) are relatively minor and balanced out on the whole. Most importantly the factors we are using are pretty similar, because these are really important. Also it would be good to improve our understanding of ratios of mt/ht/lcvs because thats important for electrification. 
# %%
