#import eei data from input_data/EEI/activity.xlsx
#%%
import pandas as pd
import numpy as np
import os
import re
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
# Load data for every sheet in the workbook. We will concat them all together, except activity for which we will create a separate dataframe
eei = pd.read_excel('input_data/EEI/buildings exploration.xlsx', sheet_name=None)
sheets = ['services indicators', 'residential indicators','activity','res energy','services energy']
#check that all those sheets are in the data and no extra ones
for sheet_name, sheet_data in eei.items():
    if sheet_name not in sheets:
        print(sheet_name + ' not in sheets')
for sheet in sheets:
    if sheet not in eei.keys():
        print(sheet + ' not in data')
    
    
activity =pd.DataFrame()
data = pd.DataFrame()
for sheet_name in sheets:
    print(sheet_name)
    sheet_data = pd.read_excel('input_data/EEI/buildings exploration.xlsx', sheet_name=sheet_name)
    print(sheet_data.head())
    print('\n \n')
    if sheet_name == 'activity':
        activity = sheet_data
        activity.rename(columns={'Product': 'Measure'}, inplace=True)
    else:
        if 'Product' in sheet_data.columns:
            sheet_data.rename(columns={'Product': 'Fuel'}, inplace=True)
            sheet_data['Measure'] = 'Energy'
        elif 'Indicator' in sheet_data.columns:
            sheet_data.rename(columns={'Indicator': 'Measure'}, inplace=True)
            sheet_data['Fuel'] = 'All'
            
        data = pd.concat([data, sheet_data])
        
#%%
#also repalce vlaues that are just '..' with np.nan
data.replace('..', np.nan, inplace=True)
data.replace('x', np.nan, inplace=True)
# data.columns
# Index(['Country', 'End use', 'Measure',      2000,      2001,      2002,
#             2003,      2004,      2005,      2006,      2007,      2008,
#             2009,      2010,      2011,      2012,      2013,      2014,
#             2015,      2016,      2017,      2018,      2019,      2020,
#             2021,    'Fuel'],
#       dtype='object')
#%%
activity.replace('..', np.nan, inplace=True)
activity.replace('x', np.nan, inplace=True)

#cols:
# activity.columns
# Index([ 'Country', 'Activity',  'Measure',       2000,       2001,       2002,
#  2003,       2004,       2005,       2006,       2007,       2008,
#  2009,       2010,       2011,       2012,       2013,       2014,
#  2015,       2016,       2017,       2018,       2019,       2020,
#  2021],
#%%
#Convert Product into Measure/Unit columns:

#first, we have an issue where there are a lot of trailing spaces in the Product column. so remove them then see the unique vlaues:
data['Measure'] = data['Measure'].str.strip()
data['Measure'].unique()
# array(['Per value added energy intensity (MJ/USD PPP 2015)',
#        'Per capita energy intensity (GJ/cap)',
#        'Per floor area energy intensity (GJ/m2)',
#        'Per floor area TC energy intensity (GJ/m2)',
#        'Per services employee energy intensity (GJ/employee)',
#        'Per dwelling energy intensity (GJ/dw)',
#        'Per dwelling TC energy intensity (GJ/dw)',
#        'Per unit equipment energy intensity (GJ/unit)', 'Energy'],
#       dtype=object)
#%%
data['End use'].unique()

# array(['Total Services', 'Services space heating',
#        'Services space cooling', 'Services lighting', 'Total Residential',
#        'Residential space heating', 'Residential space cooling',
#        'Residential water heating', 'Residential cooking',
#        'Residential lighting', 'Refrigerators', 'Freezers',
#        'Refrigerator/Freezer combinations', 'Dish washers',
#        'Clothes washers', 'Clothes dryers', 'Televisions',
#        'Personal computers', 'Other appliances', 'Residential appliances',
#        'Non-specified', 'Other building energy use',
#        'Non-building energy use',
#        'Sewerage, waste and remediation [ISIC 37-39]',
#        'Wholesale and retail [ISIC 46-47]',
#        'Warehousing, support for transport, postal [ISIC 52-53]',
#        'Accommodation and food [ISIC 55-56]',
#        'Information and communication [ISIC 58-63]',
#        'Finance, insurance, real estate, science, admin [ISIC 64-82]',
#        'Public admin, excluding defence [ISIC 84]', 'Education [ISIC 85]',
#        'Health and social work [ISIC 86-88]',
#        'Arts, entertainment and recreation [ISIC 90-93]',
#        'Other services [ISIC 33; 45; 94-96]'], dtype=object)
#%%
activity['Activity'].unique()
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
#%%

activity['Measure'] = activity['Measure'].str.strip()
activity['Measure'].unique()
#%%
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
    'Occupied dwellings of which heated by oil products (%)': 'Occupied dwellings of which heated by oil products (%)',
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
#rename columns:
activity['Detailed_sector'] = activity['Activity']
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

# 'Total Services', 'Services space heating',
#        'Services space cooling', 'Services lighting', 'Total Residential',
#        'Residential space heating', 'Residential space cooling',
#        'Residential water heating', 'Residential cooking',
#        'Residential lighting', 'Refrigerators', 'Freezers',
#        'Refrigerator/Freezer combinations', 'Dish washers',
#        'Clothes washers', 'Clothes dryers', 'Televisions',
#        'Personal computers', 'Other appliances', 'Residential appliances',
#        'Non-specified', 'Other building energy use',
#        'Non-building energy use',
#        'Sewerage, waste and remediation [ISIC 37-39]',
#        'Wholesale and retail [ISIC 46-47]',
#        'Warehousing, support for transport, postal [ISIC 52-53]',
#        'Accommodation and food [ISIC 55-56]',
#        'Information and communication [ISIC 58-63]',
#        'Finance, insurance, real estate, science, admin [ISIC 64-82]',
#        'Public admin, excluding defence [ISIC 84]', 'Education [ISIC 85]',
#        'Health and social work [ISIC 86-88]',
#        'Arts, entertainment and recreation [ISIC 90-93]',
#        'Other services [ISIC 33; 45; 94-96]']
data_end_use_to_sector_dict = {
    'Total Services': 'services',
    'Services space heating': 'services',
    'Services space cooling': 'services',
    'Services lighting': 'services',
    'Total Residential': 'residential',
    'Residential space heating': 'residential',
    'Residential space cooling': 'residential',
    'Residential water heating': 'residential',
    'Residential cooking': 'residential',
    'Residential lighting': 'residential',
    'Refrigerators': 'residential',
    'Freezers': 'residential',
    'Refrigerator/Freezer combinations': 'residential',
    'Dish washers': 'residential',
    'Clothes washers': 'residential',
    'Clothes dryers': 'residential',
    'Televisions': 'residential',
    'Personal computers': 'residential',
    'Other appliances': 'residential',
    'Residential appliances': 'residential',
    'Non-specified': 'residential',
    'Other building energy use': 'residential',
    'Non-building energy use': 'residential',
    'Sewerage, waste and remediation [ISIC 37-39]': 'services',
    'Wholesale and retail [ISIC 46-47]': 'services',
    'Warehousing, support for transport, postal [ISIC 52-53]': 'services',
    'Accommodation and food [ISIC 55-56]': 'services',
    'Information and communication [ISIC 58-63]': 'services',
    'Finance, insurance, real estate, science, admin [ISIC 64-82]': 'services',
    'Public admin, excluding defence [ISIC 84]': 'services',
    'Education [ISIC 85]': 'services',
    'Health and social work [ISIC 86-88]': 'services',
    'Arts, entertainment and recreation [ISIC 90-93]': 'services',
    'Other services [ISIC 33; 45; 94-96]': 'services'
}
###################################################
#%%
#ok now do the mappings:
data['Sector'] = data['End use'].replace(data_end_use_to_sector_dict)

activity['Sector'] = activity['Detailed_sector'].replace(sector_dict)
activity['Measure'] = activity['Measure'].replace(product_to_measure_dict)
activity['Unit'] = activity['Measure'].replace(product_to_unit_dict)
activity.drop(columns=['Activity'], inplace=True)
#and convert any economies we know to their iso codes
country_codes = pd.read_csv('config/economy_code_to_name.csv')
#stack the following cols: Economy_name	Alt_name	Alt_name2	Alt_name3

country_codes = country_codes.melt(id_vars='Economy', value_vars=['Economy_name', 'Alt_name', 'Alt_name2', 'Alt_name3'], value_name='Country')
#and drop nas
country_codes.dropna(inplace=True)

activity = activity.merge(country_codes[['Country', 'Economy']], on='Country', how='left')
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

buildings_activity = activity[activity['Sector'].isin(['residential', 'services', 'macro'])]

#find the unique combos of Measure and Detailed_sector
buildings_activity[['Measure', 'Detailed_sector']].drop_duplicates()
# Measure	Detailed_sector
# 0	Population	General Activity
# 1	Services employment	General Activity
# 2	Occupied dwellings	General Activity
# 3	Residential floor area	General Activity
# 4	Heating degree days	General Activity
# 5	Cooling degree days	General Activity
# 6	Residential technology stocks	Freezers
# 7	Residential technology stocks	Refrigerator/Freezer combinations
# 8	Residential technology stocks	Dish washers
# 9	Residential technology stocks	Clothes washers
# 10	Residential technology stocks	Clothes dryers
# 11	Residential technology stocks	Televisions
# 12	Residential technology stocks	Personal computers
# 27	GDP	Total Services
# 53	Occupied dwellings of which heated by oil prod...	General Activity
# 54	Occupied dwellings of which heated by gas (%)	General Activity
# 55	Occupied dwellings of which heated by biofuels...	General Activity
# 56	Occupied dwellings of which heated by district...	General Activity
# 57	Occupied dwellings of which heated by electric...	General Activity
# 113	Residential technology stocks	Refrigerators
# 120	Residential technology stocks	Air conditioners
# 162	Services floor area	General Activity
# 359	Residential technology stocks	Heat pump
# 360	Residential technology stocks	Solar thermal panels
# 361	Peak power	Solar photovoltaic panels
#%%
for i in data[['End use', 'Measure']].drop_duplicates().iterrows():
    end_use = i[1]['End use']
    measure = i[1]['Measure']
    print(end_use + ' : ' + measure)
# Total Services : Per value added energy intensity (MJ/USD PPP 2015)
# Total Services : Per capita energy intensity (GJ/cap)
# Total Services : Per floor area energy intensity (GJ/m2)
# Total Services : Per floor area TC energy intensity (GJ/m2)
# Total Services : Per services employee energy intensity (GJ/employee)
# Services space heating : Per capita energy intensity (GJ/cap)
# Services space heating : Per floor area energy intensity (GJ/m2)
# Services space heating : Per floor area TC energy intensity (GJ/m2)
# Services space heating : Per services employee energy intensity (GJ/employee)
# Services space cooling : Per capita energy intensity (GJ/cap)
# Services space cooling : Per floor area energy intensity (GJ/m2)
# Services space cooling : Per floor area TC energy intensity (GJ/m2)
# Services space cooling : Per services employee energy intensity (GJ/employee)
# Services lighting : Per capita energy intensity (GJ/cap)
# Services lighting : Per floor area energy intensity (GJ/m2)
# Services lighting : Per services employee energy intensity (GJ/employee)
# Total Residential : Per capita energy intensity (GJ/cap)
# Total Residential : Per floor area energy intensity (GJ/m2)
# Total Residential : Per floor area TC energy intensity (GJ/m2)
# Total Residential : Per dwelling energy intensity (GJ/dw)
# Total Residential : Per dwelling TC energy intensity (GJ/dw)
# Residential space heating : Per capita energy intensity (GJ/cap)
# Residential space heating : Per floor area energy intensity (GJ/m2)
# Residential space heating : Per floor area TC energy intensity (GJ/m2)
# Residential space heating : Per dwelling energy intensity (GJ/dw)
# Residential space heating : Per dwelling TC energy intensity (GJ/dw)
# Residential space cooling : Per capita energy intensity (GJ/cap)
# Residential space cooling : Per floor area energy intensity (GJ/m2)
# Residential space cooling : Per floor area TC energy intensity (GJ/m2)
# Residential space cooling : Per dwelling energy intensity (GJ/dw)
# Residential space cooling : Per dwelling TC energy intensity (GJ/dw)
# Residential water heating : Per capita energy intensity (GJ/cap)
# Residential water heating : Per floor area energy intensity (GJ/m2)
# Residential water heating : Per dwelling energy intensity (GJ/dw)
# Residential cooking : Per capita energy intensity (GJ/cap)
# Residential cooking : Per floor area energy intensity (GJ/m2)
# Residential cooking : Per dwelling energy intensity (GJ/dw)
# Residential lighting : Per capita energy intensity (GJ/cap)
# Residential lighting : Per floor area energy intensity (GJ/m2)
# Residential lighting : Per dwelling energy intensity (GJ/dw)
# Refrigerators : Per capita energy intensity (GJ/cap)
# Refrigerators : Per dwelling energy intensity (GJ/dw)
# Refrigerators : Per unit equipment energy intensity (GJ/unit)
# Freezers : Per capita energy intensity (GJ/cap)
# Freezers : Per dwelling energy intensity (GJ/dw)
# Freezers : Per unit equipment energy intensity (GJ/unit)
# Refrigerator/Freezer combinations : Per capita energy intensity (GJ/cap)
# Refrigerator/Freezer combinations : Per dwelling energy intensity (GJ/dw)
# Refrigerator/Freezer combinations : Per unit equipment energy intensity (GJ/unit)
# Dish washers : Per capita energy intensity (GJ/cap)
# Dish washers : Per dwelling energy intensity (GJ/dw)
# Dish washers : Per unit equipment energy intensity (GJ/unit)
# Clothes washers : Per capita energy intensity (GJ/cap)
# Clothes washers : Per dwelling energy intensity (GJ/dw)
# Clothes washers : Per unit equipment energy intensity (GJ/unit)
# Clothes dryers : Per capita energy intensity (GJ/cap)
# Clothes dryers : Per dwelling energy intensity (GJ/dw)
# Clothes dryers : Per unit equipment energy intensity (GJ/unit)
# Televisions : Per capita energy intensity (GJ/cap)
# Televisions : Per dwelling energy intensity (GJ/dw)
# Televisions : Per unit equipment energy intensity (GJ/unit)
# Personal computers : Per capita energy intensity (GJ/cap)
# Personal computers : Per dwelling energy intensity (GJ/dw)
# Personal computers : Per unit equipment energy intensity (GJ/unit)
# Other appliances : Per capita energy intensity (GJ/cap)
# Other appliances : Per dwelling energy intensity (GJ/dw)
# Residential appliances : Per capita energy intensity (GJ/cap)
# Residential appliances : Per dwelling energy intensity (GJ/dw)
# Total Residential : Energy
# Residential space heating : Energy
# Residential space cooling : Energy
# Residential water heating : Energy
# Residential cooking : Energy
# Residential lighting : Energy
# Residential appliances : Energy
# Refrigerators : Energy
# Freezers : Energy
# Refrigerator/Freezer combinations : Energy
# Dish washers : Energy
# Clothes washers : Energy
# Clothes dryers : Energy
# Televisions : Energy
# Personal computers : Energy
# Other appliances : Energy
# Non-specified : Energy
# Total Services : Energy
# Services space heating : Energy
# Services space cooling : Energy
# Services lighting : Energy
# Other building energy use : Energy
# Non-building energy use : Energy
# Sewerage, waste and remediation [ISIC 37-39] : Energy
# Wholesale and retail [ISIC 46-47] : Energy
# Warehousing, support for transport, postal [ISIC 52-53] : Energy
# Accommodation and food [ISIC 55-56] : Energy
# Information and communication [ISIC 58-63] : Energy
# Finance, insurance, real estate, science, admin [ISIC 64-82] : Energy
# Public admin, excluding defence [ISIC 84] : Energy
# Education [ISIC 85] : Energy
# Health and social work [ISIC 86-88] : Energy
# Arts, entertainment and recreation [ISIC 90-93] : Energy
# Other services [ISIC 33; 45; 94-96] : Energy

#%%
#extract the 'per XYZ energy intensity (GJ/ABC)' values and put XYZ in a 'per' column and GJ/ABC in the Unit column

# Create new columns 'per' and 'Unit'
data['per'] = None
data['Unit'] = None

# Extract 'per' and 'Unit' values
for i, row in data.iterrows():
    measure = row['Measure']
    match = re.search(r'Per (.+?) energy intensity \((.+?)\)', measure)
    if match:
        data.at[i, 'per'] = match.group(1)
        data.at[i, 'Unit'] = match.group(2)
        data.at[i, 'Measure'] = 'Energy intensity'

# print(data)





#%%
# buildings_activity['Detailed_sector'].unique()'General Activity', 'Freezers',
#        'Refrigerator/Freezer combinations', 'Dish washers',
#        'Clothes washers', 'Clothes dryers', 'Televisions',
#        'Personal computers', 'Total Services', 'Refrigerators',
#        'Air conditioners', 'Heat pump', 'Solar thermal panels',
#        'Solar photovoltaic panels'
#TRY TO CONVERT THE ABOVE IN ACTIVITY['DETAILED_SECTOR'] TO SIMILAR END USES AS ARE IN DATA['END USE']
detailed_sector_to_end_use_dict = {
    'General Activity': np.nan,
    'Freezers': 'Freezers',
    'Refrigerator/Freezer combinations': 'Refrigerator/Freezer combinations',
    'Dish washers': 'Dish washers',
    'Clothes washers': 'Clothes washers',
    'Clothes dryers': 'Clothes dryers',
    'Televisions': 'Televisions',
    'Personal computers': 'Personal computers',
    'Total Services': np.nan,#this is total gdp so not an end use
    'Refrigerators': 'Refrigerators',
    'Air conditioners': 'Air conditioners',
    'Heat pump': 'Heat pump',
    'Solar thermal panels': 'Solar thermal panels',
    'Solar photovoltaic panels': 'Solar photovoltaic panels'
}

buildings_activity['End use'] = buildings_activity['Detailed_sector'].replace(detailed_sector_to_end_use_dict)

#and then drop detailed_sector
buildings_activity.drop(columns=['Detailed_sector', 'Unit'], inplace=True)

# #also, where Measure is one of Occupied dwellings of which heated by oil products
# Occupied dwellings of which heated by gas (%)
# Occupied dwellings of which heated by biofuels (%)
# Occupied dwellings of which heated by district heating (%)
# Occupied dwellings of which heated by electricity (%)
#then set the End use to 'Space heating' and the Measure to Percent of Occupied dwellings heated by fuel type
#and then set fuel to the fuel type e.g. oil, gas, biofuels, district heating, electricity
#and then set the unit to '%'

measure_to_fuel_dict = {
    'Occupied dwellings of which heated by oil products (%)': 'oil',
    'Occupied dwellings of which heated by gas (%)': 'gas',
    'Occupied dwellings of which heated by biofuels (%)': 'biofuels',
    'Occupied dwellings of which heated by district heating (%)': 'district heating',
    'Occupied dwellings of which heated by electricity (%)': 'electricity'
}
measure_to_end_use_dict = {
    'Occupied dwellings of which heated by oil products (%)': 'Space heating',
    'Occupied dwellings of which heated by gas (%)': 'Space heating',
    'Occupied dwellings of which heated by biofuels (%)': 'Space heating',
    'Occupied dwellings of which heated by district heating (%)': 'Space heating',
    'Occupied dwellings of which heated by electricity (%)': 'Space heating'
}
measure_to_measure_dict = {
    'Occupied dwellings of which heated by oil products (%)': 'Percent of Occupied dwellings heated by fuel type',
    'Occupied dwellings of which heated by gas (%)': 'Percent of Occupied dwellings heated by fuel type',
    'Occupied dwellings of which heated by biofuels (%)': 'Percent of Occupied dwellings heated by fuel type',
    'Occupied dwellings of which heated by district heating (%)': 'Percent of Occupied dwellings heated by fuel type',
    'Occupied dwellings of which heated by electricity (%)': 'Percent of Occupied dwellings heated by fuel type'
}
measure_to_unit_dict = {
    'Occupied dwellings of which heated by oil products (%)': '%',
    'Occupied dwellings of which heated by gas (%)': '%',
    'Occupied dwellings of which heated by biofuels (%)': '%',
    'Occupied dwellings of which heated by district heating (%)': '%',
    'Occupied dwellings of which heated by electricity (%)': '%'
}
#lets also change ALL sectors based on the measurte in this df
# array(['Population', 'Services employment', 'Occupied dwellings',
#        'Residential floor area', 'Heating degree days',
#        'Cooling degree days', 'Residential technology stocks', 'GDP',
#        'Occupied dwellings of which heated by oil products (%)',
#        'Occupied dwellings of which heated by gas (%)',
#        'Occupied dwellings of which heated by biofuels (%)',
#        'Occupied dwellings of which heated by district heating (%)',
#        'Occupied dwellings of which heated by electricity (%)',
#        'Services floor area', 'Peak power'], dtype=object)
measure_to_sector_dict = {
    'Occupied dwellings of which heated by oil products (%)': 'residential',
    'Occupied dwellings of which heated by gas (%)': 'residential',
    'Occupied dwellings of which heated by biofuels (%)': 'residential',
    'Occupied dwellings of which heated by district heating (%)': 'residential',
    'Occupied dwellings of which heated by electricity (%)': 'residential',
    'Population': 'macro',
    'Services employment': 'services',
    'Occupied dwellings': 'residential',
    'Residential floor area': 'residential',
    'Heating degree days': 'macro',
    'Cooling degree days': 'macro',
    'Residential technology stocks': 'residential',
    'GDP': 'macro',
    'Services floor area': 'services',
    'Peak power': 'all'
}

#now update the columns
buildings_activity['Fuel'] = buildings_activity['Measure'].map(measure_to_fuel_dict)
buildings_activity['End use'] = buildings_activity['Measure'].map(measure_to_end_use_dict)
buildings_activity['Sector'] = buildings_activity['Measure'].map(measure_to_sector_dict)
buildings_activity['Unit'] = buildings_activity['Measure'].map(measure_to_unit_dict)
buildings_activity['Measure'] = buildings_activity['Measure'].replace(measure_to_measure_dict)



#%%
data['End use'].unique()
# array(['Total Services', 'Services space heating',
#        'Services space cooling', 'Services lighting', 'Total Residential',
#        'Residential space heating', 'Residential space cooling',
#        'Residential water heating', 'Residential cooking',
#        'Residential lighting', 'Refrigerators', 'Freezers',
#        'Refrigerator/Freezer combinations', 'Dish washers',
#        'Clothes washers', 'Clothes dryers', 'Televisions',
#        'Personal computers', 'Other appliances', 'Residential appliances',
#        'Non-specified', 'Other building energy use',
#        'Non-building energy use',
#        'Sewerage, waste and remediation [ISIC 37-39]',
#        'Wholesale and retail [ISIC 46-47]',
#        'Warehousing, support for transport, postal [ISIC 52-53]',
#        'Accommodation and food [ISIC 55-56]',
#        'Information and communication [ISIC 58-63]',
#        'Finance, insurance, real estate, science, admin [ISIC 64-82]',
#        'Public admin, excluding defence [ISIC 84]', 'Education [ISIC 85]',
#        'Health and social work [ISIC 86-88]',
#        'Arts, entertainment and recreation [ISIC 90-93]',
#        'Other services [ISIC 33; 45; 94-96]'], dtyp

#clean up end use, such as where it is not an end use, replace with nan and also repalce thigns like Services space heating with space heating, Leave things like ISIC codes as they are as they are still end uses, but remove the ISIC part
end_use_update_dict = {
    'Total Services': 'Total',
    'Services space heating': 'Space heating',
    'Services space cooling': 'Space cooling',
    'Services lighting': 'Lighting',
    'Total Residential': 'Total',
    'Residential space heating': 'Space heating',
    'Residential space cooling': 'Space cooling',
    'Residential water heating': 'Water heating',
    'Residential cooking': 'Cooking',
    'Residential lighting': 'Lighting',
    'Refrigerators': 'Refrigerators',
    'Freezers': 'Freezers',
    'Refrigerator/Freezer combinations': 'Refrigerator/Freezer combinations',
    'Dish washers': 'Dish washers',
    'Clothes washers': 'Clothes washers',
    'Clothes dryers': 'Clothes dryers',
    'Televisions': 'Televisions',
    'Personal computers': 'Personal computers',
    'Other appliances': 'Other appliances',
    'Residential appliances': 'Residential appliances',
    'Non-specified': 'Non-specified',
    'Other building energy use': 'Other building energy use',
    'Non-building energy use': 'Non-building energy use',
    'Sewerage, waste and remediation [ISIC 37-39]': 'Sewerage, waste and remediation',
    'Wholesale and retail [ISIC 46-47]': 'Wholesale and retail',
    'Warehousing, support for transport, postal [ISIC 52-53]': 'Warehousing, support for transport, postal',
    'Accommodation and food [ISIC 55-56]': 'Accommodation and food',
    'Information and communication [ISIC 58-63]': 'Information and communication',
    'Finance, insurance, real estate, science, admin [ISIC 64-82]': 'Finance, insurance, real estate, science, admin',
    'Public admin, excluding defence [ISIC 84]': 'Public admin, excluding defence',
    'Education [ISIC 85]': 'Education',
    'Health and social work [ISIC 86-88]': 'Health and social work',
    'Arts, entertainment and recreation [ISIC 90-93]': 'Arts, entertainment and recreation',
    'Other services [ISIC 33; 45; 94-96]': 'Other services'
}

data['End use'] = data['End use'].replace(end_use_update_dict)
# data['Sector'] = data['End use'].replace(data_end_use_to_sector_dict)

#%%
#melt the data so that the years are in a single column
data_tall = data.melt(id_vars=['Country', 'End use', 'Measure', 'per', 'Unit', 'Economy', 'Fuel', 'Sector'], var_name='Year', value_name='Value')

#same for activity
buildings_activity_tall = buildings_activity.melt(id_vars=['Country','End use', 'Sector', 'Measure', 'Economy', 'Fuel', 'Unit'], var_name='Year', value_name='Value')

#since we now have energy intensity, energy and then activity (which contains many measures, we will separate these into different dataframes)
energy = data_tall[data_tall['Measure'] == 'Energy']
energy_intensity = data_tall[data_tall['Measure'] == 'Energy intensity']

#within energy, remove any ' (PJ)' from the end of the Fuel and add PJ to the unit
energy['Fuel'] = energy['Fuel'].str.strip(' (PJ)')
energy['Unit'] = 'PJ'
#%%
energy['Fuel'].unique()
# 'Oil and oil products', 'Gas', 'Coal and coal products',
#        'Biofuels and waste', 'Heat', 'Electricity', 'Other sources',
#        'Total final energy use', 'Of which: Solar thermal'
#repalce with more simple names
fuel_dict = {
    'Oil and oil products': 'Oil',
    'Gas': 'Gas',
    'Coal and coal products': 'Coal',
    'Biofuels and waste': 'Biofuels',
    'Heat': 'Heat',
    'Electricity': 'Electricity',
    'Other sources': 'Other',
    'Total final energy use': 'All',
    'Of which: Solar thermal': 'Solar thermal'
}
energy['Fuel'] = energy['Fuel'].replace(fuel_dict)


#%%

def convert_string_to_snake_case(string):
    """
    Converts a string to snake case
    """
    # Convert anything to snake case, inclkuding a string with spaces
    string = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    string = re.sub('([a-z0-9])([A-Z])', r'\1_\2', string).lower()
    #replace any / - or , with _
    string = string.replace('-', '_')
    string = string.replace(',', '_')
    string = string.replace('/', '_')
    #repalce spaces with underscores
    string = string.replace(' ', '_')
    #replace any double underscores with single underscores
    string = string.replace('__', '_')
    return string

def convert_all_cols_to_snake_case(df):
    #will convert all vlaues in cols to snake case
    for col in df.columns:
        if col not in ['economy', 'value', 'date', 'year']:
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


energy.columns = [replace_bad_col_names(col) for col in energy.columns]
energy = convert_all_cols_to_snake_case(energy)

energy_intensity.columns = [replace_bad_col_names(col) for col in energy_intensity.columns]
energy_intensity = convert_all_cols_to_snake_case(energy_intensity)

buildings_activity_tall.columns = [replace_bad_col_names(col) for col in buildings_activity_tall.columns]
buildings_activity_tall = convert_all_cols_to_snake_case(buildings_activity_tall)

#%%
#all data concated
buildings_final_df = pd.concat([energy, energy_intensity, buildings_activity_tall], ignore_index=True)

#where economy is nan, replace with non_apec_iea
buildings_final_df['economy'] = buildings_final_df['economy'].fillna('non_apec_iea')
#save to csv iin input
buildings_final_df.to_csv('input_data/EEI/buildings_final.csv', index=False)














############################################################################################################
#VISUALISATION
############################################################################################################


#%%
#time for som visualisation:
import plotly.express as px

#by economy (as the facet col), create a time series of the energy with color as the end use and y as the year

for sector in buildings_final_df['sector'].unique():
    print(sector)
    #get the data for this sector
    sector_data = buildings_final_df[(buildings_final_df['sector'] == sector) & (buildings_final_df['measure'] == 'energy') & (buildings_final_df['end_use'] != 'total') & (buildings_final_df['fuel'] == 'all')]# & (~buildings_final_df['economy'].isna())]
    #extract sector_data for apec economies to add that at the end as end_use=all_apec_only
    apec_sector_data = sector_data[sector_data['economy'] != 'non_apec_iea']
    if sector_data.empty:
        continue
    #sum up in case there are multiple entries for the same economy and year
    sector_data = sector_data.groupby(['year', 'economy', 'end_use']).sum().reset_index()
    #convert to proportions
    sector_data['value'] = sector_data['value'] / sector_data.groupby(['year','economy'])['value'].transform('sum')
    apec_sector_data = apec_sector_data.groupby(['year', 'economy', 'end_use']).sum().reset_index()
    apec_sector_data['value'] = apec_sector_data['value'] / apec_sector_data.groupby(['year','economy'])['value'].transform('sum')
    
    #create an economy which is the avg of the proportion of all the economies
    all_econ = sector_data.copy()
    all_econ['economy'] = 'All'
    all_econ = all_econ.groupby(['year', 'economy', 'end_use']).mean(numeric_only=True).reset_index()
    sector_data = pd.concat([sector_data, all_econ], ignore_index=True)
    apec_sector_data['economy'] = 'All APEC'
    apec_sector_data = apec_sector_data.groupby(['year', 'economy', 'end_use']).mean(numeric_only=True).reset_index()
    sector_data = pd.concat([sector_data, apec_sector_data], ignore_index=True)
    
    #plot
    fig = px.line(sector_data, x='year', y='value', color='end_use', facet_col='economy', facet_col_wrap=3, title=f'{sector} energy use by end use')
    
        
    WRITE_HTML = False
    if WRITE_HTML:
    #save to file
        fig.write_html(f'plotting_output/analysis/buildings/{sector}_by_economy_energy_use_by_end_use.html')
    #create bar versions which will use the average of all years
    sector_data = sector_data.groupby(['economy', 'end_use']).mean(numeric_only=True).reset_index()
    fig = px.bar(sector_data, x='end_use', y='value', color='end_use', facet_col='economy', facet_col_wrap=3, title=f'{sector} energy use by end use')
        
    WRITE_HTML = False
    if WRITE_HTML:
        fig.write_html(f'plotting_output/analysis/buildings/{sector}_by_economy_energy_use_by_end_use_bar.html')

#by end use (as the facet col), create a time series of the energy with color as the fuel and y as the year
for sector in buildings_final_df['sector'].unique():
    print(sector)
    #get the data for this sector
    sector_data = buildings_final_df[(buildings_final_df['sector'] == sector) & (buildings_final_df['measure'] == 'energy') & (buildings_final_df['end_use'] != 'total') & (buildings_final_df['fuel'] != 'all')]# & (~buildings_final_df['economy'].isna())]
    #extract sector_data for apec economies to add that at the end as end_use=all_apec_only
    apec_sector_data = sector_data[sector_data['economy'] != 'non_apec_iea']
    if sector_data.empty:
        continue
    #sum up in case there are multiple entries for the same economy and year
    sector_data = sector_data.groupby(['year', 'end_use', 'fuel']).sum().reset_index()
    apec_sector_data = apec_sector_data.groupby(['year', 'end_use', 'fuel']).sum().reset_index()
    #convert to proportions
    sector_data['value'] = sector_data['value'] / sector_data.groupby(['year','end_use'])['value'].transform('sum')
    apec_sector_data['value'] = apec_sector_data['value'] / apec_sector_data.groupby(['year','end_use'])['value'].transform('sum')
    
    #create an end use which is the avg of the proportion of all the end uses
    all_econ = sector_data.copy()
    all_econ['end_use'] = 'All'
    all_econ = all_econ.groupby(['year', 'end_use', 'fuel']).mean(numeric_only=True).reset_index()
    sector_data = pd.concat([sector_data, all_econ], ignore_index=True)
    apec_sector_data['end_use'] = 'All APEC'
    apec_sector_data = apec_sector_data.groupby(['year', 'end_use', 'fuel']).mean(numeric_only=True).reset_index()
    sector_data = pd.concat([sector_data, apec_sector_data], ignore_index=True)
    
    #plot
    fig = px.line(sector_data, x='year', y='value', color='fuel', facet_col='end_use', facet_col_wrap=3, title=f'{sector} energy use by fuel')
        
    WRITE_HTML = False
    if WRITE_HTML:
        #save to file
        fig.write_html(f'plotting_output/analysis/buildings/{sector}_by_end_use_energy_use_by_fuel.html')
    #create bar versions which will use the average of all years
    sector_data = sector_data.groupby(['end_use', 'fuel']).mean(numeric_only=True).reset_index()
    fig = px.bar(sector_data, x='fuel', y='value', color='fuel', facet_col='end_use', facet_col_wrap=3, title=f'{sector} energy use by fuel')
        
    WRITE_HTML = False
    if WRITE_HTML:
        fig.write_html(f'plotting_output/analysis/buildings/{sector}_by_end_use_energy_use_by_fuel_bar.html')

#%%
#do similar things with energy intensity but we will ahve to include the effect of the per column which is wat the energy intensity's activity is per (e.g. per capita, per floor area etc).
#we want to understand this data by economy and end use so we might have to create a lot of charts. maybe we can drop the year and just have the economy as the facet col, x and color as the end use and a new chart for each per column and sector. THen after we can normalise all per columns to be the same and then compare the indexed intensity of end uses by economy. When we normalise i guess it should be done in two ways, across the columns:  end use and sector and then across the rows: sector


for sector in buildings_final_df['sector'].unique():
    for per_col in buildings_final_df['per'].unique():
        sector_data = buildings_final_df[(buildings_final_df['sector'] == sector) & (buildings_final_df['measure'] == 'energy_intensity') & (buildings_final_df['end_use'] != 'total') & (buildings_final_df['per'] == per_col) & (buildings_final_df['fuel'] == 'all')]
        #extract sector_data for apec economies to add that at the end as end_use=all_apec_only
        apec_sector_data = sector_data[sector_data['economy'] != 'non_apec_iea']
        if sector_data.empty:
            continue
        #sum up in case there are multiple entries for the same economy and year
        sector_data = sector_data.groupby(['economy', 'end_use']).mean(numeric_only=True).reset_index()
        apec_sector_data = apec_sector_data.groupby(['economy', 'end_use']).mean(numeric_only=True).reset_index()
        #create an end use which is the avg of all economies intensities (not weighted!)
        all_econ = sector_data.copy()
        all_econ['economy'] = 'All'
        all_econ = all_econ.groupby(['economy', 'end_use']).mean(numeric_only=True).reset_index()
        sector_data = pd.concat([sector_data, all_econ], ignore_index=True)
        apec_sector_data['economy'] = 'All APEC'
        apec_sector_data = apec_sector_data.groupby(['economy', 'end_use']).mean(numeric_only=True).reset_index()
        sector_data = pd.concat([sector_data, apec_sector_data], ignore_index=True)
        
        #plot
        fig = px.bar(sector_data, x='end_use', y='value', color='end_use', facet_col='economy', facet_col_wrap=3, title=f'{sector} energy intensity by end use per {per_col}')
        #save to file
                
        WRITE_HTML = False
        if WRITE_HTML:
            fig.write_html(f'plotting_output/analysis/buildings/{sector}_by_economy_energy_intensity_by_end_use_per_{per_col}.html')
        
    #normalise by the per columns 'THen after we can normalise all per columns to be the same and then compare the indexed intensity of end uses by economy. When we normalise i guess it should be done by removing the end use column and then normlising across the rows: sector'
    normalised_1 = buildings_final_df[(buildings_final_df['sector'] == sector) & (buildings_final_df['measure'] == 'energy_intensity') & (buildings_final_df['end_use'] != 'total') & (buildings_final_df['fuel'] == 'all')]
    if normalised_1.empty:
        continue
    normalised_apec_1 = normalised_1[normalised_1['economy'] != 'non_apec_iea']
    #first take the mean across all other columns but the per and grouping columns
    normalised_1 = normalised_1.groupby(['economy', 'end_use', 'per']).mean(numeric_only=True).reset_index()
    #create the all apec economy now
    normalised_apec_1 = normalised_apec_1.groupby(['economy', 'end_use', 'per']).mean(numeric_only=True).reset_index()
    normalised_apec_1['economy'] = 'All APEC'
    normalised_apec_1 = normalised_apec_1.groupby(['economy', 'end_use', 'per']).mean(numeric_only=True).reset_index()
    normalised_1 = pd.concat([normalised_1, normalised_apec_1], ignore_index=True)
    
    #now normalise the data in the value column by the mean of the value column for each per column
    normalised_1['value'] = normalised_1['value'] / normalised_1.groupby(['economy', 'per'])['value'].transform('mean')
    #to uderstand the effect, take the mean of the normalised values for each end use and then by each economy, and then by both and then by per and then by per and end use and then by per and economy
    # to understand the effect of the normalisation
    for groups in [['economy'], ['end_use'], ['economy', 'end_use'], ['per'], ['per', 'end_use']]:
        #plot a bar
        normalised_2 = normalised_1.groupby(groups).mean(numeric_only=True).reset_index().copy()
        
        if len(groups) != 1:        
            normalised_2['group'] = normalised_2[groups[0]] + ' ' + normalised_2[groups[1]]
            fig = px.bar(normalised_2, x='group', y='value', color='group', title=f'{sector} normalised energy intensity by end use per {groups}')
        else:
            normalised_2['group'] = normalised_2[groups[0]]
            
        
                
        WRITE_HTML = False
        if WRITE_HTML:
            fig.write_html(f'plotting_output/analysis/buildings/{sector}_normalised_energy_intensity_by_end_use_per_{groups}.html')
        
    #plot
    fig = px.bar(normalised_1, x='end_use', y='value', color='per', facet_col='economy', facet_col_wrap=3, title=f'{sector} normalised energy intensity by end use')
        
    WRITE_HTML = False
    if WRITE_HTML:
        fig.write_html(f'plotting_output/analysis/buildings/{sector}_normalised_energy_intensity_by_end_use_with_per.html')
    #plot with average of all pers
    normalised_2 = normalised_1.groupby(['economy', 'end_use']).mean(numeric_only=True).reset_index()
    fig = px.bar(normalised_2, x='end_use', y='value', color='end_use', facet_col='economy', facet_col_wrap=3, title=f'{sector} normalised energy intensity by end use')
    #save to file
        
    WRITE_HTML = False
    if WRITE_HTML:
        fig.write_html(f'plotting_output/analysis/buildings/{sector}_normalised_energy_intensity_by_end_use.html')
    
#%% 





#try taking the average intensity by 'per', 'end_use' and economy then plot that with per and end use as facet cols and rows and then economyas x
for sector in buildings_final_df['sector'].unique():
    print(sector)
    #get the data for this sector
    sector_data = buildings_final_df[(buildings_final_df['sector'] == sector) & (buildings_final_df['measure'] == 'energy_intensity') & (buildings_final_df['end_use'] != 'total') & (buildings_final_df['fuel'] == 'all')]# & (~buildings_final_df['economy'].isna())]
    
    #############
    #lets also average out all the applicances (that is teh end uses: 'clothes_dryers', 'clothes_washers','dish_washers',
    #     'freezers', 'personal_computers',
    #     'refrigerator_freezer_combinations', 'refrigerators',
    #     'televisions', 
    # sector_data.end_use.unique()
    # array(['clothes_dryers', 'clothes_washers', 'cooking', 'dish_washers',
    #     'freezers', 'lighting', 'other_appliances', 'personal_computers',
    #     'refrigerator_freezer_combinations', 'refrigerators',
    #     'residential_appliances', 'space_cooling', 'space_heating',
    #     'televisions', 'water_heating'], dtype=object)
    
    appliances = ['clothes_dryers', 'clothes_washers','dish_washers', 'freezers', 'personal_computers', 'refrigerator_freezer_combinations', 'refrigerators', 'televisions']
    sector_data['end_use'] = sector_data['end_use'].replace(appliances, 'appliances')#this really should be weighted. lets jsut rmeove them since qwe have 'residential_appliances'
    sector_data = sector_data[sector_data['end_use'] != 'appliances']
    #############
    
    #then drop per = unit_equipment as we are not interested in that now all appliances are grouped
    sector_data = sector_data[sector_data['per'] != 'unit_equipment']
    
    #extract sector_data for apec economies to add that at the end as end_use=all_apec_only
    apec_sector_data = sector_data[sector_data['economy'] != 'non_apec_iea']
    if sector_data.empty:
        continue
    #sum up in case there are multiple entries for the same economy and year
    sector_data = sector_data.groupby(['economy', 'end_use', 'per']).mean(numeric_only=True).reset_index()
    apec_sector_data = apec_sector_data.groupby(['economy', 'end_use', 'per']).mean(numeric_only=True).reset_index()
    #create an end use which is the avg of all economies intensities (not weighted!)
    all_econ = sector_data.copy()
    all_econ['economy'] = 'All'
    all_econ = all_econ.groupby(['economy', 'end_use', 'per']).mean(numeric_only=True).reset_index()
    sector_data = pd.concat([sector_data, all_econ], ignore_index=True)
    apec_sector_data['economy'] = 'All APEC'
    apec_sector_data = apec_sector_data.groupby(['economy', 'end_use', 'per']).mean(numeric_only=True).reset_index()
    sector_data = pd.concat([sector_data, apec_sector_data], ignore_index=True)
    
    #try taking the average intensity by 'per', 'end_use' and economy then plot that with per and end use as facet cols and rows and then economyas x
    #plot
    fig = px.bar(sector_data, x='economy', y='value', color='economy', facet_col='per', facet_row='end_use', title=f'{sector} energy intensity by end use and per')
        
    WRITE_HTML = False
    if WRITE_HTML:
    #save to file
        fig.write_html(f'plotting_output/analysis/buildings/{sector}_by_economy_energy_intensity_by_end_use.html')
    
    #what about a scatter with x = end use, facet row = per, y = value, color = economy
    breakpoint()
    fig = px.scatter(sector_data, x='end_use', y='value', color='economy', facet_col='per', title=f'{sector} energy intensity by end use and per') 
    #make axes independent usign matches = False
    
    #make the facets y axis independent
    fig.update_yaxes(matches=None, showticklabels=True)
    fig.update_xaxes(matches=None, showticklabels=True)
    
    WRITE_HTML = False
    if WRITE_HTML:
        fig.write_html(f'plotting_output/analysis/buildings/{sector}_by_economy_energy_intensity_by_end_use_scatter.html')
    
    
    
    
#%%
#from this we've found some things that we can do to clean up the data and simplify it. We will:
#drop the following appliances since they contain more detial than we need:
appliances = ['clothes_dryers', 'clothes_washers','dish_washers', 'freezers', 'personal_computers', 'refrigerator_freezer_combinations', 'refrigerators', 'televisions']
buildings_final_df['end_use'] = buildings_final_df['end_use'].replace(appliances, 'appliances')#this really should be weighted. lets jsut rmeove them since qwe have 'residential_appliances'
buildings_final_df = buildings_final_df[buildings_final_df['end_use'] != 'appliances']

#drop where per = dwelling_tc and floor_area_tc and unit_equipment and serices_employee (even though they are super interestin!)
buildings_final_df = buildings_final_df[~buildings_final_df['per'].isin(['dwelling_tc', 'floor_area_tc', 'unit_equipment', 'services_employee'])]

#%%
#now create anotehr set of charts just lkike the last but use the shape argument to show the sector.

#try taking the average intensity by 'per', 'end_use' and economy then plot that with per and end use as facet cols and rows and then economyas x

#get the data for this sector
intensity_plot_final = buildings_final_df[(buildings_final_df['measure'] == 'energy_intensity') & (buildings_final_df['end_use'] != 'total') & (buildings_final_df['fuel'] == 'all')]# & (~buildings_final_df['economy'].isna())]

#extract intensity_plot_final for apec economies to add that at the end as end_use=all_apec_only
apec_intensity_plot_final = intensity_plot_final[intensity_plot_final['economy'] != 'non_apec_iea']

#sum up in case there are multiple entries for the same economy and year
intensity_plot_final = intensity_plot_final.groupby(['economy', 'end_use', 'per', 'sector']).mean(numeric_only=True).reset_index()
apec_intensity_plot_final = apec_intensity_plot_final.groupby(['economy', 'end_use', 'per', 'sector']).mean(numeric_only=True).reset_index()
#create an end use which is the avg of all economies intensities (not weighted!)
all_econ = intensity_plot_final.copy()
all_econ['economy'] = 'All'
all_econ = all_econ.groupby(['economy', 'end_use', 'per', 'sector']).mean(numeric_only=True).reset_index()
intensity_plot_final = pd.concat([intensity_plot_final, all_econ], ignore_index=True)
apec_intensity_plot_final['economy'] = 'All APEC'
apec_intensity_plot_final = apec_intensity_plot_final.groupby(['economy', 'end_use', 'per', 'sector']).mean(numeric_only=True).reset_index()
intensity_plot_final = pd.concat([intensity_plot_final, apec_intensity_plot_final], ignore_index=True)

#try taking the average intensity by 'per', 'end_use' and economy then plot that with per and end use as facet cols and rows and then economyas x
#plot
fig = px.bar(intensity_plot_final, x='economy', y='value', color='economy', facet_col='per', facet_row='end_use', title=f'energy intensity by end use and per', pattern_shape='sector')

WRITE_HTML = False
if WRITE_HTML:
#save to file
    fig.write_html(f'plotting_output/analysis/buildings/by_economy_energy_intensity_by_end_use.html')

#what about a scatter with x = end use, facet row = per, y = value, color = economy
fig = px.scatter(intensity_plot_final, x='end_use', y='value', color='economy', facet_col='per', title=f'energy intensity by end use and per', symbol='sector')
#make axes independent usign matches = False

#make the facets y axis independent
fig.update_yaxes(matches=None, showticklabels=True)
fig.update_xaxes(matches=None, showticklabels=True)

WRITE_HTML = False
if WRITE_HTML:
    fig.write_html(f'plotting_output/analysis/buildings/by_economy_energy_intensity_by_end_use_scatter.html')
#%%
#since we know its a good chart, also show it by year, economy end use - normalised.
#we will normalis within each per sicne we know that the remaining values should all be on the same scale:

#get the data for this sector
intensity_plot_final = buildings_final_df[(buildings_final_df['measure'] == 'energy_intensity') & (buildings_final_df['end_use'] != 'total') & (buildings_final_df['fuel'] == 'all')]# & (~buildings_final_df['economy'].isna())]
#extract intensity_plot_final for apec economies to add that at the end as end_use=all_apec_only
apec_intensity_plot_final = intensity_plot_final[intensity_plot_final['economy'] != 'non_apec_iea']

#sum up in case there are multiple entries for the same economy and year
intensity_plot_final = intensity_plot_final.groupby(['year', 'economy', 'end_use', 'per', 'sector']).mean(numeric_only=True).reset_index()
apec_intensity_plot_final = apec_intensity_plot_final.groupby(['year', 'economy', 'end_use', 'per', 'sector']).mean(numeric_only=True).reset_index()

#create an end use which is the avg of all economies intensities (not weighted!)
all_econ = intensity_plot_final.copy()
all_econ['economy'] = 'All'
all_econ = all_econ.groupby(['year', 'economy', 'end_use', 'per', 'sector']).mean(numeric_only=True).reset_index()
intensity_plot_final = pd.concat([intensity_plot_final, all_econ], ignore_index=True)
apec_intensity_plot_final['economy'] = 'All APEC'
apec_intensity_plot_final = apec_intensity_plot_final.groupby(['year', 'economy', 'end_use', 'per', 'sector']).mean(numeric_only=True).reset_index()
intensity_plot_final = pd.concat([intensity_plot_final, apec_intensity_plot_final], ignore_index=True)


normalisation = intensity_plot_final.copy()
#do min max normalisation
normalisation['value'] = (normalisation['value'] - normalisation.groupby('per')['value'].transform('min')) / (normalisation.groupby('per')['value'].transform('max') - normalisation.groupby('per')['value'].transform('min'))
intensity_plot_final = normalisation.copy()

#now average out the normalised values
intensity_plot_final = intensity_plot_final.groupby(['year', 'economy', 'end_use', 'sector']).mean(numeric_only=True).reset_index()

fig = px.line(intensity_plot_final, x='year', y='value', color='end_use', facet_col='economy', facet_col_wrap=3, title=f'Normalised energy intensity by end use', line_dash='sector')
#make axes independent usign matches = False

#make the facets y axis independent
fig.update_yaxes(matches=None, showticklabels=True)
fig.update_xaxes(matches=None, showticklabels=True)

WRITE_HTML = False
if WRITE_HTML:
    fig.write_html(f'plotting_output/analysis/buildings/by_economy_energy_intensity_by_end_use_line_normalised.html')

#%%
##################################

















##################################
#GOOD CHART:
##################################
#and do a normalised scatter which will include the ability to look at how per cpita differs from the avg:

#sep the per capita out sicne we know tahts a a scale we would liek to use, then show that on the graph too so we can see how its normalised value compares to the others
normalisation_per_capita = normalisation[normalisation['per'] == 'capita']
normalisation_per_capita['per_capita'] = True
normalisation_per_capita = normalisation_per_capita.groupby([ 'end_use', 'sector','per_capita']).mean(numeric_only=True).reset_index()

#create a copy of it for each per in dweling and floor area so we can plot it against those:
normalisation_dwelling = normalisation[normalisation['per'] == 'dwelling']
normalisation_dwelling = normalisation_dwelling.groupby([ 'end_use', 'sector']).mean(numeric_only=True).reset_index()
normalisation_dwelling['per_capita'] = False
normalisation_dwelling = pd.concat([normalisation_dwelling, normalisation_per_capita], ignore_index=True)
normalisation_dwelling['per'] = 'dwelling'

normalisation_floor_area = normalisation[normalisation['per'] == 'floor_area']
normalisation_floor_area = normalisation_floor_area.groupby([ 'end_use', 'sector']).mean(numeric_only=True).reset_index()
normalisation_floor_area['per_capita'] = False
normalisation_floor_area = pd.concat([normalisation_floor_area, normalisation_per_capita], ignore_index=True)
normalisation_floor_area['per'] = 'floor_area'

normalisation_avg = normalisation.groupby([ 'end_use', 'sector']).mean(numeric_only=True).reset_index()
normalisation_avg['per_capita'] = False
normalisation_avg = pd.concat([normalisation_avg, normalisation_per_capita], ignore_index=True)
normalisation_avg['per'] = 'avg'

normalisation_per_capita['per'] ='capita'

normalisation_all = pd.concat([normalisation_dwelling, normalisation_floor_area, normalisation_avg, normalisation_per_capita], ignore_index=True)

#now show the scatter
fig = px.scatter(normalisation_all, x='end_use', y='value', color='sector', facet_col='per', title=f'Normalised energy intensity by end use', symbol='per_capita')

#add sentence at the bottom which explains this: per column is wat the energy intensity's activity is per (e.g. per capita, per floor area etc)
fig.add_annotation(
    x=0.5, y=1.25, xref='paper', yref='paper', showarrow=False,
    text=(
        'Per column is what the energy intensity\'s activity is per (e.g. per capita, per floor area etc).<br>'
        'Per capita will be True or False depending on if the datapoint is for the per capita intensity measure or not. The symbol is the sector of the data point.<br>'
        'When reading this chart, remember our goal is to see if we can use per capita intensity values as intensity values, since they are easily calculated and scaled against population in the projections.<br>'
        'And note that the per capita values are normalised to be on the same scale as the other values.<br>'
        'Therefore, if the per capita values are much different than the other values then maybe we should use other intensity measures for that end use.<br>'
        'But if the other values are relatively similar to each other in scale then if at least one of them is accurate then the others should be too.'
    ),
    font=dict(size=10)
)

fig.write_html(f'plotting_output/analysis/buildings/by_economy_energy_intensity_by_end_use_line_normalised_comp_with_per_cap.html')
##################################
#above is a good chart. shows how the per capita values compare to the other values when normalised. therefore it shows how the scale of the per capita values compare to the other values and between each other. i.e. per capita space heating intensity is a lot higher than the other 'per' values, which means that maybe we should use other intensity measures for space heating. But the other values are relatively similar to each other in scale which means that if at least one of them is accurate then the others should be too.
##################################























#%%Now lets do a simialr thig for energy use but jsut take the average proportion of energy use by end use, sector, economy, year and fuel
#then show a line and scatter of the proportion of energy use with :
# line: color = end use - sector, facet= economy and line_dash = fuel and y = value with x = year 
# scatter: color = fuel, facet = economy, x = year, y = value, symbol = end use - sector

#but also reduce the amount of end use categories by dropping the appliances:
appliances = ['clothes_dryers', 'clothes_washers','dish_washers', 'freezers', 'personal_computers', 'refrigerator_freezer_combinations', 'refrigerators', 'televisions']
energy = buildings_final_df[(buildings_final_df['measure'] == 'energy') & (buildings_final_df['end_use'] != 'total') & (buildings_final_df['fuel'] != 'all')]# & 

energy = energy[~energy['end_use'].isin(appliances)]

#extract energy for apec economies to add that at the end as end_use=all_apec_only
apec_energy = energy[energy['economy'] != 'non_apec_iea']
apec_energy['economy'] = 'All APEC'

#sum up in case there are multiple entries for the same economy and year
energy = energy.groupby(['year', 'economy', 'end_use', 'sector', 'fuel']).sum(numeric_only=True).reset_index()

apec_energy = apec_energy.groupby(['year', 'economy', 'end_use', 'sector', 'fuel']).sum(numeric_only=True).reset_index()

energy = pd.concat([energy, apec_energy], ignore_index=True)
#%%
for sector in energy['sector'].unique():
    print(sector)
    #get the data for this sector
    sector_data = energy[energy['sector'] == sector]
    if sector_data.empty:
        continue
    
    #now calculate the proportion of energy used for each economy/year combination
    sector_data['value'] = sector_data['value'] / sector_data.groupby(['year', 'economy', 'sector'])['value'].transform('sum')

    #create color and line_dash columns
    sector_data['color'] = sector_data['end_use'] + ' - ' + sector_data['sector']
    sector_data['line_dash'] = sector_data['fuel']

    #plot
    fig = px.line(sector_data, x='year', y='value', color='color', facet_col='economy', facet_col_wrap=3, title=f'{sector} - energy use by end use', line_dash='line_dash')
        
    WRITE_HTML = False
    if WRITE_HTML:
        #save to file
        fig.write_html(f'plotting_output/analysis/buildings/{sector}_by_economy_energy_use_by_end_use.html')
    ##################
    #and then remove the fuel and reclacualte: (by end use, no fuel)
    sector_data = energy[energy['sector'] == sector]
    sector_data = sector_data.groupby(['year', 'economy', 'end_use', 'sector']).sum(numeric_only=True).reset_index()
    #calculate the proportion
    sector_data['value'] = sector_data['value'] / sector_data.groupby(['year', 'economy', 'sector'])['value'].transform('sum')
    fig = px.line(sector_data, x='year', y='value', color='end_use', facet_col='economy', facet_col_wrap=3, title=f'{sector} - energy use by end use')
        
    WRITE_HTML = False
    if WRITE_HTML:
        fig.write_html(f'plotting_output/analysis/buildings/{sector}_by_economy_energy_use_by_end_use_no_fuel.html')
    ##################
    #and then do it by fuel, no end use
    sector_data = energy[energy['sector'] == sector]
    sector_data = sector_data.groupby(['year', 'economy','sector', 'fuel']).sum(numeric_only=True).reset_index()
    sector_data['value'] = sector_data['value'] / sector_data.groupby(['year', 'economy','sector'])['value'].transform('sum')
    fig = px.line(sector_data, x='year', y='value', color='fuel', facet_col='economy', facet_col_wrap=3, title=f'{sector} - energy use by fuel')
        
    WRITE_HTML = False
    if WRITE_HTML:
        fig.write_html(f'plotting_output/analysis/buildings/{sector}_by_economy_energy_use_by_fuel.html')
    ##################
    #and then do some scatter plots which remove the year 
    ##################
    #remove the year and recalcualte:
    sector_data = energy[energy['sector'] == sector]
    sector_data = sector_data.groupby(['year', 'economy', 'end_use', 'sector', 'fuel']).sum(numeric_only=True).reset_index()
    
    #take a look at the scatter plot of energy use by end use, with color as fuel 
    sector_data['value'] = sector_data['value'] / sector_data.groupby(['year', 'economy', 'end_use','sector'])['value'].transform('sum')
    #average out the proportions with no year
    sector_data = sector_data.groupby(['economy', 'end_use', 'sector', 'fuel']).mean(numeric_only=True).reset_index()
    #reclculate the proportion
    sector_data['value'] = sector_data['value'] / sector_data.groupby(['economy', 'end_use','sector'])['value'].transform('sum')
    fig = px.scatter(sector_data, x='end_use', y='value', color='fuel', facet_col='economy', facet_col_wrap=3, title=f'{sector} - energy use by end use', symbol='fuel')
    
    WRITE_HTML = False
    if WRITE_HTML:
        fig.write_html(f'plotting_output/analysis/buildings/{sector}_by_economy_energy_use_by_end_use_scatter.html')
    ##################
    #take a look at the scatter plot of energy use by fuel, with color as end use
    sector_data = energy[energy['sector'] == sector]
    sector_data = sector_data.groupby(['year', 'economy', 'end_use', 'sector', 'fuel']).sum(numeric_only=True).reset_index()
    
    #take a look at the scatter plot of energy use by end use, with color as fuel 
    sector_data['value'] = sector_data['value'] / sector_data.groupby(['year', 'economy', 'fuel','sector'])['value'].transform('sum')
    #average out the proportions with no year
    sector_data = sector_data.groupby(['economy', 'end_use', 'sector', 'fuel']).mean(numeric_only=True).reset_index()
    #reclculate the proportion
    sector_data['value'] = sector_data['value'] / sector_data.groupby(['economy', 'fuel','sector'])['value'].transform('sum')
    fig = px.scatter(sector_data, x='fuel', y='value', color='end_use', facet_col='economy', facet_col_wrap=3, title=f'{sector} - energy use by fuel', symbol='end_use')
        
    WRITE_HTML = False
    if WRITE_HTML:
        fig.write_html(f'plotting_output/analysis/buildings/{sector}_by_economy_energy_use_by_fuel_scatter.html')
    
#%%
#at the end of the day its not that interesting to see the energy use since we kind of expect it. more useful as a modelling input:












#%%
#ok lets see if we can calcualte the proportion of energy used for each end use
# as well as the proportion of end use by technology
# and proportion of technology by fuel type
#PROPORTIONS:
energy['value'] = energy['value'] / energy.groupby(['year', 'economy', 'sector', 'fuel'])['value'].transform('sum')
energy['value'] = energy['value'] / energy.groupby(['year', 'economy', 'sector'])['value'].transform('sum')
energy['value'] = energy['value'] / energy.groupby(['year', 'economy'])['value'].transform('sum')


#%%