#intention
#1. import the data
#2. clean the data
#3. filter out unneeded data

#this first part of the script will just be about epxlporation of the data
#load datra IN 
#%%

from matplotlib.pyplot import title
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import plotly.express as px
pd.options.plotting.backend = "plotly"#set pandas backend to plotly plotting instead of matplotlib
import plotly.io as pio
pio.renderers.default = "browser"#allow plotting of graphs in the interactive notebook in vscode #or set to notebook
import plotly.graph_objects as go
import plotly
import itertools
import datetime
import pickle
#set cwd to the root of the project
import re
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
import sys
folder_path = './aggregation_code'  # Replace with the actual path of the folder you want to add
sys.path.append(folder_path)
import utility_functions 

PRINT_GRAPHS_AND_STATS = False

#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/ATO/', 'ATO_extracted_data_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
#%%
#first, to skip some of the data cleaning, see if there is a pickle file for the FILE_DATE_ID available, if so we can use that:
try:
    with open('intermediate_data/ATO/ATO_data_cleaned_'+FILE_DATE_ID+'.pickle', 'rb') as handle:
        ATO_data = pickle.load(handle)
        pickle_file_available = True
    print('loaded pickle file for ATO_data_cleaned_'+FILE_DATE_ID+'.pickle')
except FileNotFoundError:
    print('no pickle file found for ATO_data_cleaned_'+FILE_DATE_ID+'.pickle')
    pickle_file_available = False

    #load in the data
    ATO_data = pd.read_csv('intermediate_data/ATO/ATO_extracted_data_'+FILE_DATE_ID+'.csv', engine="python")#load with python so as avoid warning about mixed dtypes

country_codes = pd.read_csv('config/economy_code_to_name.csv') 

#%%
if PRINT_GRAPHS_AND_STATS:
    #print basic details like the col nanmes, unique values in some cols
    print(ATO_data.columns)
    ATO_data.head()


#%%
if not pickle_file_available:
    #DEALING WITH VALUE COLUMN
    #there are some values in vlaue col that have weird formatting. eg. commas or \t. Remove and convert to float
    ATO_data_copy = ATO_data.copy()
    strings_to_remove = ['\t',' ','n/a','na','n.a.', '-',',',  '_']#, '55.0g']#'...',
    for string in strings_to_remove:
        ATO_data['value'] = ATO_data['value'].str.replace(string, '')
    #replace '' with nan
    ATO_data['value'] = ATO_data['value'].replace('', np.nan)
    #and then if any match these exactly then set them to nan
    strings_to_remove = ['.','...','55.0g', 'N/Appl.']#somehow '...' removes long numbers unless you remove ewxactly that
    for string in strings_to_remove:
        ATO_data['value'] = ATO_data['value'].replace(string, np.nan)

    #remove these sheets since its too hard to utilise their categorical data just yet
    sheets_to_remove = ['MIS-SUM-006',
    'MIS-SUM-007',
    'MIS-SUM-008',
    'MIS-SUM-005',
    'MIS-SUM-004',
    'SEC-SEG-009',
    'MIS-SUM-003',
    'MIS-SUM-002']
    #if there are still any values cannot be converted to float, show the user the rows that have them
    unique_sheets = set()
    for row in ATO_data.itertuples():
        value = row.value
        try:
            float(value)
        except ValueError:
            if row.Sheet not in sheets_to_remove:
                print(value, row.Sheet)
                unique_sheets = unique_sheets.union({row.Sheet})
            else:
                ATO_data = ATO_data.loc[ATO_data['Sheet'] != row.Sheet]
    #make the value column a float
    ATO_data['value'] = ATO_data['value'].astype(float)

    #save as pickle file
    with open('intermediate_data/ATO/ATO_data_cleaned_'+FILE_DATE_ID+'.pickle', 'wb') as handle:
        pickle.dump(ATO_data, handle, protocol=pickle.HIGHEST_PROTOCOL)
else:
    print('skipping some data cleaning as pickle file available')
#%%
#COPY THE DATAFRAME
# ATO_data = ATO_data_copy.copy()
#filter onlyt for data in sheet = CLC-VRE-013
# ATO_data = ATO_data.loc[ATO_data['Sheet'] == 'CLC-VRE-013']

#%%
#extract only APEC economys and map to the country codes df
ATO_data = ATO_data.merge(country_codes[['Economy', 'Economy_name', 'iso_code']], how='right', right_on='iso_code', left_on='Economy Code')

#%%
#a little bit of analysis:
# ATO_data.columns
# Index(['Economy Code', 'Economy Name', 'year', 'value', 'extra_col_name',
#        'Indicator:', 'Indicator ATO Code:', 'Scope:', 'Mode:', 'Sector:',
#        'Units:', 'Website:', 'Sheet', 'Remarks', 'Source (2022-04)',
#        'Source (2021-10)', 'Reference date (dd/mm/yyyy)', 'Notes',
#        'Local Currency', 'Economy', 'Economy_name', 'iso_code','Subcategory'],
#       dtype='object')


##############################################
##############################################
#%%
#rename columns and vlaues within columns where it is clear what they are
ATO_data = ATO_data.rename(columns={'Mode:':'medium', 'Sector:':'Transport Type', 'Indicator:':'measure', 'Source:': 'source', 'Units:':'unit'})
#decapiatlise the other columns names
ATO_data.columns = ATO_data.columns.str.lower()
#remove ':' from the end of the column names
ATO_data.columns = ATO_data.columns.str.replace(':','')

##############################################
##############################################
#%%
#bit more analysis:
# ATO_data.medium.unique()
# array(['All Modes', 'Road', 'Aviation', 'Shipping/Waterways/Navigation',
#        'Rail', nan, 'Rail, Road'], dtype=object)
# ATO_data.Transport Type.unique()
# array(['Passenger & Freight', 'Freight', 'Passenger', nan], dtype=object)
# ATO_data.measure.unique()
# #ABOVE output is too long to show here
# ATO_data.subcategory.unique()
# array(['Total Transport Infrastructure (TTI)',
#        'Urban Transport Infrastructure (UTI)',
#        'Vehicle Manufacture (VMF)', 'ICT Infrastructure (ICT)',
#        'Alternative Fuel Production (AFP)',
#        'Conventional Fuel Producing Infrastructure (CFP)',
#        'Passenger Activity  Transit (PAT)',
#        'Passenger Activity General (PAG)', 'Freight Activity (FRA)',
#        'Vehicle Parc (VEP)',
#        'Shared & Innovative Mobility Services (SIM)',
#        'Transport Activity and Services General (TSG)',
#        'Rural Access (RAC)', 'Urban Access (UAC)',
#        'National and Regional Connectivity (NRC)',
#        'Access and Connectivity General  (ACG)',
#        'Road Safety incidents (RSI)',
#        'Safety Rating Infrastructure (SRI)',
#        'Ambient Air Pollution (AAP)',
#        'Vehicle related Air Pollution (VAP)',
#        'Health Aspects Transport (HAT)',
#        'Vehicle Related Energy and GHG Emissions (VRE)',
#        'Climate Vulnerability Transport (CVT)',
#        'Climate Change General (CCG)', 'Demographic Variables (DEV)',
#        'Household Variables (HOV)', 'Transport investments (TIV)',
#        'Transport Financial Instruments (TFI)',
#        'Socio Economic General (SEG)',
#        'Transport Related Employment (TRE)',
#        'General Rest Category (GRC)', 'COVID (COV)', 'SUM4all (SUM)', nan],
#       dtype=object)

##############################################
##############################################
#%%
#Sort through the different measures and see what we can do with them. It will be easier to do this by using the Subcategory col
most_useful_subcats = ['Vehicle Manufacture (VMF)', 'Passenger Activity  Transit (PAT)','Passenger Activity General (PAG)', 'Freight Activity (FRA)','Vehicle Parc (VEP)','Vehicle Related Energy and GHG Emissions (VRE)', 'COVID (COV)']
ATO_data = ATO_data[ATO_data.subcategory.isin(most_useful_subcats)]

##############################################
##############################################
#%%
#bit more analysis:
#note some of the measures below are discontinued
# ATO_data.measure.unique()
# array(['LDV Production', 'Bus Vehicle Production',
#        'Minerals Country Availability - cobalt, lithium, graphite, manganese',
#        'Ships built by country of building, annual',
#        'Heavy Truck Production', 'Total Vehicle Production',
#        'Passengers Kilometer Travel - Railways',
#        'Passengers Kilometer Travel - Waterways/shipping',#DISCONTINUED2024
#        'Passengers Kilometer Travel - Domestic Aviation',#DISCONTINUED2024
#        'Passengers Kilometer Travel - Bus',#DISCONTINUED2024
#        'Passengers Kilometer Travel - HSR',#DISCONTINUED2024
#    'Air transport, carrier departures',#DISCONTINUED2024
#    'Aviation International Passenger Kilometers',#DISCONTINUED2024
#    'Aviation Trips per capita ',#DISCONTINUED2024
#    'Aviation Trips per capita  -2030 Forecast (BAU)',#DISCONTINUED2024
#    'Aviation Total Passenger Kilometers',#DISCONTINUED2024
#    'Total Passenger Kilometer Travel (Domestic+International)',
#    'Total Passenger Kilometer Travel/Capita (Domestic+International)',
#    'Total Passenger Kilometer Travel/GDP (Domestic+International)',
#    'Total Passenger Kilometer Travel Mode Share (Domestic+International)',
#    'Passengers Kilometer Travel - Roads',
#    'Land Transport Passenger Kilometers Travel',
#    'Freight Transport - Tonne-km (Total) (Domestic+International)',
#    'Freight tonne-km/GDP (Domestic+International)',
#    'Freight tonne-km/capita (Domestic+International)',
#    'Total Freight Kilometer Travel Mode Share (Domestic+International)',
#    'Freight Transport - Tonne-km for Roads',
#    'Freight Transport - Tonne-km for Railways',
#    'Freight Transport - Tonne-km for Waterways/shipping (Domestic)',
#    'Freight Transport - Tonne-km for Waterways/shipping (Domestic+International)',
#    'Freight Transport - Tonne-km for Aviation (Domestic)',
#    'Freight Transport - Tonne-km for Aviation (Domestic+International)',
#    'Port call and performance statistics',
#    'Container port traffic (TEU)',
#    'Land Transport Freight Kilometers Travel',
#    'Motorised Two Wheeler Sales', 
# 'Motorised Three Wheeler Sales',#DISCONTINUED2024
#    'LDV Sales', 'Total Vehicle sales (motorised)',
#    'Commercial Vehicle Sales (motorised)',
#    'Vehicle registration  (Motorised 2W)',
#    'Vehicle registration  (Motorised 3W)',
#    'Vehicle registration  (LDV)', 'Vehicle registration  (Bus)',
#    'Vehicle registration (Others)', 'Freight Vehicle registration',
#    'Total Vehicle Registration',
#    'Merchant fleet by country of beneficial ownership, annual',
#    'Motorisation Index', 'LDV Motorisation Index',
#    'Two and Three Wheelers Motorisation Index',
#    'Bus Motorisation Index', 'Freight Vehicles Motorisation Index',
#    'Total Transport Energy Consumption', 'Electricity Transport',
#    'Electricity Road', 'Electricity Rail', 'Gasoline Road',
#    'Diesel Road', 'Diesel Rail', 'Bio-Diesel Road',
#    'Bio-Gasoline Road', 'Natural Gas Road',
#    'Transport Energy Consumption Mode share',
#    'Transport - Final consumption of renewable energy (PJ)',
#    'Electricity share in total transport energy consumption in Transport',
#    'Share of Biofuels in Transport Energy Consumption',
#    'Road Energy Consumption', 'Railway Energy Consumption',
#    'Domestic Navigation Energy Consumption',
#    'Domestic Aviation Energy Consumption',
#    'Fossil Transport CO2 emissions',
#    'Fossil Transport share in consumption emissions',
#    'Fossil Transport share in Territorial emissions',
#    'Total Transport CO2 emissions',
#    'Transport CO2 Emissions per Capita',
#    'Transport CO2 Emissions by GDP',
#    'Transport CO2 Emissions - Mode share',
#    'Road in Transport CO2 Emissions',
#    'Railways in Transport CO2 Emissions',
#    'Shipping/Inland Waterways Transport CO2 Emissions',
#    'Domestic Aviation Transport CO2 Emissions',
#    'Share of Transport CO2 Emissions in Total Fuel Combustion Energy CO2 Emissions'],
#   dtype=object)
#%%
#we can now categorise a lot of these so their measures are the same. For example, we can combine all the passenger km measures into one, same as: freight km, vehicle registration, vehicle sales, energy use, co2 emissions, etc.
#we will create lists of the measures we want to combine and put them into a dictionary
dict_of_measures = {}
dict_of_measures['passenger_km'] = ['Total Passenger Kilometer Travel (Domestic+International)', 'Passengers Kilometer Travel - Roads', 'Land Transport Passenger Kilometers Travel']
dict_of_measures['freight_tonne_km'] = ['Freight Transport - Tonne-km (Total) (Domestic+International)',  'Freight Transport - Tonne-km for Roads', 'Freight Transport - Tonne-km for Railways', 'Freight Transport - Tonne-km for Waterways/shipping (Domestic)', 'Freight Transport - Tonne-km for Waterways/shipping (Domestic+International)', 'Freight Transport - Tonne-km for Aviation (Domestic)', 'Freight Transport - Tonne-km for Aviation (Domestic+International)', 'Land Transport Freight Kilometers Travel']
dict_of_measures['vehicle_registration'] = ['Vehicle registration (Motorised 2W)', 'Vehicle registration (Motorised 3W)', 'Vehicle registration (LDV)', 'Vehicle registration (Bus)', 'Vehicle registration (Others)', 'Freight Vehicle registration', 'Total Vehicle Registration','Vehicle registration - Electric 4-wheelers',
'Vehicle registration - Electric 2/3-wheelers',
'Vehicle registration - Electric Buses',
'Vehicle registration - Electric Trucks',
'Vehicle registration - Electric Total', 'Rail diesel locomotives', 'Rail electric locomotives',
'Rail diesel/electric multiple units', 'Rail passenger carriages',
'Rail freight wagons']#do these need to be keps as is for some cases?#TODO eg 2/3 wheelers
dict_of_measures['Sales'] = ['Motorised Two Wheeler Sales',  'LDV Sales', 'Total Vehicle sales (motorised)', 'Commercial Vehicle Sales (motorised)']
dict_of_measures['Energy'] = ['Total Transport Energy Consumption', 'Electricity Transport', 'Electricity Road', 'Electricity Rail', 'Gasoline Road', 'Diesel Road', 'Diesel Rail', 'Bio-Diesel Road', 'Bio-Gasoline Road', 'Natural Gas Road', 'Road Energy Consumption', 'Railway Energy Consumption', 'Domestic Navigation Energy Consumption', 'Domestic Aviation Energy Consumption','Energy consumption (Total)','Biofuel consumption (Total)']
dict_of_measures['co2_emissions'] = ['Fossil Transport CO2 emissions',  'Total Transport CO2 emissions',  'Road in Transport CO2 Emissions', 'Railways in Transport CO2 Emissions', 'Shipping/Inland Waterways Transport CO2 Emissions', 'Domestic Aviation Transport CO2 Emissions']

# dict_of_measures['others'] = []
dict_of_measures['freight_tonnes'] = ['Freight tonnes transported - Total', 'Freight tonnes transported - Shipping', 'Freight tonnes transported - Aviation',
'Freight tonnes transported - Railways', 'Freight tonnes transported - Road','Freight tonnes transported - Total']


measures_to_keep_as_original = ['Trade - LDV', 'Trade - Bus', 'Trade - Goods vehicle', 'Trade - Motorized 2/3-wheeler', 'Trade - Bicycle', 'Trade - Other vehicles', 'Trade - Vehicle manufacturing', 'Trade - Road vehicle total', 'Trade - Electric LDV (including hybrid-electric)', 'Trade - Electric bus (including hybrid-electric)', 'Trade - Electric goods vehicle (including hybrid-electric)', 'Trade - Electric 2/3-wheeler (including hybrid-electric)', 'Trade - Electric road vehicle total (including hybrid-electric)', 'Trade - Hybrid-electric LDV', 'Trade - Hybrid-electric bus', 'Trade - Hybrid-electric goods vehicle', 'Trade - Hybrid-electric road vehicle total', 'Trade - Rail total', 'Trade - Rail locomotive', 'Trade - Rail diesel/electric multiple unit', 'Trade - Rail passenger carriage', 'Trade - Rail freight wagon', 'Trade - Navigation total', 'Trade - Aviation total', 
#covid
'COVID Google Mobility Data - Grocery and pharmacy', 'COVID Google Mobility Data - Retail and recreation', 'COVID Google Mobility Data - Parks', 'COVID Google Mobility Data - Public transport stations', 'COVID Google Mobility Data - Workplaces', 'COVID Google Mobility Data - Residential', 'COVID Apple Mobility Data - Driving', 'COVID Apple Mobility Data - Walking', 'COVID Apple Mobility Data - Public transport', 'COVID Restrictions - Public Transport', 'COVID Restrictions - Domestic Transport', 'COVID Restrictions - International Transport',
#others:
'Freight tonne-km/capita (Domestic+International)','Freight tonne-km/GDP (Domestic+International)', 'Port call and performance statistics', 'Container port traffic (TEU)', 'Merchant fleet by country of beneficial ownership, annual', 'Motorisation Index', 'LDV Motorisation Index', 'Two and Three Wheelers Motorisation Index', 'Bus Motorisation Index', 'Freight Vehicles Motorisation Index', 'Transport Energy Consumption Mode share', 'Transport - Final consumption of renewable energy (PJ)', 'Electricity share in total transport energy consumption in Transport', 'Share of Biofuels in Transport Energy Consumption', 'Fossil Transport share in consumption emissions', 'Fossil Transport share in Territorial emissions', 'Transport CO2 Emissions per Capita', 'Transport CO2 Emissions by GDP', 'Transport CO2 Emissions - Mode share', 'Share of Transport CO2 Emissions in Total Fuel Combustion Energy CO2 Emissions','LDV Production', 'Bus Vehicle Production','Minerals Country Availability - cobalt, lithium, graphite, manganese','Ships built by country of building, annual','Heavy Truck Production','Total Vehicle Production','Total Passenger Kilometer Travel/Capita (Domestic+International)','Total Passenger Kilometer Travel/GDP (Domestic+International)','Total Passenger Kilometer Travel Mode Share (Domestic+International)','Total Freight Kilometer Travel Mode Share (Domestic+International)', 'Energy production (Total)','Biofuel production (Total)','Operational bus fleet']

#quick flag to keep track of measures we consider as 'other' (ones that we will seaparate before doing finer formatting, since we dont know if they are useful yet)
ATO_data['other'] = False
for measure in measures_to_keep_as_original:
    ATO_data.loc[ATO_data['measure'] == measure, 'other'] = True 
    
#check that we have all the measures in our dict and also that we have none extra
all_measures = measures_to_keep_as_original.copy()
missing_measures = []
extra_measures = []
for key in dict_of_measures:
    all_measures.extend(dict_of_measures[key])
for measure in ATO_data['measure'].unique():
    if measure not in all_measures:
        print(measure + ' not in dict')
        missing_measures.append(measure)
for measure in all_measures:
    if measure not in ATO_data['measure'].unique():
        print(measure + ' not in ATO_data')
        extra_measures.append(measure)
if len(missing_measures) > 0 or len(extra_measures) > 0:
    raise ValueError('missing or extra measures: /n missing: {} /n extra: {}'.format(missing_measures, extra_measures))
        
#%%
#create an extra column in the ATO_data df that will be the measures original_measure and then cahnge measure to the new measure
ATO_data['original_measure'] = ATO_data['measure']
for key in dict_of_measures:
    ATO_data.loc[ATO_data['measure'].isin(dict_of_measures[key]), 'measure'] = key

##############################################
##############################################
#%%
#Quick fix because of a double up in the data
#change the measure for original_measure == 'Aviation Total Passenger Kilometers' and units == 'Million passenger kilometers' to 'passenger_km'
ATO_data.loc[(ATO_data['original_measure'] == 'Aviation Total Passenger Kilometers') & (ATO_data['unit'] == 'Million passenger kilometers'), 'measure'] = 'passenger_km'

##############################################
##############################################
#%%
#now take a quick look at the data in the other cols for each measure
# for measure in ATO_data['measure'].unique():
#     print('\n\n' + 'Looking at unique data for ' + measure + ':\n')
#     temp_data = ATO_data[ATO_data['measure'] == measure]
#     print('Unique mediums: {}'.format(temp_data['medium'].unique()))
#     print('Unique scope: {}'.format(temp_data['scope'].unique()))
#     print('Unique units: {}'.format(temp_data['units'].unique()))
#     print('Unique Transport Type: {}'.format(temp_data['Transport Type'].unique()))
#%%
#and take a look at the original measures and Transport Type and medium for each unqiue value of scope
# for scope in ATO_data['scope'].unique():
#     print('\n\n' + 'Looking at unique data for ' + scope + ':\n')
#     temp_data = ATO_data[ATO_data['scope'] == scope]
#     print('Unique mediums: {}'.format(temp_data['medium'].unique()))
#     print('Unique Transport Type: {}'.format(temp_data['Transport Type'].unique()))
#     print('Unique original_measure: {}'.format(temp_data['original_measure'].unique()))


#%%
# #lets set the medium to have '_international' appended to it if the scope is 'National, International' or 'Regional'
# ATO_data.loc[ATO_data['scope'].isin(['National, International', 'Regional']), 'medium'] = ATO_data.loc[ATO_data['scope'].isin(['National, International', 'Regional']), 'medium'] + '_international'

#%%

##############################################
##############################################

#%%
#sort out the measures that have different units
#to make all the following more simple, jsut decapitalise all the units values
ATO_data['unit'] = ATO_data['unit'].str.lower()
#Now, 1. where a unit contains a magnitude such as 'million' or 'billion' we can just multiply the value by the magnitude to get the correct value and then remove the magnitude from the unit
magnitudes = {'million': 10**6, 'billion': 10**9, 'thousand': 10**3}
for magnitude in magnitudes.keys():
    ATO_data.loc[ATO_data['unit'].str.contains(magnitude), 'value'] = ATO_data.loc[ATO_data['unit'].str.contains(magnitude), 'value'] * magnitudes[magnitude]
    ATO_data.loc[ATO_data['unit'].str.contains(magnitude), 'unit'] = ATO_data.loc[ATO_data['unit'].str.contains(magnitude), 'unit'].str.replace(magnitude, '')
#make sure there are no spaces at the end or start of the unit
ATO_data['unit'] = ATO_data['unit'].str.strip()
#remove any double spaces
ATO_data['unit'] = ATO_data['unit'].str.replace('  ', ' ')
#remove any commas
ATO_data['unit'] = ATO_data['unit'].str.replace(',', '')

#and if any unit are in 'tj', convert to 'pj' and times vlaue by 1000
ATO_data.loc[ATO_data['unit'] == 'tj', 'value'] = ATO_data.loc[ATO_data['unit'] == 'tj', 'value'] / 1000
ATO_data.loc[ATO_data['unit'] == 'tj', 'unit'] = 'pj'

#QUICK FIX
#where unit is kilowatt-hours and the original_measure is Electricity Rail  then remove that data because it doesnt seem to convert to TJ properly (we have a sheet with all the same data but in TJ, and the values are much differrent)
ATO_data = ATO_data[~((ATO_data['unit'] == 'kilowatt-hours') & (ATO_data['original_measure'] == 'Electricity Rail'))]

#also where unit are 'kilowatt-hours' then times by 3.6e-9 to get to 'pj' #please note that in the quick fix above we identified that some kwh values werent converting as expected so this problem might persist below too
ATO_data.loc[ATO_data['unit'] == 'kilowatt-hours', 'value'] = ATO_data.loc[ATO_data['unit'] == 'kilowatt-hours', 'value'] * 3.6e-9
ATO_data.loc[ATO_data['unit'] == 'kilowatt-hours', 'unit'] = 'pj'

#similarly for gwh
ATO_data.loc[ATO_data['unit'] == 'gwh', 'value'] = ATO_data.loc[ATO_data['unit'] == 'gwh', 'value'] * 3.6e-6
ATO_data.loc[ATO_data['unit'] == 'gwh', 'unit'] = 'pj'

#and convert metric_tons_of_oil_equivalent (just onnes in enlgand) to PJ
ATO_data.loc[ATO_data['unit'] == 'metric tons of oil equivalent', 'value'] = ATO_data.loc[ATO_data['unit'] == 'metric tons of oil equivalent', 'value'] * (41.868*1e-6) #1 toe is 41.868 GJ or 41.868e-6 PJ
ATO_data.loc[ATO_data['unit'] == 'metric tons of oil equivalent', 'unit'] = 'pj'

#and convert ktoe to PJ # 1 ktoe is 1000 toe. 
ATO_data.loc[ATO_data['unit'] == 'ktoe', 'value'] = ATO_data.loc[ATO_data['unit'] == 'ktoe', 'value'] * (41.868*1e-6)*1000
#shouldnt it be (41*1000)/1e6? which is 0.041868
ATO_data.loc[ATO_data['unit'] == 'ktoe', 'unit'] = 'pj'


#there is some data where measre is energy_use thats measured in unit= tonnes. For now, jsut convert the measure to 'energy_use_tonnes'
ATO_data.loc[(ATO_data['measure'] == 'Energy') & (ATO_data['unit'] == 'tonnes'), 'measure'] = 'energy_use_tonnes'

#and convert co2_emissions where the unit are tonnes to mtco2e. This involves just diving by 10**6 to convert to million tonnes (please note that Mtc02e is not metric tonnes!)
ATO_data.loc[ATO_data['unit'] == 'tonnes', 'value'] = ATO_data.loc[ATO_data['unit'] == 'tonnes', 'value'] * 10**-6
ATO_data.loc[(ATO_data['unit'] == 'tonnes') & (ATO_data['measure'] == 'co2_emissions'), 'unit'] = 'MtC02e'
#also convert any thing that is mtco2e to Mtc02e
ATO_data.loc[ATO_data['unit'] == 'mtco2e', 'unit'] = 'MtC02e'
#%%
############################################################################################################################################################################


#THIS NEXT PART IS A BIT MESSY AND SPECIFIC. We will clean up particular instances of the data, so it will be a bit long. 
#deal with the extra_col_name col
ATO_data = ATO_data.rename(columns={'extra_col_name': 'extra_detail'})
look_at_original_measure = True
if look_at_original_measure:
    #first keep the only extra_col_name and original_measure and then look at unique rows there

    x = ATO_data[['extra_detail','original_measure']].drop_duplicates().dropna()
    #set any na's to 'na'
    x['extra_detail'] = x['extra_detail'].fillna('na')
    #now fro each unique original_measure, create a list of the unique extra_col_name values and drop the %% characters

    dict_of_extra_col_names = {}
    for original_measure in x['original_measure'].unique():
        dict_of_extra_col_names[original_measure] = x[x['original_measure'] == original_measure]['extra_detail'].unique()
        for i in range(len(dict_of_extra_col_names[original_measure])):
            dict_of_extra_col_names[original_measure][i] = dict_of_extra_col_names[original_measure][i].replace('%%', '')

    #make it into a dataframe
    df = pd.DataFrame.from_dict(dict_of_extra_col_names, orient='index')
    #remove any rows where col 0 is Series and col 1 is nan
    df = df.drop(df[(df[0] == 'Series') & (df[1].isna())].index)
#%%
#what we learned from above is that we should just remove the %%'s from the extra_detail col, then set any Series values to nan. Then rename the col to 'extra_detail'
ATO_data['extra_detail'] = ATO_data['extra_detail'].str.replace('%%', '')
ATO_data.loc[ATO_data['extra_detail'] == 'Series', 'extra_detail'] = np.nan

#%%
#now we have some more info that can be used!

##############################################
##############################################
#%%
#Make a few fixes to the data before we can start analysing it 

#first, it seems that Total Transport Energy Consumption and some other measures have extra_detail values that are more specific than the medium values. So lets double check that and then switch the extra_detail values with medium values
temp_list = ['Total Transport Energy Consumption', 'Total Passenger Kilometer Travel Mode Share (Domestic+International)', 'Total Freight Kilometer Travel Mode Share (Domestic+International)', 'Transport Energy Consumption Mode share', 'Transport CO2 Emissions - Mode share','Freight tonnes transported - Total']#these are basically any where the extra detail contains a mode type and the medium is All Modes
#double check medium is still == All Modes  
if (ATO_data[ATO_data['original_measure'].isin(temp_list)]['medium'] != 'All Modes').all():
    print('medium is not All Modes where original_measure is Total Transport Energy Consumption')
else:
    #replace the medium values with the extra_detail values where they arent na and then set the extra_detail values to nan
    ATO_data.loc[(ATO_data['original_measure'].isin(temp_list))&(ATO_data['extra_detail'].notna()), 'medium'] = ATO_data.loc[(ATO_data['original_measure'].isin(temp_list))&(ATO_data['extra_detail'].notna()), 'extra_detail']
    ATO_data.loc[ATO_data['original_measure'].isin(temp_list), 'extra_detail'] = np.nan
    # #in addition, for these measures, where medium now = Transport total, change it to All Modes
    ATO_data.loc[(ATO_data['original_measure'].isin(temp_list)) & (ATO_data['medium'] == 'Transport total'), 'medium'] = 'All Modes'

##############################################
##############################################
#%%
#also in same vein these measures below have actual measures in the extra_detail col, so lets switch them around
temp_list = ['Port call and performance statistics','Merchant fleet by country of beneficial ownership, annual']
#replace the original_measure values with the extra_detail values and then set the extra_detail values to the original_measure values
for i in range(len(temp_list)):
    #get the index of the rows with those original_measure values
    temp_indexes = ATO_data[ATO_data['original_measure'] == temp_list[i]].index
    #replace the original_measure values with the extra_detail values
    ATO_data.loc[temp_indexes, 'original_measure'] = ATO_data.loc[temp_indexes, 'extra_detail']
    #replace the extra_detail values with the original_measure values
    ATO_data.loc[temp_indexes, 'extra_detail'] = temp_list[i]
    
##############################################
##############################################
#%%
#then some trade values have export and import in extra_detail, so lets add them to the measure name:
temp_list = ['Freight tonnes transported - Shipping',
       'Freight tonnes transported - Aviation', 'Trade - LDV',
       'Trade - Bus', 'Trade - Goods vehicle',
       'Trade - Motorized 2/3-wheeler', 'Trade - Bicycle',
       'Trade - Other vehicles', 'Trade - Vehicle manufacturing',
       'Trade - Road vehicle total',
       'Trade - Electric LDV (including hybrid-electric)',
       'Trade - Electric bus (including hybrid-electric)',
       'Trade - Electric goods vehicle (including hybrid-electric)',
       'Trade - Electric 2/3-wheeler (including hybrid-electric)',
       'Trade - Electric road vehicle total (including hybrid-electric)',
       'Trade - Hybrid-electric LDV', 'Trade - Hybrid-electric bus',
       'Trade - Hybrid-electric goods vehicle',
       'Trade - Hybrid-electric road vehicle total', 'Trade - Rail total',
       'Trade - Rail locomotive',
       'Trade - Rail diesel/electric multiple unit',
       'Trade - Rail passenger carriage', 'Trade - Rail freight wagon',
       'Trade - Navigation total', 'Trade - Aviation total']
extra_details_to_add = ['Export', 'Import']
for measure in temp_list:
    # grab rows where the measure is in temp_list and the extra_detail is in extra_details_to_add
    temp_indexes = ATO_data[(ATO_data['original_measure'] == measure) & (ATO_data['extra_detail'].isin(extra_details_to_add))].index
    #add the original_measure values with the extra_detail values and then set the extra_detail values to na
    ATO_data.loc[temp_indexes, 'original_measure'] = ATO_data.loc[temp_indexes, 'original_measure'] + ' - ' + ATO_data.loc[temp_indexes, 'extra_detail']
    ATO_data.loc[temp_indexes, 'extra_detail'] = np.nan
    
##############################################
##############################################
    
#%%
#Looks like teh rest of the values in extras detal are fuels. Let's move them to a fuel col
#if extra_detail is in ['Oil Products', 'Natural Gas', 'Biofuels and Waste', 'Electricity', 'Renewables (of which)', 'Primary Coal and Peat'] then move it to a fuel col and set extra_detail to nan
ATO_data.loc[ATO_data['extra_detail'].isin(['Oil Products', 'Natural Gas', 'Biofuels and Waste', 'Electricity', 'Renewables (of which)', 'Primary Coal and Peat', 'Nuclear, renewable, and others']), 'fuel_type'] = ATO_data.loc[ATO_data['extra_detail'].isin(['Oil Products', 'Natural Gas', 'Biofuels and Waste', 'Electricity', 'Renewables (of which)', 'Primary Coal and Peat', 'Nuclear, renewable, and others']), 'extra_detail']
ATO_data.loc[ATO_data['extra_detail'].isin(['Oil Products', 'Natural Gas', 'Biofuels and Waste', 'Electricity', 'Renewables (of which)', 'Primary Coal and Peat', 'Nuclear, renewable, and others']), 'extra_detail'] = np.nan
#also if extra detail is 'Total' just set it to nan
ATO_data.loc[ATO_data['extra_detail'] == 'Total', 'extra_detail'] = np.nan


##############################################
##############################################
#%%
#In the COVID data we also have the data in months so we will create a month col and add the month to that where the year col is YYYY-mm
ATO_data['month'] = np.nan
#convert year to string
ATO_data['year'] = ATO_data['year'].astype(str)
#find the indexes where the year col is YYYY-mm
temp_indexes = ATO_data[ATO_data['year'].str.contains('-')].index
#add the month to the month col
ATO_data.loc[temp_indexes, 'month'] = ATO_data.loc[temp_indexes, 'year'].str.split('-').str[1]
#remove the month from the year col
ATO_data.loc[temp_indexes, 'year'] = ATO_data.loc[temp_indexes, 'year'].str.split('-').str[0]

##############################################
##############################################
#%%
#we can rename medium from Domestic aviation or 'Domestic navigation' to Aviation or Shipping/Waterways/Navigation.
ATO_data = ATO_data.replace({'medium': {'Domestic aviation': 'Aviation', 'Domestic navigation': 'Shipping/Waterways/Navigation'}})

##############################################
##############################################
#%%
#There are a lot of values in original_measures that contain data about the vehicle type and/or fuel AND DRIVE. We can extract this data and put it in a new col called vehicle_type/fuel/DRIVE:
#Please note that a lot of thsi was done using ai copilot, so it uses a bit of copy pasting where a usual human might find a better way to do it!
# ATO_data['original_measure'].unique()
#first we will handle the vehicle type:
# array(['Passengers Kilometer Travel - Railways',
#        'Passengers Kilometer Travel - Waterways/shipping',
#        'Passengers Kilometer Travel - Domestic Aviation',
#        'Passengers Kilometer Travel - Bus',
#        'Passengers Kilometer Travel - HSR',
#        'Aviation International Passenger Kilometers',
#        'Aviation Total Passenger Kilometers',
#        'Total Passenger Kilometer Travel (Domestic+International)',
#        'Passengers Kilometer Travel - Roads',
#        'Land Transport Passenger Kilometers Travel',
#        'Freight Transport - Tonne-km (Total) (Domestic+International)',
#        'Freight tonne-km/GDP (Domestic+International)',
#        'Freight tonne-km/capita (Domestic+International)',
#        'Freight Transport - Tonne-km for Roads',
#        'Freight Transport - Tonne-km for Railways',
#        'Freight Transport - Tonne-km for Waterways/shipping (Domestic)',
#        'Freight Transport - Tonne-km for Waterways/shipping (Domestic+International)',
#        'Freight Transport - Tonne-km for Aviation (Domestic)',
#        'Freight Transport - Tonne-km for Aviation (Domestic+International)',
#        'Land Transport Freight Kilometers Travel',
#        'Motorised Two Wheeler Sales', 'Motorised Three Wheeler Sales',
#        'LDV Sales', 'Total Vehicle sales (motorised)',
#        'Commercial Vehicle Sales (motorised)',
#        'Vehicle registration  (Motorised 2W)',
#        'Vehicle registration  (Motorised 3W)',
#        'Vehicle registration  (LDV)', 'Vehicle registration  (Bus)',
#        'Vehicle registration (Others)', 'Freight Vehicle registration',
#        'Total Vehicle Registration', 'Total Transport Energy Consumption',
#        'Electricity Transport', 'Electricity Road', 'Electricity Rail',
#        'Gasoline Road', 'Diesel Road', 'Diesel Rail', 'Bio-Diesel Road',
#        'Bio-Gasoline Road', 'Natural Gas Road', 'Road Energy Consumption',
#        'Railway Energy Consumption',
#        'Domestic Navigation Energy Consumption',
#        'Domestic Aviation Energy Consumption',
#        'Fossil Transport CO2 emissions', 'Total Transport CO2 emissions',
#        'Road in Transport CO2 Emissions',
#        'Railways in Transport CO2 Emissions',
#        'Shipping/Inland Waterways Transport CO2 Emissions',
#        'Domestic Aviation Transport CO2 Emissions',
#        'COVID Google Mobility Data - Grocery and pharmacy',
#        'COVID Google Mobility Data - Retail and recreation',
#        'COVID Google Mobility Data - Parks',
#        'COVID Google Mobility Data - Public transport stations',
#        'COVID Google Mobility Data - Workplaces',
#        'COVID Google Mobility Data - Residential',
#        'COVID Apple Mobility Data - Driving',
#        'COVID Apple Mobility Data - Walking',
#        'COVID Apple Mobility Data - Public transport',
#        'COVID Restrictions - Public Transport',
#        'COVID Restrictions - Domestic Transport',
#        'COVID Restrictions - International Transport'], dtype=object)

#ADDITION TO THE ABOVE (JUNE 2024 UPDATES)
# ['Freight tonnes transported - Total',
#  'Freight tonnes transported - Road',
#  'Freight tonnes transported - Railways',
#  'Freight tonnes transported - Shipping - Import',
#  'Freight tonnes transported - Shipping - Export',
#  'Freight tonnes transported - Shipping',
#  'Freight tonnes transported - Aviation - Import',
#  'Freight tonnes transported - Aviation - Export',
#  'Freight tonnes transported - Aviation',
#  'Vehicle registration - Electric 4-wheelers',
#  'Vehicle registration - Electric 2/3-wheelers',
#  'Vehicle registration - Electric Buses',
#  'Vehicle registration - Electric Trucks',
#  'Vehicle registration - Electric Total',
#  'Rail diesel locomotives',
#  'Rail electric locomotives',
#  'Rail diesel/electric multiple units',
#  'Rail passenger carriages',
#  'Rail freight wagons',
#  'Operational bus fleet',
#  'Trade - LDV - Import',
#  'Trade - LDV - Export',
#  'Trade - Bus - Import',
#  'Trade - Bus - Export',
#  'Trade - Goods vehicle - Import',
#  'Trade - Goods vehicle - Export',
#  'Trade - Motorized 2/3-wheeler - Import',
#  'Trade - Motorized 2/3-wheeler - Export',
#  'Trade - Bicycle - Import',
#  'Trade - Bicycle - Export',
#  'Trade - Other vehicles - Import',
#  'Trade - Other vehicles - Export',
#  'Trade - Vehicle manufacturing - Import',
#  'Trade - Vehicle manufacturing - Export',
#  'Trade - Road vehicle total - Import',
#  'Trade - Road vehicle total - Export',
#  'Trade - Electric LDV (including hybrid-electric) - Import',
#  'Trade - Electric LDV (including hybrid-electric) - Export',
#  'Trade - Electric bus (including hybrid-electric) - Import',
#  'Trade - Electric bus (including hybrid-electric) - Export',
#  'Trade - Electric goods vehicle (including hybrid-electric) - Import',
#  'Trade - Electric goods vehicle (including hybrid-electric) - Export',
#  'Trade - Electric 2/3-wheeler (including hybrid-electric) - Import',
#  'Trade - Electric 2/3-wheeler (including hybrid-electric) - Export',
#  'Trade - Electric road vehicle total (including hybrid-electric) - Import',
#  'Trade - Electric road vehicle total (including hybrid-electric) - Export',
#  'Trade - Hybrid-electric LDV - Import',
#  'Trade - Hybrid-electric LDV - Export',
#  'Trade - Hybrid-electric bus - Import',
#  'Trade - Hybrid-electric bus - Export',
#  'Trade - Hybrid-electric goods vehicle - Import',
#  'Trade - Hybrid-electric goods vehicle - Export',
#  'Trade - Hybrid-electric road vehicle total - Import',
#  'Trade - Hybrid-electric road vehicle total - Export',
#  'Trade - Rail total - Import',
#  'Trade - Rail total - Export',
#  'Trade - Rail locomotive - Import',
#  'Trade - Rail locomotive - Export',
#  'Trade - Rail diesel/electric multiple unit - Import',
#  'Trade - Rail diesel/electric multiple unit - Export',
#  'Trade - Rail passenger carriage - Import',
#  'Trade - Rail passenger carriage - Export',
#  'Trade - Rail freight wagon - Import',
#  'Trade - Rail freight wagon - Export',
#  'Trade - Navigation total - Import',
#  'Trade - Navigation total - Export',
#  'Trade - Aviation total - Import',
#  'Trade - Aviation total - Export',
#  'Energy production (Total)',
#  'Energy consumption (Total)',
#  'Biofuel production (Total)',
#  'Biofuel consumption (Total)']

#%%
#set sopme drives based on the measure:
measure_to_drive = {
    'diesel_rail':['Rail diesel locomotives'],
    'electric_rail':['Rail electric locomotives'],
    'diesel_electric_rail':['Rail diesel/electric multiple units']}
measure_to_drive['ev'] = ['Vehicle registration - Electric 4-wheelers',
    'Vehicle registration - Electric 2/3-wheelers',
    'Vehicle registration - Electric Buses',
    'Vehicle registration - Electric Trucks',
    'Vehicle registration - Electric Total',
    'Trade - Electric LDV (including hybrid-electric) - Import', 'Trade - Electric LDV (including hybrid-electric) - Export', 'Trade - Electric bus (including hybrid-electric) - Import', 'Trade - Electric bus (including hybrid-electric) - Export', 'Trade - Electric goods vehicle (including hybrid-electric) - Import', 'Trade - Electric goods vehicle (including hybrid-electric) - Export', 'Trade - Electric 2/3-wheeler (including hybrid-electric) - Import', 'Trade - Electric 2/3-wheeler (including hybrid-electric) - Export', 'Trade - Electric road vehicle total (including hybrid-electric) - Import', 'Trade - Electric road vehicle total (including hybrid-electric) - Export',  'Trade - Electric road vehicle total (including hybrid-electric) - Import', 'Trade - Electric road vehicle total (including hybrid-electric) - Export']
measure_to_drive['hybrid'] = ['Trade - Hybrid-electric LDV - Import',
    'Trade - Hybrid-electric LDV - Export',
    'Trade - Hybrid-electric bus - Import',
    'Trade - Hybrid-electric bus - Export',
    'Trade - Hybrid-electric goods vehicle - Import',
    'Trade - Hybrid-electric goods vehicle - Export',
    'Trade - Hybrid-electric road vehicle total - Import',
    'Trade - Hybrid-electric road vehicle total - Export']

for key, value in measure_to_drive.items():
    ATO_data.loc[(ATO_data['original_measure'].isin(value)), 'drive'] = key
    # ATO_data.loc[ATO_data['original_measure'].isin(value), 'original_measure'] = ATO_data.loc[ATO_data['original_measure'].isin(value), 'measure']

#%%
#extract the measures that contain vehicle type data and then set their measures to from original_measure to measure, to indicate that we are happy with grouping them with other rows of that measure
original_measures = ATO_data['original_measure'].unique()
vehicle_type_dict = {}
vehicle_type_dict['2w'] = ['Motorised Two Wheeler Sales', 'Vehicle registration  (Motorised 2W)', 'Trade - Motorized 2/3-wheeler - Import', 'Trade - Motorized 2/3-wheeler - Export', 'Trade - Electric 2/3-wheeler (including hybrid-electric) - Import', 'Trade - Electric 2/3-wheeler (including hybrid-electric) - Export', 
'Vehicle registration - Electric 2/3-wheelers','Vehicle registration (Motorised 2W)']
vehicle_type_dict['3w'] = ['Motorised Three Wheeler Sales', 'Vehicle registration (Motorised 3W)']
vehicle_type_dict['bus'] = ['Vehicle registration (Bus)','Passengers Kilometer Travel - Bus','Operational bus fleet', 'Trade - Bus - Import', 'Trade - Bus - Export','Vehicle registration - Electric Buses']
vehicle_type_dict['lpv'] = ['LDV Sales', 'Vehicle registration (LDV)',  'Trade - Hybrid-electric LDV - Import', 'Trade - Hybrid-electric LDV - Export', 'Trade - Electric LDV (including hybrid-electric) - Import', 'Trade - Electric LDV (including hybrid-electric) - Export', 'Trade - LDV - Import', 'Trade - LDV - Export']
vehicle_type_dict['others'] = ['Vehicle registration (Others)', 'Trade - Other vehicles - Import', 'Trade - Other vehicles - Export', 'Trade - Electric goods vehicle (including hybrid-electric) - Import', 'Trade - Electric goods vehicle (including hybrid-electric) - Export', 'Trade - Goods vehicle - Import', 'Trade - Goods vehicle - Export',  'Vehicle registration - Electric 4-wheelers']
vehicle_type_dict['all'] = ['Freight Vehicle registration','Commercial Vehicle Sales (motorised)','Freight tonnes transported - Road']#this is all because it is all freight vehicles. we will split this into the different types using data fromoutside the ato data
vehicle_type_dict['truck'] = ['Vehicle registration - Electric Trucks']

# vehicle_type_dict['all'] = ['Commercial Vehicle Sales (motorised)']
#where original_measure is one of the values in the lists above, change the vehicle type to the key anbd then change the original_measure to the measure
for key, value in vehicle_type_dict.items():
    ATO_data.loc[ATO_data['original_measure'].isin(value), 'vehicle type'] = key
    #if the key is others then dont change the original_measure
    if key != 'others':
        ATO_data.loc[ATO_data['original_measure'].isin(value), 'original_measure'] = ATO_data.loc[ATO_data['original_measure'].isin(value), 'measure']

#%%

#there are some measures where there is no vehicle type, isntead just a medium. eg. they may contain: 'Road', 'Railways', Rail,  Waterways/shipping, Aviation 
# set their original measure to their measure indicating that we are happy having them with that measure
medium_dict = {}
medium_dict['rail_measures'] = ['Passengers Kilometer Travel - Railways', 'Passengers Kilometer Travel - HSR', 'Freight Transport - Tonne-km for Railways', 'Railway Energy Consumption', 'Railways in Transport CO2 Emissions', 
'Rail passenger carriages',
'Rail freight wagons', 'Freight tonnes transported - Railways', 'Rail diesel locomotives', 'Rail electric locomotives','Rail diesel/electric multiple units']
medium_dict['road_measures'] = ['Passengers Kilometer Travel - Roads', 'Freight Transport - Tonne-km for Roads', 'Road Energy Consumption', 'Road in Transport CO2 Emissions', 'Total Vehicle sales (motorised)', 'Commercial Vehicle Sales (motorised)', 'Freight Vehicle registration', 'Total Vehicle Registration','Land Transport Passenger Kilometers Travel','Freight tonnes transported - Road','Vehicle registration - Electric Total', 'Trade - Road vehicle total - Import', 'Trade - Road vehicle total - Export', 'Trade - Electric road vehicle total (including hybrid-electric) - Import', 'Trade - Electric road vehicle total (including hybrid-electric) - Export', 'Trade - Hybrid-electric road vehicle total - Import', 'Trade - Hybrid-electric road vehicle total - Export']
medium_dict['water_measures'] = ['Passengers Kilometer Travel - Waterways/shipping', 'Freight Transport - Tonne-km for Waterways/shipping (Domestic)', 'Freight Transport - Tonne-km for Waterways/shipping (Domestic+International)', 'Domestic Navigation Energy Consumption', 'Shipping/Inland Waterways Transport CO2 Emissions',
'Freight tonnes transported - Shipping', 'Trade - Navigation total - Import', 'Trade - Navigation total - Export']
medium_dict['aviation_measures'] = ['Passengers Kilometer Travel - Domestic Aviation', 'Aviation International Passenger Kilometers', 'Aviation Total Passenger Kilometers', 'Freight Transport - Tonne-km for Aviation (Domestic)', 'Freight Transport - Tonne-km for Aviation (Domestic+International)', 'Domestic Aviation Energy Consumption', 'Domestic Aviation Transport CO2 Emissions',
'Freight tonnes transported - Aviation', 'Trade - Aviation total - Import', 'Trade - Aviation total - Export']

##these were removed as its important to keep their measure as is 'Freight tonnes transported - Aviation - Import', 'Freight tonnes transported - Aviation - Export','Freight tonnes transported - Shipping - Import', 'Freight tonnes transported - Shipping - Export',

# #first where original measure is Passengers Kilometer Travel - HSR then set 'comments' to 'High Speed Rail'
#ATO_data.loc[ATO_data['original_measure'] == 'Passengers Kilometer Travel - HSR', 'comments'] = 'High Speed Rail'
ATO_data.loc[ATO_data['original_measure'] == 'Passengers Kilometer Travel - HSR', 'vehicle type'] = 'High Speed Rail'
#where original_measure is one of the values in the lists above, then just change the original_measure to the measure (ignore changing the medium)
for key, value in medium_dict.items():
    ATO_data.loc[ATO_data['original_measure'].isin(value), 'original_measure'] = ATO_data.loc[ATO_data['original_measure'].isin(value), 'measure']

#%%

#also if original_measure  is Biofuel consumption (Total) or Biofuel production (Total) then set the fuel_type to Biofuels
#and now deal with fuel types
fuel_type_dict = {}
fuel_type_dict['electricity'] = ['Electricity Transport', 'Electricity Road', 'Electricity Rail']  
fuel_type_dict['gasoline'] = ['Gasoline Road']
fuel_type_dict['diesel'] = ['Diesel Road', 'Diesel Rail']
fuel_type_dict['natural_gas'] = ['Natural Gas Road']
fuel_type_dict['bio_gasoline'] = ['Bio-Gasoline Road']
fuel_type_dict['bio_diesel'] = ['Bio-Diesel Road']
fuel_type_dict['biofuels'] = [ 'Biofuel consumption (Total)']#,'Biofuel production (Total)'
#where original_measure is one of the values in the lists above, then change the fuel_type to the key and the original_measure to the measure
for key, value in fuel_type_dict.items():
    ATO_data.loc[ATO_data['original_measure'].isin(value), 'fuel_type'] = key
    ATO_data.loc[ATO_data['original_measure'].isin(value), 'original_measure'] = ATO_data.loc[ATO_data['original_measure'].isin(value), 'measure']


##############################################
##############################################
#%%
#some changes i made quickly:
#now rename any fuel types = Natural Gas to natural_gas
ATO_data.loc[ATO_data['fuel_type'] == 'Natural Gas', 'fuel_type'] = 'natural_gas'
#and any fuel types from Electricity to electricity
ATO_data.loc[ATO_data['fuel_type'] == 'Electricity', 'fuel_type'] = 'electricity'
#%%

##############################################
##############################################
#and finally there are some original measures for which we now have enough detail in their other cols to set them to their measure col (GIVEN THE ASSUMPTION THAT WE WILL CONVERT THE MEASURE COL TO THE ORIGINAL MEASURE COL LATER IN THIS SCRIPT)
original_measures_to_change = [#below ones dont seem to be used
    'Total Passenger Kilometer Travel (Domestic+International)', 'Freight Transport - Tonne-km (Total) (Domestic+International)','Freight tonne-km/GDP (Domestic+International)', 'Freight tonne-km/capita (Domestic+International)','Land Transport Freight Kilometers Travel', 
    #but these are used definitely:
    'Freight tonnes transported - Total']
ATO_data.loc[ATO_data['original_measure'].isin(original_measures_to_change), 'original_measure'] = ATO_data.loc[ATO_data['original_measure'].isin(original_measures_to_change), 'measure']

#%%
################################################################################################################################################################################
#to make things easier in following, more precise steps, lets remove the rwos flagged with 'others' since we can save any formatting till we know what we want to do with them
others = ATO_data.copy()
others = others[others['other']]
ATO_data = ATO_data[~ATO_data['other']]
#%%


##############################################
##############################################
#NOW REMOVE UNESSECARY COLUMNS

#if the extra_detail col is all nan, remove the col!
if ATO_data['extra_detail'].isna().all():
    print('extra_detail col is all nan, dropping it')
    ATO_data = ATO_data.drop(columns=['extra_detail'])
else:
    print('extra_detail col is not all nan, not dropping it')
    #throw error because if we do still have an extra detail col then the data has changed and we need to update the code to either remove the extra detail col (and add its detail somewhere else if it is important to keep) or to keep the extra detail col and update all additional code to deal with it
    raise Exception('extra_detail col is not all nan, not dropping it')
#%%
#drop the scope col
#now if the only other value in scope is 'National' then just remove the scope column. We can check for this by checking that unique is the same as ['National, International', 'Regional','National'], in some order
# if ATO_data['scope'].unique().tolist().sort() == ['National, International', 'Regional','National'].sort():
#     print('Removing scope column as it only has one value: National')
#     ATO_data = ATO_data.drop(columns = ['scope'])
# else:
#     print('Not removing scope column as it has more than one value')
#     #thorw error (same reason as above)
#     raise Exception('Not removing scope column as it has more than one value')
#%%

##############################################
##############################################
#We're actually going to set the measure col to the original measure col, and then remove the original measure col... but as long as the original measure col contains these values (if not then the data has changed amnd the user should deal with the new original_measure values)
#this is a kind of safeguard against too quickly setting the value in the measure col without considering what other values with that name are, and whether these vlaues should be in that category, e.g. choosing to keep the following separate: 'Total Transport Energy Consumption', 'Energy'
#things that we're 100% certain we want to keep with a different measure than the original_measure shold not have their actual original_measure in the list below and isntead have their  original_measure set to what you want the measure to be already
#our lsit:
temp_list = ['passenger_km', 'freight_tonne_km', 'Sales','vehicle_registration', 'Vehicle registration (Others)','Total Transport Energy Consumption', 'Energy','Fossil Transport CO2 emissions', 'Total Transport CO2 emissions','co2_emissions','freight_tonnes',
#maybes (these are ones that we think we want to keep their measure as is and probably that means not simplifying them, but we're not 100% sure yet):
'Energy consumption (Total)',
 'Vehicle registration - Electric 4-wheelers',
 'Vehicle registration - Electric Trucks', 'energy_use_tonnes','Freight tonnes transported - Aviation - Import', 'Freight tonnes transported - Aviation - Export','Freight tonnes transported - Shipping - Import', 'Freight tonnes transported - Shipping - Export']

if ATO_data['original_measure'].isin(temp_list).all():
    print('Changing measure col to original_measure col, and then removing original_measure col')
    ATO_data['measure'] = ATO_data['original_measure']
    ATO_data = ATO_data.drop(columns=['original_measure'])
else:
    print('original_measure col contains values not in the list above, not removing original_measure col')
    #print new values that aret in the list above
    new_values = ATO_data[~ATO_data['original_measure'].isin(temp_list)]['original_measure'].unique().tolist()
    print('New values are: ', new_values)
    #thorw error (same reason as above)
    raise Exception('original_measure col contains values not in the list above, not removing original_measure col')

##############################################
##############################################
#%%
#remove other non necessary cols and say what we're removing by comapring cols_to_keep against the cols in the df
if 'comments' not in ATO_data.columns:
    ATO_data['comments'] = np.nan
cols_to_keep = ['measure', 'medium', 'value', 'unit', 'year','month','economy', 'sheet', 'transport type', 'vehicle type', 'fuel_type', 'source', 'comments','scope', 'drive']
print('removing cols: ', [col for col in ATO_data.columns if col not in cols_to_keep])
ATO_data_before_removing_cols = ATO_data.copy()
ATO_data = ATO_data[cols_to_keep]

#check for any duplicates when we dont consider the value col, nad put them in a df for viewing
duplicates = ATO_data[ATO_data.duplicated(subset=cols_to_keep.remove('value'), keep=False)]
if duplicates.empty:
    print('no duplicates found')
else:
    #put all duplicates in order so we can see them side by side
    duplicates = duplicates.sort_values(by=cols_to_keep)
    print('duplicates found, best thing to check is that cols_to_keep is correct and then that the duplicates arent in the original data')

##############################################
##############################################
#%%
#create a date and frequency column and remove the year and month cols
#if month is na, set frequency to 'Yearly', else set frequency to 'Monthly'
ATO_data['frequency'] = 'Yearly'
ATO_data.loc[ATO_data['month'].notna(), 'frequency'] = 'Monthly'
#%%
#If month is na, then make it 1.
ATO_data['month'] = ATO_data['month'].fillna(12)
#make month an int
ATO_data['month'] = ATO_data['month'].astype(int)
#now create date using the year and month
ATO_data['date'] = pd.to_datetime(ATO_data['year'].astype(str) + '-' + ATO_data['month'].astype(str))
#make date include the last day of the month
ATO_data['date'] = ATO_data['date'] + pd.offsets.MonthEnd(0)
#now drop the year and month cols
ATO_data = ATO_data.drop(columns=['year', 'month'])
 
#%%

##############################################
##############################################
#fixes to make data similar to other datasets
ATO_dataset_clean = ATO_data.copy()
#change column names to capitalise first letter of each word
ATO_dataset_clean.columns = ATO_dataset_clean.columns.str.title()
#note that there now may be duplicate rows with different values. This is because different sheet may have diff versions of the same datapoint. We'll deal with this issue if we come to it later. 
# #change the units
#%%

##############################################
##############################################
#change the measures:
ATO_dataset_clean['Measure'] = ATO_dataset_clean['Measure'].replace('Vehicle registration (Others)', 'Stocks')
ATO_dataset_clean['Measure'] = ATO_dataset_clean['Measure'].replace('Total Transport Energy Consumption', 'Energy')
ATO_dataset_clean['Measure'] = ATO_dataset_clean['Measure'].replace('vehicle_registration', 'Stocks')
ATO_dataset_clean['Measure'] = ATO_dataset_clean['Measure'].replace('Total Transport CO2 emissions', 'co2_emissions')

#where measure is Sales, make unit 'Stocks'
ATO_dataset_clean.loc[ATO_dataset_clean['Measure'] == 'Sales', 'Unit'] = 'Stocks'
ATO_dataset_clean.loc[ATO_dataset_clean['Measure'] == 'Stocks', 'Unit'] = 'Stocks'
ATO_dataset_clean['Unit'] = ATO_dataset_clean['Unit'].replace('passenger kilometers', 'passenger_km')
ATO_dataset_clean['Unit'] = ATO_dataset_clean['Unit'].replace('tonne kilometers', 'freight_tonne_km')
ATO_dataset_clean['Unit'] = ATO_dataset_clean['Unit'].replace('pj', 'PJ')


#change transport type values
ATO_dataset_clean['Transport Type'] = ATO_dataset_clean['Transport Type'].str.lower()
ATO_dataset_clean['Transport Type'] = ATO_dataset_clean['Transport Type'].replace('passenger & freight', 'combined')

# #change mediums:
ATO_dataset_clean['Medium'] = ATO_dataset_clean['Medium'].replace('Rail', 'rail')
ATO_dataset_clean['Medium'] = ATO_dataset_clean['Medium'].replace('Shipping/Waterways/Navigation', 'ship')
ATO_dataset_clean['Medium'] = ATO_dataset_clean['Medium'].replace('Aviation', 'air')
ATO_dataset_clean['Medium'] = ATO_dataset_clean['Medium'].replace('Road', 'road')
ATO_dataset_clean['Medium'] = ATO_dataset_clean['Medium'].replace('Aviation_international', 'air_international')
ATO_dataset_clean['Medium'] = ATO_dataset_clean['Medium'].replace('All Modes_international', 'all_international')
ATO_dataset_clean['Medium'] = ATO_dataset_clean['Medium'].replace('Rail, Road', 'rail_road')
ATO_dataset_clean['Medium'] = ATO_dataset_clean['Medium'].replace('Shipping/Waterways/Navigation_international', 'ship_international')
ATO_dataset_clean['Medium'] = ATO_dataset_clean['Medium'].replace('All Modes', 'all')
#%%

##############################################
##############################################
#remove nan values in the value column
ATO_dataset_clean = ATO_dataset_clean[ATO_dataset_clean['Value'].notna()]

ATO_dataset_clean['Dataset'] = 'ATO'

#foramt datetime so its only yyyy-mm-dd
ATO_dataset_clean['Date'] = ATO_dataset_clean['Date'].dt.strftime('%Y-%m-%d')
#%%


##############################################
##############################################
#removing duplicates:
#first remove dupes where the value is the same but sheet is different (keep the first one)
ATO_dataset_clean = ATO_dataset_clean.drop_duplicates(subset=ATO_dataset_clean.drop(columns=['Sheet']).columns.tolist(), keep='first')

#then we will separate the duplicates when we remove the vlaue and sheet col, decide what to keep and then add the value and sheet back in
dupes = ATO_dataset_clean.loc[ATO_dataset_clean.drop(columns=['Value', 'Sheet']).duplicated(keep=False)]
#remove the dupes from the original dataset
ATO_dataset_clean = ATO_dataset_clean.drop_duplicates(subset=ATO_dataset_clean.drop(columns=['Value', 'Sheet']).columns.tolist(), keep=False)

#then sort so that wen we lok we can see the dupes togehtr
dupes = dupes.sort_values(by=ATO_dataset_clean.drop(columns=['Value', 'Sheet']).columns.tolist())
#%%

##############################################
##############################################
#now remove any dupes that are from the sheets: CLC-VRE-081 , CLC-VRE-004(2),  CLC-VRE-027 (we checkedc these and foundb that their values were almost the same as the other sheets, implying that it was either a minor revision or mistake that caused them to have dfiferent values in value col)
#for future: 
#026 was removed as it was too similar to 001
#027 was removed as it was too similar to 001
#081 was removed as it was too similar to 003 and 010 and 001

dupes = dupes[~dupes['Sheet'].isin(['CLC-VRE-027','CLC-VRE-081', 'CLC-VRE-026',])]#'CLC-VRE-004(2)', 'CLC-VRE-027','CLC-VRE-048(1)'
#now check if there are any dupes left:
dupes = dupes.loc[dupes.drop(columns=['Value', 'Sheet']).duplicated(keep=False)]


if len(dupes) > 0:
    #throw an error
    raise ValueError('There are still dupes in the ATO dataset')
#%%

##############################################
##############################################
#and remove the values for Japan Road passenger km from sheet TAS-PAG-005(3) where vehiclke type is nan and source is Country Official Statistics as the total looks to be the same as the public transpoort total, not road. Have let ATO know about this. If the value for 2018 is higher than 64813000000 then throw an error to let user know the value has changed.
#filter for sheet
rows_to_remove = ATO_dataset_clean.loc[ATO_dataset_clean['Sheet'] == 'TAS-PAG-005(3)']
#filter for economy
rows_to_remove = rows_to_remove.loc[rows_to_remove['Economy'] == '08_JPN']

#grb value for 2018 to check if it is higher than 64813000000
value_2018 = rows_to_remove.loc[rows_to_remove['Date'] == '2018-12-31']['Value'].values[0]
if value_2018 != 64813000000:
    raise ValueError('The value for Japan Road passenger km in sheet TAS-PAG-005(3) has changed. Please check the ATO dataset and update the code if necessary.')
else:
    #remove the rows from the original dataset
    ATO_dataset_clean = ATO_dataset_clean.drop(rows_to_remove.index)

##############################################
##############################################
#%%
#Where measure is one of the following, and medium is road and vehicle type is na, then set the vehicle type to 'all'
# 'passenger_km', 'freight_tonne_km', 'Sales', 'Stocks', 'Energy',
#        'energy_use_tonnes', 'co2_emissions'
ATO_dataset_clean.loc[(ATO_dataset_clean['Measure'].isin(['passenger_km', 'freight_tonne_km', 'Sales', 'Stocks', 'Energy', 'energy_use_tonnes', 'co2_emissions'])) & (ATO_dataset_clean['Medium'] == 'road') & (ATO_dataset_clean['Vehicle Type'].isna()), 'Vehicle Type'] = 'all'

#and set transport type where its missing:
ATO_dataset_clean.loc[(ATO_dataset_clean['Transport Type'].isna()) &(ATO_dataset_clean['Measure'].isin(['Energy consumption (Total)', 'Energy'])), 'Transport Type'] = 'combined'
#and where medium is misisng
ATO_dataset_clean.loc[(ATO_dataset_clean['Medium'].isna()) &(ATO_dataset_clean['Measure'].isin(['Energy'])), 'Medium'] = 'all'
#and where medium is 'rail', 'air', 'ship', 'all' or 'rail_road' then set vehicle type to all if it is na
ATO_dataset_clean.loc[(ATO_dataset_clean['Medium'].isin(['rail', 'air', 'ship','all', 'rail_road'])) & (ATO_dataset_clean['Vehicle Type'].isna()), 'Vehicle Type'] = 'all'
#and if fuel_type is not anything, then set it to all
ATO_dataset_clean.loc[ATO_dataset_clean['Fuel_Type'].isna(), 'Fuel_Type'] = 'all'
##############################################
##############################################
#%%
#COVID DATA NOW SEPARATED AND NOT USED UNTIL WE NEED IT
# #where measure is COVID Google Mobility Data - Grocery and pharmacy, then set mediumn to road, and vehicle type to all and transport type to passenger
# road_passenger_covid_measures = [
#        'COVID Google Mobility Data - Grocery and pharmacy',
#        'COVID Google Mobility Data - Retail and recreation',
#        'COVID Google Mobility Data - Parks',
#        'COVID Google Mobility Data - Public transport stations',
#        'COVID Google Mobility Data - Workplaces',
#        'COVID Google Mobility Data - Residential',
#        'COVID Apple Mobility Data - Driving',
#        'COVID Apple Mobility Data - Walking',
#        'COVID Apple Mobility Data - Public transport',
#        'COVID Restrictions - Public Transport'
# ]

# ATO_dataset_clean.loc[(ATO_dataset_clean['Measure'].isin(road_passenger_covid_measures), 'Medium')] = 'road'
# ATO_dataset_clean.loc[(ATO_dataset_clean['Measure'].isin(road_passenger_covid_measures), 'Vehicle Type')] = 'all'
# ATO_dataset_clean.loc[(ATO_dataset_clean['Measure'].isin(road_passenger_covid_measures), 'Transport Type')] = 'passenger'

# #then set medium to all, vehicle type to all and transport type to passenger for the following measures:
# #  'COVID Restrictions - International Transport',
# #        'COVID Restrictions - Domestic Transport'
# ATO_dataset_clean.loc[(ATO_dataset_clean['Measure'].isin(['COVID Restrictions - International Transport', 'COVID Restrictions - Domestic Transport'])), 'Medium'] = 'all'
# ATO_dataset_clean.loc[(ATO_dataset_clean['Measure'].isin(['COVID Restrictions - International Transport', 'COVID Restrictions - Domestic Transport'])), 'Vehicle Type'] = 'all'
# ATO_dataset_clean.loc[(ATO_dataset_clean['Measure'].isin(['COVID Restrictions - International Transport', 'COVID Restrictions - Domestic Transport'])), 'Transport Type'] = 'passenger'
#%%
#and now for the measures in 
# 'Trade - LDV - Import',
#  'Trade - LDV - Export',
#  'Trade - Bus - Import',
#  'Trade - Bus - Export',
#  'Trade - Goods vehicle - Import',
#  'Trade - Goods vehicle - Export',
#  'Trade - Motorized 2/3-wheeler - Import',
#  'Trade - Motorized 2/3-wheeler - Export',
#  'Trade - Bicycle - Import',
#  'Trade - Bicycle - Export',
#  'Trade - Other vehicles - Import',
#  'Trade - Other vehicles - Export',
#  'Trade - Vehicle manufacturing - Import',
#  'Trade - Vehicle manufacturing - Export',
#  'Trade - Road vehicle total - Import',
#  'Trade - Road vehicle total - Export',
#  'Trade - Electric LDV (including hybrid-electric) - Import',
#  'Trade - Electric LDV (including hybrid-electric) - Export',
#  'Trade - Electric bus (including hybrid-electric) - Import',
#  'Trade - Electric bus (including hybrid-electric) - Export',
#  'Trade - Electric goods vehicle (including hybrid-electric) - Import',
#  'Trade - Electric goods vehicle (including hybrid-electric) - Export',
#  'Trade - Electric 2/3-wheeler (including hybrid-electric) - Import',
#  'Trade - Electric 2/3-wheeler (including hybrid-electric) - Export',
#  'Trade - Electric road vehicle total (including hybrid-electric) - Import',
#  'Trade - Electric road vehicle total (including hybrid-electric) - Export',
#  'Trade - Hybrid-electric LDV - Import',
#  'Trade - Hybrid-electric LDV - Export',
#  'Trade - Hybrid-electric bus - Import',
#  'Trade - Hybrid-electric bus - Export',
#  'Trade - Hybrid-electric goods vehicle - Import',
#  'Trade - Hybrid-electric goods vehicle - Export',
#  'Trade - Hybrid-electric road vehicle total - Import',
#  'Trade - Hybrid-electric road vehicle total - Export',
#  'Trade - Rail total - Import',
#  'Trade - Rail total - Export',
#  'Trade - Rail locomotive - Import',
#  'Trade - Rail locomotive - Export',
#  'Trade - Rail diesel/electric multiple unit - Import',
#  'Trade - Rail diesel/electric multiple unit - Export',
#  'Trade - Rail passenger carriage - Import',
#  'Trade - Rail passenger carriage - Export',
#  'Trade - Rail freight wagon - Import',
#  'Trade - Rail freight wagon - Export',
#  'Trade - Navigation total - Import',
#  'Trade - Navigation total - Export',
#  'Trade - Aviation total - Import',
#  'Trade - Aviation total - Export',
#we will just set them to have medium as all, vehicle type as all and transport type as combined. this is only for simplicity:

#%%

##############################################
##############################################
#where medium is road and drive is not already set, then set drive to all, otherwise set it to the same value as medium
ATO_dataset_clean.loc[(ATO_dataset_clean['Medium'] == 'road') & (ATO_dataset_clean['Drive'].isna()), 'Drive'] = 'all'
ATO_dataset_clean.loc[(ATO_dataset_clean['Medium'] != 'road') & (ATO_dataset_clean['Drive'].isna()), 'Drive'] = ATO_dataset_clean['Medium']

    
#%%

##############################################
##############################################
#lets see if we can remove the orginal_measure col and instead keep the sheet col
visualise = False
if visualise:
    #first visualise the data
    import plotly.express as px
    columns_to_plot =['Measure', 'Transport Type', 'Medium']
    #set any na's to 'NA'
    ATO_data_concordance_new = ATO_dataset_clean.fillna('NA')
    fig = px.treemap(ATO_data_concordance_new, path=columns_to_plot)#, values='Value')
    #make it bigger
    fig.update_layout(width=1000, height=1000)
    #show it in browser rather than in the notebook
    fig.show()
    fig.write_html("plotting_output/ATO analysis/all_data_tree.html")

    #and make one that can fit on my home screen which will be 1.3 times taller and 3 times wider
    fig = px.treemap(ATO_dataset_clean, path=columns_to_plot)
    #make it bigger
    fig.update_layout(width=2500, height=1300)
    #show it in browser rather than in the notebook
    fig.write_html("plotting_output/ATO analysis/all_data_tree_big.html")

##############################################
##############################################
#%%
ATO_dataset_clean_with_sheet = ATO_dataset_clean.copy()

#check for duplicates and decide which one to keep based on the sheet, before removing the sheet col
dupes = ATO_dataset_clean.loc[ATO_dataset_clean.drop(columns=['Sheet', 'Value']).duplicated(keep=False)]

if len(dupes) > 0:
    #throw an error
    raise ValueError('There are still dupes in the ATO dataset')

ATO_dataset_clean = ATO_dataset_clean.drop(columns=['Sheet'])

#%%

##############################################
##############################################
#create Load values from the measures: Freight tonnes transported - Road',
# 'Freight tonnes transported - Railways',
# 'Freight tonnes transported - Shipping',
# 'Freight tonnes transported - Aviation'] by dividing by freight_tonne_km
freight_tonnes_transported = ATO_dataset_clean.loc[ATO_dataset_clean['Measure'].isin(['freight_tonnes'])]
freight_tonne_km = ATO_dataset_clean.loc[ATO_dataset_clean['Measure'] == 'freight_tonne_km']
#join on siilar cols
#Replace NaN values with a unique value
freight_tonnes_transported.fillna('unique_value', inplace=True)
freight_tonne_km.fillna('unique_value', inplace=True)

# Perform the merge
load = pd.merge(freight_tonnes_transported, freight_tonne_km, on=['Date', 'Economy',  'Medium', 'Transport Type', 'Vehicle Type', 'Fuel_Type', 'Scope', 'Drive', 'Frequency'], how='inner')#

# Replace the unique value back with NaN
load.replace('unique_value', np.nan, inplace=True)
#calculate the load
load['Value'] = load['Value_x'] / load['Value_y']

load['Measure'] = 'load'
load['Unit'] = 'tonnes'
load['Dataset'] = 'ATO'
load['Source']= load['Source_x'] + '&' + load['Source_y']
load['Comments'] = 'Calculated by dividing the value of Freight tonnes transported by the value of Freight tonne-km'
# load['Drive'] = load['Drive_x'] 
#drop any _x and _y columns after checking they have had replacements
x_cols = [col for col in load.columns if col.endswith('_x')]
for col in x_cols:
    if col[:-2] in load.columns:
        load.drop(columns=[col, col[:-2]+'_y'], inplace=True)
    else:
        raise ValueError('The column {} needs to be added as it has not been a replacement'.format(col[:-2]))

# #add the load values to the dataset
ATO_dataset_clean = pd.concat([ATO_dataset_clean, load])

#%%
##############################################
##############################################
#separate some measures out and put them in the others df: (but make sure to set a flag called 'CLEAN' so we can easily separate the good stuff)
measures_to_shift_to_others = ['Vehicle registration - Electric 4-wheelers','freight_tonnes','energy_use_tonnes', 'Energy consumption (Total)', 'Fossil Transport CO2 emissions','Freight tonnes transported - Shipping - Export', 'Freight tonnes transported - Shipping - Import','Freight tonnes transported - Aviation - Import','Freight tonnes transported - Aviation - Export']

others['CLEAN'] = False

for measure in measures_to_shift_to_others:
    
    others = pd.concat([ATO_dataset_clean.loc[ATO_dataset_clean['Measure'] == measure], others])
    
    ATO_dataset_clean = ATO_dataset_clean[ATO_dataset_clean['Measure'] != measure]
    others.loc[others['Measure'] == measure, 'CLEAN'] = True
    
    
##############################################
##############################################

#%%
#lastly check there are no index cols whnich have nas in them, since theyll get pulled up in the data aggregation system:
index_cols = ['Date', 'Economy', 'Medium', 'Transport Type', 'Vehicle Type', 'Fuel_Type', 'Scope', 'Drive', 'Frequency']
for col in index_cols:
    nas = ATO_dataset_clean[ATO_dataset_clean[col].isna()]
    if len(nas) > 0:
        raise ValueError('There are nas in the {} column. The rows are {}'.format(col, nas))
    
#and check that all non value cols are objects and value is a float
for col in ATO_dataset_clean.columns:
    if col != 'Value':
        if ATO_dataset_clean[col].dtype != 'object':
            raise ValueError('The {} column is not an object'.format(col))
    else:
        if ATO_dataset_clean[col].dtype != 'float64':
            raise ValueError('The {} column is not a float'.format(col))
        
##############################################
##############################################
#%%
#save data
ATO_dataset_clean.to_csv('intermediate_data/ATO/ATO_data_cleaned_{}.csv'.format(FILE_DATE_ID), index=False)
ATO_dataset_clean_with_sheet.to_csv('intermediate_data/ATO/ATO_data_sheet_col_{}.csv'.format(FILE_DATE_ID), index=False)#this file is just for reference to where the data came from
#ALSO SAVE THE OTHERS DF, AS IT MIGHT BE USEFUL ONE DAY:
others.to_csv('intermediate_data/ATO/ATO_data_others_{}_unclean.csv'.format(FILE_DATE_ID), index=False)
##############################################
##############################################
#%%
#convert all 'ldv' to 'lpv' abecvause we are now moving to the new naming convention
# ATO_dataset_clean.loc[(ATO_dataset_clean['Vehicle Type'] == 'lpv'), 'Vehicle Type'] = 'car'
# ATO_dataset_clean_with_sheet.loc[(ATO_dataset_clean_with_sheet['Vehicle Type'] == 'lpv'), 'Vehicle Type'] = 'car'
# #set dataset to ato_car
# ATO_dataset_clean['Dataset'] = 'ato_car'
# ATO_dataset_clean_with_sheet['Dataset'] = 'ato_car'
#save 
# ATO_dataset_clean.to_csv('intermediate_data/ATO/ATO_data_cleaned_car_not_lpv_{}.csv'.format(FILE_DATE_ID), index=False)
# ATO_dataset_clean_with_sheet.to_csv('intermediate_data/ATO/ATO_data_sheet_col_car_not_lpv_{}.csv'.format(FILE_DATE_ID), index=False)#this file is just for reference to where the data came from
#%%

#%%

#for testing:
# #extract the duplicates in intermediate_data\testing\erroneus_duplicates.csv
# dupes = pd.read_csv('intermediate_data/testing/erroneus_duplicates.csv')
# #extract thsoe duplicates from the ATO dataset by finding rows that have the same values in their columns, wehre that column is in the dupes df
# #easiest way is to merge the two datasets on the columns that are in both
# #remove Dataset column from dupes
# dupes = dupes.drop(columns=['Dataset','Drive'])
# #set values in all cols to strings
# dupes = dupes.astype(str)
# ATO_dupes = ATO_dataset_clean.copy()
# ATO_dupes = ATO_dupes.astype(str)
# #merge the two datasets

# ATO_dupes = ATO_dupes.merge(dupes, how='inner', on=dupes.columns.tolist())

# #sort by all except value and sheet cols
# cols = ATO_dupes.columns.tolist()
# cols.remove('Value')
# cols.remove('Sheet')
# ATO_dupes = ATO_dupes.sort_values(by=cols)
# %%
