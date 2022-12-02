#abnout exploring the data that we extracted in extract_ATO_dataset.py
#we will have to convert the measures into the same names and categories as we use in the transport model. then we c na chekc the data against the concordances we have made to see what data we are and are not missing. This will end up as a df the same size as the model concordances that will ahve true or false vlaues in each cell to inidcate if we have that spec ific data point or not. because the concrdances may change it simportant that this is done in a way that can be replicated for different concordances.

#this first part of the script will just be about epxlporation of the data
#load datra IN 
#%%

from dis import show_code
from tokenize import Special
from matplotlib.pyplot import title
import pandas as pd
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

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

#%%
#load in the data
ATO_data = pd.read_csv('output_data/ATO_data/ATO_actual_data_2022-10-19.csv')
country_codes = pd.read_csv('config/economy_code_to_name.csv')
#drop na from country codes
country_codes = country_codes.dropna()
#%%
#print basic details like the col nanmes, unique values in some cols
print(ATO_data.columns)
ATO_data.head()

#%%
#map ATO members, plus Iran and Russia	to APEC economy codes using the country_codes df 
#using a right join so we only get the apec members

#rename 'ATO members, plus Iran and Russia' ot economy_name
ATO_data.rename(columns={'ATO members, plus Iran and Russia':'ATO_economy_name'}, inplace=True)
#for now replace any 'China' vlaues with 'China, People's Republic of China'
ATO_data['ATO_economy_name'] = ATO_data['ATO_economy_name'].replace('China', 'China, People\'s Republic of China')
#reaplce Roads with Road
ATO_data['Mode'] = ATO_data['Mode'].replace('Roads', 'Road')

#non necessary:
#reaplce Waterways/shipping/Navigation with shipping
ATO_data['Mode'] = ATO_data['Mode'].replace('Waterways/shipping/Navigation', 'Shipping')
#repalce Shipping/Waterways/Navigation with shipping
ATO_data['Mode'] = ATO_data['Mode'].replace('Shipping/Waterways/Navigation', 'Shipping')
#reaplce Inland waterways/shipping/Navigation with shipping
ATO_data['Mode'] = ATO_data['Mode'].replace('Inland waterways/shipping/Navigation', 'Shipping')
#replace 'Shipping/Waterways' with Shipping
ATO_data['Mode'] = ATO_data['Mode'].replace('Shipping/Waterways', 'Shipping')

#reaplce Aviation with Air
ATO_data['Mode'] = ATO_data['Mode'].replace('Aviation', 'Air')
#repalce Railways with Rail
ATO_data['Mode'] = ATO_data['Mode'].replace('Railways', 'Rail')

#%%

#merge the data with the country codes
ATO_data = ATO_data.merge(country_codes, on='ATO_economy_name', how='right')

ATO_data = ATO_data.rename(columns={'Mode':'medium', 'Sector':'transport_type', 'Indicator':'measure', 'Economy':'economy'})
#decapiatlise the other columns names
ATO_data.columns = ATO_data.columns.str.lower()
#%%
#print unique measure
print(ATO_data['measure'].unique())

####################################################################################################################################################################################################################################################################################################################################
#ACTIVCITY DATA
#%%
#take a look at the medium column where inidcator contains  "Freight Transport - Tonne-km"
print('\n\n\ntake a look at the medium column where inidcator contains  "Freight Transport - Tonne-km \n\n')
ft = ATO_data[ATO_data['measure'].str.contains('Freight Transport - Tonne-km')]
print(ft['medium'].unique())
print(ft['scope'].unique())
print(ft['unit'].unique())
print(ft['transport_type'].unique())
print(ft['adb_economy_name'].unique())

#take a look at the medium column where inidcator contains 'Passenger Kilometer Travel' or 'Passengers Kilometer Travel'  or "PKM" 
print('\n\n\n take a look at the medium column where inidcator contains "Passenger Kilometer Travel" or "PKM" \n\n')
pkm = ATO_data[ATO_data['measure'].str.contains('Passenger Kilometer Travel|Passengers Kilometer Travel|PKM')]
print(pkm['medium'].unique())
print(pkm['scope'].unique())
print(pkm['unit'].unique())
print(pkm['transport_type'].unique())
print(pkm['adb_economy_name'].unique())

#we will get all the data above into the same format and then graph it so we can visually check it
####################################################################################################################################################################################################################################################################################################################################
#%%
#Convert whole dataset into the correct format for the transport model

#remove all cols except economy, measure, medium, scope, unit, value, year, transport_type.
ATO_data = ATO_data[['economy', 'measure', 'medium', 'scope', 'unit', 'value', 'year', 'transport_type', 'sheet']]
#replace unit values where they contain 'million' with values that dont have million.
#if the value does contain 'Million' or 'million' init then times the value by the magnitude and replace the unit with 'tonne-km'

#convert unit to lower first
ATO_data['unit'] = ATO_data['unit'].str.lower()
ATO_data['value'] = ATO_data['value'].astype(float)

#times the values where the unit contains 'million' by 1,000,000
#but to help to ignore the nan values because of the error from ValueError: Cannot mask with non-boolean array containing NA / NaN values, we will fix that first
ATO_data['unit'] = ATO_data['unit'].replace(np.nan, 'nan')
ATO_data.loc[ATO_data['unit'].str.contains('million'), 'value'] = ATO_data.loc[ATO_data['unit'].str.contains('million'), 'value'].multiply(1000000)
#repalce values where the unit contains 'million' with the same vlaue but without the 'million' in the unit
ATO_data.loc[ATO_data['unit'].str.contains('million'), 'unit'] = ATO_data.loc[ATO_data['unit'].str.contains('million'), 'unit'].str.replace('million ', '')
#replace 'nan' with np.nan
ATO_data['unit'] = ATO_data['unit'].replace('nan', np.nan)
####################################################################################################################################################################################################################################################################################################################################

#%%
#STOCKS DATA
#take a look at the medium column where inidcator contains  "Freight Transport - Tonne-km"
print('\n\n\ntake a look at the medium column where inidcator contains Stock related data \n\n')
Passenger_km = ['Passengers Kilometer Travel - Railways',
       'Passengers Kilometer Travel - Waterways/shipping',
       'Passengers Kilometer Travel - Aviation (Domestic)',
       'Passengers Kilometer Travel -Bus',
       'Passengers Kilometer Travel -HSR',
       'Passenger Kilometer Travelled - Walking',
       'Passenger Kilometer Travelled - Cycling', 'PKM By LDV',
       'PKM by Motorised 2W', 'Passengers Kilometer Travel - Roads']
Freight_km = ['Freight Transport - Tonne-km for Roads',
       'Freight Transport - Tonne-km for Railways',
       'Freight Transport - Tonne-km for Waterways/shipping (Domestic)',
       'Freight Transport - Tonne-km for Waterways/shipping (Domestic+International)',
       'Freight Transport - Tonne-km for Aviation (Domestic)']
Stocks_indicators = ["Aviation Regional Fleet in Service (Passenger/freight)"]
Sales_indicators = ['Motorized Two Wheeler Sales', 'Electric Two Wheeler Sales'
 'Motorized Three Wheeler Sales', 'Electric Three Wheeler Sales'
 'LDV Sales', 'Electric LDV Sales', 'Bus  Sales', 'Electric Bus Sales'
 'LCV Sales', 'Heavy Truck Sales', 'Bicycle Sales', 'Total Vehicle sales (motorised)', 'Passenger Vehicle Sales (motorised)',
 'Commercial Vehicle Sales (motorised)']
Registrations = [ 'Vehicle registration  (Motorised 2W)',
 'Vehicle registration  (Motorised 3W)', 'Vehicle registration  (LDV)',
 'Vehicle registration  (Bus)', 'Vehicle registration (Others)',
 'Freight Vehicle registration', 'Total Vehicle Registration',
 'Electric Vehicle registration  (2W)',
 'Electric Vehicle registration  (LDV)',
 'Electric Vehicle registration  (3w)',
 ' Electric Vehicle registration  (Bus)', 'Data on non-motorized 2 wheelers -  e.g. pedicabs, bike rickshaws',
 'Vehicle registration  (Utility Vehicle/Mini Bus)']
Special = ['Electric vehicle share in Total vehicle registrations', 'Total Passengers Kilometer Travel (Domestic)', 'Freight Transport - Tonne-km (Total) (Domestic+International)']
International =[ 'Merchant fleet by country of beneficial ownership, annual', 'Total Passengers Kilometer Travel (Domestic+International)', 'Freight Transport - Tonne-km for Aviation (Domestic+International)', 'Freight Transport - Tonne-km for Waterways/shipping (Domestic+International)']
Weird = [ 'Motorisation Index', 'LDV Motorisation Index',
 'Two and Three Wheelers Motorisation Index', 'Bus Motorisation Index',
 'Freight Vehicles Motorisation Index', 'Efficiency of air transport services' ,'Efficiency of Train Services',
 'Efficiency of seaport services']

#%%

freight_km = ATO_data[ATO_data['measure'].isin(Freight_km)]
print(freight_km['medium'].unique())
print(freight_km['scope'].unique())
print(freight_km['unit'].unique())
print(freight_km['transport_type'].unique())
#create alt_measure column that is just the name of the dataframe
freight_km['alt_measure'] = 'Freight_km'
#divide all freight km data by 1000000 and add ' million' to the unit
freight_km['value'] = freight_km['value'].divide(1000000)
freight_km['unit'] = freight_km['unit'].str.replace('tonne-km', 'million tonne-km')

passenger_km = ATO_data[ATO_data['measure'].isin(Passenger_km)]
print(passenger_km['medium'].unique())
print(passenger_km['scope'].unique())
print(passenger_km['unit'].unique())
print(passenger_km['transport_type'].unique())
#create alt_measure column that is just the name of the dataframe
passenger_km['alt_measure'] = 'Passenger_km'
#divide all passenger km data by 1000000 and add ' million' to the unit
passenger_km['value'] = passenger_km['value'].divide(1000000)
passenger_km['unit'] = passenger_km['unit'] + ' million'

stocks = ATO_data[ATO_data['measure'].isin(Stocks_indicators)]
print(ft['medium'].unique())
print(ft['scope'].unique())
print(ft['unit'].unique())
print(ft['transport_type'].unique())
#create alt_measure column that is just the name of the dataframe
stocks['alt_measure'] = 'Stocks'

sales = ATO_data[ATO_data['measure'].isin(Sales_indicators)]
print(ft['medium'].unique())
print(ft['scope'].unique())
print(ft['unit'].unique())
print(ft['transport_type'].unique())
#create alt_measure column that is just the name of the dataframe
sales['alt_measure'] = 'Sales'

registrations = ATO_data[ATO_data['measure'].isin(Registrations)]
print(ft['medium'].unique())
print(ft['scope'].unique())
print(ft['unit'].unique())
print(ft['transport_type'].unique())
#create alt_measure column that is just the name of the dataframe
registrations['alt_measure'] = 'Registrations'

special = ATO_data[ATO_data['measure'].isin(Special)]
print(ft['medium'].unique())
print(ft['scope'].unique())
print(ft['unit'].unique())
print(ft['transport_type'].unique())
#create alt_measure column that is just the name of the dataframe
special['alt_measure'] = 'Special'

international = ATO_data[ATO_data['measure'].isin(International)]
print(ft['medium'].unique())
print(ft['scope'].unique())
print(ft['unit'].unique())
print(ft['transport_type'].unique())
#create alt_measure column that is just the name of the dataframe
international['alt_measure'] = 'International'

weird = ATO_data[ATO_data['measure'].isin(Weird)]
print(ft['medium'].unique())
print(ft['scope'].unique())
print(ft['unit'].unique())
print(ft['transport_type'].unique())
#create alt_measure column that is just the name of the dataframe
weird['alt_measure'] = 'Weird'

#concatenate above dfs
ATO_data_transport = pd.concat([freight_km, passenger_km, stocks, sales, registrations, special, international, weird], axis=0)

########################################################################################################################################################################################################################################################################################################################################################
#%%
ATO_data_transport_copy = ATO_data_transport.copy()

#%%
# #create a set of data from which we will test the interpolation works. the data will be for '01_AUS', 'Air', 'Freight_km', 'Freight Transport - Tonne-km for Aviation (Domestic)', 'Freight', ' ton-km', 'National' for the columns 'year','economy', 'medium', 'alt_measure', 'measure', 'transport_type', 'unit', 'scope'
# ATO_data_transport = ATO_data_transport[ATO_data_transport['economy'] == '0_AUS']
# ATO_data_transport = ATO_data_transport[ATO_data_transport['medium'] == 'Air']
# ATO_data_transport = ATO_data_transport[ATO_data_transport['alt_measure'] == 'Freight_km']
# ATO_data_transport = ATO_data_transport[ATO_data_transport['measure'] == 'Freight Transport - Tonne-km for Aviation (Domestic)']
# ATO_data_transport = ATO_data_transport[ATO_data_transport['transport_type'] == 'Freight']
# ATO_data_transport = ATO_data_transport[ATO_data_transport['unit'] == 'million ton-km']
# ATO_data_transport = ATO_data_transport[ATO_data_transport['scope'] == 'National']


#%%
interpolate = False#please note that the last time i sued this script some weird things were happening. i think that was just because some rows were duplicated with nas in the values column but didnt check
if interpolate:
    #interpolate between years where data is missing between years where data is available:
    #group by 'economy', 'medium', 'alt_measure', 'measure', 'transport_type', 'unit', 'sheet', 'scope'

    #sort by 'year'
    ATO_data_transport = ATO_data_transport.sort_values(by=['year','economy', 'medium', 'alt_measure', 'measure', 'transport_type', 'unit', 'scope', 'sheet'])

    #because we have issues with how interpolate works in python we will break the dataframe into its groups using a for loop. then we will interpolate each group and then concatenate the groups back together.
    #to be fair, i havent double checked the results from this are not the same as using the original method, so dont just assume this is better. i just wanted to try it out.
    ATO_data_transport_interp_groups = ATO_data_transport.groupby(['economy', 'medium', 'alt_measure', 'measure', 'transport_type', 'unit', 'scope', 'sheet'])

    ATO_data_transport_interp= pd.DataFrame()
    i = 0
    for name, group in ATO_data_transport_interp_groups:
        if i == 0:
            ATO_data_transport_interp = group.copy()
            ATO_data_transport_interp['value'] = group['value'].interpolate(method='linear', axis=0, limit_direction='both', limit_area='inside')
            i = 1
        else:
            #interpolate between years where data is missing:
            group['value'] = group['value'].interpolate(method='linear', axis=0, limit_direction='both', limit_area='inside')
            #print(group)
            #concatenate above dfs
            ATO_data_transport_interp = pd.concat([ATO_data_transport_interp, group], axis=0)

    ########################################################################################################
    
    #copy ATO_data_transport_interp to ATO_data_transport_interp2 so we dont lose the original data
    ATO_data_transport_interp2 = ATO_data_transport_interp.copy()

    #create col which indicates whether data is interpolated or not. to be sure we will do this by joining the original data with the interpolated data and then checking if the values are the same
    ATO_data_transport_interp['interpolated'] = 0
    ATO_data_transport_interp = ATO_data_transport_interp.merge(ATO_data_transport, how='left', on=['year', 'economy', 'medium', 'alt_measure', 'measure', 'transport_type', 'unit', 'scope', 'sheet'], suffixes=('', '_orig'))

    ATO_data_transport_interp['interpolated'] = np.where((ATO_data_transport_interp['value'] == ATO_data_transport_interp['value_orig']) | (ATO_data_transport_interp['value'].isna()), False, True)

    #remove the cols with the _orig suffix from the columns
    ATO_data_transport_interp = ATO_data_transport_interp.loc[:, ~ATO_data_transport_interp.columns.str.endswith('_orig')]

    
    #plot a line graph of all the series we have data for, but faceted for each alt_measure. also make line dashed for where interpolated is true
    #create a column which is the concat of economy, medium, measure, transport_type, unit
    ATO_data_transport_interp['economy_measure_transport_type_unit_scope_sheet'] = ATO_data_transport_interp['economy'] + ' ' + ATO_data_transport_interp['measure'] + ' ' + ATO_data_transport_interp['transport_type'] + ' ' + ATO_data_transport_interp['unit'] + ' ' + ATO_data_transport_interp['scope'] + ' ' + ATO_data_transport_interp['sheet']
    #since there is too much data we will create a graph for each alt_measure using a for loop, and then a facet for each medium
    for measure in ATO_data_transport_interp['measure'].unique():
        #filter for the alt_measure
        ATO_data_transport_concordance_check_alt_measure = ATO_data_transport_interp[ATO_data_transport_interp['measure'] == measure]
        #create a title
        title = 'ATO Data for ' + measure
        #create a line graph where the x axis is the year, the y axis is the value, the color is the economy_medium_measure_transport_type_unit, and the line is dashed if interpolated is true
        fig = px.line(ATO_data_transport_concordance_check_alt_measure, x='year', y='value', color='economy_measure_transport_type_unit_scope_sheet', title=title, line_dash='interpolated', hover_name='economy', hover_data=['medium', 'measure', 'transport_type', 'unit', 'scope', 'sheet'], facet_col='medium', facet_col_wrap=3)
        fig.show()

    ATO_data_transport = ATO_data_transport_interp.copy()
#%%
########################################################################################################################################################################################################################################################################################################################################################
#create concordance to check what data we have vs dont:
ATO_data_transport_checking = ATO_data_transport.copy()
#FIRST CHECK FOR DUPLICATES SINCE THEY ARENT HELPFUL IN THIS FOLLOWING ANALYSIS:
#extract rows that are duplicates for all cols except sheet and value 
ATO_data_transport_checking_duplicates = ATO_data_transport_checking[ATO_data_transport_checking.duplicated(subset=['year', 'economy', 'medium', 'alt_measure', 'measure', 'transport_type', 'unit','scope'], keep=False)]
#for these rows, where both values aren't na we will keep the first, and drop the second. where both are na then we will keep the first, and drop the second. where one is na and the other is not then we will keep the one that is not na
ATO_data_transport_checking_duplicates = ATO_data_transport_checking_duplicates.sort_values(by=['year', 'economy', 'medium', 'alt_measure', 'measure', 'transport_type', 'unit', 'sheet', 'scope'])
ATO_data_transport_checking_duplicates = ATO_data_transport_checking_duplicates.drop_duplicates(subset=['year', 'economy', 'medium', 'alt_measure', 'measure', 'transport_type', 'unit','scope'], keep='first')
#now we will drop the duplicates from the concordance check
ATO_data_transport_checking = ATO_data_transport_checking.drop_duplicates(subset=['year', 'economy', 'medium', 'alt_measure', 'measure', 'transport_type', 'unit','scope'], keep=False)
#now we will add the duplicates back to the concordance check
ATO_data_transport_checking = ATO_data_transport_checking.append(ATO_data_transport_checking_duplicates)

#%%
#to create the concordance we will need to merge with 'how' = 'cross' on separate dfs for the unique values of the following columns: year, economy, medium, measure.
#Then based on the value of measure we will join on df2 which is a freight/passenger df which will contian cols for: measure, unit, transport_type
#in some cases there will be two rows in df2 for each measure since the measure may be valid for both freight and passenger. 
# Then we will create a df for each of the transport types
#NOTE THAT FOR NOW WE WILL TREAT ALT_MEASURE AS A MEASURE SINCE IT MAKES COMPARISON AND CHECKING MORE SIMPLE
year = ATO_data_transport_checking['year'].unique()
economy = ATO_data_transport_checking['economy'].unique()
medium = ATO_data_transport_checking['medium'].unique()
measure = ATO_data_transport_checking['measure'].unique()
alt_measure = ATO_data_transport_checking['alt_measure'].unique()
df1 = pd.DataFrame(list(itertools.product(year, economy, measure)), columns=['year', 'economy', 'measure']).drop_duplicates()
df2 = ATO_data_transport_checking[['measure', 'alt_measure', 'unit', 'medium', 'transport_type', 'sheet']].drop_duplicates()#having sheet in here allows us to have multiple rows for each possible row in the concordance if there are multiple sheets for the same measure #i think this is the best thing to do here. but it doesnt really suit the idea of building a concordance, like im not sure what the diff between the result of this compared to the original dataframe is. 
df3 = pd.merge(df1, df2, on='measure', how='left')
df3 = df3.dropna().drop_duplicates()
df3 = df3.sort_values(by=['year', 'economy', 'alt_measure', 'measure'])
#note that we are ignoreing sheet in the column for the concordance since we dont require that detail in the data (we dont care if there are multiple sheets for the same measure or not)
#%%
#clean data df
ATO_data_transport_checking_clean = ATO_data_transport_checking.copy()
#remove all rows where the value is NA
ATO_data_transport_checking_clean = ATO_data_transport_checking_clean[~ATO_data_transport_checking_clean['value'].isna()]
#remove duplicate rows
ATO_data_transport_checking_clean = ATO_data_transport_checking_clean.drop_duplicates()

#%%
#now check what data we have vs dont:
#first join the ATO_data_transport_checking to the concordances on the concordances columns, using a left join.
ATO_data_transport_checking_concordance_check = df3.merge(ATO_data_transport_checking_clean, on=['year', 'economy', 'medium', 'alt_measure', 'measure', 'transport_type', 'unit', 'sheet'], how='left')

#OKAY how to stop getting NA for sheet? i think i could have it in the df3?
#%%
#create a columnn that states whether or not the value is NA. We will use this to count the number of missing values vs the number of values we have
#to make things easier we will create a column called: 'data_available' which will be true if value_na is false, and false if value_na is true
ATO_data_transport_checking_concordance_check['data_available'] = ~ATO_data_transport_checking_concordance_check['value'].isna()

#%%

#and show the amount of data we have vs dont have by counting number of vallues that are null vs not null in value col
ATO_data_transport_checking_concordance_check_count2 = ATO_data_transport_checking_concordance_check.groupby(['data_available']).count().reset_index()

print('We have data for ' + str(ATO_data_transport_checking_concordance_check_count2['year'][0]) + ' rows and dont have data for ' + str(ATO_data_transport_checking_concordance_check_count2['year'][1]) + ' rows')


#%%
#show the count of NA's we have for each economy
economy_na_count = ATO_data_transport_checking_concordance_check.groupby(['economy', 'data_available']).count().reset_index()
#plot using a bar chart where the x axis is the economy, the y axis is the count, and the color is the economy
title = 'ATO Economy NA Count'
fig = px.bar(economy_na_count, x='economy', y='year', color='data_available', title=title)
fig.show()

#%%
#show the count of NA's we have for each medium
medium_na_count = ATO_data_transport_checking_concordance_check.groupby(['medium', 'data_available']).count().reset_index()
#plot using a bar chart where the x axis is the medium, the y axis is the count, and the color is the medium
title = 'ATO Medium NA Count'
fig = px.bar(medium_na_count, x='medium', y='year', color='data_available', title=title)
fig.show()


#%%
#show the count of NA's we have for each measure
measure_na_count = ATO_data_transport_checking_concordance_check.groupby(['alt_measure', 'data_available']).count().reset_index()
#plot using a bar chart where the x axis is the measure, the y axis is the count, and the color is the measure
title = 'ATO Alt Measure NA Count'
fig = px.bar(measure_na_count, x='alt_measure', y='year', color='data_available', title=title)
fig.show()

#%%
#show the count of NA's we have for each measure
measure_na_count = ATO_data_transport_checking_concordance_check.groupby(['measure', 'data_available']).count().reset_index()
#plot using a bar chart where the x axis is the measure, the y axis is the count, and the color is the measure
title = 'ATO Measure NA Count'
fig = px.bar(measure_na_count, x='measure', y='year', color='data_available', title=title)
fig.show()

#%%
#show the count of NA's we have for each transport_type
transport_type_na_count = ATO_data_transport_checking_concordance_check.groupby(['transport_type', 'data_available']).count().reset_index()
#plot using a bar chart where the x axis is the transport_type, the y axis is the count, and the color is the transport_type
title = 'ATO Transport Type NA Count'
fig = px.bar(transport_type_na_count, x='transport_type', y='year', color='data_available', title=title)
fig.show()


#%%
#show the count of NA's we have for each year
year_na_count = ATO_data_transport_checking_concordance_check.groupby(['year', 'data_available']).count().reset_index()
#plot using a bar chart where the x axis is the year, the y axis is the count, and the color is the year
title = 'ATO Year NA Count'
fig = px.bar(year_na_count, x='year', y='economy', color='data_available', title=title)
fig.show()

#%%
#show the count of NA's we have for each transport type medium combination
transport_type_medium_na_count = ATO_data_transport_checking_concordance_check.groupby(['transport_type', 'medium', 'data_available']).count().reset_index()
#concat transport type and medium to make a new column
transport_type_medium_na_count['transport_type_medium'] = transport_type_medium_na_count['transport_type'] + ' ' + transport_type_medium_na_count['medium']

#plot using a bar chart where the x axis is the transport_type, the y axis is the count, and the color is the transport_type
title = 'ATO Transport Type Medium NA Count'
fig = px.bar(transport_type_medium_na_count, x='transport_type_medium', y='year', color='data_available', title=title)
fig.show()

#%%
#create a multi facet grid plot, with a facet for each measure, with y axis = economy, x = year which will show a green square if data_available is false for that year and economy, and a red square if data_available is true for that year and economy
#bvut we will do this in a for loop on each alt_measure
for alt_measure in ATO_data_transport_checking_concordance_check['alt_measure'].unique():
    #create a dataframe that only has the alt_measure we are looking at
    alt_measure_df = ATO_data_transport_checking_concordance_check[ATO_data_transport_checking_concordance_check['alt_measure'] == alt_measure]
    #because of the addition of the sheet column we will have to create a col for measure + sheet
    alt_measure_df['measure_sheet'] = alt_measure_df['measure'] + ' ' + alt_measure_df['sheet']
    title = 'ATO Economy Year NA Count'
    #make sure that if data_available is true, the color is green, and red if false
    fig = px.scatter(alt_measure_df, x='year', y='economy', color='data_available', facet_col='measure_sheet', facet_col_wrap=7, title=title, color_discrete_map={True: 'green', False: 'red'})
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))#remove 'Economy=X' from titles
    #fig.show()
    #save graph as html in plotting_output/exploration_archive
    fig.write_html('plotting_output/exploration_archive/' + title + ' ' + alt_measure + '.html')

#%%
#next steps:
#convert data to the measures we need in the transport model. This will involve:filtering out measures we dont need and changing the names of measures we do need (we should do this by creating asn extra column called original_measure and then changing the measure column to the measure we need). 
# We should also check what dates are the earliest for which we have a reasonable amount of data
#we should also convert medium to the values we need in the transport model

#once this is done we will have a much smaller dataframe, and we can start to look at the data in more detail, perhaps using teh same processes as above.
#how about wee start those processes in a new file called 'explore_ATO_dataset_2nd_step.py'

#so save data
ATO_data_transport_checking_concordance_check.to_csv('intermediate_data/ATO_data_transport_checking_concordance_check.csv', index=False)



#%%


















#for some reason the df has some rows where the vlaues are different but the other columns are the same. We will separate these rows into their own df to analyse. We need to do this in a way that extracts both rows and their values for each duplication
# #first we will create a df that has the duplicated rows and no value columns
# duplicated_rows = ATO_data_transport_copy[ATO_data_transport_copy.duplicated(subset=['year','economy', 'medium', 'alt_measure', 'measure', 'transport_type', 'unit', 'scope', 'sheet'], keep=False)]
# #now we will create a df that has the duplicated rows and the value columns by extracting the rows that have the same values as duplicated_rows from ATO_data_transport_copy
# duplicated_rows_values = ATO_data_transport_copy.merge(duplicated_rows,  how='inner')
# #sort so that wee can see the duplicated rows
# duplicated_rows_values.sort_values(by=['year', 'economy', 'medium', 'alt_measure', 'measure', 'transport_type', 'unit', 'scope', 'sheet'], inplace=True)
# #remove na's and then repeat the process to get the rows that are duplicated
# duplicated_rows_values_no_na = duplicated_rows_values.dropna()
# duplicated_rows_values_no_na = duplicated_rows_values_no_na[duplicated_rows_values_no_na.duplicated(subset=['year','economy', 'medium', 'alt_measure', 'measure', 'transport_type', 'unit', 'scope', 'sheet'], keep=False)]
# #now we will create a df that has the duplicated rows and the value columns by extracting the rows that have the same values as duplicated_rows from ATO_data_transport_copy
# duplicated_rows_values = duplicated_rows_values.merge(duplicated_rows_values_no_na,  how='inner')
