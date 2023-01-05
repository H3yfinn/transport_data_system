#intention is to look at item data (item harmonized dataset). This is to get an understanding of how their different categories work, prepare for future updates to the database and to understand if there is any useful information there as of yet (given that it will be updated in the future)
#%%
# Import libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
#change directory to root absed on the location of this file
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('../')
#%%

file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
#%%
#import data
item_data = pd.read_csv('input_data/item/iTEM harmonized_dataset.csv')
#import economy name to code mapping
economy_name_to_code = pd.read_csv('config/economy_code_to_name.csv')

#%%
#check data
# item_data.head()

#%%
#grab countries we can use using the iso code in the economy name to code mapping
iso_codes = economy_name_to_code['iso_code'].unique()
item_data_apec = item_data[item_data['ISO Code'].isin(iso_codes)]

#join on economy name to code mapping so we habe the Economy col
item_data_apec = item_data_apec.merge(economy_name_to_code[['iso_code', 'Economy']], left_on='ISO Code', right_on='iso_code', how='left')
#%%
#print col names
# print(item_data_apec.columns)
#where the vlaue of any category is 'All' set it to NA so that in the tree map we dont have a category for all
plotting_df = item_data_apec.copy()
# plotting_df = plotting_df.replace('All', np.nan)


#%%
VISUALISE = False
if VISUALISE:
    #now check data by creating a visualisation of it
    #for now we'll use a treemap in plotly to visualise the data
    import plotly.express as px
    columns_to_plot =[ 'Variable', 'Service', 'Mode', 'Vehicle Type', 'Technology', 'Fuel','Country']
    fig = px.treemap(plotting_df, path=columns_to_plot)#, values='Value')
    #make it bigger
    fig.update_layout(width=1000, height=1000)
    #show it in browser rather than in the notebook
    fig.show()
    fig.write_html("plotting_output/item_analysis/all_data_tree.html")

    #and make one that can fit on my home screen which will be 1.3 times taller and 3 times wider
    fig = px.treemap(plotting_df, path=columns_to_plot)
    #make it bigger
    fig.update_layout(width=2500, height=1300)
    #show it in browser rather than in the notebook
    fig.write_html("plotting_output/item_analysis/all_data_tree_big.html")

    #try a sunburst
    import plotly.express as px

    fig = px.sunburst(plotting_df, path=columns_to_plot)
    #make it bigger
    fig.update_layout(width=1000, height=1000)
    #show it in browser rather than in the notebook
    fig.show()
    fig.write_html("plotting_output/item_analysis/all_data_sun.html")
    
    #and make one that can fit on my home screen which will be 1.3 times taller and 3 times wider
    fig = px.sunburst(plotting_df, path=columns_to_plot)
    #make it bigger
    fig.update_layout(width=3000, height=2000)
    #show it in browser rather than in the notebook
    fig.write_html("plotting_output/item_analysis/all_data_sun_big.html")


#%%
#now get into the data and work out what we can do with it/how to maneuver it into the database
#first rename cols that can be renamed
item_data_apec.rename(columns={'Service':'Transport Type', 'Mode': 'Medium', 'Variable': 'Measure', 'Technology': 'Drive'}, inplace=True)
#remove the iso code, ID, and Region cols
item_data_apec.drop(columns=['ISO Code', 'iso_code', 'ID', 'Region', 'Country'], inplace=True)
#%%
# item_data_apec.columns
#%%
#then transfer all Year cols to one col (make tall)
item_data_apec_tall = item_data_apec.melt(id_vars=['Source','Economy', 'Measure','Unit','Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel'], var_name='Year', value_name='Value')

#drop rows where value is NA
item_data_apec_tall.dropna(subset=['Value'], inplace=True)

#%%
#create a column which specifies the max and min date for each group using the columns below as the groupby
group_cols = [ 'Measure', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel','Economy']
#group by the cols and get the min and max Year for each group. Indicate that in a new col for each group
item_data_apec_tall['Year_range_count'] = item_data_apec_tall.groupby(group_cols)['Year'].transform(lambda x: f'{x.min()}-{x.max()}-{x.nunique()}')

#%%
if VISUALISE:
    #try and make a sunburst
    import plotly.express as px
    #and make one that can fit on my home screen which will be 1.3 times taller and 3 times wider
    columns_to_plot =[ 'Measure', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel','Economy','Year_range_count']
    #when plotting make it so that when you hover over a category it shows the col name as the id
    fig = px.sunburst(item_data_apec_tall, path=columns_to_plot, hover_data=columns_to_plot)
    #make it bigger
    fig.update_layout(width=3000, height=2000)
    #show it in browser rather than in the notebook
    fig.write_html("plotting_output/item_analysis/all_data_sun_big_years.html")

# %%
# graet. now lets do something about the category values to make them match the database
if VISUALISE:
    #first, lets define all the unique values in each col
    for col in item_data_apec_tall.columns:
        print(col)
        print(item_data_apec_tall[col].unique())
        print('')
#%%
#first Measure can be changed to the following:
# 'Passenger Activity' -> 'passenger_km'
# 'Freight Activity' -> 'freight_tonne_km'
#and leave the rest as is, for now
item_data_apec_tall['Measure'] = item_data_apec_tall['Measure'].replace({'Passenger Activity': 'passenger_km', 'Freight Activity': 'freight_tonne_km', 'Stock':'Stocks'})

#and convert values so units are in standard units
#first print the unique units for each unique value of Measure
# print(item_data_apec_tall.groupby('Measure')['Unit'].unique())

#%%
#where unit is '10^9 tonne-km / yr', convert to 'freight_tonne_km' and times Value by 10^9
item_data_apec_tall.loc[item_data_apec_tall['Unit'] == '10^9 tonne-km / yr', 'Value'] = item_data_apec_tall.loc[item_data_apec_tall['Unit'] == '10^9 tonne-km / yr', 'Value'] * 10**9
#convert the unit to 'freight_tonne_km'
item_data_apec_tall.loc[item_data_apec_tall['Unit'] == '10^9 tonne-km / yr', 'Unit'] = 'freight_tonne_km'
#where unit is '10^9 passenger-km / yr', convert to 'passenger_km' and times by 10^9
item_data_apec_tall.loc[item_data_apec_tall['Unit'] == '10^9 passenger-km / yr', 'Value'] = item_data_apec_tall.loc[item_data_apec_tall['Unit'] == '10^9 passenger-km / yr', 'Value'] * 10**9
#convert the unit to 'passenger_km'
item_data_apec_tall.loc[item_data_apec_tall['Unit'] == '10^9 passenger-km / yr', 'Unit'] = 'passenger_km'
#where stock is 10^6 vehicles, convert to 'Stocks' and times by 10^6
item_data_apec_tall.loc[item_data_apec_tall['Unit'] == '10^6 vehicles', 'Value'] = item_data_apec_tall.loc[item_data_apec_tall['Unit'] == '10^6 vehicles', 'Value'] * 10**6
#convert the unit to 'Stocks'
item_data_apec_tall.loc[item_data_apec_tall['Unit'] == '10^6 vehicle', 'Unit'] = 'Stocks'
#%%
#then also check if the groups of [ 'Measure', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel','Economy','Year_range_count'] where Units in Vehicles per 1000 inhabitants are availabl are different to the ones where Units in Stocks are available
#first create new df and filter for only Measure == 'Stocks'
x = item_data_apec_tall[item_data_apec_tall['Measure'] == 'Stocks']
x = x.groupby(['Measure', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel','Economy','Year_range_count'])['Unit'].unique().reset_index()

#and do the same wher Units in freight_tonne_km is available vs units in 10^3 tonne-km / vehicle
y = item_data_apec_tall[item_data_apec_tall['Measure'] == 'freight_tonne_km']
y = y.groupby(['Measure', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel','Economy','Year_range_count'])['Unit'].unique().reset_index()

#and do the same wher Units in passenger_km is available vs units in 10^3 passenger-km / vehicle
z = item_data_apec_tall[item_data_apec_tall['Measure'] == 'passenger_km']
z = z.groupby(['Measure', 'Transport Type', 'Medium', 'Vehicle Type', 'Drive', 'Fuel','Economy','Year_range_count'])['Unit'].unique().reset_index()

#%%
#so THE ONLY ISSUE:
#  in the cell above we can see that in y, there is data in 10^3 tonne-km / vehicle for LDV's in about 8 cases where it is not available in freight_tonne_km. So we will times these values by 10^3 and convert the unit to freight_tonne_km by timesing by Stocks too. 
#first identify that we have stocks of LDV's in these economys for the same years..... UNFORTUNATELY WE DONT HAVE STOCKS FOR LDVS AT ALL FOR THEES ECOJOMYS. MAYBE WE CAN GET THEM FROM THE OTHER DATA SETS
#):

#%%
#So weve been through the data now. Letsmake small adjsustments like decapitilsinag vlaues in cols and so on:
#convert Motorcycles and Mopeds to 2W, Bus to bus, 'Coastal', 'Inland Waterway' to 'ship', Light Truck to lt, Heavy Truck to ht, Trams to rail
item_data_apec_tall['Vehicle Type'] = item_data_apec_tall['Vehicle Type'].replace({'Motorcycles': '2W', 'Mopeds': '2W', 'Bus': 'bus', 'Coastal': 'ship', 'Inland Waterway': 'ship', 'Light Truck': 'lt', 'Heavy Truck': 'ht', 'Trams': 'rail'})
#and Medium: 'Rail':rail, 'Road':road, 'Air':air, 'Shipping':ship
item_data_apec_tall['Medium'] = item_data_apec_tall['Medium'].replace({'Rail': 'rail', 'Road': 'road', 'Air': 'air', 'Shipping': 'ship'})
#repalce drive: 'BEV' 'bev'
item_data_apec_tall['Drive'] = item_data_apec_tall['Drive'].replace({'BEV': 'bev'})
#transport type: 'Passenger' 'passenger', 'Freight' 'freight'
item_data_apec_tall['Transport Type'] = item_data_apec_tall['Transport Type'].replace({'Passenger': 'passenger', 'Freight': 'freight'})

#%%

#and remove cols we dont need (Year_range_count)
item_data_apec_tall = item_data_apec_tall.drop(['Year_range_count'], axis=1)

#%%
#and now we can save the data with FILE_DATE_ID
item_data_apec_tall.to_csv('intermediate_data/item_data/item_dataset_clean_' + FILE_DATE_ID + '.csv', index=False)

#%%