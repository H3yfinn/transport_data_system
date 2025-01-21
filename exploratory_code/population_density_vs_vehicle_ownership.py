#take in popualtion density from owid and also the stocks output from our modelling, and th popualtion data. then calcaulte the vehicle ownership per 1000 people and plot it against population density:


#%%
import pandas as pd
import numpy as np
import os
import re
import datetime as datetime
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')

#%%
def get_latest_date_for_data_file(data_folder_path, file_name, file_name_regex=None):
    #get list of all files in the data folder
    all_files = os.listdir(data_folder_path)
    #filter for only the files with the correct file extension
    if file_name_regex:
        all_files = [file for file in all_files if re.search(file_name_regex, file)]
    else:
        all_files = [file for file in all_files if file_name in file]
    #drop any files with no date in the name
    all_files = [file for file in all_files if re.search(r'\d{8}', file)]
    #get the date from the file name
    all_files = [re.search(r'\d{8}', file).group() for file in all_files]
    #convert the dates to datetime objects
    all_files = [datetime.datetime.strptime(date, '%Y%m%d') for date in all_files]
    #get the latest date
    try:
        latest_date = max(all_files)
    except ValueError:
        print('No files found for ' + file_name)
        return None
    #convert the latest date to a string
    latest_date = latest_date.strftime('%Y%m%d')
    return latest_date

#%%
# Load data
density = pd.read_csv('input_data/our_world_in_data/population-density.csv')#Entity	Code	Year	Population density
#merge with config/economy_code_to_name on the code and iso code columns, to get economy:
economy_code_to_name = pd.read_csv('config/economy_code_to_name.csv')#Economy,Economy_name,Alt_name,Alt_name2,Alt_name3,iso_code,alt_aperc_name
density = pd.merge(density, economy_code_to_name, left_on='Code', right_on='iso_code', how='right')
#keep only year = 2021
density = density[density['Year'] == 2021]
#remas,e year to date
# density = density.rename(columns={'Year': 'Date'})
density = density[['Population density', 'Economy', 'Economy_name']]#, 'Date']]#since density is ponoly histrical, we can drop the date column
#%%
# Load stocks from ..\transport_model_9th_edition\output_data\model_output\all_economies_20240709_model_output20240709.csv
#where 20240709 is the date of the model output
file_name_regex = r'all_economies_\d{8}_model_output\d{8}.csv'

date_id = get_latest_date_for_data_file('../transport_model_9th_edition/output_data/model_output', 'all_economies', file_name_regex)

stocks = pd.read_csv('../transport_model_9th_edition/output_data/model_output/all_economies_'+date_id+'_model_output'+date_id+'.csv')

#drop non raod mediums 
stocks = stocks[stocks['Medium'] == 'road']
#keep only 2021 data
# stocks = stocks[stocks['Date'] == 2021]
# Scenario == 'Target'
stocks = stocks[stocks['Scenario'] == 'Target']
#drop data after 2070
stocks = stocks[stocks['Date'] <= 2060]
stocks = stocks[['Economy', 'Stocks', 'Date']]
#sum by economy
stocks = stocks.groupby(['Economy', 'Date']).sum().reset_index()
#%% 
#pop is here: APEC_GDP_data_20240201.csv where 20240201 is the date of the model output: \transport_model_9th_edition\input_data\macro
date_id = get_latest_date_for_data_file('../transport_model_9th_edition/input_data/macro', 'APEC_GDP_data', r'APEC_GDP_data_\d{8}.csv')
macro = pd.read_csv('../transport_model_9th_edition/input_data/macro/APEC_GDP_data_'+date_id+'.csv')#economy_code	economy	year	variable	value	units
#find variable == population and date == 2021
pop = macro[macro['variable'] == 'population'][['economy_code', 'year', 'value']]
# pop = pop[pop['year'] == 2021]
#rename value to popualtion
pop = pop.rename(columns={'value': 'Population', 'economy_code': 'Economy', 'year': 'Date'})
pop = pop[['Economy', 'Population', 'Date']]
#rename 17_SIN to 17_SGP and 15_RP to 15_PHL
pop['Economy'] = pop['Economy'].replace('17_SIN', '17_SGP')
pop['Economy'] = pop['Economy'].replace('15_RP', '15_PHL')
#%%
#also get gdp per cpita
gdp_per_capita = macro[macro['variable'] == 'GDP_per_capita'][['economy_code', 'year', 'value']]
# gdp_per_capita = gdp_per_capita[gdp_per_capita['year'] == 2021]
gdp_per_capita = gdp_per_capita.rename(columns={'value': 'GDP_per_capita', 'economy_code': 'Economy', 'year': 'Date'})
gdp_per_capita = gdp_per_capita[['Economy', 'GDP_per_capita', 'Date']]
#rename 17_SIN to 17_SGP and 15_RP to 15_PHL
gdp_per_capita['Economy'] = gdp_per_capita['Economy'].replace('17_SIN', '17_SGP')

#%%

#units:
#stocks: vehicles millions
#pop: thousands of people
#density: people per km^2
# convert all to the same units
stocks['Stocks'] = stocks['Stocks']*10**6
pop['Population'] = pop['Population']*10**3

all_data = pd.merge(stocks, pop, on=['Economy', 'Date'], how='left')
#and density
all_data = pd.merge(all_data, density, on=['Economy'], how='left')
#and gdp per capita
all_data = pd.merge(all_data, gdp_per_capita, on=['Economy', 'Date'], how='left')
#calculate vehicle ownership per 1000 people
all_data['Vehicle_ownership_per_1000_people'] = all_data['Stocks']/all_data['Population']*1000
#convert to a % of the population, i.e. the cahance of owning a vehicle
all_data['Vehicle_ownership'] = all_data['Stocks']/all_data['Population']
all_data = all_data[['Economy', 'Economy_name', 'Date', 'Population density', 'Vehicle_ownership_per_1000_people', 'GDP_per_capita', 'Vehicle_ownership', 'Population', 'Stocks']]
all_data = all_data.dropna()

#then calc averages for each economy and date
all_data_avg = all_data.groupby(['Economy', 'Economy_name']).mean().reset_index()

#then filternfor only 2021 
all_data = all_data[all_data['Date'] == 2021]
#then merge, make suffixes avg
all_data = pd.merge(all_data, all_data_avg, on=['Economy', 'Economy_name'], suffixes=('', '_avg'))

#drop any outliers (singapore and hong kong have too high density)
# all_data = all_data[~all_data['Economy'].isin(['17_SGP', '06_HKC'])]
#make pop density have a log scale
all_data['Population density_avg'] = np.log(all_data['Population density_avg'])
all_data['Population density'] = np.log(all_data['Population density'])

mapping = {
    'High income, high density': ['08_JPN', '09_ROK', '18_CT'],
        'Lower income low density': ['04_CHL', '14_PE', '11_MEX', '13_PNG', '16_RUS'],
    'High income low density': ['12_NZ', '20_USA', '01_AUS', '03_CDA'],
    'Lower income high density': ['19_THA', '10_MAS', '15_PHL', '21_VN', '07_INA'],
    'Microstates': ['17_SGP', '06_HKC', '02_BD'],
    'China': ['05_PRC']
}

#map to categories
all_data['Category'] = None
for category, economies in mapping.items():
    all_data.loc[all_data['Economy'].isin(economies), 'Category'] = category
all_data = all_data.sort_values(['Category', 'GDP_per_capita', 'Population density'], ascending=[True, False, False])
#%%
#map colors for them
mapping_colors = {
    'High income, high density':'rgb(30, 144, 255)',  # Dodger Blue
    'Lower income low density': 'rgb(220, 20, 60)',  # Crimson
    'High income low density': 'rgb(34, 139, 34)',  # Forest Green
    'Lower income high density':  'rgb(255, 165, 0)',  # Orange
    'Microstates': 'rgb(186, 85, 211)',  # Medium Orchid
    'China': 'rgb(0, 206, 209)'  # Dark Turquoise
}
#now plot the data using px.scatter
import plotly.express as px
fig = px.scatter(all_data, x='Population density', y='Vehicle_ownership', text='Economy_name', color='Category', color_discrete_map=mapping_colors)
fig.update_traces(textposition='top center')
fig.update_layout(title='Vehicle ownership vs population density', xaxis_title='Population density (people per km^2 - log scale)', yaxis_title='Vehicle ownership (vehicles per person)')
#make ext bigger and points bigger
fig.update_layout(font=dict(size=18))
fig.update_traces(marker=dict(size=18))
#save the plot
fig.write_html('plotting_output/vehicle_ownership_vs_population_density.html')
#%%
#then create a 3d plot with gdp per capita as the z axis
fig = px.scatter_3d(all_data, x='Population density', y='Vehicle_ownership', z='GDP_per_capita', text='Economy_name', color='Category', color_discrete_map=mapping_colors)
fig.update_traces(textposition='top center')
fig.update_layout(title='Vehicle ownership vs population density vs GDP per capita', xaxis_title='Population density )people per km^2 - log scale)', yaxis_title='Vehicle ownership (vehicles per person)')
fig.write_html('plotting_output/vehicle_ownership_vs_population_density_vs_gdp_per_capita.html')

#%%
#and then do avgs of the above
fig = px.scatter(all_data, x='Population density_avg', y='Vehicle_ownership_avg', text='Economy_name', color='Category', color_discrete_map=mapping_colors)
fig.update_traces(textposition='top center')
fig.update_layout(title='Average vehicle ownership vs average population density', xaxis_title='Average population density (people per km^2 - log scale)', yaxis_title='Average vehicle ownership (vehicles per person)')
fig.update_layout(font=dict(size=18))
fig.update_traces(marker=dict(size=18))
fig.write_html('plotting_output/vehicle_ownership_vs_population_density_avg.html')

fig = px.scatter_3d(all_data, x='Population density_avg', y='Vehicle_ownership_avg', z='GDP_per_capita_avg', text='Economy_name', color='Category', color_discrete_map=mapping_colors)
fig.update_traces(textposition='top center')
fig.update_layout(title='Average vehicle ownership vs average population density vs average GDP per capita', xaxis_title='Average population density (people per km^2 - log scale)', yaxis_title='Average vehicle ownership (vehicles per person)')
fig.write_html('plotting_output/vehicle_ownership_vs_population_density_vs_gdp_per_capita_avg.html')

#%%

fig = px.bar(all_data, x='Economy', y='GDP_per_capita_avg', color='Category', color_discrete_map=mapping_colors)
#label the y axis as gdp per capita and the x axis will be dropped. then title it as Average gdp per capita between 2000 and 2050
#and amke text a bit bigger, and make the labels for x axis a bit vertical
fig.update_layout(
    yaxis_title='GDP per capita',
    title='Average GDP per capita between 2021 and 2060',
    font=dict(
        size=22
    ),
    xaxis_tickangle=-45,
    xaxis_title=None)
fig.write_html('plotting_output/gdp_per_capita_avg.html')



#%%
# %%

