#this will use data from the output of this system (stocks) and the output from C:\Users\finbar.maunsell\github\transport_data_system\grooming_code\1_extract_used_vehicle_data_itf.py 
#it will calculate the ratio of new to used vehicles for each country and vehicle type.
#it will then use this ratio to estimate the number of new vehicles that were used imports in the stocks data, apply this to economies which we dont have thisd data for , based on lvl of development, etc. and then plot it for analysis.

#%%
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import re
import datetime

os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')

import sys
folder_path = './aggregation_code'  # Replace with the actual path of the folder you want to add
sys.path.append(folder_path)
import utility_functions 
#%%
#get latest date of used vehicles data

# file_date = utility_functions.get_latest_date_for_data_file('./input_data/previous_selections/9th_dataset', 'combined_dataset')
# FILE_DATE_ID = 'DATE{}'.format(file_date)
# combined_data_9th = pd.read_csv('./input_data/previous_selections/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('./input_data/ITF', 'usedvehicles_PROCESSED')
FILE_DATE_ID = 'DATE{}'.format(file_date)
used_vehicles_data = pd.read_csv(f'input_data/ITF/{FILE_DATE_ID}_usedvehicles_PROCESSED_v1.2.csv')

#get latest output in output_data
file_date = utility_functions.get_latest_date_for_data_file('./output_data', 'combined_data')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data = pd.read_csv(f'output_data/combined_data_{FILE_DATE_ID}.csv')
#%%
#compare the two datasets
used_vehicles_data.columns
#'Exp_Country_Name', 'Exp_Country_ISO3', 'Imp_Country_Name',
#    'Imp_Country_ISO3', 'Vehicle_type', 'Year', 'Value',
#    'import_country_apec', 'export_country_apec', 'Measure', 'Dataset'
combined_data.columns
# 'economy', 'date', 'medium', 'measure', 'dataset', 'unit', 'fuel',
#        'comment', 'scope', 'frequency', 'drive', 'vehicle_type',
#        'transport_type', 'value'

#to simplify things, what we really care about is used vehicle imports. Knowing where they come from adds complications that we dont have enough certainty to deal with. so lets filter the used vehicles data to just where import_country_apec is not False and where export_country_apec is 'all'. Then we can compare these against the yearly sales, which would be the change in stocks each year. We should make sure to plot these agaisnt each other so we hae a visual representation of the data and cna spot any outliers/ effects of missing data or data from different sources
imports = used_vehicles_data[(used_vehicles_data['import_country_apec'] != 'False') & (used_vehicles_data['export_country_apec'] == 'all')]

#%%
#now do stocks data
#THERES A BIG ISSUE HERE. tO BE ACCURATE WE SHOULD BASE THE SALES OFF CHANGE IN STOCKS FOR SIMILAR 'dataset' VALUES AS WELL AS ALL OTHERS, BUT THIS LEAVES US WITH VERY LITTLE DATA. sO WE CAN HAVE A VARIABLE BELOW WHICH DETERMINES IF WE DO THSI:
USE_DATASET_FILTER = True
GROUP_COLS = ['economy', 'medium', 'measure', 'unit', 'fuel','scope', 'frequency','vehicle_type','transport_type']
if USE_DATASET_FILTER:
    GROUP_COLS = GROUP_COLS + ['dataset']
    
combined_data_sales = combined_data[combined_data['measure'] == 'stocks'].copy()

#theres often duplicates when we ignore the dataset col, from having drives come from different datasets (e.g. ev data comes from iea whereas ice data is from the econmoy). For simplicity let's just drop ev data from the df. its such a small amount of data that it wont make a difference
combined_data_sales = combined_data_sales[combined_data_sales['drive'].isin(['ice_d', 'ice_g'])]

#group and sum by 'economy', 'date', 'medium', 'measure', 'unit', 'fuel','scope', 'frequency','vehicle_type',       'transport_type'
combined_data_sales = combined_data_sales.groupby(GROUP_COLS+['date']).agg({'value':'sum'}).reset_index()
#%%


# # We should sum these together and make their dataset a list of the datasets they came from, then order that list and concatenate them back together
# #if there are duplicates when we ignore the dataset col, lets separate them then sum them together and make their dataset a list of the datasets they came from, then order that list and concatenate them back together
# # if USE_DATASET_FILTER:
# #     GROUP_COLS_no_dataset = GROUP_COLS.copy()
# #     GROUP_COLS_no_dataset.remove('dataset')
# #     if combined_data_sales.duplicated(GROUP_COLS_no_dataset+['date']).any():
# #         duplicates = combined_data_sales[combined_data_sales.duplicated(GROUP_COLS_no_dataset+['date'], keep=False)]
# #         duplicates['dataset'] = 'all'
# #         combined_data_sales = combined_data_sales[~combined_data_sales.duplicated(GROUP_COLS_no_dataset+['date'], keep=False)]
# #         combined_data_sales = pd.concat([combined_data_sales, duplicates], axis=0)
# #nah that doesnt work.    

#check for duplicates when we ignore the dataset col
INCLUDE_DUPLICATES = True
GROUP_COLS_no_dataset = GROUP_COLS.copy()
GROUP_COLS_no_dataset.remove('dataset')
if combined_data_sales.duplicated(GROUP_COLS_no_dataset+['date']).any():
    duplicates = combined_data_sales[combined_data_sales.duplicated(GROUP_COLS_no_dataset+['date'], keep=False)]
    print('There are duplicates when we ignore the dataset col.')
    print(duplicates)
    
    if INCLUDE_DUPLICATES:
        print('Including duplicates in the dataset')
        duplicates['dataset'] = 'all'
        combined_data_sales = combined_data_sales[~combined_data_sales.duplicated(GROUP_COLS_no_dataset+['date'], keep=False)]
        
        combined_data_sales = pd.concat([combined_data_sales, duplicates], axis=0)
    else:
        print('Dropping duplicates')
        combined_data_sales = combined_data_sales[~combined_data_sales.duplicated(GROUP_COLS_no_dataset+['date'], keep=False)]
#we want to calc diff by year but only when the year is 1 ahead of the previous year. So we need to fill in the missing years with nas
#so group by each row and fill in the missing years:
unique_years = combined_data_sales['date'].unique()

index_df = combined_data_sales[GROUP_COLS].drop_duplicates()
#then we can join this to the unique years to get all the combinations of the above cols and the years
index_df['key'] = 0
unique_years_df = pd.Series(unique_years, name='date').to_frame()
unique_years_df['key'] = 0
index_df = index_df.merge(unique_years_df, on='key').drop(columns='key')

combined_data_sales = index_df.merge(combined_data_sales, how='left').reset_index(drop=True)

#%%
#sort by date
combined_data_sales = combined_data_sales.sort_values(GROUP_COLS + ['date'])
#find the change in stocks each year
combined_data_sales['sales'] = combined_data_sales.groupby(GROUP_COLS)['value'].diff()
#%%
#drop na rows where sales and value are na )but not either or
combined_data_sales = combined_data_sales.dropna(subset=['sales', 'value'], how='all')
#%%
#great. now we can compare the sales to the imports
#Let's join on economy, vehicle_type, date. amke sure to call Value in imports 'imports' and value in sales 'stocks'
combined_data_sales = combined_data_sales.rename(columns={'value':'stocks'})
imports = imports.rename(columns={'Value':'imports'})
combined_data_sales = combined_data_sales.merge(imports, left_on=['economy', 'vehicle_type', 'date'], right_on=['import_country_apec', 'Vehicle_type', 'Year'], how='left')
merged_df = combined_data_sales.copy()
#calculate the ratio of imports to sales
combined_data_sales['imports_to_sales_ratio'] = combined_data_sales['imports'] / combined_data_sales['sales']

#now drop where there is no data for imports
combined_data_sales = combined_data_sales.dropna(subset=['imports_to_sales_ratio'])
#%%
#now plot. First we will melt so we have all measure in one column and then we can facet by measure and make the y axis independent
GROUP_COLS_no_measure = GROUP_COLS.copy()
GROUP_COLS_no_measure.remove('measure') 
combined_data_sales_melted =combined_data_sales[GROUP_COLS_no_measure + ['date','imports_to_sales_ratio', 'sales', 'imports']].copy()
combined_data_sales_melted = combined_data_sales_melted.melt(id_vars=GROUP_COLS_no_measure+['date'], value_vars=['imports_to_sales_ratio', 'sales', 'imports'], var_name='measure', value_name='value')
#%%
#where measure is sales or imports create another col and set that to 'absolute' and then set other to 'ratio'
mapping = {
    'imports_to_sales_ratio':'ratio',
    'sales':'absolute',
    'imports':'absolute'
}
combined_data_sales_melted['measure_type'] = combined_data_sales_melted['measure'].map(mapping)

#%%
#sort the vlaues by  categories we will plot using
combined_data_sales_melted = combined_data_sales_melted.sort_values(['date', 'economy','measure', 'measure_type'])
#%%
fig = px.line(combined_data_sales_melted, x='date', y='value', color='economy', line_dash='measure', facet_row='measure_type',  facet_col_wrap=3,   hover_data=['dataset'], title='Imports to sales ratio, sales and imports by economy and year')

fig.update_yaxes(matches=None)
fig.write_html('plotting_output/analysis/imports_to_sales_ratio.html', auto_open=True)
#%%

#this data doesnt seem to be ready for analyss. maybe give it some time before checkingif itf have added more data. seems like the data is too sparse to be useful or the numbers are too small to be useful.Or even that since the data is from different sources than our registrations data its too hard to compare. (e.g. nz imports are higher than sales which is not possible)
#%%