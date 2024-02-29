
"""
data from esto is not split into transport types for non road mediums. So we will use the splits from other datasets to do this.

Note that we updated this in 12/dec/2023 to use data from the 9th outlook esto input, rather than earlier esto data. 
It is a bit unsure why only 2017 is used. perhaps we should use all available years???
"""
#%%
#set working directory as one folder back
import os
import re
import pandas as pd
import numpy as np
import datetime
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')
import sys
sys.path.append("./aggregation_code")
import utility_functions as utility_functions

#%%
#load egeda data
# file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/EGEDA/', 'EGEDA_transport_output')
# FILE_DATE_ID = 'DATE{}'.format(file_date)
# EGEDA_transport_output = pd.read_csv('intermediate_data/EGEDA/EGEDA_transport_output' + FILE_DATE_ID + '.csv')

#this data was created by the 8th outlook edition transport model
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/8th_edition_transport_model/', 'eigth_edition_transport_data_final_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
eigth_edition_transport_data = pd.read_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_final_{}.csv'.format(FILE_DATE_ID))

#%%
#this data is created in 1_format_outlook_esto_data.py
file_date = utility_functions.get_latest_date_for_data_file(f'intermediate_data/EGEDA/', f'_9th_outlook_esto.csv')
esto_9th_outlook= pd.read_csv(f'intermediate_data/EGEDA/DATE{file_date}_9th_outlook_esto.csv')

#%%
#extract data from esto_9th_outlook esto energy dataset
# EGEDA_transport_output = esto_9th_outlook[esto_9th_outlook['Fuel_Type']=='19 Total']
EGEDA_transport_output = esto_9th_outlook.copy()
EGEDA_transport_output = EGEDA_transport_output.melt(id_vars=['Economy', 'Measure', 'Vehicle Type', 'Medium', 'Transport Type', 'Drive', 'Scenario', 'Unit'], var_name='Date', value_name='Value')
EGEDA_transport_output= EGEDA_transport_output[['Value', 'Medium', 'Date', 'Economy']]
#groupby and sum
EGEDA_transport_output = EGEDA_transport_output.groupby(['Medium', 'Date', 'Economy']).sum().reset_index()

#drop medium = nonspecified and pipeline
EGEDA_transport_output = EGEDA_transport_output[EGEDA_transport_output['Medium']!='nonspecified']
EGEDA_transport_output = EGEDA_transport_output[EGEDA_transport_output['Medium']!='pipeline']

#where economy is 17_SIN, set to 17_SGP and where 15_RP set to 15_PHL
EGEDA_transport_output.loc[EGEDA_transport_output['Economy']=='17_SIN', 'Economy'] = '17_SGP'
EGEDA_transport_output.loc[EGEDA_transport_output['Economy']=='15_RP', 'Economy'] = '15_PHL'

#grab only data for 2017 and then make the date = 2017-12-31
EGEDA_transport_output = EGEDA_transport_output[EGEDA_transport_output['Date']=='2017']#TODODOTODOTODODOD wat was i doing here? why am iu useing 1980? (changed it to 2017 in case that was right thing to do)
EGEDA_transport_output['Date'] = '2017-12-31'
#%%
#get ratios of passenger to freight for total energy use in other datasets then apply it to the egeda datya
# eigth_edition_transport_data.columns#'Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Date', 'Economy',
    #    'Frequency', 'Measure', 'Unit', 'Value', 'Dataset', 'Source'

#filter for medium = road and then group by sum for everything except drive and vehicle type. then set drive and vehicle type to np.nan
eigth_edition_transport_data_road_total = eigth_edition_transport_data[eigth_edition_transport_data['Medium']=='road']
cols = eigth_edition_transport_data_road_total.columns.to_list()
cols.remove('Drive')
cols.remove('Vehicle Type')
cols.remove('Value')

#where economy is 17_SIN, set to 17_SGP and where 15_RP set to 15_PHL
eigth_edition_transport_data_road_total.loc[eigth_edition_transport_data_road_total['Economy']=='17_SIN', 'Economy'] = '17_SGP'
eigth_edition_transport_data_road_total.loc[eigth_edition_transport_data_road_total['Economy']=='15_RP', 'Economy'] = '15_PHL'

eigth_edition_transport_data_road_total = eigth_edition_transport_data_road_total.groupby(cols).sum().reset_index()
eigth_edition_transport_data_road_total['Drive'] = np.nan
eigth_edition_transport_data_road_total['Vehicle Type'] = np.nan
#concat with original data
eigth_edition_transport_data = pd.concat([eigth_edition_transport_data, eigth_edition_transport_data_road_total], axis=0)
#now we can use that to estimate total energy use per transport type in road.

#and then since we dont have drive or vehicle type in egeda, drop all rows where drive or vehicle type is not nan
eigth_edition_transport_data = eigth_edition_transport_data[eigth_edition_transport_data['Drive'].isna()]
eigth_edition_transport_data = eigth_edition_transport_data[eigth_edition_transport_data['Vehicle Type'].isna()]
#%%
# #pviot so we ahve a column for passenger km and a column for freight km form the measure col
# p_f_ratio = eigth_edition_transport_data.copy()
# p_f_ratio.drop(['Transport Type','Unit'], axis=1, inplace=True)
# #drop duplicates
# p_f_ratio.drop_duplicates(inplace=True)
# #filter for just passenger and freight km
# p_f_ratio = p_f_ratio[p_f_ratio['Measure'].isin(['passenger_km','freight_tonne_km'])]#TODO WHY ARE WE USING ACTIVITY HERE TBH
# eigth_edition_transport_data_pivot = p_f_ratio.pivot(index=['Medium', 'Vehicle Type', 'Drive', 'Date', 'Economy','Frequency', 'Dataset', 'Source'], columns='Measure', values='Value')
# eigth_edition_transport_data_pivot.reset_index(inplace=True)

# #set any nan values in passenger_km and freight_tonne_km to 0
# eigth_edition_transport_data_pivot['passenger_km'].fillna(0, inplace=True)
# eigth_edition_transport_data_pivot['freight_tonne_km'].fillna(0, inplace=True)
# #get poportion in passenger
# eigth_edition_transport_data_pivot['passenger_to_freight_prop'] = eigth_edition_transport_data_pivot['passenger_km']/(eigth_edition_transport_data_pivot['freight_tonne_km']+eigth_edition_transport_data_pivot['passenger_km'])

#%%
#CREATE VERSION OF CELL ABOVE BUT USE ENERGY. wE WILL HAVE TO PIVOT SO WE AHVE PASSENGER_ENERGY AND FREIGHT_ENERGY COLS
p_f_ratio = eigth_edition_transport_data.copy()
p_f_ratio = p_f_ratio[p_f_ratio['Measure'].isin(['Energy'])]
p_f_ratio.drop(['Measure','Unit'], axis=1, inplace=True)
eigth_edition_transport_data_pivot = p_f_ratio.pivot(index=['Medium', 'Vehicle Type', 'Drive', 'Date', 'Economy','Frequency', 'Dataset', 'Source'], columns='Transport Type', values='Value')
eigth_edition_transport_data_pivot.reset_index(inplace=True)
#rename passenger and freight to passenger_energy and freight_energy
eigth_edition_transport_data_pivot.rename(columns={'passenger':'passenger_energy', 'freight':'freight_energy'}, inplace=True)
#set any nan values in passenger_energy and freight_energy to 0
eigth_edition_transport_data_pivot['passenger_energy'].fillna(0, inplace=True)
eigth_edition_transport_data_pivot['freight_energy'].fillna(0, inplace=True)
#get poportion in passenger
eigth_edition_transport_data_pivot['passenger_to_freight_prop'] = eigth_edition_transport_data_pivot['passenger_energy']/(eigth_edition_transport_data_pivot['freight_energy']+eigth_edition_transport_data_pivot['passenger_energy'])

#%%
#for any rows where the prop is nan remove the row as it would be 0/0 and that is not going to give us anything useful
eigth_edition_transport_data_pivot.dropna(subset=['passenger_to_freight_prop'], inplace=True)
#%%
#filter for Source = Reference
eigth_edition_transport_data_pivot = eigth_edition_transport_data_pivot[eigth_edition_transport_data_pivot['Source']=='Reference']
#filter for fuel = total in egeda

#where economy is 17_SIN, set to 17_SGP and where 15_RP set to 15_PHL
eigth_edition_transport_data_pivot.loc[eigth_edition_transport_data_pivot['Economy']=='17_SIN', 'Economy'] = '17_SGP'
eigth_edition_transport_data_pivot.loc[eigth_edition_transport_data_pivot['Economy']=='15_RP', 'Economy'] = '15_PHL'
#%%
##################################################
#join on data from egeda
EGEDA_merged = pd.merge(EGEDA_transport_output, eigth_edition_transport_data_pivot, how='right', on=['Medium',  'Date', 'Economy'])
##################################################
#%%

#FILL MISSING DATA
#we might find that some data is missing from egeda and will show up as nan in the vlaue col. show the user. where that data is available for a different date then see if we can use that
missing = EGEDA_merged[EGEDA_merged['Value'].isna()]
#print them for the user and get them to manually replace the vlaue below:
if len(missing) > 0:
    raise ValueError('These values are missing from EGEDA_merged. Please replace them in the code below: {}'.format(missing))
    # print('These values are missing from EGEDA_merged. Please replace them in the code below: {}'.format(missing))

# #1 missing_peru_2017_ship (no longer an issue)
# if len(missing.loc[(missing['Medium']=='ship') & (missing['Date']=='2017-12-31') & (missing['Economy']=='14_PE')]) > 0:
#     #extract new vlaue from egeda
#     ship_peru_2017_new_value = EGEDA_transport_output.loc[(EGEDA_transport_output['Medium']=='ship') & (EGEDA_transport_output['Date']=='2020-12-31') & (EGEDA_transport_output['Economy']=='14_PE')]
#     #replace value in EGEDA_merged
#     EGEDA_merged.loc[(EGEDA_merged['Medium']=='ship') & (EGEDA_merged['Date']=='2017-12-31') & (EGEDA_merged['Economy']=='14_PE'), 'Value'] = ship_peru_2017_new_value['Value'].values[0]

#%%
#times passenger_to_freight_prop by value to get passenger value
EGEDA_merged['passenger'] = EGEDA_merged['Value']*EGEDA_merged['passenger_to_freight_prop']
EGEDA_merged['freight'] = EGEDA_merged['Value']-EGEDA_merged['passenger']

#%%
#SEMI TEMPORARY ERROR FIX, SPLITTING 
#%%
#create dataset and source columns as Energy_non_road, EGEDA/8th_ref 
EGEDA_merged['Dataset'] = 'EGEDA_split_into_transport_types'
EGEDA_merged['Source'] = '8th_transport_splits_9th_outlook_esto'

#freq = annual
EGEDA_merged['Frequency'] = 'Yearly'

#drop any cols ending in _x or _y
EGEDA_merged_clean = EGEDA_merged.copy()

EGEDA_merged_clean.drop([col for col in EGEDA_merged_clean.columns if col.endswith('_x') or col.endswith('_y')], axis=1, inplace=True)

#drop the passenger_to_freight_prop col as well as other vlaues we don't need
EGEDA_merged_clean.drop(['passenger_to_freight_prop', 'Value', 'passenger_energy', 'freight_energy'], axis=1, inplace=True)
#now melt so  we have a single value column
EGEDA_merged_clean = pd.melt(EGEDA_merged_clean, id_vars=['Medium', 'Date', 'Economy', 'Frequency', 'Dataset', 'Source', 'Drive', 'Vehicle Type'], value_vars=['passenger', 'freight'], var_name='Transport Type', value_name='Value')
#reset index
EGEDA_merged_clean.reset_index(inplace=True, drop=True)
#make Measure =  'Energy'
EGEDA_merged_clean['Measure'] = 'Energy'

EGEDA_merged_clean['Unit'] = 'PJ'
EGEDA_merged_clean['Fuel'] = 'all'
EGEDA_merged_clean['Scope'] = 'national'
#%%
#where vehicle type is na and the medium is road then set it to all, and same for drive
EGEDA_merged_clean.loc[(EGEDA_merged_clean['Medium']=='road') & (EGEDA_merged_clean['Vehicle Type'].isna()), 'Vehicle Type'] = 'All'
EGEDA_merged_clean.loc[(EGEDA_merged_clean['Medium']=='road') & (EGEDA_merged_clean['Drive'].isna()), 'Drive'] = 'All'

#%%
#save
FILE_DATE_ID = 'DATE{}'.format(datetime.datetime.now().strftime('%Y%m%d'))
EGEDA_merged_clean.to_csv('./intermediate_data/estimated/EGEDA_split_into_transport_types{}.csv'.format(FILE_DATE_ID), index=False)
#%%














analyse = False
if analyse:
    #DO ANALYSIS:
    #we especailly want to analyse the accuracy of the EGEDA data since it seems some vlaues are drastically underestimated (meaning that data is somewhere else in the EGEDA data)
    #we wantr to graph a bar chart for each economy which shows the proportion of energy use in each non-road medium. We want the facets to be individually scaled so that we can see the differences in the data
    #import plotly
    import plotly.express as px
    import plotly.graph_objects as go
    #plot EGEDA_merged using Medium col as x, Value col as y, and facet_col = Economy
    #drop road from medium
    EGEDA_merged_no_road = EGEDA_merged.copy()
    EGEDA_merged_no_road = EGEDA_merged_no_road[EGEDA_merged_no_road['Medium']!='road']
    #plot and make sure make each facets y axis scale according to the data in that facet
    fig = px.bar(EGEDA_merged_no_road, x='Medium', y='Value', facet_col='Economy', facet_col_wrap=3, barmode='group')
    fig = fig.update_yaxes(matches=None)
    fig.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
        

    fig.update_layout(height=1000, width=1000)
    fig.show()
    #save in html
    fig.write_html('./plotting_output/exploration_archive/EGEDA_merged{}.html'.format(FILE_DATE_ID))
# %%
analyse = False
if analyse:
    #to compare against the above we will load in the ato data and extract data for sheet CLC-VRE-013, and plot the percent aneegry share for 2019 using the same method as above
    #load in ato data
    file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/ATO_data/', 'ATO_data_cleaned_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    ATO_dataset_clean = pd.read_csv('intermediate_data/ATO_data/ATO_data_cleaned_{}.csv'.format(FILE_DATE_ID))

    file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/ATO_data/', 'ATO_data_others_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    ATO_data_others = pd.read_csv('intermediate_data/ATO_data/ATO_data_others_{}.csv'.format(FILE_DATE_ID))


    #filter
    ATO_data_others = ATO_data_others[ATO_data_others['sheet']=='CLC-VRE-013']


    #extract 2019 data
    ATO_data_others_2019 = ATO_data_others[ATO_data_others['year']==2019]
    #drop medium = 'road'
    ATO_data_others_2019 = ATO_data_others_2019[ATO_data_others_2019['medium']!='Road']

    #plot
    import plotly.express as px
    import plotly.graph_objects as go
    fig = px.bar(ATO_data_others_2019, x='medium', y='value', facet_col='economy', facet_col_wrap=3, barmode='group')
    fig = fig.update_yaxes(matches=None)
    fig.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))

    fig.update_layout(height=1000, width=1000)
    fig.show()
    #save in html
    fig.write_html('./plotting_output/exploration_archive/ATO_data_others_2019.html'.format(FILE_DATE_ID))


    #also plot datra from CLC-VRE-001 in the same way
    ATO_dataset_clean = ATO_dataset_clean[ATO_dataset_clean['Date']=='2019-12-31']
    #drop medium = 'road'
    ATO_dataset_clean = ATO_dataset_clean[ATO_dataset_clean['Medium']!='road']
    #and all
    ATO_dataset_clean = ATO_dataset_clean[ATO_dataset_clean['Medium']!='all']
    #get sheet
    ATO_dataset_clean = ATO_dataset_clean[ATO_dataset_clean['Sheet']=='CLC-VRE-001']
    #plot
    import plotly.express as px
    import plotly.graph_objects as go
    fig = px.bar(ATO_dataset_clean, x='Medium', y='Value', facet_col='Economy', facet_col_wrap=3, barmode='group')
    fig = fig.update_yaxes(matches=None)
    fig.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))

    fig.update_layout(height=1000, width=1000)
    fig.show()
    #save in html
    fig.write_html('./plotting_output/exploration_archive/ATO_dataset_clean_2019.html'.format(FILE_DATE_ID))
    #SAVE PNG
    fig.write_image('./plotting_output/exploration_archive/ATO_dataset_clean_2019.png'.format(FILE_DATE_ID))


    ATO_dataset_clean = pd.read_csv('intermediate_data/ATO_data/ATO_data_cleaned_{}.csv'.format(FILE_DATE_ID))
    ATO_dataset_clean = ATO_dataset_clean[ATO_dataset_clean['Date']=='2019-12-31']
    #plot using road as well
    ATO_dataset_clean = ATO_dataset_clean[ATO_dataset_clean['Medium']!='all']
    #get sheet
    ATO_dataset_clean = ATO_dataset_clean[ATO_dataset_clean['Sheet']=='CLC-VRE-001']
    #plot
    import plotly.express as px
    import plotly.graph_objects as go
    fig = px.bar(ATO_dataset_clean, x='Medium', y='Value', facet_col='Economy', facet_col_wrap=3, barmode='group')
    fig = fig.update_yaxes(matches=None)
    fig.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))

    fig.update_layout(height=1000, width=1000)
    fig.show()
    #save in html
    fig.write_html('./plotting_output/exploration_archive/ATO_dataset_clean_2019_incl_road.html'.format(FILE_DATE_ID))
    #SAVE PNG
    fig.write_image('./plotting_output/exploration_archive/ATO_dataset_clean_2019_incl_road.png'.format(FILE_DATE_ID))

# %%
