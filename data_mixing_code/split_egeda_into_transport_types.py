
#%%
#set working directory as one folder back
import os
import re
import pandas as pd
import numpy as np
import datetime
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')
import utility_functions as utility_functions

#%%
#load egeda data
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/EGEDA/', 'EGEDA_transport_output')
FILE_DATE_ID = 'DATE{}'.format(file_date)
EGEDA_transport_output = pd.read_csv('intermediate_data/EGEDA/EGEDA_transport_output' + FILE_DATE_ID + '.csv')

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/8th_edition_transport_model/', 'eigth_edition_transport_data_final_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
eigth_edition_transport_data = pd.read_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_final_{}.csv'.format(FILE_DATE_ID))
#%%
#get ratios of passenger to freight for total energy use in other datasets then apply it to the egeda datya
# eigth_edition_transport_data.columns#'Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Date', 'Economy',
    #    'Frequency', 'Measure', 'Unit', 'Value', 'Dataset', 'Source'
#%%
#pviot so we ahve a column for passenger km and a column for freight km form the measure col
p_f_ratio = eigth_edition_transport_data.copy()
p_f_ratio.drop(['Transport Type','Unit'], axis=1, inplace=True)
#drop duplicates
p_f_ratio.drop_duplicates(inplace=True)
#filter for just passenger and freight km
p_f_ratio = p_f_ratio[p_f_ratio['Measure'].isin(['passenger_km','freight_tonne_km'])]
eigth_edition_transport_data_pivot = p_f_ratio.pivot(index=['Medium', 'Vehicle Type', 'Drive', 'Date', 'Economy','Frequency', 'Dataset', 'Source'], columns='Measure', values='Value')
eigth_edition_transport_data_pivot.reset_index(inplace=True)

#get ratio
eigth_edition_transport_data_pivot['passenger_freight_ratio'] = eigth_edition_transport_data_pivot['passenger_km']/eigth_edition_transport_data_pivot['freight_tonne_km']
#%%
#filter for Source = Reference
eigth_edition_transport_data_pivot = eigth_edition_transport_data_pivot[eigth_edition_transport_data_pivot['Source']=='Reference']
#filter for fuel = total in egeda
EGEDA_transport_output = EGEDA_transport_output[EGEDA_transport_output['Fuel']=='19 Total']
EGEDA_transport_output.drop(['Drive', 'Transport Type', 'Measure', 'Unit', 'Vehicle Type'], axis=1, inplace=True)

#%%
#join on data from egeda
EGEDA_merged = pd.merge(EGEDA_transport_output, eigth_edition_transport_data_pivot, how='right', on=['Medium',  'Date', 'Economy'])

#times passenger_freight_ratio by value to get passenger value
EGEDA_merged['passenger'] = EGEDA_merged['Value']*EGEDA_merged['passenger_freight_ratio']
EGEDA_merged['freight'] = EGEDA_merged['Value']-EGEDA_merged['passenger']

#create dataset and source columns as Energy_non_road, EGEDA/8th_ref 
EGEDA_merged['Dataset'] = 'Energy_non_road_est'
EGEDA_merged['Source'] = 'EGEDA/8th_ref'

#freq = annual
EGEDA_merged['Frequency'] = 'Yearly'

#drop any cols ending in _x or _y
EGEDA_merged.drop([col for col in EGEDA_merged.columns if col.endswith('_x') or col.endswith('_y')], axis=1, inplace=True)

#drop the passenger_freight_ratio col as well as other vlaues we don't need
EGEDA_merged.drop(['passenger_freight_ratio', 'Value', 'passenger_km', 'freight_tonne_km'], axis=1, inplace=True)
#now melt so  we have a single value column
EGEDA_merged = pd.melt(EGEDA_merged, id_vars=['Medium', 'Date', 'Economy', 'Frequency', 'Dataset', 'Source', 'Drive', 'Vehicle Type'], value_vars=['passenger', 'freight'], var_name='Transport Type', value_name='Value')
#reset index
EGEDA_merged.reset_index(inplace=True, drop=True)
#make Measure =  'Energy'
EGEDA_merged['Measure'] = 'Energy'

EGEDA_merged['Unit'] = 'PJ'

#%%
#save
EGEDA_merged.to_csv('./intermediate_data/estimated/EGEDA_merged{}.csv'.format(FILE_DATE_ID), index=False)
#%%