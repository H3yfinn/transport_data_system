#create functions to estiamte data using other data, in a way that can be repeated
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
from PIL import Image
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import itertools
import data_formatting_functions
import logging
import analysis_and_plotting_functions
import utility_functions
logger = logging.getLogger(__name__)

#eg from
#  Estimate Energy and passenger km using stocks:
# vkm = mileage * stocks
# energy = vkm * efficiency
# pkm = vkm * occupancy

#put it all thorugh calculate_new_data() which will:
# Using mileage, eff, occ and stocks we can estimate passenger km and energy. Where we are missing data eg. stocks, we will leave an na. 
# enable adjusting the mileage, occ and eff data by specified ranges to get a range of options

def calculate_energy_and_passenger_km(stocks_mileage_occupancy_efficiency_combined_data_concordance, paths_dict):

    #make combined data wide so we have a column for each measure
    INDEX_COLS_no_measure = paths_dict['INDEX_COLS'].copy()
    INDEX_COLS_no_measure.remove('measure')
    data = stocks_mileage_occupancy_efficiency_combined_data_concordance.pivot(index = INDEX_COLS_no_measure, columns = 'measure', values = 'value').reset_index()
    #to prevent any issues with div by 0 we will replace all 0s with nans. then any nans timesed by anything will be nan
    data = data.replace(0,np.nan)


    #TODO REMOVE THIS WHEN WE HAVE REAL DATA
    logger.info('\n\n\ndata=estimate_fake_values(data) TODO REMOVE THIS WHEN WE HAVE REAL DATA\n\n\n\n')
    data=estimate_fake_values(data)
    #TODO REMOVE THIS WHEN WE HAVE REAL DATA



    data['vehicle_km'] = data['mileage'] * data['stocks']
    data['passenger_km'] = data['vehicle_km'] * data['occupancy']
    data['energy'] = data['vehicle_km'] * data['new_vehicle_efficiency']
    
    #make long again
    data = data.melt(id_vars = INDEX_COLS_no_measure, var_name = 'measure', value_name = 'value')#todo i dont know how this will interact with the other possible columns we'll have. maybrwe should have id vars = data.columns

    #drop any nans in value col
    data = data.dropna(subset = ['value'])

    #set units based on measure. if the measure is not in the dict then leave it as what unit was in the original data 
    measure_to_units_dict = {'vehicle_km': 'km', 'passenger_km': 'passenger_km', 'energy': 'pj'}
    data['unit_original'] = data['unit']
    data['unit'] = data['measure'].map(measure_to_units_dict)
    data['unit'] = data['unit'].fillna(data['unit_original'])
    data = data.drop(columns = ['unit_original'])
    
    #set dataset to 'estimated'
    data['dataset'] = 'estimated $ calculate_energy_and_passenger_km()'
    #set comment to 'no_comment'
    data['comment'] = 'no_comment'
    #todo: add in the range options
    return data

def estimate_fake_values(data):
    #where we have na in mileage, occupancy or new_vehicle_efficiency we will replace it with a fake value of 1. this is so that we can still calculate the other values to test this program.
    data['mileage'] = data['mileage'].fillna(1)
    data['occupancy'] = data['occupancy'].fillna(1)
    data['new_vehicle_efficiency'] = data['new_vehicle_efficiency'].fillna(1)
    return data




#%%
def estimate_road_freight_energy_use(unfiltered_combined_data,passenger_road_combined_data, set_negative_to_one = True):
    #load in the combined_data from paths_dict['combined_data']
    egeda_energy_road_selection_dict = {'measure': 
        ['energy'],
    'medium': ['road'],
    'dataset': ['energy_non_road_est $ egeda/8th_ref']}
    egeda_energy_road_combined_data = data_formatting_functions.filter_for_specifc_data(egeda_energy_road_selection_dict, unfiltered_combined_data)

    #and also grab the energy data for passenger road
    passenger_road_combined_data = passenger_road_combined_data[passenger_road_combined_data['measure'] == 'energy']

    #sum the energy use for passenger road by economy and date
    passenger_road_combined_data = passenger_road_combined_data.groupby(['economy','date']).sum().reset_index()

    #sum the energy use for egeda energy data by transport type, economy and date
    egeda_energy_road_combined_data = egeda_energy_road_combined_data.groupby(['transport_type','economy','date']).sum().reset_index()

    #pivot transport type to a column
    egeda_energy_road_combined_data = egeda_energy_road_combined_data.pivot(index = ['economy','date'], columns = 'transport_type', values = 'value').reset_index()

    #calcualte total road energy use by economy and date
    egeda_energy_road_combined_data['egeda_total_road_energy_use'] = egeda_energy_road_combined_data['freight'] + egeda_energy_road_combined_data['passenger']

    #merge the passenger road energy use with the egeda energy data
    #first rename value to calculated_passenger_road_energy_use
    passenger_road_combined_data = passenger_road_combined_data.rename(columns = {'value': 'calculated_passenger_road_energy_use'})
    egeda_energy_road_combined_data = egeda_energy_road_combined_data.merge(passenger_road_combined_data, on = ['economy','date'], how = 'outer')

    analysis_and_plotting_functions.analyse_missing_energy_road_data(egeda_energy_road_combined_data)   

    #calcualte the remaining freight energy use as total_road_energy_use - calculated_passenger_road_energy_use
    egeda_energy_road_combined_data['calculated_remaining_freight_energy_use'] = egeda_energy_road_combined_data['egeda_total_road_energy_use'] - egeda_energy_road_combined_data['calculated_passenger_road_energy_use']
        
    #check if any values in calculated_remaining_freight_energy_use are negative, if so throw error
    if any(egeda_energy_road_combined_data['calculated_remaining_freight_energy_use'] < 0):
        if set_negative_to_one:
            egeda_energy_road_combined_data['calculated_remaining_freight_energy_use'] = egeda_energy_road_combined_data['calculated_remaining_freight_energy_use'].apply(lambda x: 1 if x < 0 else x)
        else:
            raise ValueError('calculated_remaining_freight_energy_use is negative for at least one economy and date')

    #compare that vlaue to the egeda energy use for freight by graphing it:
    #frist rename columns then put all values data in one column with the names of the columns in a new column called 'type'
    egeda_energy_road_combined_data = egeda_energy_road_combined_data.rename(columns = {'freight': 'egeda_freight', 'passenger': 'egeda_passenger'})
    egeda_energy_road_combined_data = pd.melt(egeda_energy_road_combined_data, id_vars = ['economy','date'], var_name = 'type', value_name = 'value')

    analysis_and_plotting_functions.graph_egeda_road_energy_use_vs_calculated(egeda_energy_road_combined_data)
    do_prompt = False
    if do_prompt:
        prompt = 'Does the graph show that the calculated remaining road energy use matches the egeda energy use for freight? y/n'

        input1 = input(prompt)
        if input1 == 'y':
            pass
        elif input1 == 'n':
            #save results to csv and exit
            egeda_energy_road_combined_data.to_csv('egeda_energy_road_combined_data.csv')
            logger.info('egeda_energy_road_combined_data saved to csv')
            sys.exit()
    
    egeda_energy_road_combined_data = clean_energy_and_passenger_km(unfiltered_combined_data,egeda_energy_road_combined_data)
    
    return egeda_energy_road_combined_data


def clean_energy_and_passenger_km(unfiltered_combined_data,egeda_energy_road_combined_data):
    #if the calculated remaining road energy use is suitable then we can use the calculated remaining road energy use as the energy use for freight
    #so clean up the data and return it as the freight energy use for each eocnomy for each date
    egeda_energy_road_combined_data = egeda_energy_road_combined_data[egeda_energy_road_combined_data['type'] == 'calculated_remaining_freight_energy_use']
    egeda_energy_road_combined_data = egeda_energy_road_combined_data[['economy','date','value']]

    #create columns for everything in the combined data columns:
    egeda_energy_road_combined_data['measure'] = 'energy'
    egeda_energy_road_combined_data['unit'] = 'pj'
    egeda_energy_road_combined_data['medium'] = 'road'
    egeda_energy_road_combined_data['transport_type'] = 'freight'
    egeda_energy_road_combined_data['dataset'] = 'calculation $ egeda'
    egeda_energy_road_combined_data['unit'] = 'pj'
    egeda_energy_road_combined_data['fuel'] = 'all'
    egeda_energy_road_combined_data['comment'] = 'no_comment'
    egeda_energy_road_combined_data['scope'] = 'national'
    egeda_energy_road_combined_data['frequency'] = 'yearly'
    egeda_energy_road_combined_data['drive'] = 'all'
    egeda_energy_road_combined_data['vehicle_type'] = 'all'

    #see what cols are different between the two dataframes and if they are differnt then show user and throw error
    if len(egeda_energy_road_combined_data.columns.difference(unfiltered_combined_data.columns)) > 0:
        print(egeda_energy_road_combined_data.columns.difference(unfiltered_combined_data.columns))
        raise ValueError('columns in egeda_energy_road_combined_data are different to columns in unfiltered_combined_data')
    return egeda_energy_road_combined_data

# def estimate_freight_activity(egeda_energy_road_combined_data):
    
    
#     # Estimate_freight_activity()
#     # Its so hard to know what freight activity is. But for some economys we do have estimates. So for now, using the data we have we could esimate intensity and then estiamte activity for everyone using a weighted average from that
#     #lets jsut set it o 1 fr now. Then later on we can base this on real data
#     freight_activity_road_combined_data = egeda_energy_road_combined_data.copy()
#     freight_activity_road_combined_data['value'] = 1
#     freight_activity_road_combined_data['measure'] = 'activity'
#     freight_activity_road_combined_data['unit'] = 'tonne_km'
#     return freight_activity_road_combined_data





def prepare_egeda_energy_data_for_estimating_non_road(unfiltered_combined_data, road_combined_data):
    #prep:
    #get egeda data
    egeda_energy_selection_dict = {'measure': 
        ['energy'],
    'dataset': ['energy_non_road_est $ egeda/8th_ref']}
    egeda_energy_combined_data = data_formatting_functions.filter_for_specifc_data(egeda_energy_selection_dict, unfiltered_combined_data)
    #drop all cols except economy, date, medium and value
    egeda_energy_combined_data = egeda_energy_combined_data[['economy','date','medium','value']]
    #sum up the values for each economy, date and medium
    egeda_energy_combined_data = egeda_energy_combined_data.groupby(['economy','date','medium']).sum()
    egeda_energy_combined_data = egeda_energy_combined_data.reset_index()
    #pivot so the medium is a column
    egeda_energy_combined_data = egeda_energy_combined_data.pivot(index=['economy','date'], columns='medium', values='value')
    egeda_energy_combined_data = egeda_energy_combined_data.reset_index()
    #calculate the proportions of energy use for each medium
    egeda_energy_combined_data['total_energy_use'] = egeda_energy_combined_data['rail'] + egeda_energy_combined_data['ship'] + egeda_energy_combined_data['air']+ egeda_energy_combined_data['road']
    egeda_energy_combined_data['rail_proportion'] = egeda_energy_combined_data['rail'] / egeda_energy_combined_data['total_energy_use']
    egeda_energy_combined_data['ship_proportion'] = egeda_energy_combined_data['ship'] / egeda_energy_combined_data['total_energy_use']
    egeda_energy_combined_data['air_proportion'] = egeda_energy_combined_data['air'] / egeda_energy_combined_data['total_energy_use']
    egeda_energy_combined_data['road_proportion'] = egeda_energy_combined_data['road'] / egeda_energy_combined_data['total_energy_use']

    #get road enegry that has been calcualted:
    road_energy = road_combined_data[road_combined_data['measure'] == 'energy']
    #set transport type to all and sum up road energy
    road_energy['transport_type'] = 'all'
    road_energy = road_energy.groupby(['economy','date']).sum()
    road_energy = road_energy.reset_index()
    #name the value col 'road_energy_calculated' 
    road_energy = road_energy.rename(columns={'value':'road_energy_calculated'})
    #drop unnecessary cols

    #merge the two dataframes on economy and date
    egeda_energy_combined_data = pd.merge(egeda_energy_combined_data, road_energy, on=['economy','date'], how='left')
    #calculate the proportion of road energy that is calculated
    egeda_energy_combined_data['road_energy_calculated_proportion'] = egeda_energy_combined_data['road_energy_calculated'] / egeda_energy_combined_data['total_energy_use']
    return egeda_energy_combined_data

def scale_egeda_energy_data_scaled_for_estimating_non_road(egeda_energy_combined_data):
    #1.use the values from egeda and scale them according to how large the total road energy is compared to what it is for egeda (so the same proportion of enegry is used for non road as it is in egeda, but the total egeda enegry use will change)
    #copy the egeda data
    egeda_energy_combined_data_scaled = egeda_energy_combined_data.copy()
    #find what % the calculated road energy changed compared to the egeda road energy
    egeda_energy_combined_data_scaled['road_energy_calculated_proportion_change'] = egeda_energy_combined_data_scaled['road_energy_calculated'] / egeda_energy_combined_data_scaled['road']
    #scale the other energy uses by the same amount
    egeda_energy_combined_data_scaled['rail'] = egeda_energy_combined_data_scaled['rail'] * egeda_energy_combined_data_scaled['road_energy_calculated_proportion_change']
    egeda_energy_combined_data_scaled['ship'] = egeda_energy_combined_data_scaled['ship'] * egeda_energy_combined_data_scaled['road_energy_calculated_proportion_change']
    egeda_energy_combined_data_scaled['air'] = egeda_energy_combined_data_scaled['air'] * egeda_energy_combined_data_scaled['road_energy_calculated_proportion_change']
    #calculate the new total energy use
    egeda_energy_combined_data_scaled['total_energy_use'] = egeda_energy_combined_data_scaled['rail'] + egeda_energy_combined_data_scaled['ship'] + egeda_energy_combined_data_scaled['air']+ egeda_energy_combined_data_scaled['road_energy_calculated']
    #drop road and then rename road enegry calculated to road
    egeda_energy_combined_data_scaled = egeda_energy_combined_data_scaled.drop(columns=['road'])
    egeda_energy_combined_data_scaled = egeda_energy_combined_data_scaled.rename(columns={'road_energy_calculated':'road'})
    #keep only cols we need
    egeda_energy_combined_data_scaled = egeda_energy_combined_data_scaled[['economy','date','rail','ship','air','road','rail_proportion', 'ship_proportion', 'air_proportion', 'road_proportion', 'total_energy_use']]
    #done
    return egeda_energy_combined_data_scaled

def scale_egeda_energy_data_remainder_for_estimating_non_road(egeda_energy_combined_data):
    #2.use the values from egeda but make it the remainder of the total road energy use (so scale between non-road energy uses are the same but the proportion comapred to total road energy use is different)
    #copy the egeda data
    egeda_energy_combined_data_remainder = egeda_energy_combined_data.copy()
    #calculate the remainder of the total energy use
    egeda_energy_combined_data_remainder['total_energy_use_remainder'] = egeda_energy_combined_data_remainder['total_energy_use'] - egeda_energy_combined_data_remainder['road_energy_calculated']
    #calcualte previous remainder
    egeda_energy_combined_data_remainder['total_energy_use_remainder_previous'] = egeda_energy_combined_data_remainder['total_energy_use_remainder'] - egeda_energy_combined_data_remainder['road']
    #clacualte the percentage change of the remainder
    egeda_energy_combined_data_remainder['total_energy_use_remainder_change'] = egeda_energy_combined_data_remainder['total_energy_use_remainder'] / egeda_energy_combined_data_remainder['total_energy_use_remainder_previous']
    #scale the other energy uses by the same amount
    egeda_energy_combined_data_remainder['rail'] = egeda_energy_combined_data_remainder['rail'] * egeda_energy_combined_data_remainder['total_energy_use_remainder_change']
    egeda_energy_combined_data_remainder['ship'] = egeda_energy_combined_data_remainder['ship'] * egeda_energy_combined_data_remainder['total_energy_use_remainder_change']
    egeda_energy_combined_data_remainder['air'] = egeda_energy_combined_data_remainder['air'] * egeda_energy_combined_data_remainder['total_energy_use_remainder_change']
    #calculate the new total energy use
    egeda_energy_combined_data_remainder['total_energy_use'] = egeda_energy_combined_data_remainder['rail'] + egeda_energy_combined_data_remainder['ship'] + egeda_energy_combined_data_remainder['air']+ egeda_energy_combined_data_remainder['road_energy_calculated']
    #reclaulte the percent of each medium comapred to total energy use
    egeda_energy_combined_data_remainder['rail_proportion'] = egeda_energy_combined_data_remainder['rail'] / egeda_energy_combined_data_remainder['total_energy_use']
    egeda_energy_combined_data_remainder['ship_proportion'] = egeda_energy_combined_data_remainder['ship'] / egeda_energy_combined_data_remainder['total_energy_use']
    egeda_energy_combined_data_remainder['air_proportion'] = egeda_energy_combined_data_remainder['air'] / egeda_energy_combined_data_remainder['total_energy_use']
    egeda_energy_combined_data_remainder['road_proportion'] = egeda_energy_combined_data_remainder['road_energy_calculated'] / egeda_energy_combined_data_remainder['total_energy_use']
    #drop road and then rename road enegry calculated to road
    egeda_energy_combined_data_remainder = egeda_energy_combined_data_remainder.drop(columns=['road'])
    egeda_energy_combined_data_remainder = egeda_energy_combined_data_remainder.rename(columns={'road_energy_calculated': 'road'})
    #keep only cols we need
    egeda_energy_combined_data_remainder = egeda_energy_combined_data_remainder[['economy', 'date', 'rail_proportion', 'ship_proportion', 'air_proportion', 'road_proportion', 'rail', 'ship', 'air', 'road','total_energy_use']]
    #done
    return egeda_energy_combined_data_remainder

def clean_up_finalised_non_road_energy_df(new_egeda_energy_data):
    new_egeda_energy_data = new_egeda_energy_data.drop(columns=['proportion', 'option'])
    new_egeda_energy_data = new_egeda_energy_data.rename(columns={'absolute': 'value'})

    new_egeda_energy_data['measure'] = 'energy'
    new_egeda_energy_data['dataset'] = 'energy_non_road_est $ egeda/8th_ref'
    new_egeda_energy_data['source'] = 'egeda'
    new_egeda_energy_data['unit'] = 'pj'
    new_egeda_energy_data['transport_type'] = 'all'
    new_egeda_energy_data['fuel'] = 'all'
    new_egeda_energy_data['comment'] = 'no_comment'
    new_egeda_energy_data['scope'] = 'national'
    new_egeda_energy_data['frequency'] = 'yearly'
    new_egeda_energy_data['drive'] = 'all'
    new_egeda_energy_data['vehicle_type'] = 'all'

    #remove road from the medium column sincew it will always be the same as it was when loaded in
    non_road_egeda_energy_combined_data = new_egeda_energy_data[new_egeda_energy_data['medium'] != 'road']
    #and drop total
    non_road_egeda_energy_combined_data = non_road_egeda_energy_combined_data[non_road_egeda_energy_combined_data['medium'] != 'total']

    return non_road_egeda_energy_combined_data



def aggregate_non_road_energy_estimates(egeda_energy_combined_data_scaled,egeda_energy_combined_data_remainder,option_chosen = 'scaled'):

    
    #create a new 'option' column to differentiate the two options
    egeda_energy_combined_data_remainder['option'] = 'remainder'
    egeda_energy_combined_data_scaled['option'] = 'scaled'

    #now graph the results on 2 plotly time series one for proporitons and one for absolute values, faceted by economy, with the egeda_energy_combined_data_remainder as a dashed line and the egeda_energy_combined_data_scaled as a solid line,m then color as the rail, ship, air, road proportions
    egeda_energy_combined_data_merged = pd.concat([egeda_energy_combined_data_remainder, egeda_energy_combined_data_scaled])

    if option_chosen is not None:
        logging.info('Option chosen is ' + option_chosen)
        egeda_energy_combined_data_merged = egeda_energy_combined_data_merged[egeda_energy_combined_data_merged['option'] == option_chosen]

    #melt so all the proportions and absolute are in one column
    egeda_energy_combined_data_merged_tall = pd.melt(egeda_energy_combined_data_merged, id_vars=['economy', 'date','option'], var_name='type', value_name='value')
    #separate the type into medium and proportion. 
    #first, where type is total_energy_use, rename to total
    egeda_energy_combined_data_merged_tall.loc[egeda_energy_combined_data_merged_tall['type'] == 'total_energy_use', 'type'] = 'total'
    #then, split the type into medium and proportion
    egeda_energy_combined_data_merged_tall[['medium','proportion']] = egeda_energy_combined_data_merged_tall['type'].str.split('_', expand=True)
    #drop the type column
    egeda_energy_combined_data_merged_tall = egeda_energy_combined_data_merged_tall.drop(columns=['type'])
    #where proportion is none, rename to absolute
    egeda_energy_combined_data_merged_tall.loc[egeda_energy_combined_data_merged_tall['proportion'].isnull(), 'proportion'] = 'absolute'
    #pivot to get proportion and absolute in different cols
    egeda_energy_combined_data_merged_tall = egeda_energy_combined_data_merged_tall.pivot_table(index=['economy', 'date', 'option', 'medium'], columns='proportion', values='value').reset_index()
    return egeda_energy_combined_data_merged_tall

def estimate_non_road_energy(unfiltered_combined_data,road_combined_data):

    egeda_energy_combined_data = prepare_egeda_energy_data_for_estimating_non_road(unfiltered_combined_data, road_combined_data) 
    egeda_energy_combined_data_scaled = scale_egeda_energy_data_scaled_for_estimating_non_road(egeda_energy_combined_data)
    egeda_energy_combined_data_remainder = scale_egeda_energy_data_remainder_for_estimating_non_road(egeda_energy_combined_data)    
    #we will commit to scaled for now, but we can change this later. So filter for remainder only and then drop proportion and option. then rename absolute to value. Then lastly create a whole lot of new cols.
    egeda_energy_combined_data_merged_tall = aggregate_non_road_energy_estimates(egeda_energy_combined_data_scaled,egeda_energy_combined_data_remainder,option_chosen = 'scaled')
    ###########
    #analysis:   
    logger.info('\n\n\n\nCOMMITTING TO SCALED\n\n\n\n')
    egeda_energy_combined_data_merged_tall = analysis_and_plotting_functions.plot_scaled_and_remainder(egeda_energy_combined_data_merged_tall)#NOTE THAT BY CHOOSING OPTION CHOSEN WE ARE FILTERING FOR THAT OPTION ONLY SO THE OUTPUT DATAFRAME WILL ONLY HAVE ONE OPTION
    ###########
    non_road_egeda_energy_combined_data = clean_up_finalised_non_road_energy_df(egeda_energy_combined_data_merged_tall)

    return non_road_egeda_energy_combined_data


def split_non_road_energy_into_transport_types(non_road_energy_no_transport_type,unfiltered_combined_data):
    #prep:
    egeda_energy_selection_dict = {'measure': 
        ['energy'],
    'dataset': ['energy_non_road_est $ egeda/8th_ref']}
    egeda_transport_type_energy_proportions = calcualte_egeda_non_road_energy_proportions(unfiltered_combined_data,egeda_energy_selection_dict)

    #merge on economy and date
    non_road_energy = non_road_energy_no_transport_type.merge(egeda_transport_type_energy_proportions, on=['economy','date', 'medium'], how='left')
    #times the energy use by the proportion of energy use for each transport type
    non_road_energy['freight'] = non_road_energy['value'] * non_road_energy['freight_energy_proportion']
    non_road_energy['passenger'] = non_road_energy['value'] * non_road_energy['passenger_energy_proportion']
    #drop value and proportions
    non_road_energy = non_road_energy.drop(columns=['value','freight_energy_proportion','passenger_energy_proportion','transport_type'])
    #melt so the transport type is a column
    cols = non_road_energy.columns.tolist()
    cols.remove('freight')
    cols.remove('passenger')
    non_road_energy = non_road_energy.melt(id_vars=cols, value_vars=['freight','passenger'], var_name='transport_type', value_name='value')

    analysis_and_plotting_functions.plot_non_road_energy_use_by_transport_type(non_road_energy)

    return non_road_energy

def calcualte_egeda_non_road_energy_proportions(unfiltered_combined_data,egeda_energy_selection_dict):
    #get egeda data
    egeda_transport_type_energy_proportions = data_formatting_functions.filter_for_specifc_data(egeda_energy_selection_dict, unfiltered_combined_data)
    #drop all cols except economy, date, medium transport_type and value
    egeda_transport_type_energy_proportions = egeda_transport_type_energy_proportions[['economy','date','medium','value', 'transport_type']]
    #drop mediumis road
    egeda_transport_type_energy_proportions = egeda_transport_type_energy_proportions[egeda_transport_type_energy_proportions['medium']!='road']
    #check for duplicates
    if egeda_transport_type_energy_proportions.duplicated().any():
        raise ValueError('There are duplicates in the egeda energy data')
    #pivot so the transport type is a column
    egeda_transport_type_energy_proportions = egeda_transport_type_energy_proportions.pivot(index=['economy','date','medium'], columns='transport_type', values='value')
    egeda_transport_type_energy_proportions = egeda_transport_type_energy_proportions.reset_index()

    #calculate the proportions of energy use for each medium
    egeda_transport_type_energy_proportions['total_energy_use'] = egeda_transport_type_energy_proportions['freight'] + egeda_transport_type_energy_proportions['passenger'] 
    egeda_transport_type_energy_proportions['freight_energy_proportion'] = egeda_transport_type_energy_proportions['freight'] / egeda_transport_type_energy_proportions['total_energy_use']
    egeda_transport_type_energy_proportions['passenger_energy_proportion'] = egeda_transport_type_energy_proportions['passenger'] / egeda_transport_type_energy_proportions['total_energy_use']

    #drop non proportional columns
    egeda_transport_type_energy_proportions = egeda_transport_type_energy_proportions.drop(columns=['freight','passenger','total_energy_use'])

    return egeda_transport_type_energy_proportions


def import_previous_draft_selections():
    """These are selections from th early stages of this library, which can now be used as the basis for the activity of the non road transport sector and freight road sector. We will however, calcualte the intensity using energy/activity within this dataset and then times that by the energy values weve calcualted."""
    FILE_DATE_ID2 = 'DATE20230216'

    previous_draft_transport_data_system_df = pd.read_csv('input_data/previous_selections/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID2))

    #if they are there, remove cols called index, level_0
    if 'index' in previous_draft_transport_data_system_df.columns:
        previous_draft_transport_data_system_df = previous_draft_transport_data_system_df.drop(columns=['index'])
    if 'level_0' in previous_draft_transport_data_system_df.columns:
        previous_draft_transport_data_system_df = previous_draft_transport_data_system_df.drop(columns=['level_0'])   

    #convert all cols to snake case
    previous_draft_transport_data_system_df.columns = [utility_functions.convert_string_to_snake_case(col) for col in previous_draft_transport_data_system_df.columns]
    #convert all values in cols to snake case
    previous_draft_transport_data_system_df = utility_functions.convert_all_cols_to_snake_case(previous_draft_transport_data_system_df)
    #convert date to yyyy format
    previous_draft_transport_data_system_df = data_formatting_functions.TEMP_FIX_ensure_date_col_is_year(previous_draft_transport_data_system_df)

    return previous_draft_transport_data_system_df



def prepare_previous_non_road_energy_activity_data(previous_draft_transport_data_system_df):
    #grab the activity an energy data for non road
    non_road = previous_draft_transport_data_system_df.loc[previous_draft_transport_data_system_df['medium']!='road']
    non_road_energy_activity = non_road.loc[non_road['measure'].isin(['energy','freight_tonne_km', 'passenger_km'])]
    #drop nonspecified transport type an medium
    non_road_energy_activity = non_road_energy_activity.loc[non_road_energy_activity['transport_type']!='nonspecified']
    non_road_energy_activity = non_road_energy_activity.loc[non_road_energy_activity['medium']!='nonspecified']
    #rename freight_tonne and passenger_km to activity
    non_road_energy_activity = non_road_energy_activity.replace({'freight_tonne_km':'activity','passenger_km':'activity'})
    #drop wehre scope is not national
    non_road_energy_activity = non_road_energy_activity.loc[non_road_energy_activity['scope']=='national']
    #keep only: date	economy	medium	transport_type energy	freight_tonne_km	passenger_km
    non_road_energy_activity = non_road_energy_activity[['date','economy','medium','transport_type','measure','value']]
    #identify duplicates
    if non_road_energy_activity.duplicated().sum()>0:
        duplicate_rows = non_road_energy_activity.loc[non_road_energy_activity.duplicated()]
        logger.warning('duplicates found in non_road_energy_activity.')
        non_road_energy_activity = non_road_energy_activity.drop_duplicates()

    #pivot so energy and activity are in the same row. 
    cols = non_road_energy_activity.columns.tolist()
    cols.remove('value')
    cols.remove('measure')
    non_road_energy_activity_wide = non_road_energy_activity.pivot(index=cols, columns='measure', values='value').reset_index()

    return non_road_energy_activity_wide

def calcualte_non_road_intensity_from_previous_data(non_road_energy_activity_wide):
    #calvc intensity
    non_road_energy_activity_wide['intensity'] = non_road_energy_activity_wide['energy']/non_road_energy_activity_wide['activity']

    #we will really only have intensity for 2017 because we lack energy data for the other years. But we can just use the 2017 intensity for all years if needed. Also some economys will have really weird intensity values. So we will calcualte the avergae intensity for each transport type/medium combination and use that for the other economys.
    #so lets group by transport type and medium, filter out outliers and then calcualte the average intensity for each group
    intensity_average = non_road_energy_activity_wide.dropna(subset=['intensity'])
    #join medium and transport type
    intensity_average['medium$transport_type'] = intensity_average['medium'] + '$' + intensity_average['transport_type']
    unique_medium_transport_type = intensity_average['medium$transport_type'].unique()
    #drop activity and energy
    intensity_average = intensity_average.drop(columns=['activity','energy'])
    #pivot so we ahve a col for each unique medium$transport_type
    intensity_average_wide = intensity_average.pivot(index=['date','economy'], columns='medium$transport_type', values='intensity').reset_index()

    #do some stats magic to remove outliers and then fill then wth the mean of the rest of the values
    import scipy.stats as stats
    for col in unique_medium_transport_type:
        #get z score for this col and set any values to na where z score is > 2
        #make col int
        intensity_average_wide[col] = intensity_average_wide[col].astype(float)
        #replace 0's with nan
        intensity_average_wide.loc[intensity_average_wide[col]==0, col] = np.nan
        intensity_average_wide[col+'_z'] = np.abs(stats.zscore(intensity_average_wide[col], nan_policy='omit'))
        intensity_average_wide.loc[intensity_average_wide[col+'_z']>2, col] = np.nan
        #calcualte mean for this col excluding na
        mean = intensity_average_wide[col].mean()
        #replace na with mean
        intensity_average_wide[col] = intensity_average_wide[col].fillna(mean)
        #drop z score col
        intensity_average_wide = intensity_average_wide.drop(columns=[col+'_z'])
    del stats
    #melt back to long format
    intensity_average = intensity_average_wide.melt(id_vars=['date','economy'], var_name='medium$transport_type', value_name='intensity')

    #separate medium and transport type
    intensity_average['medium'] = intensity_average['medium$transport_type'].str.split('$').str[0]
    intensity_average['transport_type'] = intensity_average['medium$transport_type'].str.split('$').str[1]
    #drop medium$transport_type
    intensity_average = intensity_average.drop(columns=['medium$transport_type'])
    non_road_intensity = intensity_average

    return non_road_intensity

def clean_non_road_activity(non_road_energy_activity):
    non_road_activity = non_road_energy_activity.drop(columns=['value','intensity'])
    #for a few economys we have no data because we have no energy data. So we will drop activty = na
    logging.info('dropping na values from non_road_activity, for the following rows: {}'.format(non_road_activity[non_road_activity['activity'].isna()]))
    non_road_activity = non_road_activity.dropna(subset=['activity'])
    #rename measure to either freight_tonne_km or passenger_km depending on the transport type
    non_road_activity.loc[non_road_activity['transport_type']=='freight','measure'] = 'freight_tonne_km'
    non_road_activity.loc[non_road_activity['transport_type']=='passenger','measure'] = 'passenger_km'
    #for unit make it equal to the measure
    non_road_activity['unit'] = non_road_activity['measure']
    #rename actitvity to vlaue
    non_road_activity = non_road_activity.rename(columns={'activity':'value'})
    #make source to previous_data_system
    non_road_activity['source'] = 'previous_data_system'
    #dataset to non_road_activity_est
    non_road_activity['dataset'] = 'non_road_activity_est $ previous_data_system'
    return non_road_activity

def estimate_non_road_activity(non_road_energy):
    previous_draft_transport_data_system_df = import_previous_draft_selections()
    previous_non_road_energy_activity_wide = prepare_previous_non_road_energy_activity_data(previous_draft_transport_data_system_df)
    previous_non_road_intensity = calcualte_non_road_intensity_from_previous_data(previous_non_road_energy_activity_wide)

    #now times the intenstiy by the energy values weve calcualted to get the activity
    non_road_energy_activity = non_road_energy.merge(previous_non_road_intensity,how='outer',on=['date',	'economy',	'medium', 'transport_type'])
    non_road_energy_activity['activity'] = non_road_energy_activity['value']/non_road_energy_activity['intensity']
    non_road_activity = clean_non_road_activity(non_road_energy_activity)
    plot=False
    if plot:
        analysis_and_plotting_functions.plot_non_road_activity(non_road_activity,previous_non_road_energy_activity_wide)

    return non_road_activity


#     # Non road:
#     # This will be just like road frieght. For now we will jsutn split non road into freight/passenger after everything else.
    
#     #load in the combined_data from paths_dict['combined_data']
#     egeda_energy_road_selection_dict = {'measure': 
#         ['energy'],
#     'medium': ['road'],
#     'dataset': ['energy_non_road_est $ egeda/8th_ref']}
#     egeda_energy_road_combined_data = data_formatting_functions.filter_for_specifc_data(egeda_energy_road_selection_dict, unfiltered_combined_data)
#     # estimate_air_ship_rail_energy_use()
#     # Use esto values. Where we aren't confident we could:
#     # 	- pick a vlaue from another dataset and rescale the other transport energy uses
#     # 	-  change by a proportiona and rescale other transport enegry uses


#     # estiamte_passenger_freight_splits()
#     # Where we do have data from other datasets we can calcualte a split between passs and freight, if not, we can use a weighted average from what we do have

#     # Estimate_activity()
#     # Its so hard to know what activity is. But for some economys we do have estimates. So for now, using the data we have we could esimate intensity and then estiamte activity for everyone using a weighted average from that
#     return freight_activity_non_road_combined_data


