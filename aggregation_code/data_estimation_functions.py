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
plotting = True#change to false to stop plots from appearing
#eg from
#  Estimate Energy and passenger km using stocks:
# vkm = mileage * stocks
# energy = vkm * efficiency
# pkm = vkm * occupancy

#put it all thorugh calculate_new_data() which will:
# Using mileage, eff, occ and stocks we can estimate passenger km and energy. Where we are missing data eg. stocks, we will leave an na. 
# enable adjusting the mileage, occ and eff data by specified ranges to get a range of options


 
def prepare_egeda_energy_data_for_estimating_non_road(unfiltered_combined_data, all_combined_data):
    #prep:
    #get egeda data
    egeda_energy_selection_dict = {'measure': 
        ['energy'],
    'dataset': ['egeda_split_into_transport_types_using_8th $ egeda_9th_cleansed']}
    #make them all lower case:
    unfiltered_combined_data['measure'] = unfiltered_combined_data['measure'].str.lower()
    egeda_energy_combined_data = data_formatting_functions.filter_for_specifc_data(egeda_energy_selection_dict, unfiltered_combined_data)
    #drop all cols except economy, date, medium and value
    egeda_energy_combined_data = egeda_energy_combined_data[['economy','date','medium','value']]
    #sum up the values for each economy, date and medium
    egeda_energy_combined_data = egeda_energy_combined_data.groupby(['economy','date','medium']).sum()
    egeda_energy_combined_data = egeda_energy_combined_data.reset_index()
    #pivot so the medium is a column
    egeda_energy_combined_data = egeda_energy_combined_data.pivot(index=['economy','date'], columns='medium', values='value')
    egeda_energy_combined_data = egeda_energy_combined_data.reset_index()
    #if the mediums are missing then set them to 0:
    if 'air' not in egeda_energy_combined_data.columns:
        egeda_energy_combined_data['air'] = 0
    if 'rail' not in egeda_energy_combined_data.columns:
        egeda_energy_combined_data['rail'] = 0
    if 'ship' not in egeda_energy_combined_data.columns:
        egeda_energy_combined_data['ship'] = 0
        
    #calculate the proportions of energy use for each medium
    egeda_energy_combined_data['total_energy_use'] = egeda_energy_combined_data['rail'] + egeda_energy_combined_data['ship'] + egeda_energy_combined_data['air']+ egeda_energy_combined_data['road']
    egeda_energy_combined_data['rail_proportion'] = egeda_energy_combined_data['rail'] / egeda_energy_combined_data['total_energy_use']
    egeda_energy_combined_data['ship_proportion'] = egeda_energy_combined_data['ship'] / egeda_energy_combined_data['total_energy_use']
    egeda_energy_combined_data['air_proportion'] = egeda_energy_combined_data['air'] / egeda_energy_combined_data['total_energy_use']
    egeda_energy_combined_data['road_proportion'] = egeda_energy_combined_data['road'] / egeda_energy_combined_data['total_energy_use']

    #get road enegry that has been calcualted:
    road_energy = all_combined_data[all_combined_data['measure'] == 'energy']
    #set transport type to all and sum up road energy
    road_energy['transport_type'] = 'all'
    road_energy = road_energy.groupby(['economy','date']).sum().reset_index()
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
    new_egeda_energy_data['dataset'] ='egeda_split_into_transport_types_using_8th $ egeda_9th_cleansed'
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

def estimate_non_road_energy(unfiltered_combined_data,all_combined_data,paths_dict):

    egeda_energy_combined_data = prepare_egeda_energy_data_for_estimating_non_road(unfiltered_combined_data, all_combined_data) 
    egeda_energy_combined_data_scaled = scale_egeda_energy_data_scaled_for_estimating_non_road(egeda_energy_combined_data)
    egeda_energy_combined_data_remainder = scale_egeda_energy_data_remainder_for_estimating_non_road(egeda_energy_combined_data)    
    #we will commit to scaled for now, but we can change this later. So filter for remainder only and then drop proportion and option. then rename absolute to value. Then lastly create a whole lot of new cols.
    egeda_energy_combined_data_merged_tall = aggregate_non_road_energy_estimates(egeda_energy_combined_data_scaled,egeda_energy_combined_data_remainder,option_chosen = 'scaled')
    ###########
    #analysis:   
    logger.info('\n\n\n\nCOMMITTING TO SCALED\n\n\n\n')
    if plotting:#global variable in main() 
        analysis_and_plotting_functions.plot_scaled_and_remainder(egeda_energy_combined_data_merged_tall,paths_dict)#NOTE THAT BY CHOOSING OPTION CHOSEN WE ARE FILTERING FOR THAT OPTION ONLY SO THE OUTPUT DATAFRAME WILL ONLY HAVE ONE OPTION
    ###########
    non_road_egeda_energy_combined_data = clean_up_finalised_non_road_energy_df(egeda_energy_combined_data_merged_tall)

    return non_road_egeda_energy_combined_data


def split_non_road_energy_into_transport_types(non_road_energy_no_transport_type,unfiltered_combined_data, paths_dict):
    #prep:
    egeda_energy_selection_dict = {'measure': 
        ['energy'],
    'dataset': ['egeda_split_into_transport_types_using_8th $ egeda_9th_cleansed']} 
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
    cols.remove('passenger')#does this need to ave other columns added to it, eg. comment?
    non_road_energy = non_road_energy.melt(id_vars=cols, value_vars=['freight','passenger'], var_name='transport_type', value_name='value')

    if plotting:#global variable in main() 
        analysis_and_plotting_functions.plot_non_road_energy_use_by_transport_type(non_road_energy, paths_dict)

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

    #first, if the cols are not there, set them to 0
    if 'freight' not in egeda_transport_type_energy_proportions.columns:
        egeda_transport_type_energy_proportions['freight'] = 0
    if 'passenger' not in egeda_transport_type_energy_proportions.columns:
        egeda_transport_type_energy_proportions['passenger'] = 0
        
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
    previous_draft_transport_data_system_df = utility_functions.ensure_date_col_is_year(previous_draft_transport_data_system_df)

    return previous_draft_transport_data_system_df


def prepare_previous_energy_activity_data(previous_draft_transport_data_system_df):
    #grab the activity an energy data for non road
    energy_activity = previous_draft_transport_data_system_df.loc[previous_draft_transport_data_system_df['measure'].isin(['energy','freight_tonne_km', 'passenger_km'])]
    #drop nonspecified transport type an medium
    energy_activity = energy_activity.loc[energy_activity['transport_type']!='nonspecified']
    energy_activity = energy_activity.loc[energy_activity['medium']!='nonspecified']
    #rename freight_tonne and passenger_km to activity
    energy_activity = energy_activity.replace({'freight_tonne_km':'activity','passenger_km':'activity'})
    #drop wehre scope is not national
    energy_activity = energy_activity.loc[energy_activity['scope']=='national']
    #keep only: date	economy	medium	transport_type energy	freight_tonne_km	passenger_km
    energy_activity = energy_activity[['date','economy','medium','transport_type','measure','value']]
    #sum energy and activity by date, economy, medium and transport type
    energy_activity = energy_activity.groupby(['date','economy','medium','transport_type','measure']).sum().reset_index()

    #pivot so energy and activity are in the same row. 
    cols = energy_activity.columns.tolist()
    cols.remove('value')
    cols.remove('measure')
    energy_activity_wide = energy_activity.pivot(index=cols, columns='measure', values='value').reset_index()

    return energy_activity_wide

# def calcualte_intensity_from_previous_data(energy_activity_wide,paths_dict):


#     #calvc intensity
#     energy_activity_wide['intensity'] = energy_activity_wide['energy']/energy_activity_wide['activity']

#     #we will really only have intensity for 2017 because we lack energy data for the other years. But we can just use the 2017 intensity for all years if needed. Also some economys will have really weird intensity values. So we will calcualte the avergae intensity for each transport type/medium combination and use that for the other economys.
#     #so lets group by transport type and medium, filter out outliers and then calcualte the average intensity for each group
#     intensity_average = energy_activity_wide.dropna(subset=['intensity'])
#     #join medium and transport type
#     intensity_average['medium$transport_type'] = intensity_average['medium'] + '$' + intensity_average['transport_type']
#     unique_medium_transport_type = intensity_average['medium$transport_type'].unique()
#     #drop activity and energy
#     intensity_average = intensity_average.drop(columns=['activity','energy'])
#     #pivot so we ahve a col for each unique medium$transport_type
#     intensity_average_wide = intensity_average.pivot(index=['date','economy'], columns='medium$transport_type', values='intensity').reset_index()

#     #do some stats magic to remove outliers and then fill then wth the mean of the rest of the values
#     import scipy.stats as stats
#     for col in unique_medium_transport_type:
#         #get z score for this col and set any values to na where z score is > 2
#         #make col int
#         intensity_average_wide[col] = intensity_average_wide[col].astype(float)
#         #replace 0's with nan
#         intensity_average_wide.loc[intensity_average_wide[col]==0, col] = np.nan
#         intensity_average_wide[col+'_z'] = np.abs(stats.zscore(intensity_average_wide[col], nan_policy='omit'))
#         intensity_average_wide.loc[intensity_average_wide[col+'_z']>2, col] = np.nan
#         #calcualte mean for this col excluding na
#         mean = intensity_average_wide[col].mean()
#         #replace na with mean
#         intensity_average_wide[col] = intensity_average_wide[col].fillna(mean)
#         #drop z score col
#         intensity_average_wide = intensity_average_wide.drop(columns=[col+'_z'])
#     del stats
#     #melt back to long format
#     intensity_average = intensity_average_wide.melt(id_vars=['date','economy'], var_name='medium$transport_type', value_name='intensity')
    
#     if plotting:
#         analysis_and_plotting_functions.plot_intensity(intensity_average, paths_dict)

#     #separate medium and transport type
#     intensity_average['medium'] = intensity_average['medium$transport_type'].str.split('$').str[0]
#     intensity_average['transport_type'] = intensity_average['medium$transport_type'].str.split('$').str[1]
#     #drop medium$transport_type
#     intensity_average = intensity_average.drop(columns=['medium$transport_type'])
#     intensity = intensity_average

#     return intensity

def clean_activity(energy_activity):
    activity = energy_activity.drop(columns=['value','intensity'])
    #for a few economys we have no data because we have no energy data. So we will drop activty = na
    logging.info('dropping na values from activity, for the following rows: {}'.format(activity[activity['activity'].isna()]))
    activity = activity.dropna(subset=['activity'])
    # #rename measure to actviity
    activity['measure'] = 'activity'
    # #for unit make it equal to the measure
    activity['unit'] = 'passenger_km_or_freight_tonne_km'
    #rename actitvity to vlaue
    activity = activity.rename(columns={'activity':'value'})
    #dataset to activity_est
    activity['dataset'] = 'activity_est $ intensity_times_energy'
    #and set these:	fuel	comment	scope	frequency	drive	vehicle_type
    activity['fuel'] = 'all'
    activity['comment'] = 'no_comment'
    activity['scope'] = 'national'
    activity['frequency'] = 'yearly'
    activity['drive'] = 'all'
    activity['vehicle_type'] = 'all'
    return activity

def extract_calculated_road_energy_activity(all_combined_data):
    #extract raod energy and activity
    road_energy_activity = all_combined_data.loc[(all_combined_data['measure'].isin(['energy', 'activity'])) & (all_combined_data['medium']=='road')]
    #sum up road activiy adn energy so we only ahve one row per date, economy, medium, transport type, measure
    road_energy_activity = road_energy_activity.groupby(['date','economy','medium','transport_type','unit','measure'])['value'].sum().reset_index()
    #sepreate road energy and activity
    road_energy = road_energy_activity.loc[road_energy_activity['measure']=='energy']
    road_activity = road_energy_activity.loc[road_energy_activity['measure']=='activity']
    #drop measure
    road_energy = road_energy.drop(columns=['measure'])
    road_activity = road_activity.drop(columns=['measure'])
    return road_energy, road_activity


def extract_intensity_non_road(all_combined_data):
    #get intensity data from all_combined_data for non road
    intensity = all_combined_data.loc[(all_combined_data['measure']=='intensity') & (all_combined_data['medium']!='road')]
    #drop measure
    intensity = intensity.drop(columns=['measure'])
    intensity.rename(columns={'value':'intensity'}, inplace=True)
    return intensity

def estimate_activity_non_road_using_intensity(non_road_energy,all_combined_data,paths_dict):

    intensity = extract_intensity_non_road(all_combined_data)

    #now times the intenstiy by the energy values weve calcualted to get the activity
    energy_activity = non_road_energy.merge(intensity,how='left',on=['date',	'economy',	'medium', 'transport_type'], suffixes=('', '_intensity'))
    #drop any cols ending in _intensity
    energy_activity = energy_activity.drop(columns=[col for col in energy_activity.columns if col.endswith('_intensity')]) 
    
    energy_activity['activity'] = energy_activity['value']/energy_activity['intensity']

    activity = clean_activity(energy_activity)

    return activity

def find_percent_diff_for_missing_years_in_egeda(merged_data):
    #since we mayu be missing some years in the dataset we use to rescale total energy use we will need to find the % diff for the years we are missing. We will do this by finding the average % diff for the years we have and then applying this to the years we are missing
    #first find the data we are not missing which is where the % diff is not 0
    not_missing = merged_data.loc[merged_data['%_diff']!=0]
    #now find the average % diff for every economy
    average_percent_diff = not_missing.groupby(['economy'])['%_diff'].mean().reset_index()
    #now merge this to the merged data so we can replace 0's with the average % diff
    merged_data = pd.merge(merged_data, average_percent_diff, on='economy', how='outer', suffixes=('', '_avg'))
    #where %_diff_y is 0 replace with %_diff_x
    merged_data.loc[merged_data['%_diff']==0,'%_diff'] = merged_data['%_diff_avg']
    merged_data = merged_data.drop(columns=['%_diff_avg'])
    return merged_data

def rescale_total_energy_to_egeda_totals(all_new_combined_data,unfiltered_combined_data,paths_dict):
    #     #take in new data and compare the total energy for each medium to the total energy in egeda. If it is not the same, then we need to rescale the data. Given that the proportions of all the mediums to each other are the same as in the egeda data, we should decrease all by the % needed. However to decrease passenger road we should choose on one of either mileage, stocks, load/occ or efficiency. We can do this by doing data['energy'] = data['mileage'] * data['stocks'] * data['efficiency'] > mileage = data['energy'] / data['stocks'] * data['efficiency'] where energy is the new energy we need.

    #FIRST GET THE DATA IN
    egeda_energy_combined_data, new_energy = compare_total_energy_to_egeda_totals(all_new_combined_data,unfiltered_combined_data,paths_dict)
    #now we need to rescale the data by the % diff between the two. First calcualte the % diff
    
    #join the two 
    merged_data = pd.merge(egeda_energy_combined_data, new_energy, on=['economy','date','medium','transport_type'], how='outer', suffixes=('_egeda', '_new'))
    #drop cols besides economy date and val
    merged_data = merged_data[['economy','date','value_egeda','value_new']]
    #sum
    merged_data = merged_data.groupby(['economy','date']).sum().reset_index()
    #calc % diff > as in the factor we need to times the new data by to get the same total
    merged_data['%_diff'] = merged_data['value_egeda'] / merged_data['value_new']
    
    merged_data = find_percent_diff_for_missing_years_in_egeda(merged_data)
    #now we will grab the road data to recalcualte either mileage or efficiency
    road_energy = all_new_combined_data[(all_new_combined_data['medium'] == 'road')]

    #pivot so the measure col is wide. first have to drop unit col 
    road_energy = road_energy.drop(columns=['unit','dataset'])
    cols = road_energy.columns.tolist()
    cols.remove('value')
    cols.remove('measure')
    cols.remove('comment')
    road_energy = road_energy.pivot(index=cols, columns='measure', values='value').reset_index()
    #join the two on economy	date	
    road_combined = pd.merge(road_energy, merged_data[['economy','date','%_diff']], on=['economy','date'], how='left')
    #find new energy for each row
    road_combined['NEW_energy'] = road_combined['energy'] * road_combined['%_diff']

    #if we want to reclaculate mileage then we will need to do it using the efficiency and stocks data, and vice versa for efficiency and stocks
    road_combined['NEW_mileage'] = (road_combined['NEW_energy'] * road_combined['efficiency']) / (road_combined['stocks'] )

    road_combined['NEW_efficiency'] =(road_combined['stocks'] * road_combined['mileage']) / road_combined['NEW_energy'] 

    road_combined['NEW_stocks'] = (road_combined['efficiency'] *road_combined['NEW_energy']) / ( road_combined['mileage'])
    
    measures = ['mileage','efficiency','stocks']
    new_measures = ['NEW_' + measure for measure in measures]

    #create new dataset col called 'rescaled'
    road_combined['dataset'] = 'rescaled'
    plotting=False
    if plotting:
        analysis_and_plotting_functions.plot_new_and_old_road_measures(road_combined,measures,new_measures,paths_dict)

    #TODO for now we will by default decrease mielage only because it is more useful to have constant vlaues for eff and stocks. 
    # But later we can do a more sophisticated method of splitting the difference between mileage and eff and stocks, for example by observing whtether they are above/below average and so on. eg. find % above/below average each one is, then scale them to one and times the required % change by their relative scalings to get how much each one should be changed by. Perhaps stocks average can be compared to the stocks per population average. CAN ALSO INCLUDE OCCUPANCY IN THIS METHOD ID SAY

    #And it might be nice to find an effective way of splitting the effect between mileage and eff and stocks. #TODO later. i guess it would be by finding new_mileage_eff = new_mileage*new_eff=energy/stocks. Then we can find the % diff between new_mileage_eff and old_mileage_eff. Then split that difference between the two somehow.

    #todo. finding that thius causes nas in years we dont hav e egeda datya. need to probs find avg % diff and use that for those years.
    stocks=False
    mileage=True
    if stocks:
        logging.info('rescaling road stocks by an average of {} to decrease energy by an average of {} across all economys. See graphs in plotting_output/data_selection/analysis/egeda_scaling/ +measure + _ +economy+.html for more'.format(round(road_combined['NEW_stocks'].sum() / road_combined['stocks'].sum(),2),round(merged_data['%_diff'].mean(),2)))

        #change the mileage col to the new mileage col        
        road_combined['stocks'] = road_combined['NEW_stocks']
        road_combined['energy'] = road_combined['NEW_energy']

        #keep only INDEX cols and the stocks and energy cols
        index_cols_in_df = [col for col in road_combined.columns.tolist() if col in paths_dict['INDEX_COLS']]
        breakpoint()
        road_combined = road_combined[['stocks','energy']+index_cols_in_df]
        #double check for duplicates
        if road_combined.duplicated().any():
            #road_combined.drop_duplicates(inplace=True)
            raise Exception('duplicates in road_combined')
        #melt back to long format
        road_combined = road_combined.melt(id_vars=index_cols_in_df, value_vars=['stocks','energy'], var_name='measure', value_name='value')
    
    elif mileage:
        logging.info('rescaling road mileage by an average of {} to decrease energy by an average of {} across all economys. See graphs in plotting_output/data_selection/analysis/egeda_scaling/ +measure + _ +economy+.html for more'.format(round(road_combined['NEW_mileage'].mean() / road_combined['mileage'].mean(),2),round(merged_data['%_diff'].mean(),2)))

        #change the mileage col to the new mileage col        
        road_combined['mileage'] = road_combined['NEW_mileage']
        road_combined['energy'] = road_combined['NEW_energy']

        #keep only INDEX cols and the mileage and energy cols
        index_cols_in_df = [col for col in road_combined.columns.tolist() if col in paths_dict['INDEX_COLS']]
        #drop dataset and source cols
        index_cols_in_df = [col for col in index_cols_in_df if col not in ['dataset']]
        road_combined = road_combined[['mileage','energy']+index_cols_in_df]
        #double check for duplicates
        if road_combined.duplicated().any():
            raise Exception('duplicates in road_combined')
        #melt back to long format
        road_combined = road_combined.melt(id_vars=index_cols_in_df, value_vars=['mileage','energy'], var_name='measure', value_name='value')
    # elif mileage_and_occupancy:
    #     logging.info('rescaling road mileage by an average of {} and occupancy/load by an average of {} to decrease energy by an average of {} across all economys. See graphs in plotting_output/data_selection/analysis/egeda_scaling/ +measure + _ +economy+.html for more'.format(round(road_combined['NEW_mileage'].mean() / road_combined['mileage'].mean(),2),round(road_combined['NEW_occupancy'].mean() / road_combined['occupancy'].mean(),2),round(merged_data['%_diff'].mean(),2)))

    #     #change the mileage col to the new mileage col        
    #     road_combined['mileage'] = road_combined['NEW_mileage']
    #     road_combined['energy'] = road_combined['NEW_energy']

    #     #keep only INDEX cols and the mileage and energy cols
    #     index_cols_in_df = [col for col in road_combined.columns.tolist() if col in paths_dict['INDEX_COLS']]
    #     #drop dataset and source cols
    #     index_cols_in_df = [col for col in index_cols_in_df if col not in ['dataset']]
    #     road_combined = road_combined[['mileage','energy']+index_cols_in_df]
    #     #double check for duplicates
    #     if road_combined.duplicated().any():
    #         raise Exception('duplicates in road_combined')
    #     #melt back to long format
    #     road_combined = road_combined.melt(id_vars=index_cols_in_df, value_vars=['mileage','energy'], var_name='measure', value_name='value')
        
    else:
        raise Exception('not implemented yet')

    #create a new road df which we will join these new values on and change the dataset/source vlaues for
    new_road = all_new_combined_data[(all_new_combined_data['medium'] == 'road')]
    new_road = new_road.merge(road_combined, on=index_cols_in_df+['measure'], how='left', suffixes=['','_new'])
    #change the value to the new value where its not nan
    new_road.loc[new_road['value_new'].notnull(),'value'] = new_road.loc[new_road['value_new'].notnull(),'value_new']
    #change source so it is dataset+source
    # new_road.loc[new_road['value_new'].notnull(),'source'] = new_road.loc[new_road['value_new'].notnull(),'dataset'] + '_' + new_road.loc[new_road['value_new'].notnull(),'source']
    #change dataset to 'egeda_scaling'
    new_road.loc[new_road['value_new'].notnull(),'dataset'] = 'egeda_scaling' + '_' + new_road.loc[new_road['value_new'].notnull(),'dataset']
    #drop the value_new col
    new_road = new_road.drop(columns=['value_new'])

    #now change the energy values for all other data 
    non_road_energy = all_new_combined_data[(all_new_combined_data['medium'] != 'road')]
    #merge with the %_diff and change energy:
    combined = pd.merge(non_road_energy, merged_data[['economy','date','%_diff']], on=['economy','date'], how='left')
    #where measure is energy times %_diff
    combined.loc[combined['measure'] == 'energy','value'] = combined.loc[combined['measure'] == 'energy','value'] * combined.loc[combined['measure'] == 'energy','%_diff']
    # #and change source to dataset+source
    # combined.loc[combined['measure'] == 'energy','source'] = combined.loc[combined['measure'] == 'energy','dataset'] + '_' + combined.loc[combined['measure'] == 'energy','source']
    #and change dataset to 'egeda_scaling'
    combined.loc[combined['measure'] == 'energy','dataset'] = 'egeda_scaling' + '_' + combined.loc[combined['measure'] == 'energy','dataset']
    #put pass road in
    combined_rescaled_data = pd.concat([combined,new_road], axis=0)
    
    #check the results using compare_total_energy_to_egeda_totals
    compare_total_energy_to_egeda_totals(combined_rescaled_data,unfiltered_combined_data, paths_dict,plotting_folder='rescaled')
    #drop the %_diff col
    combined_rescaled_data = combined_rescaled_data.drop(columns=['%_diff'])
    return combined_rescaled_data

def compare_total_energy_to_egeda_totals(combined_data,unfiltered_combined_data, paths_dict,plotting_folder=''):
    #get egeda data
    egeda_energy_selection_dict = {'measure': 
        ['energy'],
    'dataset': ['egeda_split_into_transport_types_using_8th $ egeda_9th_cleansed']}
    egeda_energy_combined_data = data_formatting_functions.filter_for_specifc_data(egeda_energy_selection_dict, unfiltered_combined_data)
    #drop all cols except economy, date, medium and value
    egeda_energy_combined_data = egeda_energy_combined_data[['economy','date','medium','transport_type','value']]
    #sum up the values for each economy, date and medium
    egeda_energy_combined_data = egeda_energy_combined_data.groupby(['economy','date','medium','transport_type']).sum().reset_index()

    #filter the new data to only include energy
    new_energy = combined_data.copy()
    new_energy = new_energy[new_energy['measure'] == 'energy']
    #drop all cols except economy, date, medium and value
    new_energy = new_energy[['economy','date','medium','transport_type','value']]
    #sum up the values for each economy, date and medium
    new_energy = new_energy.groupby(['economy','date','medium','transport_type']).sum().reset_index()

    analysis_and_plotting_functions.compare_egeda_and_new_energy_totals(egeda_energy_combined_data,new_energy,plotting_folder,paths_dict)

    return egeda_energy_combined_data, new_energy
