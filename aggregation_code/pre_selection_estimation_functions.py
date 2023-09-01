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
import re
logger = logging.getLogger(__name__)
plotting = True#change to false to stop plots from appearing
#eg from
#  Estimate Energy and passenger km using stocks:
# vkm = mileage * stocks
# energy = vkm * efficiency
# pkm = vkm * occupancy

def split_vehicle_types_using_distributions(unfiltered_combined_data):
    """
    Splits the vehicle types using distributions.

    Args:
        unfiltered_combined_data (pandas.DataFrame): The input data.

    Returns:
        pandas.DataFrame: The modified data.
    """

    #please nmote it will drop original vehicle type data after splitting because we want to aovid double counting it 
    #take in the data from vehicle_type_distributions.csv and then split stocks into them. note that there will be certain cases where this cannot be done because the stocks are already split into the vehicle types. The function will recognise these cases and skip them.
    #this splitting is only done on the more aggregated vehicle types like 'all', 'lpv', and 'ht' (since ht is really based off what most econmoies report as 'hts')
    #file is './intermediate_data/estimated/vehicle_type_distributionsFILE_DATE_ID.csv so find the altest version of it:
    # currently its format is with the col nbames:  Source Dataset Comments Date Medium 	Economy Transport Type Vehicle Type 	Vehicle1	Vehicle2	Vehicle3	Vehicle1_name	Vehicle2_name	Vehicle3_name
    
    date_id = utility_functions.get_latest_date_for_data_file('./intermediate_data/estimated/','vehicle_type_distributions')
    vehicle_type_distributions_file_path = './intermediate_data/estimated/vehicle_type_distributionsDATE{}.csv'.format(date_id)
    vehicle_type_distributions = pd.read_csv(vehicle_type_distributions_file_path)

    #convert cols to snake case
    vehicle_type_distributions.columns = [utility_functions.replace_bad_col_names(col) for col in vehicle_type_distributions.columns]
                    
    #convert all values in all columns to snakecase, except economy date and value
    vehicle_type_distributions = utility_functions.convert_all_cols_to_snake_case(vehicle_type_distributions)

    #because of the complexity of this we will do it line by line within the vehicle_type_distributions file:
    for index, row in vehicle_type_distributions.iterrows():
        original_vehicle_type = row['vehicle_type']
        #new vehicle types names will be in the cols with regex 'vehicle\d_name' and their vlaues iun the cols with regex 'vehicle\d'
        new_vehicle_type_names = [col for col in vehicle_type_distributions.columns if re.match('vehicle\d_name',col)]
        new_vehicle_type_values = [col for col in vehicle_type_distributions.columns if re.match('vehicle\d$',col)]
        new_vehicle_type_names = [row[col] for col in new_vehicle_type_names if not pd.isna(row[col])]
        new_vehicle_type_values = [row[col] for col in new_vehicle_type_values if not pd.isna(row[col])]
        economy = row['economy']
        transport_type = row['transport_type']

        #now we have the new vehicle types and their values. we will filter for rows in the unfiltered_combined_data that match the economy and transport type with measure = stocks, check each separate dataset/source combo (they should be concatednated alrady) for the vehicle type and then split the stocks into the new vehicle types
        rows_to_edit = unfiltered_combined_data[(unfiltered_combined_data['economy'] == economy)&(unfiltered_combined_data['medium'] == 'road') & (unfiltered_combined_data['transport_type'] == transport_type) & (unfiltered_combined_data['measure'] == 'stocks')]
        unique_datasets = rows_to_edit['dataset'].unique()
        # if economy == '10_MAS':
        #find unique vehicle types for each dataset. if they are already split into more than one of the new vehicle types then we will not edit them, else, grab the original vehicle type (if it is available) and split it into the new vehicle types
        for dataset in unique_datasets:
            unique_vtypes = rows_to_edit[rows_to_edit['dataset'] == dataset]['vehicle_type'].unique()
            if original_vehicle_type not in unique_vtypes:
                continue
            if len([v for v in unique_vtypes if v in new_vehicle_type_names]) > 1:
                continue
            #now we know that the vehicle type is in the dataset and it is not split into more than one of the new vehicle types. we will split it into the new vehicle types
            #first we will get the original vehicle type data
            original_vehicle_type_data = rows_to_edit[(rows_to_edit['dataset'] == dataset) & (rows_to_edit['vehicle_type'] == original_vehicle_type)]
            
            #drop original vehicle type data because we want to aovid double counting it. to do this, set index as all cols except value and then drop the original vehicle type data:
            temp_index_cols = [col for col in original_vehicle_type_data.columns if col != 'value']
            original_vehicle_type_data = original_vehicle_type_data.set_index(temp_index_cols)
            unfiltered_combined_data = unfiltered_combined_data.set_index(temp_index_cols)
            unfiltered_combined_data = unfiltered_combined_data.drop(original_vehicle_type_data.index)
            #reset index
            original_vehicle_type_data.reset_index(inplace=True)
            unfiltered_combined_data.reset_index(inplace=True)
            
            #now we will split it into the new vehicle types
            for new_vehicle_type_name, new_vehicle_type_value in zip(new_vehicle_type_names,new_vehicle_type_values):#todo check that this creates a kind of 2 column df or set of 2 lists, rather than keeping the originnal cols.
                # #breakpoint()
                new_vehicle_type_data = original_vehicle_type_data.copy()
                new_vehicle_type_data['vehicle_type'] = new_vehicle_type_name
                new_vehicle_type_data['value'] = new_vehicle_type_data['value'] * new_vehicle_type_value
                #change datasset to include info about how it was split
                new_vehicle_type_data['dataset'] = new_vehicle_type_data['dataset'] + ' vehicle_dist_split'

                #add new vehicle type data
                unfiltered_combined_data = pd.concat([unfiltered_combined_data,new_vehicle_type_data])

    return unfiltered_combined_data


#put it all thorugh calculate_new_data() which will:
# Using mileage, eff, occ and stocks we can estimate passenger km and energy. Where we are missing data eg. stocks, we will leave an na. 
# enable adjusting the mileage, occ and eff data by specified ranges to get a range of options
def estimate_petrol_diesel_splits(unfiltered_combined_data):
    """
    Estimates the petrol and diesel splits for the input data.

    Args:
        unfiltered_combined_data (pandas.DataFrame): The input data.

    Returns:
        dict: A dictionary containing the petrol and diesel splits.
    """
    # #breakpoint()
    #using the data we have we will find datasets where we have one of or both of ice_g and ice_d for each vehicle type and then where we do have them for a vehile type we'll use that to estimate the split between petrol and diesel. Then we can average out the splits for each vehicle type across all teh datasets and dates for each economy. This way  we can arrive at the most accurate estimate of the split between petrol and diesel for each economy.
    #where we dont have these splits, we will use the average of all otehr economys to estimate the split for that economy... this is a bit of a hack but it will have to do for now.
    #if there are no splits to average from at all then just leave it as na. we can just leave those drives as ice.
    #note that we could even include cng and lpg here but for now ignore as i think they are too economy specific
    splits_dict = {}
    for vehicle_type in unfiltered_combined_data.vehicle_type.unique():
        splits_dict[vehicle_type] = {}
        #gather vlaues to calculate the average split across all economys
        all_vehicle_type_values = []
        for economy in unfiltered_combined_data.economy.unique():
            #TEMP
            # #breakpoint if vehicle__type is 'car' or 'lpv' and economy is '19_THA'
            # if vehicle_type in ['car','lpv'] and economy == '19_THA':
            #     #breakpoint()
            splits_dict[vehicle_type][economy] = {}
            #get the datasets where we have both ice_g and ice_d for this vehicle type and economy
            datasets = unfiltered_combined_data[(unfiltered_combined_data['vehicle_type'] == vehicle_type) & (unfiltered_combined_data['economy'] == economy) & (unfiltered_combined_data['measure'] == 'stocks') & ((unfiltered_combined_data['drive'] == 'ice_g') | (unfiltered_combined_data['drive'] == 'ice_d'))]['dataset'].unique()
            
            #if we have more than just the 8th edition dataset, then drop that:
            if len(datasets) > 1 and '8th_edition_transport_model $ reference' in datasets:
                datasets = [d for d in datasets if d != '8th_edition_transport_model $ reference']
                
            if len(datasets) > 0:
                #get the average split for each dataset
                for dataset in datasets:
                    #get the average split for this dataset by getting the average of the ice_g and ice_d values and then calcualting the ratio of ice_g to ice_d
                    #also handle the case where we only have ice_g or ice_d by just setting the split to 1 or 0
                    ice_g = unfiltered_combined_data[(unfiltered_combined_data['vehicle_type'] == vehicle_type) & (unfiltered_combined_data['economy'] == economy) & (unfiltered_combined_data['measure'] == 'stocks') & (unfiltered_combined_data['drive'] == 'ice_g') & (unfiltered_combined_data['dataset'] == dataset)]['value']
                    ice_d = unfiltered_combined_data[(unfiltered_combined_data['vehicle_type'] == vehicle_type) & (unfiltered_combined_data['economy'] == economy) & (unfiltered_combined_data['measure'] == 'stocks') & (unfiltered_combined_data['drive'] == 'ice_d') & (unfiltered_combined_data['dataset'] == dataset)]['value']
                    
                    if len(ice_g) == 0:
                        ice_g_split = 0
                    elif len(ice_d) == 0:
                        ice_g_split = 1
                    else:
                        ice_g_split = ice_g.mean() / (ice_g.mean() + ice_d.mean())#times this by ice to get petrol and then (1-split)*ice to get diesel
                    splits_dict[vehicle_type][economy][dataset] = ice_g_split
                    all_vehicle_type_values.append(ice_g_split)
                #get the average split across all datasets
                splits_dict[vehicle_type][economy]['average'] = np.mean(list(splits_dict[vehicle_type][economy].values()))
            else:
                splits_dict[vehicle_type][economy]['average'] = np.nan
        if len(all_vehicle_type_values) > 0:
            splits_dict[vehicle_type]['average'] = np.mean(all_vehicle_type_values)
        else:
            splits_dict[vehicle_type]['average'] = np.nan
    #now we have the average split for each vehicle type and economy. We can use this to estimate the split of iceg to iced for ice stocks where we dont have the split alraedy or where we use the iea stock share values to estiamte stocks for ice, bev and phev.
    #we can also times this split by phev to get phevd vs phevg since you can expect them to follow similar splits to iceg and iced
    return splits_dict


def check_for_other_vtype_splits(splits_dict, vehicle_type, economy):
    #check that there are no splits for te more aggregated vesion of this vehicle type, eg. for cars,lt,suv check that there are no splits for 'lpv':
    lpv_vehicles = ['car','lt','suv']
    ht_vechicles = ['mt','ht']
    freight_all_vehicles = ['lcv','ht','mt']
    ice_g_split = np.nan
    if vehicle_type in lpv_vehicles:
        #grab the splits for lpv
        if not np.isnan(splits_dict['lpv'][economy]['average']):
            ice_g_split = splits_dict['lpv'][economy]['average']
        elif not np.isnan(splits_dict['lpv']['average']):
            ice_g_split = splits_dict['lpv']['average']
        else:
            print('no splits to use for splitting ice into p and d. vehicle type {} and economy {}'.format(vehicle_type, economy))
    elif vehicle_type in ht_vechicles:
        #grab the splits for ht
        if not np.isnan(splits_dict['ht'][economy]['average']):
            ice_g_split = splits_dict['ht'][economy]['average']
        elif not np.isnan(splits_dict['ht']['average']):
            ice_g_split = splits_dict['ht']['average']
        elif not np.isnan(splits_dict['all'][economy]['average']):
            ice_g_split = splits_dict['all'][economy]['average']
        elif not np.isnan(splits_dict['all']['average']):
            ice_g_split = splits_dict['all']['average']
        else:
            print('no splits to use for splitting ice into p and d. vehicle type {} and economy {}'.format(vehicle_type, economy))
    elif vehicle_type in freight_all_vehicles:
        #grab the splits for freight all
        if not np.isnan(splits_dict['all'][economy]['average']):
            ice_g_split = splits_dict['all'][economy]['average']
        elif not np.isnan(splits_dict['all']['average']):
            ice_g_split = splits_dict['all']['average']
        else:
            print('no splits to use for splitting ice into p and d. vehicle type {} and economy {}'.format(vehicle_type, economy))
    else:
        print('no splits to use for splitting ice into p and d. vehicle type {} and economy {}'.format(vehicle_type, economy))
        
    return ice_g_split

def split_ice_phev_into_petrol_and_diesel(unfiltered_combined_data, splits_dict):
    """
    Splits the ICE and PHEV into petrol and diesel.

    Args:
        unfiltered_combined_data (pandas.DataFrame): The input data.
        splits_dict (dict): A dictionary containing the petrol and diesel splits.

    Returns:
        pandas.DataFrame: The modified data.
    """
    #split the ice stocks into petrol and diesel. This will use data from sources specific to each region or economy and can be used in association with the iea data to split ice into petrol and diesel. This is important because diesel and petrol have different energy intensities so it is usefl to estiamte their relative data. 
    #it will use the splits from estimate_petrol_diesel_splits
    #note that this wont replace the data for ice, isntead it will jsut add new data for ice_g and ice_d
    for vehicle_type in unfiltered_combined_data.vehicle_type.unique():
        for economy in unfiltered_combined_data.economy.unique():
            # if (vehicle_type=='car'):
            #     breakpoint()
            if np.isnan(splits_dict[vehicle_type][economy]['average']):
                if np.isnan(splits_dict[vehicle_type]['average']):
                    ice_g_split = check_for_other_vtype_splits(splits_dict, vehicle_type, economy)                    
                else:
                    ice_g_split = splits_dict[vehicle_type]['average']
            else:
                ice_g_split = splits_dict[vehicle_type][economy]['average']
                
            #TEMP FIX. add .2 to ice_dsplit if vehicle tyoe is one of ['car', 'suv', 'lt', 'lpv']
            if economy in ['20_USA']:
                if vehicle_type in ['car', 'suv', 'lt', 'lpv']:
                    ice_g_split = ice_g_split - .2
            #also decrease ice_g split for lcv in all economy. currently too alrge
            if vehicle_type == 'lcv':
                ice_g_split = ice_g_split - .2
                if ice_g_split < 0.1:
                    ice_g_split = 0.1                    
            #TEMP FIX over   
                
            ice_d_split = 1 - ice_g_split
            #get data where drive = ice
            ice_data = unfiltered_combined_data[(unfiltered_combined_data['vehicle_type'] == vehicle_type) & (unfiltered_combined_data['economy'] == economy) & (unfiltered_combined_data['measure'] == 'stocks') & (unfiltered_combined_data['drive'] == 'ice')]

            #add 'ice_split' to dataset so we know this data ahs been split
            ice_data['dataset'] = ice_data['dataset'] + ' ice_split'

            #split the ice data into petrol and diesel
            ice_g_data = ice_data.copy()
            ice_g_data['drive'] = 'ice_g'
            ice_g_data['value'] = ice_g_data['value'] * ice_g_split
            ice_d_data = ice_data.copy()
            ice_d_data['drive'] = 'ice_d'
            ice_d_data['value'] = ice_d_data['value'] * ice_d_split
            #add the new data to the unfiltered_combined_data
            unfiltered_combined_data = pd.concat([unfiltered_combined_data,ice_g_data])
            unfiltered_combined_data = pd.concat([unfiltered_combined_data,ice_d_data])

            #do same phevs
            phev_data = unfiltered_combined_data[(unfiltered_combined_data['vehicle_type'] == vehicle_type) & (unfiltered_combined_data['economy'] == economy) & (unfiltered_combined_data['measure'] == 'stocks') & (unfiltered_combined_data['drive'] == 'phev')]

            phev_data['dataset'] = phev_data['dataset'] + ' ice_split'
            
            phev_g_data = phev_data.copy()
            phev_g_data['drive'] = 'phev_g'
            phev_g_data['value'] = phev_g_data['value'] * ice_g_split
            phev_d_data = phev_data.copy()
            phev_d_data['drive'] = 'phev_d'
            phev_d_data['value'] = phev_d_data['value'] * ice_d_split
            #add the new data to the unfiltered_combined_data
            unfiltered_combined_data =pd.concat([unfiltered_combined_data, phev_g_data], ignore_index=True)

            unfiltered_combined_data =pd.concat([unfiltered_combined_data, phev_d_data], ignore_index=True)
            
    return unfiltered_combined_data

def split_stocks_where_drive_is_all_into_bev_phev_and_ice(unfiltered_combined_data):
    """
    Splits the stocks where drive is all into BEV, PHEV, and ICE.

    Args:
        unfiltered_combined_data (pandas.DataFrame): The input data.

    Returns:
        pandas.DataFrame: The modified data.
    """
    #PLEASE NOTE THAT THIS IGNORES THE CNG OR LPG STOCKS THAT COULD BE IN THAT ECONOMY. SO THEY THEN WONT BE included
    #using the iea ev data explorer data we will split all estimates for stocks in drive=='all' into ev, phev and ice. this will be done by using the iea stock share for ev's and phev's and then the rest will be ice.
    #for any economys where we dont have iea data we will just set the ev and phev shares to 0 and then the rest will be ice. We can fill them in later if we want to.
    combined_data_all_drive = unfiltered_combined_data[unfiltered_combined_data['drive']=='all'].copy()

    iea_ev_explorer_selection_dict = {'measure': 
        ['stock_share'],
    'medium': ['road'],
    'dataset': ['iea_ev_explorer $ historical']}
    stock_shares = data_formatting_functions.filter_for_specifc_data(iea_ev_explorer_selection_dict, unfiltered_combined_data)
    if stock_shares.empty:
        #we may be only running this for a subset of the data so we will just return the unfiltered_combined_data. this leaves any drives that are 'all' as 'all'.
        return unfiltered_combined_data
    #make stock shares wide on drive col
    cols = stock_shares.columns.tolist()
    cols.remove('drive')
    cols.remove('value')
    stock_shares = stock_shares.pivot(index=cols, columns='drive', values='value').reset_index()
    #make the ice share  =  1- ev_share - phev_share
    stock_shares['ice'] = 1 - stock_shares['bev'] - stock_shares['phev']
    #set measure to stocks
    stock_shares['measure'] = 'stocks'
    stock_shares['unit'] = 'stocks'

    #join stock shares to combined data's stocks 
    combined_data_stocks = combined_data_all_drive[(combined_data_all_drive['measure'] == 'stocks')]
    cols.remove('dataset')
    combined_data_stocks = combined_data_stocks.merge(stock_shares, on = cols, how = 'left', indicator = True)
    #drop any right only
    combined_data_stocks = combined_data_stocks[combined_data_stocks['_merge'] != 'right_only'].copy()
    #for left only, set dataset_y to 'iea_ev_explorer_no_data' then drop _merge
    combined_data_stocks.loc[combined_data_stocks['_merge'] == 'left_only','dataset_y'] = 'iea_ev_explorer_no_data'#this allows us to keep the information that there are perhaps no evs in this row, because teh iea didnt ahve data on it. 
    
    #where ice bev and phev are na then set themn to 0 and ice to 1
    combined_data_stocks['bev'] = combined_data_stocks['bev'].fillna(0)
    combined_data_stocks['phev'] = combined_data_stocks['phev'].fillna(0)
    combined_data_stocks['ice'] = combined_data_stocks['ice'].fillna(1)

    #split stocks into ev, phev and ice suing the shares
    combined_data_stocks['stocks_bev'] = combined_data_stocks['value'] * combined_data_stocks['bev']
    combined_data_stocks['stocks_phev'] = combined_data_stocks['value'] * combined_data_stocks['phev']
    combined_data_stocks['stocks_ice'] = combined_data_stocks['value'] * combined_data_stocks['ice']

    #drop stocks and ev, phev and ice shares
    combined_data_stocks = combined_data_stocks.drop(columns = ['value','bev','phev','ice'])

    #rename stocks_ev, stocks_phev and stocks_ice to bev, phev and ice
    combined_data_stocks = combined_data_stocks.rename(columns = {'stocks_bev':'bev','stocks_phev':'phev','stocks_ice':'ice'})
    #where dataset_y si not nan: create dataset col which is dataset_x without the $ sign and then $ and dataset_y wihtout the $ sign
    combined_data_stocks['dataset'] = combined_data_stocks['dataset_x'].str.replace('$','') + ' $ ' + combined_data_stocks['dataset_y'].str.replace('$','')
    #remove dataset_x and dataset_y
    combined_data_stocks = combined_data_stocks.drop(columns = ['dataset_y','dataset_x'])
    #add dataset to cols
    cols.append('dataset')
    #melt
    combined_data_stocks_tall = combined_data_stocks.melt(id_vars = cols, value_vars = ['bev','phev','ice'], var_name = 'drive', value_name = 'value')
    
    unfiltered_combined_data = pd.concat([combined_data_stocks_tall,unfiltered_combined_data])

    #rop any duplicates that may have occured because of the same aggregations done for individual datasets(eg. 8th data)
    unfiltered_combined_data = unfiltered_combined_data.drop_duplicates()
    # #breakpoint()
    return unfiltered_combined_data



def calculate_energy_and_activity(stocks_mileage_occupancy_load_efficiency_combined_data_concordance, paths_dict):

    #make combined data wide so we have a column for each measure
    INDEX_COLS_no_measure = paths_dict['INDEX_COLS'].copy()
    INDEX_COLS_no_measure.remove('measure')
    INDEX_COLS_no_measure.remove('unit')
    data = stocks_mileage_occupancy_load_efficiency_combined_data_concordance.pivot(index = INDEX_COLS_no_measure, columns = 'measure', values = 'value').reset_index()
    #to prevent any issues with div by 0 we will replace all 0s with nans. then any nans timesed by anything will be nan
    data = data.replace(0,np.nan)

    data['travel_km'] = data['mileage'] * data['stocks']
    data['activity'] = data['travel_km'] * data['occupancy_or_load']
    data['energy'] = data['travel_km'] / data['efficiency']
    
    #make long again
    data = data.melt(id_vars = INDEX_COLS_no_measure, var_name = 'measure', value_name = 'value')#todo i dont know how this will interact with the other possible columns we'll have. maybrwe should have id vars = data.columns

    #drop any nans in value col
    data = data.dropna(subset = ['value'])

    #set units based on measure
    
    data['unit'] = data['measure'].map(paths_dict['measure_to_units_dict'])
    
    #set dataset to 'estimated'
    data['dataset'] = 'estimated $ calculate_energy_and_activity()'

    #set comment to 'no_comment'
    data['comment'] = 'no_comment'
    #todo: add in the range options
    return data

# def TEMP_make_drive_equal_all(combined_data, paths_dict):
#     #NOTE IT WOULD BE GOOD TO SOMEHOW SEPARATE EVS FROM THIS. ONE DAY
#     #because we dont have much data split by drive we will sum up all road passenger data by vehicel type in 8th editiona so it can be compared to the data for which we have drive = 'all'. We will also set efficiency to the same value as for drive = 'g' since the majority of stocks are g anyway.
#     #also need to sum up passenger km and energy by vehjicle type, and avergae occupancy and mileage by vehicle type. but why?

#     #grab 8th edition energy, stocks and passneger km for passenger road
#     combined_data_8th_edition = combined_data.loc[(combined_data['dataset'] == '8th_edition_transport_model $ reference') & (combined_data['medium'] == 'road') & (combined_data['measure'].isin(['stocks','energy','activity']))].copy()
#     combined_data_8th_edition['drive'] = 'all'
#     #sum
#     combined_data_8th_edition = combined_data_8th_edition.groupby(paths_dict['INDEX_COLS']+['dataset']).sum().reset_index()

#     # #grab the occupancy_or_load and mileage for 'g' and set it to 'all'?
#     # combined_data_means = combined_data.loc[(combined_data['medium'] == 'road') & (combined_data['measure'].isin(['occupancy_or_load','mileage']))].copy()
#     # combined_data_means['drive'] = 'all'
#     # combined_data_means = combined_data_means.groupby(paths_dict['INDEX_COLS']+['dataset']).mean().reset_index()

#     # #grab efficiency for g and set it to the efficiency for all. This is temporary until we understand the efficiency of ice vs g better
#     # combined_data_efficiency = combined_data.loc[(combined_data['medium'] == 'road')& (combined_data['measure'] == 'efficiency') & (combined_data['drive'] == 'g')].copy()
#     # combined_data_efficiency['drive'] = 'all'

#     #combine with a road version of combined data 
#     combined_data_road = combined_data.loc[(combined_data['medium'] == 'road')].copy()
#     #drop those from combined data
#     combined_data = combined_data.loc[~((combined_data['medium'] == 'road'))].copy()
#     combined_data_road = pd.concat([combined_data_road,combined_data_8th_edition],sort=False)#,combined_data_means,combined_data_efficiency
#     #drop drive != all
#     combined_data_road = combined_data_road.loc[combined_data_road['drive'] == 'all'].copy()
#     #drop duplicates
#     combined_data_road = combined_data_road.drop_duplicates().copy()
#     #set comment to 'no_comment'
#     combined_data_road['comment'] = 'no_comment'
#     #concat with combined data
#     combined_data = pd.concat([combined_data,combined_data_road],sort=False)

    # return combined_data
