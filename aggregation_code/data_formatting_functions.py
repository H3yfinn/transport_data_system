import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import time
import matplotlib.pyplot as plt
import warnings
import sys
import yaml
import utility_functions
PRINT_GRAPHS_AND_STATS = False

import logging
logger = logging.getLogger(__name__)

#ignore by message
warnings.filterwarnings("ignore", message="indexing past lexsort depth may impact performance")

"""
combined_dataset dataframe, a combination of all the data that has been aggregated for this selection process, including their values and the dataset they came from. 
combined_data_concordance = dataframe, a dataframe of all the unique index rows of data that has been aggregated for this selection process,but with a few extra columns to help with the selection process. This is created in aggregation_code\1_aggregate_cleaned_datasets.py by creating a unique index row for each row in the output dataset we intend to have. It will have columns such as 'dataseection_method' and 'potential_datapoints' which are used to help the user select the correct datapoint for each row. 

INDEX_COLS = list of strings, the columns which make up the users intended index of the combined dataset. This is used to group the data by these columns so the dataframe can be treated like a dicitonary bv accessing different rows by their index.

previous_combined_dataset  = dataset from previous run. This can be used to import settings made, such as rows to ignore, values already selected etc. It will be compared against the combined dataset and have its rows merged into the combined dataset if the indexes are the same.
    MATCHING MUST BE DONE USING value COLUMN TOO
 However, the index will also include the vlaue column, so if the value column has changed, the row will be treated as a new row. To make sure this works properly, the vlaue MAY NEED TO be rounded to 0 decimal places ?
 There may be cases where the selection process is exited due to a user error. it is important that the previous combined dataset can retrieve the users progress so they can continue where they left off. 
    To habdle this possibility its important that the combined dataset AND combined_Datset_concordance is updated every time the user makes a selection. 
    This is why we dont create copies of the combined dataset and combined data concordance, but instead update them directly. 
    This is also why we need to save the combined dataset and combined data concordance to a pickle file every time the user makes a selection, so that even if the program crashes, the user can still continue where they left off.
This should also be able to be implememted even if the new combined datraset and concoradance are smaller thanb previous one. EASY

datapoints_to_ignore = the datapoint that the user has decided to ignore. Can have a column that determines if the data is  being ignored permanently and a column for if hte data is being ignored temporarily.
    thios should probably be implemented right at the start opf the seleciton process so that the duplicated datasets can be defined early on.

Functions we need:
 sxomething to remove a dataset form the list of datasets for a row in the combined data concordance. This could be used when the user decides to ignore a datapoint but it seems like it would be useful to ahve generally.\
 

 order of functions:
 1. creat data concordance
 2. identify duplicated datapoints to select for

"""

def filter_for_most_detailed_vehicle_type_stock_breakdowns(combined_data, IGNORE_8TH_DATASETS=True, IGNORE_ATO_DATASETS=True):
    """Note this is for stocks only
    this will run through each economys data and identify if there is any datasets with data that specifies more than jsut the simplified vehicle types or less (lpv, 2w, buses, all(all is for freight where we dont split into different freight types) or isntead of all: lcv, ht).
    Currently the vehicel types we split into are:
    vehicle_types['passenger'] = ['car', '2w', 'bus', 'lt','suv']
    vehicle_types['freight'] =  ['lcv', 'mt', 'ht']

    If there is a dataset with data on all of the vehicle types for one transport type (i.e. for passenger that is cars, motorbikes, buses, lpv's, minibuses) and another dataset with data on only the simplified vehicle types or less (eg cars, motorbikes, buses, lcv's, ht's) then this will default to using the dataset with the more detailed breakdown of vehicle types. This is because we want to use the most detailed breakdown of vehicle types as possible. 
    Then it will also remove the dataset with only the simplified vehicle types.

    This is especially important so tha tthe user doesnt accidentally select a dataset with less detailed vehicle types than they intended. As those less detailed datasets may be aggregated from the more detailed datasets, so the user may be selecting a dataset that has already been aggregated from another dataset.
    """
    #breakpoint()
    #     combined_data_stocks.vehicle_type.unique()
    # array(['bus', 'lcv', '2w', 'mt', 'lt'], dtype=object)
    vehicle_types = {}
    vehicle_types['passenger'] = ['car', '2w', 'bus', 'lt','suv']
    vehicle_types['freight'] =  ['lcv', 'mt', 'ht']
    combined_data_stocks = combined_data[combined_data['measure']=='stocks']
    for economy in combined_data_stocks['economy'].unique():
        economy_data = combined_data_stocks[combined_data_stocks['economy']==economy]

        #however we also need to filter out 'vehicle_dist_split' from the dataset name because it is splitting datasets by vehicle types essentially (i.e.[8th_-_new_vtypes_and_drives $ reference ice_split: ['2w' 'bus'] , 8th_-_new_vtypes_and_drives $ reference vehicle_dist_split: ['car' 'suv' 'lt']].
        #so create copy of df with all dataset names that contain 'vehicle_dist_split', set to have vehicle_dist_split removed from the dataset name:
        economy_data_copy = economy_data.copy()
        economy_data_copy['dataset'] = economy_data_copy['dataset'].apply(lambda x: x.replace(' vehicle_dist_split', ''))

        #filter through unique datasets for each transport type:
        for transport_type in economy_data['transport_type'].unique():
            datasets_with_all_vehicle_types = []
            for dataset in economy_data['dataset'].unique():
                non_dist_split_dataset = dataset.replace(' vehicle_dist_split', '')
                #if this data contains data on all the vehicle types for this transport type then add it 

                if set(vehicle_types[transport_type]).issubset(set(economy_data_copy[(economy_data_copy['dataset']==non_dist_split_dataset)&(economy_data_copy['transport_type']==transport_type)]['vehicle_type'].unique())):
                    datasets_with_all_vehicle_types.append(dataset)
            if IGNORE_8TH_DATASETS:
                #drop any datasets that contain 8th from the list, these are our backup datasets, so probably better to go with other data
                datasets_with_all_vehicle_types = [dataset for dataset in datasets_with_all_vehicle_types if '8th' not in dataset] 
            if IGNORE_ATO_DATASETS:
                #drop any datasets that contain ATO from the list, these are our backup datasets, so probably better to go with other data
                datasets_with_all_vehicle_types = [dataset for dataset in datasets_with_all_vehicle_types if 'ato' not in dataset]
            #if there is a dataset with all the vehicle types for this transport type then remove the datasets that arent in this list
            if len(datasets_with_all_vehicle_types)>0:
                datasets_to_remove = [dataset for dataset in economy_data['dataset'].unique() if dataset not in datasets_with_all_vehicle_types]
                #drop the rows with these datasets for that economy and transport type
                combined_data = combined_data[~((combined_data['measure']=='stocks')&(combined_data['economy']==economy)&(combined_data['transport_type']==transport_type)&(combined_data['dataset'].isin(datasets_to_remove)))]

                print('removing datasets: \n'+str(datasets_to_remove)+'\n For economy: '+economy+' and transport type: '+transport_type + 'for stocks data only')
                print('\nThe datasets which were kept for this transport type with their available vehicle types are as follows:' + str(datasets_with_all_vehicle_types) + 'as they had the vehicle types: '+str(vehicle_types[transport_type]))
            else:#tell the user that there is no dataset with all the vehicle types for this transport type, and show what datasets there are with what vehicle types are availbel
                print('no dataset with all vehicle types for economy: '+economy+' and transport type: '+transport_type + 'for stocks data only')
                print('datasets available for this transport type with their available vehicle types are as follows:')
                for dataset in economy_data['dataset'].unique():
                    print(dataset+': '+str(economy_data[(economy_data['dataset']==dataset)&(economy_data['transport_type']==transport_type)]['vehicle_type'].unique()))

    return combined_data

    
# def filter_for_most_detailed_stocks_breakdown(combined_data):
#     """this will run through each economys data and identify if there is any datasets with data on lcv's and ldv's or only datasets with only ldv's. if there is a dataset with data on both then it will remove the dataset with only ldv's. This is because we want to use the most detailed breakdown of stocks as possible. 
#     This will make the assumption that the data on lcv's and ldv's is more accurate than the data on ldv's only."""
#     for economy in combined_data['economy'].unique():
#         economy_data = combined_data[combined_data['economy']==economy]
#         if 'lcv' in economy_data['vehicle_type'].unique() and 'ldv' in economy_data['vehicle_type'].unique():
#             logger.info('removing ldv data for economy: '+economy)
#             combined_data = combined_data[~((combined_data['economy']==economy)&(combined_data['vehicle_type']=='ldv'))]
#     return combined_data

def extract_latest_groomed_data():
    #open the yml and extract the datasets:
    with open('config/selection_config.yml', 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    datasets_transport = []
    datasets_other = []
    for dataset in cfg['datasets']:
        if cfg['datasets'][dataset]['included']:
            if cfg['datasets'][dataset]['type'] == 'transport':
                datasets_transport.append([cfg['datasets'][dataset]['folder'], dataset, cfg['datasets'][dataset]['file_path']])
            else:
                datasets_other.append([cfg['datasets'][dataset]['folder'], dataset, cfg['datasets'][dataset]['file_path']])
    #now replace the FILE_DATE_ID with the latest date available for each file:
    for dataset in datasets_transport:
        #get the latest file
        latest_file = utility_functions.get_latest_date_for_data_file(dataset[0], dataset[1])
        #add date to the start of latest_file
        try:
            latest_file = 'DATE'+latest_file
        except:
            breakpoint()
            raise Exception('The latest file for {} is not a date. Please make sure the file name is a date in the format DATEYYYMMDD'.format(dataset))
        #replace the FILE_DATE_ID with the latest file date
        dataset[2] = dataset[2].replace('FILE_DATE_ID', latest_file)
    for dataset in datasets_other:
        #get the latest file
        latest_file = utility_functions.get_latest_date_for_data_file(dataset[0], dataset[1])
        #add date to the start of latest_file
        latest_file = 'DATE'+latest_file
        #replace the FILE_DATE_ID with the latest file date
        dataset[2] = dataset[2].replace('FILE_DATE_ID', latest_file)
    return datasets_transport, datasets_other

def convert_concordance_to_combined_data(concordance, old_combined_data):
    new_combined_data = concordance.dropna(subset=['value'])
    #get cols not in combined_data but in concordance
    cols_to_drop = [col for col in concordance.columns if col not in old_combined_data.columns]
    new_combined_data = new_combined_data.drop(cols_to_drop, axis=1)
    return new_combined_data

def make_quick_fixes_to_datasets(combined_data):

    #and where fuel is na, set it to 'All'
    combined_data['fuel'] = combined_data['fuel'].fillna('all')
    #if scope col is na then set it to 'national'
    combined_data['scope'] = combined_data['scope'].fillna('national')
    #if comment is NA then set it to 'No comment'
    combined_data['comment'] = combined_data['comment'].fillna('no_comment')
    #remove all na values in value column
    combined_data = combined_data[combined_data['value'].notna()]

    ##########
    #where medium is not 'road' or 'Road', and the vehicle_type is either 'all', np.nan, None or the same value as medium, then set vehicle type and drive to 'all'. This is because the medium is the only thing that is relevant for non-road, currently.
    #Note this is a temp fix and we really should jsut fix tyhe input data methods

    combined_data.loc[(combined_data['medium'] != 'road') & (combined_data['vehicle_type'] == combined_data['medium']), 'vehicle_type'] = 'all'
    combined_data.loc[(combined_data['medium'] != 'road') & (combined_data['vehicle_type'].isna()), 'vehicle_type'] = 'all'
    combined_data.loc[(combined_data['medium'] != 'road') & (combined_data['vehicle_type'] == None), 'vehicle_type'] = 'all'

    # combined_data.loc[(combined_data['medium'] != 'road') & (combined_data['vehicle_type'] == 'all'), 'drive'] = 'all'
    combined_data.loc[(combined_data['medium'] != 'road') & (combined_data['drive'] == combined_data['medium']), 'drive'] = 'all'
    combined_data.loc[(combined_data['medium'] != 'road') & (combined_data['drive'].isna()), 'drive'] = 'all'
    combined_data.loc[(combined_data['medium'] != 'road') & (combined_data['drive'] == None), 'drive'] = 'all'

    ##########

    #To make things faster in the manual dataseelection process, for any rows in the eighth edition dataset where the data for both the carbon neutral and reference scenarios (in source column) is the same, we will remove the carbon neutral scenario data, as we would always choose the reference data anyways.
    combined_data = combined_data[combined_data['source'] != 'carbon_neutrality']

    #where we have measures = either occupancy or load then name them to occupancy_or_load. We know that the transport type will inidcate if it is occupancy or load.
    combined_data.loc[combined_data['measure'].isin(['occupancy', 'load']), 'measure'] = 'occupancy_or_load'
    #and set unit to passengers_or_tonnes
    combined_data.loc[combined_data['measure'].isin(['occupancy_or_load']), 'unit'] = 'passengers_or_tonnes'
    
    #do same for freight tonne km and pasenger km
    combined_data.loc[combined_data['measure'].isin(['freight_tonne_km', 'passenger_km']), 'measure'] = 'activity'
    combined_data.loc[combined_data['measure'].isin(['activity']), 'unit'] = 'passenger_km_or_freight_tonne_km'

    return combined_data

def check_dataset_for_issues(combined_data, INDEX_COLS,paths_dict):

    #if there are any nans in the index columns (EXCEPT FUEL) then throw an error and let the user know: 
    INDEX_COLS_no_fuel = INDEX_COLS.copy()
    INDEX_COLS_no_fuel.remove('fuel')
    error_file_path = paths_dict['combined_data_error.pkl']
    if combined_data[INDEX_COLS_no_fuel].isna().any().any():
        #find the columns with nans
        cols_with_nans = combined_data[INDEX_COLS_no_fuel].isna().any()
        #print the columns with nans and how many nans there are in each
        logging.info('The following columns have nans: ')
        for col in cols_with_nans[cols_with_nans].index:
            logging.info('{}: {}'.format(col, combined_data[col].isna().sum()))
            #save the data to a pickle file so that the user can see what the nans are
        combined_data.to_pickle(error_file_path)
        logging.error('There are nans in the index columns. Please fix this before continuing. The data has been saved to a pickle file. The path to the file is: {}'.format(error_file_path))
        raise Exception('There are nans in the index columns. Please fix this before continuing. The data has been saved to a pickle file. The path to the file is: {}'.format(error_file_path))
    #Important step: make sure that units are the same for each measure so that they can be compared. If they are not then the measure should be different.
    #For example, if one measure is in tonnes and another is in kg then they should just be converted. But if one is in tonnes and another is in number of vehicles then they should be different measures.
    for measure in combined_data['measure'].unique():
        if len(combined_data[combined_data['measure'] == measure]['unit'].unique()) > 1:
            logging.info(measure)
            logging.info(combined_data[combined_data['measure'] == measure]['unit'].unique())
            # save data to pickle file for viewing
            combined_data.to_pickle(error_file_path)
            logging.error('There are multiple units for this measure. This is not allowed. Please fix this before continuing. The data has been saved to a pickle file. The path to the file is: {}'.format(error_file_path))
            raise Exception('There are multiple units for this measure. This is not allowed. Please fix this before continuing. The data has been saved to a pickle file. The path to the file is: {}'.format(error_file_path))
        
    #check for any duplicates
    if len(combined_data[combined_data.duplicated()]) > 0:
        logging.info(combined_data[combined_data.duplicated()])
        # save data to pickle file for viewing
        
        combined_data.to_pickle(error_file_path)
                                                            
        raise Exception('There are {} duplicates in the combined data. Please fix this before continuing. Data saved to {}'.format(len(combined_data[combined_data.duplicated()]), error_file_path))

def combine_dataset_source_col(combined_data):
    #A make thigns easier in this process, we will concatenate the source and dataset columns into one column called dataset. But if source is na then we will just use the dataset column
    combined_data['dataset'] = combined_data.apply(lambda row: row['dataset'] if pd.isna(row['source']) else row['dataset'] + ' $ ' + row['source'], axis=1)
    #then drop source column
    combined_data = combined_data.drop(columns=['source'])
    return combined_data

def combine_datasets(datasets, paths_dict,dataset_frequency='yearly'):
    if dataset_frequency != 'yearly':
        raise Exception('The frequency for this dataset is not annual. This library is not ready for anything other than annual data, yet.')
    #loop through each dataset and load it into a dataframe, then concatenate the dataframes together
    combined_data = pd.DataFrame()
    logging.info('\nCombining datasets:\n')
    for dataset in datasets:
        #datasets can be broken into (folder, dataset name, file path)
        print('Combining dataset: {}'.format(dataset[1]))
        new_dataset = pd.read_csv(dataset[2])

        #convert cols to snake case
        new_dataset.columns = [utility_functions.replace_bad_col_names(col) for col in new_dataset.columns]
        
        #check that all the cols in index cols are in the dataset
        for col in paths_dict['INDEX_COLS']:
            if col not in new_dataset.columns:
                raise Exception('The column {} is not in the dataset {}'.format(col, dataset[1]))
        #convert all values in all columns to snakecase, except economy date and value
        new_dataset = utility_functions.convert_all_cols_to_snake_case(new_dataset)

        #filter for dataset freuqncy in frequency column
        new_dataset = new_dataset[new_dataset['frequency'] == dataset_frequency]
        if len(new_dataset) == 0:
            logging.info('No data for this dataset {} in this frequency {}. Skipping...'.format(dataset[1], dataset_frequency))
            continue

        new_dataset = utility_functions.ensure_date_col_is_year(new_dataset)

        #concatenate the dataset to the combined data
        combined_data = pd.concat([combined_data, new_dataset], ignore_index=True)
        logging.info('Finished combining dataset: {}'.format(dataset[1]))
    combined_data = make_quick_fixes_to_datasets(combined_data)

    check_dataset_for_issues(combined_data, paths_dict['INDEX_COLS'],paths_dict)

    combined_data = combine_dataset_source_col(combined_data)

    ############################################################

    #SAVE DATA

    ############################################################
    #save data to pickle file in intermediate data. If we want to use this fot other reasons we can alwasys load it from here
    combined_data.to_pickle(paths_dict['unselected_combined_data'])
    logging.info('\nFinished combining datasets:\n')
    return combined_data

def create_concordance_from_combined_data(combined_data, frequency = 'yearly'):
    
    ############################################################

    #CREATE CONCORDANCE

    ############################################################
    #CREATE CONCORDANCE
    #create a concordance which contains all the unique rows in the combined data df, when you remove the dataset source and value and economy columns.
    combined_data_concordance = combined_data.drop(columns=['dataset','comment', 'value', 'economy']).drop_duplicates()#todo is this the ebst way to handle the cols
    #we will also have to split the frequency column by its type: Yearly, Quarterly, Monthly, Daily
    #YEARLY
    economys = combined_data['economy'].unique()
    yearly = combined_data_concordance[combined_data_concordance['frequency'] == frequency]
    #YEARS:
    MAX = yearly['date'].max()
    MIN = yearly['date'].min()
        
    date_format = utility_functions.determine_date_format(yearly)
    #using datetime creates a range of dates, separated by year with the first year being the MIN and the last year being the MAX
    if frequency == 'yearly':
        if date_format == 'yyyy':
            years = [str(year) for year in range(MIN, MAX+1)]
        else:
            years = pd.date_range(start=MIN, end=MAX, freq='Y')

    elif frequency == 'monthly':#todo this means that you can ruin data selection on only one frequency at a time, whiich means the way we name files and folders needs to be changed. maybe recondier this
        #using datetime creates a range of dates, separated by month with the first month being the MIN and the last month being the MAX
        if date_format == 'yyyy':
            logging.error("ERROR: you have selected monthly data but the date format is yyyy. Please change the date format to yyyy-mm-dd")
            raise Exception('ERROR: you have selected monthly data but the date format is yyyy. Please change the date format to yyyy-mm-dd')
        years = pd.date_range(start=MIN, end=MAX, freq='M')
    else:
        logging.error("ERROR: frequency not recognised. You might have to update the code to include this frequency.")
        raise Exception('ERROR: frequency not recognised. You might have to update the code to include this frequency.')

    #drop date from ATO_data_years
    yearly = yearly.drop(columns=['date']).drop_duplicates()
    #now do a cross join between the concordance and the years array
    combined_data_concordance_new = yearly.merge(pd.DataFrame(years, columns=['date']), how='cross')
    #and cross join with the economies
    combined_data_concordance_new = combined_data_concordance_new.merge(pd.DataFrame(economys, columns=['economy']), how='cross')

    #if date_format = 'yyyy' make the date col an int64
    if date_format == 'yyyy':
        combined_data_concordance_new['date'] = combined_data_concordance_new['date'].astype('int64')
    else:
        #set it to object
        combined_data_concordance_new['date'] = combined_data_concordance_new['date'].astype('object')

    return combined_data_concordance_new

def ensure_column_types_are_correct(df): 
    #we know that the date columns can either be yyyy or yyyy-mm-dd. if they are yyyy then make sure they are ints and then if they are yyyy-mm-dd then make sure they are datetimes.
    if 'date' in df.columns:
        utility_functions.ensure_date_col_is_year(df)
    #and if value is in the columns then make sure it is a float
    if 'value' in df.columns:
        df['value'] = df['value'].astype(float)
    return df

def TEMP_add_mileage_to_concordance(model_concordances_measures):
    #we will copy the occupancy rows and then change the measure to mileage
    occupancy_rows = model_concordances_measures[model_concordances_measures['measure'] == 'occupancy_or_load']
    occupancy_rows['measure'] = 'mileage'
    occupancy_rows['unit'] = 'km_per_stock'
    model_concordances_measures = pd.concat([model_concordances_measures, occupancy_rows], ignore_index=True)
    return model_concordances_measures

def TEMP_add_drive_all_to_concordance(model_concordances_measures):
    logging.info("TEMP: adding drive_all to concordance for all road measures")
    #we will copy the rows where medium is road and then change the drive to all. Then add that on top of what is already there. Then drop duplicates.
    # #this allows us to use some of the road data for drive=all 
    drive_all_rows = model_concordances_measures[(model_concordances_measures['medium'] == 'road')]
    drive_all_rows['drive'] = 'all'
    # #remove the rows from the model_concordances_measures df
    # model_concordances_measures = model_concordances_measures[~model_concordances_measures.medium.isin(['road'])]
    model_concordances_measures = pd.concat([model_concordances_measures, drive_all_rows], ignore_index=True)
    model_concordances_measures = model_concordances_measures.drop_duplicates()
    return model_concordances_measures


# def TEMP_remove_freight_ldvs_from_concordance(model_concordances_measures):
#     #remove freight_ldvs from the concordance
#     model_concordances_measures = model_concordances_measures.loc[~((model_concordances_measures.vehicle_type == 'ldv') & (model_concordances_measures.transport_type=='freight'))]
#     return model_concordances_measures

def TEMP_replace_drive_types(df):
    # concordance = pd.read_csv('input_data/concordances/model_concordances_measures_old.csv')
    old_transport_categories = pd.read_csv('input_data/concordances/9th/manually_defined_transport_categories_OLD.csv')
        
    #make sure the columns are snake case
    old_transport_categories.columns = [utility_functions.convert_string_to_snake_case(col) for col in old_transport_categories.columns]
    #convert all values in cols to snake case
    old_transport_categories = utility_functions.convert_all_cols_to_snake_case(old_transport_categories)

    #we are going to replace the drive types we expect with new ones

    #take in the categories and do an outer join on medium, transport type and vehicle type. then we can see which rows are missing on the left or right. For the matching rows we will replace the drive type with the new one. 
    
    df = pd.merge(df, old_transport_categories, how='outer', on=['medium', 'transport_type', 'vehicle_type'], suffixes=('', '_y'))
    #find missing rows on the left
    missing_rowsl = df[df['drive'].isnull()]
    #find missing rows on the right
    missing_rowsr = df[df['drive_y'].isnull()]

    # print the missing rows 
    if len(missing_rowsl) > 0:
        logging.info("TEMP: the following rows are missing from the concordance on left")
        logging.info(missing_rowsl)
    if len(missing_rowsr) > 0:
        logging.info("TEMP: the following rows are missing from the concordance on right")
        logging.info(missing_rowsr)

    #replace the drive type with the new one
    df['drive'] = df['drive_y']
    df = df.drop(columns=['drive_y'])

    return df

def filter_for_9th_edition_data(combined_data, model_concordances_base_year_measures_file_name, paths_dict, include_drive_all):
    """
    Filters the input data for the 9th edition.

    Args:
        combined_data (pandas.DataFrame): The input data.
        model_concordances_base_year_measures_file_name (str): The file name of the model concordances base year measures.
        paths_dict (dict): A dictionary containing the paths to the input data files.
        include_drive_all (bool): Whether to include drive all in the output.

    Returns:
        pandas.DataFrame: The filtered data.
    """
    ############################################################

    #FILTER FOR 9th data only

    ############################################################
    model_concordances_measures = pd.read_csv(model_concordances_base_year_measures_file_name)
    #make sure the columns are snake case
    model_concordances_measures.columns = [utility_functions.convert_string_to_snake_case(col) for col in model_concordances_measures.columns]
    #convert all values in cols to snake case
    model_concordances_measures = utility_functions.convert_all_cols_to_snake_case(model_concordances_measures)

    model_concordances_measures = utility_functions.ensure_date_col_is_year(model_concordances_measures)

    # #TEMP. add more to the concordance. hjowever it is important to not remove anything from the concordance yet as we'll be creating the intended data later from pieces within (eg. adding up all drive types to all)
    # logging.info("TEMP: making temproary changes to concordance")
    # #change concordance to suit the data we have groomed, rather than the data we want to have. We will slowly adjsut the groomed data to be the same as the concordance
    # # model_concordances_measures = TEMP_replace_drive_types(model_concordances_measures)
    
    # # model_concordances_measures = TEMP_convert_occupancy_and_load_to_occupancy_or_load(model_concordances_measures)

    # # model_concordances_measures = TEMP_add_mileage_to_concordance(model_concordances_measures)

    # model_concordances_measures = TEMP_convert_freight_passenger_activity_to_activity(model_concordances_measures)

    # # model_concordances_measures = TEMP_remove_freight_ldvs_from_concordance(model_concordances_measures)

    # if include_drive_all:
    #     model_concordances_measures = TEMP_add_drive_all_to_concordance(model_concordances_measures)
    # #TEMP

    #Make sure that the model condordance has all the years in the input date range
    model_concordances_measures_dummy = model_concordances_measures.copy()
    original_years = model_concordances_measures['date'].unique()
    for year in range(paths_dict['EARLIEST_YEAR'],paths_dict['LATEST_YEAR']):
        if year not in original_years:
            model_concordances_measures_dummy2 = model_concordances_measures_dummy.copy()
            model_concordances_measures_dummy2['date'] = year
            model_concordances_measures = pd.concat([model_concordances_measures,model_concordances_measures_dummy2])

    # #set date #TEMP FIX TODO WE ARE CURRENTLY ASSUMING THAT THE DATE IS ALWAYS yyyy BUT THE CODE HAS BEEN MADE FLEXIBLE ENOUGH TO HANDLE yyyy-mm-dd
    # model_concordances_measures['date'] = model_concordances_measures['date'].astype(str) + '-12-31'

    #Easiest way to do this is to loop through the unique rows in model_concordances_measures and then if there are any rows that are not in the 8th dataset then add them in with 0 values. 
    INDEX_COLS_no_scope_no_fuel=paths_dict['INDEX_COLS_no_scope_no_fuel']

    #set index#todo what happens if the index is already set?
    model_concordances_measures = model_concordances_measures.set_index(INDEX_COLS_no_scope_no_fuel)
    combined_data = combined_data.set_index(INDEX_COLS_no_scope_no_fuel)

    #Use diff to remove data that isnt in the 9th edition concordance
    extra_rows = combined_data.index.difference(model_concordances_measures.index)
    filtered_combined_data = combined_data.drop(extra_rows)
    #make extra rows into a dataframe
    extra_rows_df = pd.DataFrame(index=extra_rows).reset_index()
    # #compare extra rows where measure is intensity to the rows in missing_rows where measure is intensity
    # extra_rows_df_intensity = extra_rows_df[extra_rows_df['measure'] == 'intensity']
    # missing_rows_intensity = missing_rows_df[missing_rows_df['measure'] == 'intensity']

    #now see what we are missing:
    missing_rows = model_concordances_measures.index.difference(filtered_combined_data.index)
    #create a new dataframe with the missing rows
    missing_rows_df = pd.DataFrame(index=missing_rows).reset_index()
    # # save them to a csv
    # logging.info('Saving missing rows to /intermediate_data/9th_dataset/missing_rows.csv. There are {} missing rows'.format(len(missing_rows)))
    # missing_rows_df.to_csv(paths_dict['missing_rows'])
    filtered_combined_data.reset_index(inplace=True)
    ############################################################

    #CREATE ANOTHER DATAFRAME AND REMOVE THE 0'S, TO SEE WHAT IS MISSING IF WE DO THAT

    ############################################################

    model_concordances_measures_no_zeros = model_concordances_measures.copy()

    combined_data_no_zeros = combined_data.copy()#combined_data[combined_data['value'] != 0]#NOTE THAT I REMOVED THIS IN 7/27/23 BECAUSE I REALISED I WANTED 0S NOT SURE WHAT SIDE EFFECTS MAY BE

    #Use diff to remove data that isnt in the 9th edition concordance
    extra_rows = combined_data_no_zeros.index.difference(model_concordances_measures_no_zeros.index)
    filtered_combined_data_no_zeros = combined_data_no_zeros.drop(extra_rows)


    #now see what we are missing:
    missing_rows = model_concordances_measures_no_zeros.index.difference(filtered_combined_data_no_zeros.index)
    #create a new dataframe with the missing rows
    missing_rows_df = pd.DataFrame(index=missing_rows)
    # save them to a csv
    logging.info('Saving missing rows to /intermediate_data/9th_dataset/missing_rows_no_zeros.csv. There are {} missing rows'.format(len(missing_rows)))
    missing_rows_df.to_csv(paths_dict['missing_rows_no_zeros'])
    filtered_combined_data_no_zeros.reset_index(inplace=True)

    return filtered_combined_data_no_zeros


def test_identify_erroneous_duplicates(combined_data, paths_dict):
    """
    Check for duplicates in the combined dataset when you ignore the value column.

    Parameters:
    -----------
    combined_data : pandas.DataFrame
        The combined dataset to check for duplicates.
    paths_dict : dict
        A dictionary containing the file paths for the input datasets.

    Returns:
    --------
    None
    """
    
    duplicates = combined_data.copy()
    duplicates = duplicates.drop(columns=['value'])
    duplicates = duplicates[duplicates.duplicated(keep=False)]
    if len(duplicates) > 0:
        breakpoint()
        logging.info('There are duplicate rows in the dataset with different values. Please fix them before continuing. You will probably want to split them into different datasets. The duplicates are: ')
        logging.info(duplicates)

        #extrasct the rows with duplicates and sabve them to a csv so we can import them into a spreadsheet to look at them
        duplicates = combined_data.copy()
        col_no_value = [col for col in duplicates.columns if col != 'value']
        duplicates = duplicates[duplicates.duplicated(subset=col_no_value,keep=False)]
        duplicates.to_csv(paths_dict['erroneus_duplicates'], index=False)
        logging.error('There are duplicate rows in the dataset with different values. Please fix them before continuing. they are saved to {}'.format(paths_dict['erroneus_duplicates']))
        raise Exception('There are duplicate rows in the dataset. Please fix them before continuing. they are saved to {}'.format(paths_dict['erroneus_duplicates']))
    
    return


##############################################################################
# def create_data_concordance():#TODO this would be better here than in the aggregation code to make it cleaner
#     pass
# #todo do these
# def import_previous_data_concordance():
#     #load in concordance and then use merge_previous_data_concordance() to change any rows that the same.
#     #todo, take a look at the previous selctions fucntion and see if we can just use a merge to udpate it. I guess it is jsut the concordance that needs updating?
#     def merge_previous_data_concordance():
#     pass
#     pass
# def import_previous_combined_data():
#     #whats the point in this one? todo
#     pass
# def merge_previous_combined_data():
#     pass

def filter_for_years_of_interest(combined_data_concordance,combined_data,paths_dict):
    #based on format of date column we will filter for years of interest differently:
    earliest_year = paths_dict['EARLIEST_YEAR']
    latest_year = paths_dict['LATEST_YEAR']

    #filter data where year is less than our earliest year
    combined_data = combined_data[combined_data['date'] >= earliest_year]
    combined_data_concordance = combined_data_concordance[combined_data_concordance['date'] >= earliest_year]
    
    #and also only data where year is less than the latest year
    combined_data = combined_data[combined_data['date'] < latest_year]
    combined_data_concordance = combined_data_concordance[combined_data_concordance['date'] < latest_year]

    return combined_data_concordance,combined_data

def filter_for_specifc_data(selection_dict, df, filter_for_all_other_data=False):
    #use the keys of the selection dict as the columns to filter on and the values as the values to filter on
    if not filter_for_all_other_data:
        for key, value in selection_dict.items():
            df = df[df[key].isin(value)]
    else:
        #we have to find all data where the key is not in the value list. But we ahve to do this for the confluence of all keys. This can be done by getting the union of all rows that are not in the value list for the first key, of all rows that are not in the value list for the second key, and so on. Then filter for duplicates. This is the equivalent to the oppostie of the intersection of all rows that are in the slection dict! 
        confluence = pd.DataFrame()
        copy_df = df.copy()
        for key, value in selection_dict.items():
            df = copy_df.copy()
            df = df[~df[key].isin(value)]
            confluence = pd.concat([confluence,df])
        #now filter out duplicates (but if we ahve the col potential_datapointsthen we will need to ignore it, because it is a list and you cannot compare lists)
        if 'potential_datapoints' in confluence.columns:
            cols = confluence.columns.tolist()
            cols.remove('potential_datapoints')
            confluence = confluence.drop_duplicates(subset=cols)
        else:
            confluence = confluence.drop_duplicates()
        df = confluence
    return df
#%%

def drop_detailed_drive_types_from_non_road_concordances(paths_dict):
    #set drive to all, then drop duplciates then reconcat with road, then save again but in intermediate data and change the paths_dict to point to there.
    #load concordacens:
    model_concordances_measures = pd.read_csv(paths_dict['concordances_file_path'])
    #split into road and non road:
    road = model_concordances_measures[model_concordances_measures['Medium'] == 'road']
    non_road = model_concordances_measures[model_concordances_measures['Medium'] != 'road']
    #set drive to all then drop duplicates
    non_road['Drive'] = 'all'
    non_road = non_road.drop_duplicates()
    
    #concat road and non road
    model_concordances_measures = pd.concat([road,non_road],axis=0)
    
    #set new path
    paths_dict['concordances_file_path'] = paths_dict['intermediate_folder']+'/model_concordances_measures.csv'
    #save to new path
    model_concordances_measures.to_csv(paths_dict['concordances_file_path'],index=False)
    return paths_dict
##############################################################################



# def create_config_yml_file(paths_dict):
        
#     datasets = [('intermediate_data/8th_edition_transport_typel/', 'eigth_edition_transport_data_final_', 'intermediate_data/8th_edition_transport_typel/eigth_edition_transport_data_final_FILE_DATE_ID.csv'), ('intermediate_data/estimated/', '_8th_ATO_passenger_road_updates.csv', './intermediate_data/estimated/FILE_DATE_ID_8th_ATO_passenger_road_updates.csv'), ('intermediate_data/estimated/', '_8th_ATO_bus_update.csv', './intermediate_data/estimated/FILE_DATE_ID_8th_ATO_bus_update.csv'), ('intermediate_data/estimated/', '_8th_ATO_road_freight_tonne_km.csv', './intermediate_data/estimated/FILE_DATE_ID_8th_ATO_road_freight_tonne_km.csv'), ('intermediate_data/estimated/', '_8th_iea_ev_all_stock_updates.csv', 'intermediate_data/estimated/FILE_DATE_ID_8th_iea_ev_all_stock_updates.csv'), ('intermediate_data/estimated/', '_8th_ATO_vehicle_type_update.csv', './intermediate_data/estimated/FILE_DATE_ID_8th_ATO_vehicle_type_update.csv'), ('intermediate_data/ATO/', 'ATO_data_cleaned_', 'intermediate_data/ATO/ATO_data_cleaned_FILE_DATE_ID.csv'), ('intermediate_data/item_data/', 'item_dataset_clean_', 'intermediate_data/item_data/item_dataset_clean_FILE_DATE_ID.csv'), ('intermediate_data/estimated/', '_turnover_rate_3pct', 'intermediate_data/estimated/FILE_DATE_ID_turnover_rate_3pct.csv'),  ('intermediate_data/estimated/', 'EGEDA_merged', 'intermediate_data/estimated/EGEDA_mergedFILE_DATE_ID.csv'), ('intermediate_data/estimated/', 'ATO_revenue_pkm', 'intermediate_data/estimated/ATO_revenue_pkmFILE_DATE_ID.csv'), ('intermediate_data/estimated/', 'nearest_available_date', 'intermediate_data/estimated/nearest_available_dateFILE_DATE_ID.csv'), ('intermediate_data/IEA/', '_iea_fuel_economy.csv', 'intermediate_data/IEA/FILE_DATE_ID_iea_fuel_economy.csv'), ('intermediate_data/IEA/', '_evs.csv', 'intermediate_data/IEA/FILE_DATE_ID_evs.csv'), ('intermediate_data/estimated/filled_missing_values/', 'missing_drive_values_', 'intermediate_data/estimated/filled_missing_values/missing_drive_values_FILE_DATE_ID.csv'), ('intermediate_data/estimated/', 'occ_load_guesses', 'intermediate_data/estimated/occ_load_guessesFILE_DATE_ID.csv'), ('intermediate_data/estimated/', 'new_vehicle_efficiency_estimates_', 'intermediate_data/estimated/new_vehicle_efficiency_estimates_FILE_DATE_ID.csv'), ('intermediate_data/Macro/', 'all_macro_data_', 'intermediate_data/Macro/all_macro_data_FILE_DATE_ID.csv')]


#     #saVE THESE TO config.YML 
#     # #open yml
#     # import yaml
#     # with open('config/config.yml', 'r') as ymlfile:
#     #     cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
#     #create a key called datassets then set it so the key is the second element of the tuple, then folder: is the first element of the tuple and file_path: is the third element of the tuple. Then we will also have a script: value which will be set to TBA
#     cfg = dict()
#     cfg['datasets'] = dict()
#     for dataset in datasets:
        
#         cfg['datasets'][dataset[1]] = {'folder': dataset[0], 'file_path': dataset[2], 'script': 'TBA'}

#     #save INDEX_COLS to the yml under the key 'INDEX_COLS'
#     cfg['INDEX_COLS'] = paths_dict['INDEX_COLS']
#     #save yml
#     with open('config/config.yml', 'w') as ymlfile:
#         yaml.dump(cfg, ymlfile)
#     return






















# def combine_manual_and_automatic_output(combined_data_concordance_automatic,combined_data_concordance_manual,INDEX_COLS):
#     #todo, i think its useful to have automatic selection but i think it needs to be a lot more functional. for example it should be able to easily implement the following:
#     #if the user says a dataset should be prioirtised then it should be.
#     #... anything else


#     INDEX_COLS_no_year = INDEX_COLS.copy()
#     INDEX_COLS_no_year.remove('date')

#     #COMBINE MANUAL AND AUTOMATIC DATA SELECTION OUTPUT DFs
#     #join the automatic and manual datasets so we can compare the dataset  columns from both the manual dataset automatic dataset
#     #create a final_dataset column. This will be filled with the dataset in the automatic column, except where the values in the manual and automatic dataset columns are different, for which we will use the value in the manual dataset.

#     #reset and set index of both dfs to INDEX_COLS
#     combined_data_concordance_manual = combined_data_concordance_manual.set_index(INDEX_COLS)
#     combined_data_concordance_automatic = combined_data_concordance_automatic.reset_index().set_index(INDEX_COLS)

#     #remove the datasets and dataset_selection_method columns from the manual df
#     combined_data_concordance_manual.drop(columns=['datasets','dataset_selection_method'], inplace=True)
#     #join manual and automatic data selection dfs
#     final_combined_data_concordance = combined_data_concordance_manual.merge(combined_data_concordance_automatic, how='outer', left_index=True, right_index=True, suffixes=('_manual', '_auto'))

#     #we will either have dataset names or nan values in the manual and automatic dataset columns. We want to use the manual dataset column if it is not nan, otherwise use the automatic dataset column:
#     #first set the dataset_selection_method column based on that criteria, and then use that to set other columns
#     final_combined_data_concordance.loc[final_combined_data_concordance['dataset_auto'].notnull(), 'dataset_selection_method'] = 'Automatic'
#     #if the manual dataset column is not nan then use that instead
#     final_combined_data_concordance.loc[final_combined_data_concordance['dataset_manual'].notnull(), 'dataset_selection_method'] = 'Manual'

#     #Now depending on the value of the dataset_selection_method column, we can set final_value and final_dataset columns
#     #if the dataset_selection_method is manual then use the manual dataset column
#     final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Manual', 'value'] = final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Manual','value_manual']
#     final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Manual', 'dataset'] = final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Manual','dataset_manual']
#     #if the dataset_selection_method is automatic then use the automatic dataset column
#     final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Automatic', 'dataset'] = final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Automatic','dataset_auto']
#     final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Automatic', 'value'] = final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Automatic','value_auto']

#     #drop cols ending in _manual and _auto
#     final_combined_data_concordance.drop(columns=[col for col in final_combined_data_concordance.columns if col.endswith('_manual') or col.endswith('_auto')], inplace=True)

#     return final_combined_data_concordance



# ##############################################################################

# def create_manual_data_iterator(
# combined_data_concordance_iterator,
# INDEX_COLS,
# combined_data_concordance_manual,
# duplicates_manual,
# rows_to_select_manually_df=[],
# run_only_on_rows_to_select_manually=False,
# manually_chosen_rows_to_select=None,
# user_edited_combined_data_concordance_iterator=None,
# previous_selections=None,
# previous_duplicates_manual=None,
# update_skipped_rows=False):
    
#     """
#     manually_chosen_rows_to_select: set to true if you want to manually choose the rows to select using user_edited_combined_data_concordance_iterator
#     user_edited_combined_data_concordance_iterator: a manually chosen dataframe with the rows to select. This allows user to define what they want to select manually (eg. all stocks)

#     duplicates_manual & previous_duplicates_manual need to be available if you want to use either pick_up_where_left_off or import_previous_selection. progress_csv should also be available if you want to use pick_up_where_left_off

#     This will create an iterator which will be used to manually select the dataset to use for each row. it is the same as the iterator which is input into this fucntion but it also has had data removed from it so that it only contains rows where we need to manually select the dataset to use
#     """
#     #Remove year from the current cols without removing it from original list, and set it as a new list
#     INDEX_COLS_no_year = paths_dict['INDEX_COLS_no_year']

#     #todo what function was this really filling
#     # #CREATE ITERATOR 
#     # #if we want to add to the rows_to_select_manually_df to check specific rows then set the below to True
#     # if run_only_on_rows_to_select_manually:
#     #     #if this, only run the manual process on index rows where we couldnt find a dataset to use automatically for some year
#     #     #since the automatic method is relatively strict there should be a large amount of rows to select manually
#     #     #note that if any one year cannot be chosen automatically then we will have to choose the dataset manually for all years of that row
#     #     iterator = rows_to_select_manually_df.copy()
#     #     iterator.set_index(INDEX_COLS_no_year, inplace=True)
#     #     iterator.drop_duplicates(inplace=True)#TEMP get rid of this later
#     # elif manually_chosen_rows_to_select:
#     #     #we can add rows form the combined_data_concordance_iterator as edited by the user themselves. 
#     #     iterator = user_edited_combined_data_concordance_iterator.copy()
#     #     #since user changed the data we will jsut reset index and set again
#     #     iterator.reset_index(inplace=True)
#     #     iterator.set_index(INDEX_COLS_no_year, inplace=True)

#     #     #for this example we will add all Stocks data (for the purpoose of betterunderstanding our stocks data!) and remove all the other data. But this is just an example of what the user could do to select specific rows
#     #     use_example = False
#     #     if use_example:
#     #         iterator.reset_index(inplace=True)
#     #         iterator = iterator[iterator['measure']=='Stocks']
#     #         #set the index to the index cols
#     #         iterator.set_index(INDEX_COLS_no_year, inplace=True)
#     # else:
#     #     iterator = combined_data_concordance_iterator.copy()

#     #to do this should be a default process at start of whjole process i think 
#     # #now determine whether we want to import previous progress or not:
#     # if previous_selections is not None:
#     #     combined_data_concordance_manual,iterator = import_previous_selections(previous_selections, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS,update_skipped_rows)
        
#     return iterator, combined_data_concordance_manual



##############################################################################

