#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import sys
from PIL import Image
import data_formatting_functions
import utility_functions 
import data_selection_functions
import interpolation_functions
import main_functions
import data_estimation_functions

import logging
logger = logging.getLogger(__name__)

def create_best_stocks_mileage_occupancy_efficiency_dataset(combined_data_concordance, combined_data, paths_dict, FILE_DATE_ID, INDEX_COLS, grouping_cols, sorting_cols, passsenger_road_measures_selection_dict,datasets_to_always_use=[], load_progress = False):

    if not load_progress:#when we design actual progress integration then we wont do it like this. 
        combined_data = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict, combined_data)
        combined_data_concordance = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict, combined_data_concordance)
        combined_data_concordance, combined_data = data_formatting_functions.TEMP_create_new_values(combined_data_concordance, combined_data)
        stocks_mileage_occupancy_efficiency_combined_data_concordance = data_selection_functions.manual_data_selection_handler(grouping_cols, combined_data_concordance, combined_data, paths_dict,datasets_to_always_use)
    else:
        combined_data_concordance = pd.read_pickle(paths_dict['previous_combined_data_concordance'])
        combined_data = pd.read_pickle(paths_dict['previous_combined_data'])
        stocks_mileage_occupancy_efficiency_combined_data_concordance = pd.read_pickle(paths_dict['previous_stocks_mileage_occupancy_efficiency_combined_data_concordance'])
    
    #save data to pickle
    combined_data_concordance.to_pickle(paths_dict['combined_data_concordance'])
    combined_data.to_pickle(paths_dict['combined_data'])
    pd.to_pickle(stocks_mileage_occupancy_efficiency_combined_data_concordance,paths_dict['stocks_mileage_occupancy_efficiency_combined_data_concordance'])

    ####################################################
    #interpolate missing values
    ####################################################
    stocks_mileage_occupancy_efficiency_combined_data,stocks_mileage_occupancy_efficiency_combined_data_concordance = interpolation_functions.interpolate_missing_values(stocks_mileage_occupancy_efficiency_combined_data_concordance,INDEX_COLS,paths_dict,automatic_interpolation_method = 'linear', automatic_interpolation = True, FILE_DATE_ID=FILE_DATE_ID,percent_of_values_needed_to_interpolate=0.7, INTERPOLATION_LIMIT=3,load_progress=True)

    return stocks_mileage_occupancy_efficiency_combined_data,stocks_mileage_occupancy_efficiency_combined_data_concordance

#%%


def create_best_energy_passenger_km_dataset(combined_data_concordance, combined_data,stocks_mileage_occupancy_efficiency_combined_data, paths_dict, FILE_DATE_ID, INDEX_COLS, grouping_cols, sorting_cols, passsenger_road_measures_selection_dict,datasets_to_always_use,load_progress = False):

    stocks_mileage_occupancy_efficiency_passenger_energy_combined_data = data_estimation_functions.calculate_energy_and_passenger_km(stocks_mileage_occupancy_efficiency_combined_data, paths_dict)

    #then concat it to the combined_data, filter for only data we want then create a new concordance for it. THen run the data selection process on it again.
    combined_data = pd.concat([combined_data,stocks_mileage_occupancy_efficiency_passenger_energy_combined_data],axis=0,sort=False)

    combined_data = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict,combined_data)
    combined_data_concordance  = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict,combined_data_concordance)

    #todo might be good to add an ability to choose which measures to choose from even if a whole dataset is passed. this way we can still create dashboard with occupancy and stuff on it.
    if not load_progress:#when we design actual progress integration then we wont do it like this. 
        highlighted_datasets = ['estimated $ calculate_energy_and_passenger_km()']
        energy_passenger_km_combined_data_concordance = data_selection_functions.manual_data_selection_handler(grouping_cols, combined_data_concordance, combined_data, paths_dict,datasets_to_always_use,highlighted_datasets)
        #save data to pickle
        pd.to_pickle(energy_passenger_km_combined_data_concordance,paths_dict['energy_passenger_km_combined_data_concordance'])
    else:
        energy_passenger_km_combined_data_concordance = pd.read_pickle(paths_dict['energy_passenger_km_combined_data_concordance'])
    
    #run interpolation
    energy_passenger_km_combined_data,energy_passenger_km_combined_data_concordance = interpolation_functions.interpolate_missing_values(energy_passenger_km_combined_data_concordance,INDEX_COLS,paths_dict,automatic_interpolation_method = 'linear', automatic_interpolation = True, FILE_DATE_ID=FILE_DATE_ID,percent_of_values_needed_to_interpolate=0.7, INTERPOLATION_LIMIT=3,load_progress=True)

    return combined_data_concordance, combined_data