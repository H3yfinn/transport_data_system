
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
import data_estimation_functions
import logging
create_9th_model_dataset = True

file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

INDEX_COLS = ['date',
 'economy',
 'measure',
 'vehicle_type',
 'unit',
 'medium',
 'transport_type',
 'drive',
 'fuel',
 'frequency',
 'scope']

EARLIEST_DATE="2010-01-01"
LATEST_DATE='2023-01-01'


previous_FILE_DATE_ID ='DATE20230329'
if previous_FILE_DATE_ID is not None:#you can set some of these to false if you want to do some of the steps manually
    load_data_creation_progress = True
    load_stocks_mileage_occupancy_efficiency_selection_progress = True
    load_stocks_mileage_occupancy_efficiency_interpolation_progress = True
    load_energy_passenger_km_selection_progress = True
    load_energy_passenger_km_interpolation_progress = True
    
else:  
    load_data_creation_progress = False
    load_stocks_mileage_occupancy_efficiency_selection_progress = False
    load_stocks_mileage_occupancy_efficiency_interpolation_progress = False
    load_energy_passenger_km_selection_progress = False
    load_energy_passenger_km_interpolation_progress = False
    

#%%
def main():
    ################################################################
    #SETUP
    paths_dict = utility_functions.setup_paths_dict(FILE_DATE_ID,INDEX_COLS, EARLIEST_DATE, LATEST_DATE,previous_FILE_DATE_ID)

    utility_functions.setup_logging(FILE_DATE_ID,paths_dict,testing=False)

    ################################################################
    #EXTRACT DATA
    if not load_data_creation_progress:      
        datasets_transport, datasets_other = data_formatting_functions.extract_latest_groomed_data()

        unfiltered_combined_data = data_formatting_functions.combine_datasets(datasets_transport, FILE_DATE_ID,paths_dict)

        unfiltered_combined_data = data_formatting_functions.TEMP_FIX_ensure_date_col_is_year(unfiltered_combined_data)

        if create_9th_model_dataset:
            #import snapshot of 9th concordance
            model_concordances_base_year_measures_file_name = './intermediate_data/9th_dataset/{}'.format('model_concordances_measures.csv')
            combined_data = data_formatting_functions.filter_for_9th_edition_data(unfiltered_combined_data, model_concordances_base_year_measures_file_name, paths_dict)
        else:
            combined_data = unfiltered_combined_data.copy()
        #since we dont expect to run the data selection process that often we will just save the data in a dated folder in intermediate_data/data_selection_process/FILE_DATE_ID/

        combined_data_concordance = data_formatting_functions.create_concordance_from_combined_data(combined_data, frequency = 'yearly')

        sorting_cols = ['date','economy','measure','transport_type','medium', 'vehicle_type','drive','fuel','frequency','scope']

        combined_data_concordance, combined_data = data_selection_functions.prepare_data_for_selection(combined_data_concordance,combined_data,paths_dict, sorting_cols)
    else:
        unfiltered_combined_data = pd.read_pickle(paths_dict['previous_unfiltered_combined_data'])
        combined_data_concordance = pd.read_pickle(paths_dict['previous_combined_data_concordance'])
        combined_data = pd.read_pickle(paths_dict['previous_combined_data'])

    #save data to pickle
    unfiltered_combined_data.to_pickle(paths_dict['unfiltered_combined_data'])
    combined_data_concordance.to_pickle(paths_dict['combined_data_concordance'])
    combined_data.to_pickle(paths_dict['combined_data'])
    logging.info('Saving combined_data_concordance and combined_data')
    ####################################################
    #BEGIN DATA SELECTION PROCESS FOR STOCKS MILEAGE OCCUPANCY EFFICIENCY
    ####################################################

    grouping_cols = ['economy','vehicle_type','drive']
    passsenger_road_measures_selection_dict = {'measure': 
        ['new_vehicle_efficiency', 'occupancy', 'mileage', 'stocks'],
    'medium': ['road'],
    'transport_type': ['passenger']}

    datasets_to_always_use = ['iea_ev_explorer $ historical',
       'iea_ev_explorer $ projection-aps',
       'iea_ev_explorer $ projection-steps','8th_edition_transport_model $ reference']

    if not load_stocks_mileage_occupancy_efficiency_selection_progress:#when we design actual progress integration then we wont do it like this. 
        stocks_mileage_occupancy_efficiency_combined_data = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict, combined_data)

        stocks_mileage_occupancy_efficiency_combined_data_concordance = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict, combined_data_concordance)

        stocks_mileage_occupancy_efficiency_combined_data_concordance, stocks_mileage_occupancy_efficiency_combined_data = data_formatting_functions.TEMP_create_new_values(stocks_mileage_occupancy_efficiency_combined_data_concordance, stocks_mileage_occupancy_efficiency_combined_data)

        stocks_mileage_occupancy_efficiency_combined_data_concordance = data_selection_functions.data_selection_handler(grouping_cols, stocks_mileage_occupancy_efficiency_combined_data_concordance, stocks_mileage_occupancy_efficiency_combined_data, paths_dict,datasets_to_always_use,default_user_input=1)
    else:
        stocks_mileage_occupancy_efficiency_combined_data_concordance = pd.read_pickle(paths_dict['previous_stocks_mileage_occupancy_efficiency_combined_data_concordance'])
    
    stocks_mileage_occupancy_efficiency_combined_data_concordance.to_pickle(paths_dict['stocks_mileage_occupancy_efficiency_combined_data_concordance'])
    logging.info('Saving stocks_mileage_occupancy_efficiency_combined_data_concordance')
    ####################################################
    #interpolate missing values for STOCKS MILAGE OCCUPANCY EFFICIENCY DATA
    ####################################################
    if not load_stocks_mileage_occupancy_efficiency_interpolation_progress    :#when we design actual progress integration then we wont do it like this.  
        stocks_mileage_occupancy_efficiency_combined_data_concordance = interpolation_functions.interpolate_missing_values(stocks_mileage_occupancy_efficiency_combined_data_concordance,INDEX_COLS,paths_dict,automatic_interpolation_method = 'linear', automatic_interpolation = True, FILE_DATE_ID=FILE_DATE_ID,percent_of_values_needed_to_interpolate=0.0000001, INTERPOLATION_LIMIT=10,load_progress=True)
    else:
        stocks_mileage_occupancy_efficiency_combined_data_concordance = pd.read_pickle(paths_dict['previous_interpolated_stocks_mileage_occupancy_efficiency_combined_data_concordance'])
    #save to pickle
    stocks_mileage_occupancy_efficiency_combined_data_concordance.to_pickle(paths_dict['interpolated_stocks_mileage_occupancy_efficiency_combined_data_concordance'])
    logging.info('Saving interpolated_stocks_mileage_occupancy_efficiency_combined_data_concordance')
    
    stocks_mileage_occupancy_efficiency_combined_data = data_formatting_functions.convert_concordance_to_combined_data(stocks_mileage_occupancy_efficiency_combined_data_concordance, combined_data)
    
    

    ####################################################
    #INCORPORATE NEW STOCKS MILAGE OCCUPANCY EFFICIENCY DATA TO CREATE NEW PASSANGER KM AND ENERGY DATA
    ####################################################

    passsenger_road_measures_selection_dict = {'measure': 
        ['passenger_km', 'energy'],
    'medium': ['road'],
    'transport_type': ['passenger']}
    datasets_to_always_use  = []

    stocks_mileage_occupancy_efficiency_passenger_energy_combined_data = data_estimation_functions.calculate_energy_and_passenger_km(stocks_mileage_occupancy_efficiency_combined_data, paths_dict)

    stocks_mileage_occupancy_efficiency_passenger_energy_combined_data.to_pickle(paths_dict['calculated_passenger_energy_combined_data'])
    logging.info('Saving calculated_passenger_energy_combined_data')
    #then concat it to the combined_data, filter for only data we want then create a new concordance for it. THen run the data selection process on it again.
    stocks_mileage_occupancy_efficiency_passenger_energy_combined_data = pd.concat([combined_data,stocks_mileage_occupancy_efficiency_passenger_energy_combined_data],axis=0,sort=False)

    stocks_mileage_occupancy_efficiency_passenger_energy_combined_data_concordance = data_formatting_functions.create_concordance_from_combined_data(stocks_mileage_occupancy_efficiency_passenger_energy_combined_data)

    sorting_cols = ['date','economy','measure','transport_type','medium', 'vehicle_type','drive','fuel','frequency','scope']

    stocks_mileage_occupancy_efficiency_passenger_energy_combined_data_concordance, stocks_mileage_occupancy_efficiency_passenger_energy_combined_data = data_selection_functions.prepare_data_for_selection(stocks_mileage_occupancy_efficiency_passenger_energy_combined_data_concordance,stocks_mileage_occupancy_efficiency_passenger_energy_combined_data,paths_dict, sorting_cols)

    energy_passenger_km_combined_data = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict,stocks_mileage_occupancy_efficiency_passenger_energy_combined_data)
    energy_passenger_km_combined_data_concordance  = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict,stocks_mileage_occupancy_efficiency_passenger_energy_combined_data_concordance)

    ####################################################
    #BEGIN DATA SELECTION PROCESS FOR ENERGY AND PASSENGER KM
    ####################################################

    #todo might be good to add an ability to choose which measures to choose from even if a whole dataset is passed. this way we can still create dashboard with occupancy and stuff on it. #although i kin of think the dashboard isnt very useufl. this can be  alater thing to do.
    if not load_energy_passenger_km_selection_progress: 
        highlighted_datasets = ['estimated $ calculate_energy_and_passenger_km()']
        energy_passenger_km_combined_data_concordance = data_selection_functions.data_selection_handler(grouping_cols, energy_passenger_km_combined_data_concordance, energy_passenger_km_combined_data, paths_dict,datasets_to_always_use,highlighted_datasets,default_user_input=1)#todo Need some way to only select for specified measures. as we want to include occupancy and stuff in the dashboard. will also need to filter for only energy and passenger km in the output.
    else:
        energy_passenger_km_combined_data_concordance = pd.read_pickle(paths_dict['previous_energy_passenger_km_combined_data_concordance'])

    #save data to pickle
    pd.to_pickle(energy_passenger_km_combined_data_concordance,paths_dict['energy_passenger_km_combined_data_concordance'])
    logging.info('Saving energy_passenger_km_combined_data_concordance')
    if not load_energy_passenger_km_interpolation_progress:
        #run interpolation
        energy_passenger_km_combined_data_concordance = interpolation_functions.interpolate_missing_values(energy_passenger_km_combined_data_concordance,INDEX_COLS,paths_dict,automatic_interpolation_method = 'linear', automatic_interpolation = True, FILE_DATE_ID=FILE_DATE_ID,percent_of_values_needed_to_interpolate=0.7, INTERPOLATION_LIMIT=3,load_progress=True)
    else:
        # energy_passenger_km_combined_data = pd.read_pickle(paths_dict['previous_interpolated_energy_passenger_km_combined_data_concordance'])
        energy_passenger_km_combined_data_concordance = pd.read_pickle(paths_dict['previous_interpolated_energy_passenger_km_combined_data_concordance'])

    #save to pickle
    energy_passenger_km_combined_data_concordance.to_pickle(paths_dict['interpolated_energy_passenger_km_combined_data_concordance'])
    #convert to combined data
    energy_passenger_km_combined_data = data_formatting_functions.convert_concordance_to_combined_data(energy_passenger_km_combined_data_concordance, combined_data)
    logging.info('Saving interpolated_energy_passenger_km_combined_data_concordance')
    ####################################################
    #FINALISE DATA
    ####################################################
    #join the two datasets together
    passenger_road_combined_data = pd.concat([stocks_mileage_occupancy_efficiency_combined_data,energy_passenger_km_combined_data],axis=0)

    passenger_road_combined_data_concordance = pd.concat([stocks_mileage_occupancy_efficiency_combined_data_concordance,energy_passenger_km_combined_data_concordance],axis=0)

    #save
    passenger_road_combined_data.to_csv(paths_dict['intermediate_folder']+'/passenger_road_combined_data_TEST.csv')
    passenger_road_combined_data_concordance.to_csv(paths_dict['intermediate_folder']+'/passenger_road_combined_data_concordance_TEST.csv')

    #save to pickle
    passenger_road_combined_data.to_pickle(paths_dict['intermediate_folder']+'/passenger_road_combined_data_TEST.pkl')
    passenger_road_combined_data_concordance.to_pickle(paths_dict['intermediate_folder']+'/passenger_road_combined_data_concordance_TEST.pkl')
        
    road_freight_energy_combined_data = data_estimation_functions.estimate_road_freight_energy_use(unfiltered_combined_data,passenger_road_combined_data)
    # freight_activity_road_combined_data =data_estimation_functions.estimate_freight_activity(road_freight_energy_combined_data)

    #combine all so we have one big road dataset:
    road_combined_data = pd.concat([passenger_road_combined_data,road_freight_energy_combined_data],axis=0)#freight_activity_road_combined_data

    #save to pickle
    road_combined_data.to_pickle(paths_dict['intermediate_folder']+'/road_combined_data_TEST.pkl')
    #save to csv
    road_combined_data.to_csv(paths_dict['intermediate_folder']+'/road_combined_data_TEST.csv')
    dothis = True
    if dothis:
        non_road_energy_no_transport_type = data_estimation_functions.estimate_non_road_energy(unfiltered_combined_data,road_combined_data)
        non_road_energy = data_estimation_functions.split_non_road_energy_into_transport_types(non_road_energy_no_transport_type,unfiltered_combined_data)
        activity_non_passenger_road = data_estimation_functions.estimate_activity_non_passenger_road(non_road_energy,road_combined_data)
        #concatenate all the data together
        all_new_combined_data = pd.concat([non_road_energy,road_combined_data,activity_non_passenger_road],axis=0)
        #save to pickle
        all_new_combined_data.to_pickle(paths_dict['intermediate_folder']+'/all_new_combined_data_TEST.pkl')
        #save to csv
        all_new_combined_data.to_csv(paths_dict['intermediate_folder']+'/all_new_combined_data_TEST.csv')

    #TODO INTERPOLATE AND MAYBE SELECT



################################################
################################################



#%%


################################################################################
#FOR RUNNING THROUGH JUPYTER INTERACTIVE NOTEBOOK (FINNS SETUP, allows for running the function outside of the command line through jupyter interactive)
################################################################################
def is_notebook() -> bool:
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False      # Probably standard Python interpreter
    
if is_notebook():
    #set cwd to the root of the project
    os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
    main()
elif __name__ == '__main__':

    # if len(sys.argv) != 2:
    #     msg = "Usage: python {} <input_data_sheet_file>"
    #     print(msg.format(sys.argv[0]))
    #     sys.exit(1)
    # else:
    #     input_data_sheet_file = sys.argv[1]
    #     main(input_data_sheet_file)
    main()
#%%