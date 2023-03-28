
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


previous_FILE_DATE_ID = 'DATE20230328'
if previous_FILE_DATE_ID is not None:
    load_progress2 = True#todo
    load_progress = True
    load_progress3 = False
    load_progress4 = False
    load_progress0 = True
else:  
    load_progress = False
    load_progress2 = False
    load_progress3 = False
    load_progress4 = False
    load_progress0 = False
#%%
def main():
    paths_dict = utility_functions.setup_paths_dict(FILE_DATE_ID,INDEX_COLS, EARLIEST_DATE, LATEST_DATE,previous_FILE_DATE_ID)

    utility_functions.setup_logging(FILE_DATE_ID,paths_dict,testing=False)
    if not load_progress0:
        datasets_transport, datasets_other = data_formatting_functions.extract_latest_groomed_data()

        combined_data = data_formatting_functions.combine_datasets(datasets_transport, FILE_DATE_ID,paths_dict)

        if create_9th_model_dataset:
            #import snapshot of 9th concordance
            model_concordances_base_year_measures_file_name = './intermediate_data/9th_dataset/{}'.format('model_concordances_measures.csv')
            combined_data = data_formatting_functions.filter_for_9th_edition_data(combined_data, model_concordances_base_year_measures_file_name, paths_dict)

        #since we dont expect to run the data selection process that often we will just save the data in a dated folder in intermediate_data/data_selection_process/FILE_DATE_ID/

        combined_data_concordance = data_formatting_functions.create_whole_dataset_concordance(combined_data, frequency = 'yearly')

        sorting_cols = ['date','economy','measure','transport_type','medium', 'vehicle_type','drive','fuel','frequency','scope']

        combined_data_concordance, combined_data = data_selection_functions.prepare_data_for_selection(combined_data_concordance,combined_data,paths_dict, sorting_cols)#todo reaplce everything that is combined_dataset with combined_data
    else:
        combined_data_concordance = pd.read_pickle(paths_dict['previous_combined_data_concordance'])
        combined_data = pd.read_pickle(paths_dict['previous_combined_data'])

    #save data to pickle
    combined_data_concordance.to_pickle(paths_dict['combined_data_concordance'])
    combined_data.to_pickle(paths_dict['combined_data'])

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

    if not load_progress:#when we design actual progress integration then we wont do it like this. 
        combined_data = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict, combined_data)
        combined_data_concordance = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict, combined_data_concordance)
        combined_data_concordance, combined_data = data_formatting_functions.TEMP_create_new_values(combined_data_concordance, combined_data)
        stocks_mileage_occupancy_efficiency_combined_data_concordance = data_selection_functions.data_selection_handler(grouping_cols, combined_data_concordance, combined_data, paths_dict,datasets_to_always_use,default_user_input=1)
    else:
        stocks_mileage_occupancy_efficiency_combined_data_concordance = pd.read_pickle(paths_dict['previous_stocks_mileage_occupancy_efficiency_combined_data_concordance'])
        stocks_mileage_occupancy_efficiency_combined_data = pd.read_pickle(paths_dict['previous_stocks_mileage_occupancy_efficiency_combined_data'])
    
    stocks_mileage_occupancy_efficiency_combined_data_concordance.to_pickle(paths_dict['stocks_mileage_occupancy_efficiency_combined_data_concordance'])
    stocks_mileage_occupancy_efficiency_combined_data.to_pickle(paths_dict['stocks_mileage_occupancy_efficiency_combined_data'])
    ####################################################
    #interpolate missing values
    ####################################################
    if not load_progress2:#when we design actual progress integration then we wont do it like this. 
        stocks_mileage_occupancy_efficiency_combined_data,stocks_mileage_occupancy_efficiency_combined_data_concordance = interpolation_functions.interpolate_missing_values(stocks_mileage_occupancy_efficiency_combined_data_concordance,INDEX_COLS,paths_dict,automatic_interpolation_method = 'linear', automatic_interpolation = True, FILE_DATE_ID=FILE_DATE_ID,percent_of_values_needed_to_interpolate=0.3, INTERPOLATION_LIMIT=5,load_progress=True)
    else:
        stocks_mileage_occupancy_efficiency_combined_data = pd.read_pickle(paths_dict['previous_interpolated_stocks_mileage_occupancy_efficiency_combined_data_concordance'])
        stocks_mileage_occupancy_efficiency_combined_data_concordance = pd.read_pickle(paths_dict['previous_interpolated_stocks_mileage_occupancy_efficiency_combined_data'])
    #save to pickle
    stocks_mileage_occupancy_efficiency_combined_data_concordance.to_pickle(paths_dict['interpolated_stocks_mileage_occupancy_efficiency_combined_data_concordance'])
    stocks_mileage_occupancy_efficiency_combined_data.to_pickle(paths_dict['interpolated_stocks_mileage_occupancy_efficiency_combined_data'])

    keepgoing=True
    if keepgoing:
        ####################################################
        #BEGIN DATA SELECTION PROCESS FOR ENERGY AND PASSENGER KM
        ####################################################

        passsenger_road_measures_selection_dict = {'measure': 
            ['passenger_km', 'energy'],
        'medium': ['road'],
        'transport_type': ['passenger']}
        datasets_to_always_use  = []

        stocks_mileage_occupancy_efficiency_passenger_energy_combined_data = data_estimation_functions.calculate_energy_and_passenger_km(stocks_mileage_occupancy_efficiency_combined_data, paths_dict)

        #then concat it to the combined_data, filter for only data we want then create a new concordance for it. THen run the data selection process on it again.
        combined_data = pd.concat([combined_data,stocks_mileage_occupancy_efficiency_passenger_energy_combined_data],axis=0,sort=False)

        combined_data = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict,combined_data)
        combined_data_concordance  = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict,combined_data_concordance)

        #todo might be good to add an ability to choose which measures to choose from even if a whole dataset is passed. this way we can still create dashboard with occupancy and stuff on it.
        if not load_progress3:#when we design actual progress integration then we wont do it like this. 
            highlighted_datasets = ['estimated $ calculate_energy_and_passenger_km()']
            energy_passenger_km_combined_data_concordance = data_selection_functions.data_selection_handler(grouping_cols, combined_data_concordance, combined_data, paths_dict,datasets_to_always_use,highlighted_datasets)
        else:
            energy_passenger_km_combined_data_concordance = pd.read_pickle(paths_dict['energy_passenger_km_combined_data_concordance'])

        #save data to pickle
        pd.to_pickle(energy_passenger_km_combined_data_concordance,paths_dict['energy_passenger_km_combined_data_concordance'])

        if not load_progress4:
            #run interpolation
            energy_passenger_km_combined_data,energy_passenger_km_combined_data_concordance = interpolation_functions.interpolate_missing_values(energy_passenger_km_combined_data_concordance,INDEX_COLS,paths_dict,automatic_interpolation_method = 'linear', automatic_interpolation = True, FILE_DATE_ID=FILE_DATE_ID,percent_of_values_needed_to_interpolate=0.7, INTERPOLATION_LIMIT=3,load_progress=True)
        else:
            energy_passenger_km_combined_data = pd.read_pickle(paths_dict['previous_interpolated_energy_passenger_km_combined_data_concordance'])
            energy_passenger_km_combined_data_concordance = pd.read_pickle(paths_dict['previous_interpolated_energy_passenger_km_combined_data'])

        #save to pickle
        energy_passenger_km_combined_data_concordance.to_pickle(paths_dict['interpolated_energy_passenger_km_combined_data_concordance'])
        energy_passenger_km_combined_data.to_pickle(paths_dict['interpolated_energy_passenger_km_combined_data'])
        ####################################################
        #FINALISE DATA
        ####################################################
        #join the two datasets together
        combined_data = pd.concat([stocks_mileage_occupancy_efficiency_combined_data,energy_passenger_km_combined_data],axis=0)
        combined_data_concordance = pd.concat([stocks_mileage_occupancy_efficiency_combined_data_concordance,energy_passenger_km_combined_data_concordance],axis=0)

        #save
        combined_data.to_csv(paths_dict['intermediate_folder']+'\\combined_data_TEST.csv')
        combined_data_concordance.to_csv(paths_dict['intermediate_folder']+'\\combined_data_concordance_TEST.csv')

        #save to pickle
        combined_data.to_pickle(paths_dict['intermediate_folder']+'\\combined_data_TEST.pkl')
        combined_data_concordance.to_pickle(paths_dict['intermediate_folder']+'\\combined_data_concordance_TEST.pkl')

    # Freight:
    # As it is a lot harder to estimate load, mileage and efficiency because of the various sizes of vehicles, we will just leave it as leftover road energy use after calculating total passenger energy use.
    # Later on, depending on what data is available we could start looking into brfeaking trucks into weight classes. This will allow us to better estimate the laod, mileage and efficiency vlaues we need.

    # Non road transport:
    # We will just use energy values for now, but later on we can start looking at what activity data there is and so on.

    # Public transport:
    # especially buses.




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