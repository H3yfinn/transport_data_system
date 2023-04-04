
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
import analysis_and_plotting_functions
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

previous_FILE_DATE_ID =None#'DATE20230404'
if previous_FILE_DATE_ID is not None:#you can set some of these to false if you want to do some of the steps manually
    load_data_creation_progress = True
    load_stocks_mileage_occupancy_load_efficiency_selection_progress = True
    load_stocks_mileage_occupancy_load_efficiency_interpolation_progress = True
    load_energy_activity_selection_progress = True
    load_energy_activity_interpolation_progress = True
    
else:  
    load_data_creation_progress = False
    load_stocks_mileage_occupancy_load_efficiency_selection_progress = False
    load_stocks_mileage_occupancy_load_efficiency_interpolation_progress = False
    load_energy_activity_selection_progress = False
    load_energy_activity_interpolation_progress = False
    

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

        unfiltered_combined_data = data_formatting_functions.combine_datasets(datasets_transport,paths_dict)

        if create_9th_model_dataset:
            #import snapshot of 9th concordance
            model_concordances_base_year_measures_file_name = './intermediate_data/9th_dataset/{}'.format('model_concordances_measures.csv')
            combined_data = data_formatting_functions.filter_for_9th_edition_data(unfiltered_combined_data, model_concordances_base_year_measures_file_name, paths_dict, include_drive_all = True)#added TEMP_add_drive_all_to_concordance() to function
        else:
            combined_data = unfiltered_combined_data.copy()
        #since we dont expect to run the data selection process that often we will just save the data in a dated folder in intermediate_data/data_selection_process/FILE_DATE_ID/

        #TEMP #if this works we could also consider applying it to freight!
        combined_data = data_formatting_functions.TEMP_convert_occupancy_and_load_to_occupancy_or_load(combined_data)
        combined_data = data_formatting_functions.TEMP_convert_freight_passenger_activity_to_activity(combined_data)
        combined_data = data_estimation_functions.TEMP_make_drive_equal_all(combined_data,paths_dict)
        combined_data = data_estimation_functions.split_all_into_bev_phev_and_ice(combined_data,unfiltered_combined_data)
        combined_data = data_estimation_functions.extract_bev_phev_ice_occ_mileage_efficiency_data(combined_data,unfiltered_combined_data)
        combined_data = combined_data[combined_data['drive']!='all']#until we rejig our input data to prodcue data for drive=ice and so on we will just remove it here as it is not useful wehen we are selecting for drive = all in energy and passenger km.
        #TEMP

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
    road_measures_selection_dict = {'measure': 
        ['new_vehicle_efficiency', 'occupancy_or_load', 'mileage', 'stocks'],
    'medium': ['road']}

    datasets_to_always_use = ['estimated_mileage_occupancy_load_efficiency $ transport_data_system']#['iea_ev_explorer $ historical','estimated_mileage_occupancy_efficiency $ transport_data_system']

    if not load_stocks_mileage_occupancy_load_efficiency_selection_progress:#when we design actual progress integration then we wont do it like this. 
        stocks_mileage_occupancy_load_efficiency_combined_data = data_formatting_functions.filter_for_specifc_data(road_measures_selection_dict, combined_data)

        stocks_mileage_occupancy_load_efficiency_combined_data_concordance = data_formatting_functions.filter_for_specifc_data(road_measures_selection_dict, combined_data_concordance)

        # stocks_mileage_occupancy_load_efficiency_combined_data_concordance, stocks_mileage_occupancy_load_efficiency_combined_data = data_estimation_functions.TEMP_create_new_values(stocks_mileage_occupancy_load_efficiency_combined_data_concordance, stocks_mileage_occupancy_load_efficiency_combined_data)

        stocks_mileage_occupancy_load_efficiency_combined_data_concordance = data_selection_functions.data_selection_handler(grouping_cols, stocks_mileage_occupancy_load_efficiency_combined_data_concordance, stocks_mileage_occupancy_load_efficiency_combined_data, paths_dict,datasets_to_always_use,default_user_input=1)
    else:
        stocks_mileage_occupancy_load_efficiency_combined_data_concordance = pd.read_pickle(paths_dict['previous_stocks_mileage_occupancy_load_efficiency_combined_data_concordance'])
    
    stocks_mileage_occupancy_load_efficiency_combined_data_concordance.to_pickle(paths_dict['stocks_mileage_occupancy_load_efficiency_combined_data_concordance'])
    logging.info('Saving stocks_mileage_occupancy_load_efficiency_combined_data_concordance')
    ####################################################
    #interpolate missing values for STOCKS MILAGE OCCUPANCY EFFICIENCY DATA
    ####################################################
    if not load_stocks_mileage_occupancy_load_efficiency_interpolation_progress    :#when we design actual progress integration then we wont do it like this.  
        stocks_mileage_occupancy_load_efficiency_combined_data_concordance = interpolation_functions.interpolate_missing_values(stocks_mileage_occupancy_load_efficiency_combined_data_concordance,INDEX_COLS,paths_dict,automatic_interpolation_method = 'linear', automatic_interpolation = True, FILE_DATE_ID=FILE_DATE_ID,percent_of_values_needed_to_interpolate=0.0000001, INTERPOLATION_LIMIT=10,load_progress=True)
    else:
        stocks_mileage_occupancy_load_efficiency_combined_data_concordance = pd.read_pickle(paths_dict['previous_interpolated_stocks_mileage_occupancy_load_efficiency_combined_data_concordance'])
    #save to pickle
    stocks_mileage_occupancy_load_efficiency_combined_data_concordance.to_pickle(paths_dict['interpolated_stocks_mileage_occupancy_load_efficiency_combined_data_concordance'])
    logging.info('Saving interpolated_stocks_mileage_occupancy_load_efficiency_combined_data_concordance')
    
    stocks_mileage_occupancy_load_efficiency_combined_data = data_formatting_functions.convert_concordance_to_combined_data(stocks_mileage_occupancy_load_efficiency_combined_data_concordance, combined_data)
    
    

    ####################################################
    #INCORPORATE NEW STOCKS MILAGE OCCUPANCY EFFICIENCY DATA TO CREATE NEW PASSANGER KM AND ENERGY DATA
    ####################################################

    passsenger_road_measures_selection_dict = {'measure': 
        ['activity', 'energy'],
    'medium': ['road']}
    datasets_to_always_use  = ['iea_ev_explorer $ historical']

    stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data = data_estimation_functions.calculate_energy_and_activity(stocks_mileage_occupancy_load_efficiency_combined_data, paths_dict)

    stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data.to_pickle(paths_dict['calculated_activity_energy_combined_data'])
    logging.info('Saving calculated_activity_energy_combined_data')
    #then concat it to the combined_data, filter for only data we want then create a new concordance for it. THen run the data selection process on it again.
    stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data = pd.concat([combined_data,stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data],axis=0,sort=False)

    stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data_concordance = data_formatting_functions.create_concordance_from_combined_data(stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data)

    sorting_cols = ['date','economy','measure','transport_type','medium', 'vehicle_type','drive','fuel','frequency','scope']

    stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data_concordance, stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data = data_selection_functions.prepare_data_for_selection(stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data_concordance,stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data,paths_dict, sorting_cols)

    energy_activity_combined_data = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict,stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data)
    energy_activity_combined_data_concordance  = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict,stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data_concordance)

    ####################################################
    #BEGIN DATA SELECTION PROCESS FOR ENERGY AND PASSENGER KM
    ####################################################

    #todo might be good to add an ability to choose which measures to choose from even if a whole dataset is passed. this way we can still create dashboard with occupancy and stuff on it. #although i kin of think the dashboard isnt very useufl. this can be  alater thing to do.
    if not load_energy_activity_selection_progress: 
        highlighted_datasets = ['estimated $ calculate_energy_and_activity()']
        energy_activity_combined_data_concordance = data_selection_functions.data_selection_handler(grouping_cols, energy_activity_combined_data_concordance, energy_activity_combined_data, paths_dict,datasets_to_always_use,highlighted_datasets,default_user_input=1)#todo Need some way to only select for specified measures. as we want to include occupancy and stuff in the dashboard. will also need to filter for only energy and passenger km in the output.
    else:
        energy_activity_combined_data_concordance = pd.read_pickle(paths_dict['previous_energy_activity_combined_data_concordance'])

    #save data to pickle
    pd.to_pickle(energy_activity_combined_data_concordance,paths_dict['energy_activity_combined_data_concordance'])
    logging.info('Saving energy_activity_combined_data_concordance')
    if not load_energy_activity_interpolation_progress:
        #run interpolation
        energy_activity_combined_data_concordance = interpolation_functions.interpolate_missing_values(energy_activity_combined_data_concordance,INDEX_COLS,paths_dict,automatic_interpolation_method = 'linear', automatic_interpolation = True, FILE_DATE_ID=FILE_DATE_ID,percent_of_values_needed_to_interpolate=0.7, INTERPOLATION_LIMIT=3,load_progress=True)
    else:
        # energy_activity_combined_data = pd.read_pickle(paths_dict['previous_interpolated_energy_activity_combined_data_concordance'])
        energy_activity_combined_data_concordance = pd.read_pickle(paths_dict['previous_interpolated_energy_activity_combined_data_concordance'])

    #save to pickle
    energy_activity_combined_data_concordance.to_pickle(paths_dict['interpolated_energy_activity_combined_data_concordance'])
    #convert to combined data
    energy_activity_combined_data = data_formatting_functions.convert_concordance_to_combined_data(energy_activity_combined_data_concordance, combined_data)
    logging.info('Saving interpolated_energy_activity_combined_data_concordance')
    ####################################################
    #FINALISE DATA
    ####################################################
    #join the two datasets together
    road_combined_data = pd.concat([stocks_mileage_occupancy_load_efficiency_combined_data,energy_activity_combined_data],axis=0)

    road_combined_data_concordance = pd.concat([stocks_mileage_occupancy_load_efficiency_combined_data_concordance,energy_activity_combined_data_concordance],axis=0)

    #save to pickle
    road_combined_data.to_pickle(paths_dict['intermediate_folder']+'/road_combined_data.pkl')
    road_combined_data_concordance.to_pickle(paths_dict['intermediate_folder']+'/road_combined_data_concordance.pkl')
        
    #todo nas in below
    non_road_energy_no_transport_type = data_estimation_functions.estimate_non_road_energy(unfiltered_combined_data,road_combined_data,paths_dict)
    non_road_energy = data_estimation_functions.split_non_road_energy_into_transport_types(non_road_energy_no_transport_type,unfiltered_combined_data, paths_dict)
    activity_non_road = data_estimation_functions.estimate_activity_non_road(non_road_energy,road_combined_data,paths_dict)
    #concatenate all the data together
    all_new_combined_data = pd.concat([non_road_energy,road_combined_data,activity_non_road],axis=0)
    #save to pickle
    all_new_combined_data.to_pickle(paths_dict['intermediate_folder']+'/all_new_combined_data_not_rescaled.pkl')

    # analysis_and_plotting_functions.plot_final_data_energy_activity(all_new_combined_data,paths_dict)

    combined_rescaled_data = data_estimation_functions.rescale_total_energy_to_egeda_totals(all_new_combined_data,unfiltered_combined_data,paths_dict)

    analysis_and_plotting_functions.plot_final_data_energy_activity(combined_rescaled_data,paths_dict)

    #save to pickle
    combined_rescaled_data.to_pickle(paths_dict['intermediate_folder']+'/combined_rescaled_data.pkl')
    #save to csv
    combined_rescaled_data.to_csv(paths_dict['intermediate_folder']+'/combined_rescaled_data.csv')

    #TODO INTERPOLATE AND MAYBE SELECT

    #1 todo fix the units in /run/media/deck/Elements SE/APERC/transport_data_system/data_mixing_code/aggregate_best_estimate_efficiency_occupancy_mileage.py
    #2 todo see if there is some way we can introudce more eyars quickly. why is everything for 2017 still anyway?
    #5 todo see why canada km ldv bev is so exponential
    #3 toco plot energy toitals against egeda and create function to rescale.
    #4 todo double check mileage data
    # Assum all ice use is just a mix of diesel and fuel or. Assume all freight use is diesel and the remainder of diesel goes to passenger
    # Transport data system todo
    # Make comparison to egeda proportions
    # Remember that frieght passenger split is determined by 8th
    # Think about method for decreasing mileage or another metric to decrease energy use to the egeda total - could also be intensity but I think passenger road is taking too much?
    # Look at what freight data we do have available
    # How to estimate missing Singapore and other values for non road

    # Anything else for actually making it more accurate?
    # How does stocks per Capita correlate with GDP and population

    #create fuinciton to add remove unit col because it is not userful during selectioin but is after. c an also create concordance in model
    #source col goes missing somehjwerre before road_combined_data.to_pickle(paths_dict['intermediate_folder']+'/road_combined_data_TEST.pkl')

    #dont know what to do about vans if we start estaimting things for them. would probably just put them in freight but then we are also missing so much data for them plus they might be included in ldv a lot of time
    #ldv keeps getting into the freight data for occ load eff and mileage
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