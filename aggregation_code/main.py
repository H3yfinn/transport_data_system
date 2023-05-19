
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

previous_selections_file_path = None#'input_data/previous_selections/combined_data_concordance (1).pkl'
previous_FILE_DATE_ID ='DATE20230410'

if previous_FILE_DATE_ID is not None:#you can set some of these to false if you want to do some of the steps manually
    load_data_creation_progress = True
    load_stocks_mileage_occupancy_load_efficiency_selection_progress = True
    load_stocks_mileage_occupancy_load_efficiency_interpolation_progress = True
    load_energy_activity_selection_progress = False
    load_energy_activity_interpolation_progress = False
    
else:  
    load_data_creation_progress = False
    load_stocks_mileage_occupancy_load_efficiency_selection_progress = False
    load_stocks_mileage_occupancy_load_efficiency_interpolation_progress = False
    load_energy_activity_selection_progress = False
    load_energy_activity_interpolation_progress = False
    
#This function will run the followign general steps:
#1. Extract the latest data that is provided throuh slection_config.yml. This may come from input_data and intermediate_data, depending on if the data has been processed or not.
#2. Combine the data into a single dataframe
#3. OPTIONAL Filter the data to only include the combinations of columns categories that are required for the 9th edition of the aperc transport model, via the model_concordances_measures.csv file.
#4. OPTIONAL do some temporary data cleaning to make the data more consistent with the 9th edition of the aperc transport model. This is due to the difficulty of committing to one form of input data

"""Reasoning:
wide range of datapoints
tracking of """
#%%
def main():
    ################################################################
    #SETUP
    paths_dict = utility_functions.setup_paths_dict(FILE_DATE_ID,INDEX_COLS, EARLIEST_DATE, LATEST_DATE,previous_FILE_DATE_ID,previous_selections_file_path=previous_selections_file_path)

    utility_functions.setup_logging(FILE_DATE_ID,paths_dict,testing=False)

    highlight_list = []
    ################################################################
    #EXTRACT DATA
    if not load_data_creation_progress:      
        datasets_transport, datasets_other = data_formatting_functions.extract_latest_groomed_data()

        unfiltered_combined_data = data_formatting_functions.combine_datasets(datasets_transport,paths_dict)

        #TEMP #do here because it is easier to do this process to all combined data at once tan to do it to each dataset individually
        unfiltered_combined_data = data_estimation_functions.split_stocks_where_drive_is_all_into_bev_phev_and_ice(unfiltered_combined_data)#will essentially assume that all economys have 0 phev and bev unless iea has data on them
        #TEMP

        if create_9th_model_dataset:
            #import snapshot of 9th concordance
            model_concordances_base_year_measures_file_name = './input_data/concordances/9th/{}'.format('model_concordances_measures.csv')
            combined_data = data_formatting_functions.filter_for_9th_edition_data(unfiltered_combined_data, model_concordances_base_year_measures_file_name, paths_dict, include_drive_all = True)
        else:
            combined_data = unfiltered_combined_data.copy()
        #since we dont expect to run the data selection process that often we will just save the data in a dated folder in intermediate_data/data_selection_process/FILE_DATE_ID/

        #TEMP MANUAL ADJUSTMENT FUNCTION
        combined_data = data_formatting_functions.filter_for_most_detailed_stocks_breakdown(combined_data)
        # def filter_for_most_detailed_drive_breakdown(combined_data):
        #     """ this will run through each economys data and identify if there is any datasets with data on more specific drive types than ev/ice
        #     """
        #TEMP



        combined_data_concordance = data_formatting_functions.create_concordance_from_combined_data(combined_data, frequency = 'yearly')

        sorting_cols = ['date','economy','measure','transport_type','medium', 'vehicle_type','drive','fuel','frequency','scope']

        combined_data_concordance, combined_data = data_selection_functions.prepare_data_for_selection(combined_data_concordance,combined_data,paths_dict, sorting_cols)
        
        if previous_selections_file_path is not None:
            combined_data_concordance, combined_data,highlight_list = data_selection_functions.import_previous_selections(combined_data_concordance,paths_dict,previous_selections_file_path,combined_data,option='b',highlight_list = highlight_list)#when we create new data later it means that their concordance will be different to this current concordance. So nothing selected after that will be able to be merged in through this function here..
            #i do wonder how highlighted (option b) is affected here.
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
        ['efficiency', 'occupancy_or_load', 'mileage', 'stocks'],
    'medium': ['road']}
    highlight_list = highlight_list+[]
    datasets_to_always_use =['iea_ev_explorer $ historical']#['estimated_mileage_occupancy_load_efficiency $ transport_data_system']#['iea_ev_explorer $ historical','estimated_mileage_occupancy_efficiency $ transport_data_system']

    if not load_stocks_mileage_occupancy_load_efficiency_selection_progress:#when we design actual progress integration then we wont do it like this. 
        stocks_mileage_occupancy_load_efficiency_combined_data = data_formatting_functions.filter_for_specifc_data(road_measures_selection_dict, combined_data)

        stocks_mileage_occupancy_load_efficiency_combined_data_concordance = data_formatting_functions.filter_for_specifc_data(road_measures_selection_dict, combined_data_concordance)

        # stocks_mileage_occupancy_load_efficiency_combined_data_concordance, stocks_mileage_occupancy_load_efficiency_combined_data = data_estimation_functions.TEMP_create_new_values(stocks_mileage_occupancy_load_efficiency_combined_data_concordance, stocks_mileage_occupancy_load_efficiency_combined_data)
        # 
        stocks_mileage_occupancy_load_efficiency_combined_data_concordance = data_selection_functions.data_selection_handler(grouping_cols, stocks_mileage_occupancy_load_efficiency_combined_data_concordance, stocks_mileage_occupancy_load_efficiency_combined_data, paths_dict,datasets_to_always_use,default_user_input=1, highlighted_datasets=highlight_list)
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
    #INCORPORATE NEW STOCKS MILAGE OCCUPANCY EFFICIENCY DATA TO CREATE NEW PASSANGER KM AND ENERGY DATA, THEN INCORPORATE INTO COMBINED DATA
    ####################################################

    stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data = data_estimation_functions.calculate_energy_and_activity(stocks_mileage_occupancy_load_efficiency_combined_data, paths_dict)

    stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data.to_pickle(paths_dict['calculated_activity_energy_combined_data'])
    logging.info('Saving calculated_activity_energy_combined_data')

    new_combined_data = pd.concat([combined_data,stocks_mileage_occupancy_load_efficiency_activity_energy_combined_data],axis=0,sort=False)

    new_combined_data_concordance = data_formatting_functions.create_concordance_from_combined_data(new_combined_data)

    sorting_cols = ['date','economy','measure','transport_type','medium', 'vehicle_type','drive','fuel','frequency','scope']

    new_combined_data_concordance, new_combined_data = data_selection_functions.prepare_data_for_selection(new_combined_data_concordance,new_combined_data,paths_dict, sorting_cols)

    if previous_selections_file_path is not None:
        new_combined_data_concordance, new_combined_data,highlight_list = data_selection_functions.import_previous_selections(new_combined_data_concordance,paths_dict,previous_selections_file_path,new_combined_data,option='b',highlight_list = highlight_list)#when we create new data later it means that their concordance will be different to this current concordance. So nothing selected after that will be able to be merged in through this function here..
        #i do wonder how highlighted (option b) is affected here.

    ####################################################
    #BEGIN DATA SELECTION PROCESS FOR ALL REMAINING DATA
    ####################################################
    
    #set filter_for_all_other_data to True to find all other data that is not in the road measures selection dict
    all_other_combined_data = data_formatting_functions.filter_for_specifc_data(road_measures_selection_dict,new_combined_data, filter_for_all_other_data=True)
    all_other_combined_data_concordance  = data_formatting_functions.filter_for_specifc_data(road_measures_selection_dict,new_combined_data_concordance, filter_for_all_other_data=True)


    #now drop all energy and activity for air, rail and ship, as well as for
    non_road_measures_exclusion_dict  = {'medium':['air','rail','ship'],
                                         'measure':['energy','activity']}
    all_other_combined_data = data_formatting_functions.filter_for_specifc_data(non_road_measures_exclusion_dict,all_other_combined_data, filter_for_all_other_data=True)
    all_other_combined_data_concordance  = data_formatting_functions.filter_for_specifc_data(non_road_measures_exclusion_dict,all_other_combined_data_concordance, filter_for_all_other_data=True)
    ####################################################
    #BEGIN DATA SELECTION PROCESS FOR ENERGY AND PASSENGER KM
    ####################################################

    #todo might be good to add an ability to choose which measures to choose from even if a whole dataset is passed. this way we can still create dashboard with occupancy and stuff on it. #although i kin of think the dashboard isnt very useufl. this can be  alater thing to do.
    
    
    if not load_energy_activity_selection_progress: 
        highlight_list = highlight_list +['estimated $ calculate_energy_and_activity()']
        datasets_to_always_use  = ['iea_ev_explorer $ historical']
        all_other_combined_data_concordance = data_selection_functions.data_selection_handler(grouping_cols, all_other_combined_data_concordance, all_other_combined_data, paths_dict,datasets_to_always_use,highlighted_datasets=highlight_list,default_user_input=1)#todo Need some way to only select for specified measures. as we want to include occupancy and stuff in the dashboard. will also need to filter for only energy and passenger km in the output.
    else:
        all_other_combined_data_concordance = pd.read_pickle(paths_dict['previous_all_other_combined_data_concordance'])

    #save data to pickle
    pd.to_pickle(all_other_combined_data_concordance,paths_dict['all_other_combined_data_concordance'])
    logging.info('Saving all_other_combined_data_concordance')
    if not load_energy_activity_interpolation_progress:
        #run interpolation
        all_other_combined_data_concordance = interpolation_functions.interpolate_missing_values(all_other_combined_data_concordance,INDEX_COLS,paths_dict,automatic_interpolation_method = 'linear', automatic_interpolation = True, FILE_DATE_ID=FILE_DATE_ID,percent_of_values_needed_to_interpolate=0.7, INTERPOLATION_LIMIT=3,load_progress=True)
    else:
        # all_other_combined_data = pd.read_pickle(paths_dict['previous_interpolated_all_other_combined_data_concordance'])
        all_other_combined_data_concordance = pd.read_pickle(paths_dict['previous_interpolated_all_other_combined_data_concordance'])

    #save to pickle
    all_other_combined_data_concordance.to_pickle(paths_dict['interpolated_all_other_combined_data_concordance'])
    #convert to combined data
    all_other_combined_data = data_formatting_functions.convert_concordance_to_combined_data(all_other_combined_data_concordance, combined_data)
    logging.info('Saving interpolated_all_other_combined_data_concordance')

    ####################################################
    #combine and save all data seelctions and intepolations before final calculations
    ####################################################

    #join the two datasets together
    all_combined_data = pd.concat([stocks_mileage_occupancy_load_efficiency_combined_data,all_other_combined_data],axis=0)

    all_combined_data_concordance = pd.concat([stocks_mileage_occupancy_load_efficiency_combined_data_concordance,all_other_combined_data_concordance],axis=0)

    all_combined_data_concordance.to_pickle(paths_dict['all_selections_done_combined_data_concordance'])
    all_combined_data.to_pickle(paths_dict['all_selections_done_combined_data'])
    ####################################################
    #CALCUALTE NON ROAD ENERGY AND ACTIVITY DATA
    ####################################################

    non_road_energy_no_transport_type = data_estimation_functions.estimate_non_road_energy(unfiltered_combined_data,all_combined_data,paths_dict)
    non_road_energy = data_estimation_functions.split_non_road_energy_into_transport_types(non_road_energy_no_transport_type,unfiltered_combined_data, paths_dict)
    activity_non_road = data_estimation_functions.estimate_activity_non_road_using_intensity(non_road_energy,all_combined_data,paths_dict)
    #concatenate all the data together
    all_new_combined_data = pd.concat([non_road_energy,all_combined_data,activity_non_road],axis=0)#todo check for anything unexpected here
    #save to pickle
    all_new_combined_data.to_pickle(paths_dict['final_combined_data_not_rescaled'])


    #NOTE THAT WE CAN ALWAYS DO ANOTEHR ROUND OF SELECTIONS HERE IF WE WANT TO, BUT DOESNT SEEM USEFUL.
    ####################################################
    #MAKE SURE DATA MATCHES EGEDA TOTALS
    ####################################################
    # analysis_and_plotting_functions.plot_final_data_energy_activity(all_new_combined_data,paths_dict)

    combined_rescaled_data = data_estimation_functions.rescale_total_energy_to_egeda_totals(all_new_combined_data,unfiltered_combined_data,paths_dict)

    analysis_and_plotting_functions.plot_final_data_energy_activity(combined_rescaled_data,paths_dict)

    #save to pickle
    combined_rescaled_data.to_pickle(paths_dict['final_combined_rescaled_data'])

    #save to output_data
    combined_rescaled_data.to_csv(paths_dict['final_data_csv'], index=False)

    ####################################################
    #FINALISE DATA
    ####################################################
    #TODO INTERPOLATE AND MAYBE SELECT

    #2 todo see if there is some way we can introudce more eyars quickly. why is everything for 2017 still anyway?

    #5 todo see why canada km ldv bev is so exponential

    #look at the other options for changing energy use besides mileage

    #1 figure out  NEW intensity for non road isnterad of basing it off of previous selections in import_previous_draft_selections()

    # How to estimate missing Singapore and other values for non road

    # How does stocks per Capita correlate with GDP and population. is there some way we can use this to estimate stocks or even forecast stocks

    #create fuinciton to add remove unit col because it is not userful during selectioin but is after. can also jsut use concordance from model

    #source col goes missing somehjwerre before road_combined_data.to_pickle(paths_dict['intermediate_folder']+'/road_combined_data_TEST.pkl')

    #dont know what to do about vans if we start estaimting things for them. would probably just put them in freight but then we are also missing so much data for them plus they might be included in ldv a lot of time

    #ldv keeps getting into the freight data for occ load eff and mileage

    #implememnt a function to check the datas completeness againsdt the origianl model concordance
    
    #maybe make yyyy formatted date go back to yyyy-mm-dd

    #make sure its clear what the uniots of eeverything is.

    #todo saap

    #introduce non road intensity as input
    #replace old intensity calcualtions with new one
    #introduce new vehicle efficiency as input (make sure it and any newly intorduced inpouts easily make their way into hte ouytput)
    #check new concoaradance measures works ok
    
    #stop splitting no road transport types using 8th edition model
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