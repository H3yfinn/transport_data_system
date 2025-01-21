
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
import pre_selection_estimation_functions
import logging
import analysis_and_plotting_functions
import yaml
from utility_functions import insert_economy_into_path
create_9th_model_dataset = True

file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
# FILE_DATE_ID = 'DATE20230726'
EARLIEST_DATE="2010-01-01"
LATEST_DATE='2024-01-01'

previous_selections_file_path = None#'input_data/previous_selections/combined_data_concordance_DATE20240731.pkl'
previous_FILE_DATE_ID =None#'DATE20240731'#None#'DATE20240314'

if previous_FILE_DATE_ID is not None:#you can set some of these to false if you want to do some of the steps manually
    LOAD_DATA_CREATION_PROGRESS = True#setme
    load_road_measures_selection_progress = False #setme
    load_road_measures_interpolation_progress = False#setme
    load_energy_activity_selection_progress = False#setme
    load_energy_activity_interpolation_progress = False#setme
    
else:  
    LOAD_DATA_CREATION_PROGRESS = False
    load_road_measures_selection_progress = False
    load_road_measures_interpolation_progress = False
    load_energy_activity_selection_progress = False
    load_energy_activity_interpolation_progress = False

RESCALE_DATA_TO_MATCH_EGEDA_TOTALS = False#note that this is not done aymore because of the optimisation process in the transport mdoel which uses optimsiation to choose the best values for stocks/mielage/occupancy/efficiency so their product is equal to the egeda totals.

################################################################

#This function will run the followign general steps:
#1. Extract the latest data that is provided throuh slection_config.yml. This may come from input_data and intermediate_data, depending on if the data has been processed or not.
#2. Combine the data into a single dataframe
#3. OPTIONAL Filter the data to only include the combinations of columns categories that are required for the 9th edition of the aperc transport model, via the model_concordances_measures.csv file.
#4. OPTIONAL do some temporary data cleaning to make the data more consistent with the 9th edition of the aperc transport model. This is due to the difficulty of committing to one form of input data
# paths_dict, highlight_list = setup_main()

#%% 

#if you set this to something then it will only do selections for that economy and then using the FILE_DATE_ID of a previous final output, concat the new data to the old data(with the economy removed from old data). Else, set it to None and it will run for all economies in the data
ECONOMIES_TO_RUN=['04_CHL']#20_USA', '08_JPN', '12_NZ']#['09_ROK']#['17_SGP', '06_HKC']#['15_PHL']#, '01_AUS']#[ '12_NZ']#['07_INA']#['05_PRC']#[ '12_NZ']#['03_CDA','08_JPN'] #MAKE SURE THIS IS A #'17_SGP',LIST#'19_THA'#'08_JPN'#'08_JPN'#'20_USA'# '08_JPN'#'05_PRC'

ECONOMIES_TO_RUN_PREV_DATE_ID ='DATE20250121'#'DATE20240726' #='DATE20231005_DATE20230927'#'DATE20230824'#'DATE20230810'#='DATE20230731_19_THA'#'DATE20230717'#'DATE20230712'#'DATE20230628'#make sure to update this to what you want to concat the new data to so you have a full dataset. Note it could also be somethign liek DATE20230731_19_THA just make sure its the end of the file name and not including combined_data_ since that is added automatically

def setup_main():
    global FILE_DATE_ID
    global ECONOMIES_TO_RUN_PREV_DATE_ID
    ################################################################
    if ECONOMIES_TO_RUN is not None:
        if ECONOMIES_TO_RUN_PREV_DATE_ID is None:
            ECONOMIES_TO_RUN_PREV_DATE_ID = FILE_DATE_ID
        FILE_DATE_ID = FILE_DATE_ID + '_{}'.format("".join(ECONOMIES_TO_RUN_PREV_DATE_ID))
    paths_dict = utility_functions.setup_paths_dict(FILE_DATE_ID, EARLIEST_DATE, LATEST_DATE,previous_FILE_DATE_ID=previous_FILE_DATE_ID,ECONOMIES_TO_RUN_PREV_DATE_ID=ECONOMIES_TO_RUN_PREV_DATE_ID, previous_selections_file_path=previous_selections_file_path)
    
    utility_functions.setup_logging(FILE_DATE_ID,paths_dict,testing=True)

    highlight_list = []
    
    return paths_dict, highlight_list #= setup_main()
#%%
    
def main():
    global FILE_DATE_ID
    global ECONOMIES_TO_RUN_PREV_DATE_ID
    # ################################################################
    #SETUP
    import time
    start_time = time.time()#time taken =  3103.275397539139
    print('Timing process')
    
    paths_dict, highlight_list = setup_main()
    ################################################################
    #EXTRACT DATA
    if not LOAD_DATA_CREATION_PROGRESS:   
        datasets_transport, datasets_other = data_formatting_functions.extract_latest_groomed_data()
        unfiltered_combined_data = data_formatting_functions.combine_datasets(datasets_transport,paths_dict,ADD_COLUMNS_AND_RESAVE = True)

        # if ECONOMIES_TO_RUN is not None:
        #     unfiltered_combined_data = unfiltered_combined_data[unfiltered_combined_data['economy'].isin(ECONOMIES_TO_RUN)]
        
        #EDIT ALL DATA BEFORE SELECTION
        unfiltered_combined_data = pre_selection_estimation_functions.split_stocks_where_drive_is_all_into_bev_phev_and_ice(unfiltered_combined_data)#will essentially assume that all economys have 0 phev and bev unless iea has data on them
        unfiltered_combined_data =  pre_selection_estimation_functions.split_stocks_where_drive_is_ev_or_bev_and_phev(unfiltered_combined_data)
        splits_dict_petrol_to_diesel = pre_selection_estimation_functions.estimate_petrol_diesel_splits(unfiltered_combined_data)
        #now we have to split the stocks where drive is all into bev and phev      
        unfiltered_combined_data = pre_selection_estimation_functions.split_vehicle_types_using_distributions(unfiltered_combined_data)#trying out putting this before spltting ice into phev and petrol and diesel
        unfiltered_combined_data = pre_selection_estimation_functions.split_ice_phev_into_petrol_and_diesel(unfiltered_combined_data,splits_dict_petrol_to_diesel)
        
        #EDIT ALL DATA BEFORE SELECTION END
        if create_9th_model_dataset:
            #import snapshot of 9th concordance
            #however, this doesnt include the years in the model_concordances_measures.csv file. They are determined by EARLIEST_DATE and LATEST_DATE
            model_concordances_base_year_measures_file_name = paths_dict['concordances_file_path']
                
            #TEMP EDIT CONCORDANCES FOR NON ROAD:
            #we will set drive to all for non road so that the data we currently have as input data still works. the detail is handled in the model currently, although this is not a good way to do it.
            paths_dict = data_formatting_functions.drop_detailed_drive_types_from_non_road_concordances(paths_dict)
            combined_data = data_formatting_functions.filter_for_transport_model_data_using_concordances(unfiltered_combined_data, model_concordances_base_year_measures_file_name, paths_dict,SET_NONROAD_DRIVE_TO_ALL=True)
            
        else:
            combined_data = unfiltered_combined_data.copy()
            
        #seems like this is the best place to do this.if we do it earlier ew lose useful aggregates of all economies.     
        if ECONOMIES_TO_RUN is not None:
            unfiltered_combined_data = unfiltered_combined_data[unfiltered_combined_data['economy'].isin(ECONOMIES_TO_RUN)]
            combined_data = combined_data[combined_data['economy'].isin(ECONOMIES_TO_RUN)]
            
        combined_data = data_formatting_functions.filter_for_most_detailed_vehicle_type_stock_breakdowns(combined_data)
        combined_data_concordance = data_formatting_functions.create_concordance_from_combined_data(combined_data, frequency = 'yearly')
        
        sorting_cols = ['date','economy','measure','transport_type','medium', 'vehicle_type','drive','fuel','frequency','scope']
        
        combined_data_concordance, combined_data = data_selection_functions.prepare_data_for_selection(combined_data_concordance,combined_data,paths_dict, sorting_cols)
        
        if previous_selections_file_path is not None:
            combined_data_concordance, combined_data,highlight_list = data_selection_functions.import_previous_selections(combined_data_concordance,paths_dict,previous_selections_file_path,combined_data,option='b',highlight_list = highlight_list)#when we create new data later it means that their concordance will be different to this current concordance. So nothing selected after that will be able to be merged in through this function here..
            #i do wonder how highlighted (option b) is affected here.
    else:
        unfiltered_combined_data = pd.read_pickle(paths_dict['previous_unfiltered_combined_data_pkl'])
        combined_data_concordance = pd.read_pickle(paths_dict['previous_combined_data_concordance'])
        combined_data = pd.read_pickle(paths_dict['previous_combined_data'])

    #save data to pickle
    unfiltered_combined_data.to_pickle(paths_dict['unfiltered_combined_data_pkl'])
    unfiltered_combined_data.to_csv(paths_dict['unfiltered_combined_data_csv'])
    combined_data_concordance.to_pickle(paths_dict['combined_data_concordance'])
    combined_data.to_pickle(paths_dict['combined_data'])
    logging.info('Saving combined_data_concordance and combined_data')
    
    #TEMP SHIFT TO RUN BY ECONOMY TO TAKE LESS SPACE:
    all_economies_final_df = pd.DataFrame()
    combined_data_copy = combined_data.copy()
    combined_data_concordance_copy = combined_data_concordance.copy()
    unfiltered_combined_data_copy = unfiltered_combined_data.copy()
    
    
    for economy in unfiltered_combined_data.economy.unique():
        print('Running for economy: ',economy)
        #filter for economy
        combined_data = combined_data_copy[combined_data_copy['economy'] == economy]
        combined_data_concordance = combined_data_concordance_copy[combined_data_concordance_copy['economy'] == economy]
        unfiltered_combined_data = unfiltered_combined_data_copy[unfiltered_combined_data_copy['economy'] == economy]
        
        midway_time = time.time()
        print('Time taken to extract and combine data: ',midway_time-start_time,' seconds')
        ####################################################
        #BEGIN DATA SELECTION PROCESS FOR ROAD MEASURES BESIDES ENERGY AND ACTIVITY
        ####################################################

        grouping_cols = ['economy','vehicle_type','drive']
        
        road_measures_selection_dict = {'measure': ['intensity', 'efficiency', 'occupancy_or_load', 'mileage', 'stocks', 'average_age'],
        'medium': ['road', 'air', 'rail', 'ship']}
        
        highlight_list = highlight_list+[]
        road_measures_datasets_to_always_use = yaml.load(open('config/selection_config.yml'), Loader=yaml.FullLoader)['road_measures_datasets_to_always_use']
        #['estimated_mileage_occupancy_load_efficiency $ transport_data_system']#['iea_ev_explorer $ historical','estimated_mileage_occupancy_efficiency $ transport_data_system']
        if not load_road_measures_selection_progress:#when we design actual progress integration then we wont do it like this. 
            road_measures_combined_data = data_formatting_functions.filter_for_specifc_data(road_measures_selection_dict, combined_data)

            road_measures_combined_data_concordance = data_formatting_functions.filter_for_specifc_data(road_measures_selection_dict, combined_data_concordance)
            
            road_measures_combined_data_concordance = data_selection_functions.data_selection_handler(grouping_cols, road_measures_combined_data_concordance, road_measures_combined_data, paths_dict,road_measures_datasets_to_always_use,default_user_input=['Keep_for_all_consecutive_years', 0], highlighted_datasets=highlight_list, PLOT_SELECTION_TIMESERIES=True, DATASETS_TO_DEPRIORITISE=['ato', '8th'])
            
        else:
            road_measures_combined_data_concordance = pd.read_pickle(insert_economy_into_path(paths_dict['previous_road_measures_combined_data_concordance'],economy))
        road_measures_combined_data_concordance.to_pickle(insert_economy_into_path(paths_dict['road_measures_combined_data_concordance'],economy))
        logging.info('Saving road_measures_combined_data_concordance')
        ####################################################
        #interpolate missing values for STOCKS MILAGE OCCUPANCY EFFICIENCY DATA
        ####################################################
        if not load_road_measures_interpolation_progress:#when we design actual progress integration then we wont do it like this.  
            road_measures_combined_data_concordance = interpolation_functions.interpolate_missing_values(road_measures_combined_data_concordance,paths_dict['INDEX_COLS'],paths_dict,automatic_interpolation_method = 'linear', automatic_interpolation = True, FILE_DATE_ID=FILE_DATE_ID,percent_of_values_needed_to_interpolate=0.0000001, INTERPOLATION_LIMIT=10,load_progress=True)
        else:
            road_measures_combined_data_concordance = pd.read_pickle(insert_economy_into_path(paths_dict['previous_interpolated_road_measures_combined_data_concordance'],economy))
        #save to pickle
        road_measures_combined_data_concordance.to_pickle(insert_economy_into_path(paths_dict['interpolated_road_measures_combined_data_concordance'],economy))
        logging.info('Saving interpolated_road_measures_combined_data_concordance')
        
        road_measures_combined_data = data_formatting_functions.convert_concordance_to_combined_data(road_measures_combined_data_concordance, combined_data)
        
        ####################################################
        #INCORPORATE NEW STOCKS MILAGE OCCUPANCY EFFICIENCY DATA TO CREATE NEW PASSANGER KM AND ENERGY DATA, THEN INCORPORATE INTO COMBINED DATA
        ####################################################
        #where is average age for non raod? it seems to go missing around here
        road_measures_activity_energy_combined_data = pre_selection_estimation_functions.calculate_energy_and_activity(road_measures_combined_data, paths_dict)
        road_measures_activity_energy_combined_data.to_pickle(insert_economy_into_path(paths_dict['calculated_activity_energy_combined_data'], economy))
        logging.info('Saving calculated_activity_energy_combined_data')

        new_combined_data = pd.concat([combined_data,road_measures_activity_energy_combined_data],axis=0,sort=False)
        
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

        #now drop all data in the confluence of energy and activity for air, rail and ship, as well as for 'energy','activity' measures
        non_road_measures_exclusion_dict  = {}#'medium':['air','rail','ship'],'measure':['energy']
        all_other_combined_data = data_formatting_functions.filter_for_specifc_data(non_road_measures_exclusion_dict,all_other_combined_data, filter_for_all_other_data=True)
        
        all_other_combined_data_concordance  = data_formatting_functions.filter_for_specifc_data(non_road_measures_exclusion_dict,all_other_combined_data_concordance, filter_for_all_other_data=True)
        ####################################################
        #BEGIN DATA SELECTION PROCESS FOR ENERGY AND PASSENGER KM
        ####################################################
        if not load_energy_activity_selection_progress: 
            highlight_list = highlight_list +['estimated $ calculate_energy_and_activity()']
            all_other_combined_data_datasets_to_always_use = yaml.load(open('config/selection_config.yml'), Loader=yaml.FullLoader)['all_other_combined_data_datasets_to_always_use']
            all_other_combined_data_concordance = data_selection_functions.data_selection_handler(grouping_cols, all_other_combined_data_concordance, all_other_combined_data, paths_dict,all_other_combined_data_datasets_to_always_use,highlighted_datasets=highlight_list,default_user_input=['Keep_for_all_consecutive_years', 0], PLOT_SELECTION_TIMESERIES=True, DATASETS_TO_DEPRIORITISE=['ato', '8th'])
        else:
            all_other_combined_data_concordance = pd.read_pickle(insert_economy_into_path(paths_dict['previous_all_other_combined_data_concordance'], economy))
        
        #save data to pickle
        pd.to_pickle(all_other_combined_data_concordance, insert_economy_into_path(paths_dict['all_other_combined_data_concordance'], economy))
        logging.info('Saving all_other_combined_data_concordance')
        if not load_energy_activity_interpolation_progress:
            #run interpolation
            all_other_combined_data_concordance = interpolation_functions.interpolate_missing_values(all_other_combined_data_concordance,paths_dict['INDEX_COLS'],paths_dict,automatic_interpolation_method = 'linear', automatic_interpolation = True, FILE_DATE_ID=FILE_DATE_ID,percent_of_values_needed_to_interpolate=0.7, INTERPOLATION_LIMIT=3,load_progress=True)
        else:
            # all_other_combined_data = pd.read_pickle(paths_dict['previous_interpolated_all_other_combined_data_concordance'])
            all_other_combined_data_concordance = pd.read_pickle(insert_economy_into_path(paths_dict['previous_interpolated_all_other_combined_data_concordance'], economy))

        #save to pickle
        all_other_combined_data_concordance.to_pickle(insert_economy_into_path(paths_dict['interpolated_all_other_combined_data_concordance'], economy))
        #convert to combined data
        all_other_combined_data = data_formatting_functions.convert_concordance_to_combined_data(all_other_combined_data_concordance, combined_data)
        logging.info('Saving interpolated_all_other_combined_data_concordance')

        ####################################################
        #combine and save all data seelctions and intepolations before final calculations
        ####################################################
        #join the two datasets together
        all_combined_data = pd.concat([road_measures_combined_data,all_other_combined_data],axis=0)
        
        all_combined_data_concordance = pd.concat([road_measures_combined_data_concordance,all_other_combined_data_concordance],axis=0)

        all_combined_data_concordance.to_pickle(insert_economy_into_path(paths_dict['all_selections_done_combined_data_concordance'], economy))
        all_combined_data.to_pickle(insert_economy_into_path(paths_dict['all_selections_done_combined_data'], economy))
        ####################################################
        #CALCUALTE NON ROAD ENERGY AND ACTIVITY DATA
        ####################################################
        #PLEASE NOTE THAT THIS DATA IN THIS STEP WILL ONLY BE FOR 2017. 
        non_road_energy_no_transport_type = data_estimation_functions.estimate_non_road_energy(unfiltered_combined_data,all_combined_data,paths_dict, economy)   
        
        non_road_energy = data_estimation_functions.split_non_road_energy_into_transport_types(non_road_energy_no_transport_type,unfiltered_combined_data, all_combined_data, paths_dict, economy)
        
        non_road_energy,all_combined_data,activity,old_activity= data_estimation_functions.estimate_activity_non_road_using_intensity(non_road_energy,all_combined_data,paths_dict)
        
        #remove energy from all_new_combined_data where medium != road
        all_combined_data = all_combined_data[~((all_combined_data['medium'] != 'road') & (all_combined_data['measure'] == 'energy'))]
        #concatenate all the data together
        all_new_combined_data = pd.concat([non_road_energy,all_combined_data,activity],axis=0)
        all_new_combined_data = data_estimation_functions.extract_activity_and_calculate_intensity_for_applicable_economies(economy, non_road_energy, all_combined_data, all_new_combined_data, old_activity)
        #save to pickle
        all_new_combined_data.to_pickle(insert_economy_into_path(paths_dict['final_combined_data_not_rescaled'], economy))
        ####################################################
        #MAKE SURE DATA MATCHES EGEDA TOTALS
        ####################################################
        # analysis_and_plotting_functions.plot_final_data_energy_activity(all_new_combined_data,paths_dict)
        if RESCALE_DATA_TO_MATCH_EGEDA_TOTALS:#note that this is not done aymore because of the optimisation process in the transport mdoel which uses optimsiation to choose the best values for stocks/mielage/occupancy/efficiency so their product is equal to the egeda totals.
            combined_rescaled_data = data_estimation_functions.rescale_total_energy_to_egeda_totals(all_new_combined_data,unfiltered_combined_data,paths_dict)

            # analysis_and_plotting_functions.plot_final_data_energy_activity(combined_rescaled_data,paths_dict)

            #save to pickle
            combined_rescaled_data.to_pickle(insert_economy_into_path(paths_dict['final_combined_rescaled_data'], economy))

            combined_rescaled_data.to_pickle(insert_economy_into_path(paths_dict['final_combined_data_pkl'], economy))

            combined_rescaled_data.to_csv(insert_economy_into_path(paths_dict['final_data_csv'], economy), index=False)
            
            #save data with economy name, just in case we need it later
            combined_rescaled_data.to_pickle(paths_dict['final_combined_data_pkl'][:-4]+'_'+economy+'.pkl')
            combined_rescaled_data.to_csv(paths_dict['final_data_csv'][:-4]+'_'+economy+'.csv', index=False)
            
            all_economies_final_df = pd.concat([all_economies_final_df,combined_rescaled_data],axis=0)
        else:
            #save to pickle
            all_new_combined_data.to_pickle(insert_economy_into_path(paths_dict['final_combined_data_pkl'], economy))
            
            #save to output_data
            all_new_combined_data.to_csv(insert_economy_into_path(paths_dict['final_data_csv'], economy), index=False)
            
            #save data with economy name, just in case we need it later
            all_new_combined_data.to_pickle(paths_dict['final_combined_data_pkl'][:-4]+'_'+economy+'.pkl')
            all_new_combined_data.to_csv(paths_dict['final_data_csv'][:-4]+'_'+economy+'.csv', index=False)
            
            all_economies_final_df = pd.concat([all_economies_final_df,all_new_combined_data],axis=0)
    ####################################################
    #FINALISE DATA
    ####################################################
    #save to pickle
    
    all_economies_final_df.to_pickle(paths_dict['final_combined_data_pkl'])
    all_economies_final_df.to_csv(paths_dict['final_data_csv'], index=False)
    print('Saving final_combined_data_pkl and final_data_csv')
    
    if ECONOMIES_TO_RUN is not None:
        
        # grab data for ECONOMIES_TO_RUN_PREV_DATE_ID
        previous_final_data_csv = pd.read_csv(paths_dict['previous_final_combined_data_csv'])
        previous_final_data = pd.read_pickle(paths_dict['previous_final_combined_data_pkl'])
        #TEST if the csv and pkl have same economies. if not, this is somehting ive been wathing for and need to fix
        if not previous_final_data_csv.economy.unique().tolist() == previous_final_data.economy.unique().tolist():
            breakpoint()
            print('WARNING: previous_final_data_csv.economy.unique() != previous_final_data.economy.unique()')
        #drop economy from previous data
        previous_final_data = previous_final_data[~previous_final_data['economy'].isin(ECONOMIES_TO_RUN)]
        
        #cpocnat with new data
        final_data = pd.read_pickle(paths_dict['final_combined_data_pkl'])
        final_data = pd.concat([final_data,previous_final_data],axis=0)
        #save to pickle
        final_data.to_pickle(paths_dict['final_combined_data_pkl'])
        final_data.to_csv(paths_dict['final_data_csv'], index=False)

    print('###################################\n\n')
    #print time
    end_time = time.time()
    print('time taken = ',end_time-start_time)
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
# %%
# pd.read_pickle('intermediate_data/selection_process/DATE20231018_DATE20231017/combined_data_error.pkl')[['unit', 'measure']].drop_duplicates().sort_values(by=[ 'measure','unit'])

#TODO LIST: (THESE AR OLD AND CHANCE THEY ARE NOT ALL RELEVANT ANYMORE)

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