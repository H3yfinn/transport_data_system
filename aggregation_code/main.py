
#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re


#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

import data_formatting_functions as data_formatting_functions
import utility_functions as utility_functions
import data_selection_functions_test as data_selection_functions

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
#%%
paths_dict = data_formatting_functions.setup_dataselection_process(FILE_DATE_ID,INDEX_COLS, EARLIEST_DATE, LATEST_DATE)

datasets_transport, datasets_other = data_formatting_functions.extract_latest_groomed_data()
#for now wont do anmything with datasets_other
#%%
combined_data = data_formatting_functions.combine_datasets(datasets_transport, FILE_DATE_ID,paths_dict)

#%%
if create_9th_model_dataset:
    #import snapshot of 9th concordance
    model_concordances_base_year_measures_file_name = './intermediate_data/9th_dataset/{}'.format('model_concordances_measures.csv')
    combined_data = data_formatting_functions.filter_for_9th_edition_data(combined_data, model_concordances_base_year_measures_file_name, paths_dict)

#since we dont expect to run the data selection process that often we will just save the data in a dated folder in intermediate_data/data_selection_process/FILE_DATE_ID/

#%%
combined_data_concordance = data_formatting_functions.create_whole_dataset_concordance(combined_data, frequency = 'yearly')
# combined_data_concordance, combined_data = data_formatting_functions.change_column_names(combined_data_concordance, combined_data)

#%%
#save data to pickle so we dont have to do this again
combined_data_concordance.to_pickle('combined_data_concordance.pkl')
combined_data.to_pickle('combined_data.pkl')
#%%
#laod data from pickle
combined_data_concordance = pd.read_pickle('combined_data_concordance.pkl')
combined_data = pd.read_pickle('combined_data.pkl')

#%%
sorting_cols = ['date','economy','measure','transport_type','medium', 'vehicle_type','drive','fuel','frequency','scope']
combined_data_concordance, combined_data = data_formatting_functions.prepare_data_for_selection(combined_data_concordance,combined_data,paths_dict, sorting_cols)#todo reaplce everything that is combined_dataset with combined_data
#%%
passsenger_road_measures_selection_dict = {'measure': 
    ['efficiency', 'occupancy', 'mileage', 'stocks'],
 'medium': ['road'],
 'transport_type': ['passenger']}

combined_data,combined_data_concordance = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict,combined_data_concordance, combined_data)

#%%
grouping_cols = ['economy','vehicle_type','drive']
# save groups selections to tmp folder
# close dashboard
combined_data_concordance.set_index(paths_dict['INDEX_COLS_no_year'] , inplace=True)
combined_data.set_index(paths_dict['INDEX_COLS_no_year'] , inplace=True)#todo is there some way to not have to do this? 
def TEMP_FIX_change_date_col_to_year(df):
    # %matplotlib qt
    #TEMP FIX START
    #NOTE MAKING THIS ONLY WORK FOR YEARLY DATA, AS IT WOULD BE COMPLICATED TO DO IT OTEHRWISE. lATER ON WE CAN TO IT COMPLETELY BY CHANGING LINES THAT ADD 1 TO THE DATE IN THE DATA SLECTION FUNCTIONS TO ADD ONE UNIT OF WHATEVER THE FREQUENCY IS. 
    #change dataframes date col to be just the year in that date eg. 2022-12-31 = 2022 by using string slicing
    df.date = df.date.apply(lambda x: x[:4]).astype(int)
    return df

combined_data_concordance = TEMP_FIX_change_date_col_to_year(combined_data_concordance)
combined_data = TEMP_FIX_change_date_col_to_year(combined_data)
#TEMP FIX END

#order data by date
combined_data = combined_data.sort_values(by='date')
combined_data = combined_data.sort_values(by='date')

#create progresss pickle which will be updated after every selection
progress = pd.DataFrame(columns=paths_dict['INDEX_COLS_no_year'])
progress.to_pickle('progress.pkl')#TODO set location if this is needed
#%%



#%%
grouping_cols = ['economy','vehicle_type','drive']
def manual_data_selection_handler(grouping_cols, combined_data_concordance, combined_data, paths_dict):
    
    ########PREPAREATION########
    # combined_data_concordance.set_index(paths_dict['INDEX_COLS_no_year'] , inplace=True)
    # combined_data.set_index(paths_dict['INDEX_COLS_no_year'] , inplace=True)#todo is there some way to not have to do this? 
    
    combined_data_concordance = TEMP_FIX_change_date_col_to_year(combined_data_concordance)
    combined_data = TEMP_FIX_change_date_col_to_year(combined_data)
    #TEMP FIX END

    #order data by date
    combined_data = combined_data.sort_values(by='date')
    combined_data = combined_data.sort_values(by='date')

    options_dict = prepare_user_selection_options()

    groups_concordance_names_files, groups_data_names_files = save_groups_to_tmp_folder(combined_data_concordance, combined_data, paths_dict,grouping_cols)
    
    #remove combined_data and combined_data_concordance from memory
    del combined_data_concordance
    del combined_data
    
    ########SELECTION BEGINS########
    # iterate through every group
    for group_concordance_name_file, group_data_name_file in zip(groups_concordance_names_files, groups_data_names_files):
        #load the data
        group_concordance = pd.read_pickle(group_concordance_name_file)
        group_data = pd.read_pickle(group_data_name_file)

        #open dashboard
        handle_group_dashbaord(group_concordance, group_data, paths_dict)

        #set index to INDEX_COLS_no_year
        group_concordance.set_index(paths_dict['INDEX_COLS_no_year'] , inplace=True)
        group_data.set_index(paths_dict['INDEX_COLS_no_year'] , inplace=True)

        # iterate through each index col
        for index_row_no_year in group_concordance.index:
            current_index_row_no_year = group_concordance.loc[index_row_no_year]

            if current_index_row_no_year.Num_datapoints <= 1:
                continue
            
            data_for_plotting = group_data.loc[index_row_no_year]
            #plot data
            plot_timeseries(data_for_plotting, paths_dict)

            ##CREATE USER INPUT TEXT NOW
            unique_datasets = data_for_plotting.Dataset.unique()
            user_input_options, choice_dict = print_user_input_text(unique_datasets, options_dict)

            combined_data_concordance_manual, user_input = manual_user_input_function(data_for_plotting, index_row, combined_data_concordance_manual, INDEX_COLS,choice_dict,options_dict)




###########UPTO HERE###########


















    # create_group_dashboard()
    # Make it easy for the user to create custom code for the dashbaord so that it can suit their groupings.

def print_user_input_text(unique_datasets, options_dict):
    #print to console
    user_input_options = []
    choice_dict = dict()
    return user_input_options, choice_dict
def plot_timeseries(data_for_plotting, paths_dict):
    return
def handle_group_dashbaord(group_concordance, group_data, paths_dict):
    # open and close dashboard created for this group. 
    return


def prepare_user_selection_options():
    #create a list of options
    options = ['Keep the dataset "{}" for all years that the chosen dataset is available', 'Keep the dataset "{}" for all consecutive years that the same combination of datasets is available','Keep the dataset "{}" only for that year', 'Delete the value for dataset "{}" for this year']
    #create an options dictwith keys rather than indexes to return the correct option. This will allow us to include more info in the options dict as well.
    options_dict = dict()
    options_dict['Keep_for_all_years'] = options[0]
    options_dict['Keep_for_all_consecutive_years'] = options[1]
    options_dict['Keep_for_this_year'] = options[2]
    options_dict['Delete'] = options[3]
    return options_dict

def TEMP_FIX_change_date_col_to_year(df):
    # %matplotlib qt
    #TEMP FIX START
    #NOTE MAKING THIS ONLY WORK FOR YEARLY DATA, AS IT WOULD BE COMPLICATED TO DO IT OTEHRWISE. lATER ON WE CAN TO IT COMPLETELY BY CHANGING LINES THAT ADD 1 TO THE DATE IN THE DATA SLECTION FUNCTIONS TO ADD ONE UNIT OF WHATEVER THE FREQUENCY IS. 
    #change dataframes date col to be just the year in that date eg. 2022-12-31 = 2022 by using string slicing
    df.date = df.date.apply(lambda x: x[:4]).astype(int)
    return df
def save_groups_to_tmp_folder(combined_data_concordance, combined_data, paths_dict,grouping_cols):
    # user can specify columns for which to group the data that will be input. Although only a single INDEX_ROW will be selected for at a time, the grouping will allow for a dashboard to be made for that group so the user can observe the selections in context of other datapoints in the same group
    # It is also generally useful because it will allow the suer to work on a group at a time.

    #split data into the groups and save them to a tmp folder paths_dict['tmp_selection_groups_folder']  in pickle form
    groups_concordance = combined_data_concordance.groupby(grouping_cols)
    groups_concordance_names_files = []

    groups_data = combined_data.groupby(grouping_cols)
    groups_data_names_files = []

    for group_name, group_concordance in groups_concordance:
        #concat group name to single string
        group_name = '_'.join(group_name)
        #save group concordance to tmp folder
        group_concordance.to_pickle(os.path.join(paths_dict['tmp_selection_groups_folder'],'group_concordance_{}.pkl'.format(group_name)))
        #add groupname to lsit
        groups_concordance_names_files.append(group_name)

    for group_name, group_data in groups_data:
        #concat group name to single string
        group_name = '_'.join(group_name)
        #save group concordance to tmp folder
        group_data.to_pickle(os.path.join(paths_dict['tmp_selection_groups_folder'],'group_data_{}.pkl'.format(group_name)))
        #add groupname to lsit
        groups_data_names_files.append(group_name)

    return groups_concordance_names_files, groups_data_names_files




def load_and_merge_groups_from_tmp_folder(groups_concordance_names_files, groups_data_names_files, paths_dict):
        
    #load groups from tmp folder
    groups_concordance = {}
    groups_data = {}
    for group_name in groups_concordance_names_files:
        groups_concordance[group_name] = pd.read_pickle(os.path.join(paths_dict['tmp_selection_groups_folder'],'group_concordance_{}.pkl'.format(group_name)))
    for group_name in groups_data_names_files:
        groups_data[group_name] = pd.read_pickle(os.path.join(paths_dict['tmp_selection_groups_folder'],'group_data_{}.pkl'.format(group_name)))

    #concat all groups concordance into one dataframe
    groups_concordance = pd.concat(groups_concordance)
    groups_data = pd.concat(groups_data)

    return groups_concordance, groups_data


#%%

def select_best_data_manual_by_group(combined_data_concordance_iterator,iterator,combined_data_concordance_manual,combined_data,duplicates_manual,INDEX_COLS,selection_set,sorting_cols,FILE_DATE_ID=''):
    

    ##################################

    #START MANUAL DATA SELECTION

    ##################################
    INDEX_COLS_no_year = INDEX_COLS.copy()#todo move these out of functions
    INDEX_COLS_no_year.remove('Date')
    #if Date is in sorting_cols then remove it
    if 'Date' in sorting_cols:
        sorting_cols.remove('Date')
    #loop through the unique combinations, plot a timeseries for each one and then ask the user what dataset they want to choose for that instance and then update the dataset column in combined_data for that row to be the chosen dataset
    # %matplotlib qt 
    import matplotlib
    matplotlib.use('TkAgg')#todo is this needed? espec if we save anbd load as png

    combined_data_concordance_manual.set_index(INDEX_COLS_no_year, inplace=True)
    duplicates_manual.set_index(INDEX_COLS_no_year, inplace=True)#todo is there some way to not have to do this? 

    # %matplotlib qt
    #TEMP FIX START
    #NOTE MAKING THIS ONLY WORK FOR YEARLY DATA, AS IT WOULD BE COMPLICATED TO DO IT OTEHRWISE. lATER ON WE CAN TO IT COMPLETELY BY CHANGING LINES THAT ADD 1 TO THE DATE IN THE DATA SLECTION FUNCTIONS TO ADD ONE UNIT OF WHATEVER THE FREQUENCY IS. 
    #change dataframes date col to be just the year in that date eg. 2022-12-31 = 2022 by using string slicing
    combined_data_concordance_manual.Date = combined_data_concordance_manual.Date.apply(lambda x: x[:4]).astype(int)
    combined_data.Date = combined_data.Date.apply(lambda x: x[:4]).astype(int)
    duplicates_manual.Date = duplicates_manual.Date.apply(lambda x: x[:4]).astype(int)#todo CHECK WHY WE ARE GETTING FLOAT YEARS

    #order data by date
    combined_data_concordance_manual = combined_data_concordance_manual.sort_values(by='Date')
    combined_data = combined_data.sort_values(by='Date')
    duplicates_manual = duplicates_manual.sort_values(by='Date')
    #TEMP FIX END
    #Create bad_index_rows as a empty df with the same columns as index_rows
    bad_index_rows = pd.DataFrame(columns=combined_data_concordance_iterator.columns)
    num_bad_index_rows = 0
    #create progresss csv so we can add lines to it as we go
    progress_csv = 'intermediate_data/data_selection/{}_progress.csv'.format(FILE_DATE_ID)

    ####################################
    #split data into groups based on the selection set so that the data selection process can be done for each group separately
    options = ['Keep the dataset "{}" for all years that the chosen dataset is available', 'Keep the dataset "{}" for all consecutive years that the same combination of datasets is available','Keep the dataset "{}" only for that year', 'Delete the value for dataset "{}" for this year']
    #create an options dictwith keys rather than indexes to return the correct option. This will allow us to include more info in the options dict as well.
    options_dict = dict()
    options_dict['Keep_for_all_years'] = options[0]
    options_dict['Keep_for_all_consecutive_years'] = options[1]
    options_dict['Keep_for_this_year'] = options[2]
    options_dict['Delete'] = options[3]

    #make a var called iterator_group and repalce nas with 'nan' so that we can group by it
    iterator_group = iterator.reset_index().copy()
    iterator_group = iterator_group.fillna('nan')
    iterator_group = iterator_group.groupby(selection_set)

    #create set of 2 one for plotting the current row and one for plotting the user options in text. 
    fig_row, ax_row = plt.subplots()
    fig_text, ax_text = plt.subplots()

    # combined_data_concordance_manual_group = combined_data_concordance_manual.groupby(selection_set)
    # combined_data_group = combined_data.groupby(selection_set)
    # duplicates_manual_group = duplicates_manual.groupby(selection_set)
    for group in iterator_group.groups:
        iterator_for_group = iterator_group.get_group(group)
        #now replace any 'nan' values with np.nan so that we can use the isna() function
        iterator_for_group = iterator_for_group.replace('nan',np.nan)
        #sort iterator_for_group by set of index columns so the way the data comes in is easy to understand
        iterator_for_group = iterator_for_group.sort_values(by=sorting_cols)
        #set index of iterator_for_group to be the index columns
        iterator_for_group.set_index(INDEX_COLS_no_year, inplace=True)

        #extract the data in iterator for group from combined data using the index
        combined_data_group = combined_data.loc[iterator_for_group.index].reset_index()

        plotly_dashboard(combined_data_group,group)

        user_input = ''
        for index_row in iterator_for_group.index:
            
            if 'Yearly' not in index_row:
                print('Skipping row {} as it is not yearly data'.format(index_row))
                continue

            #filter for only the current row in our duplicates_manual dataset by using the row as an index
            try:
                current_row_filter = duplicates_manual.loc[index_row]
            except KeyError:
                num_bad_index_rows += 1
                print('Bad unique combination {}: {}. Please check it to find out what the error was'.format(num_bad_index_rows,index_row))
                bad_index_rows = bad_index_rows.append(combined_data_concordance_iterator.loc[index_row])
                #if the row is not in the duplicates_manual dataset then continue to the next row
                continue
            
            #identify how many datasets there are for each year by looking at the Num_datapoints column
            #if there are rows where count is greater than 1 then we will plot and ask user to choose which dataset to use
            if (current_row_filter.Num_datapoints.max() > 1):# | (add_to_rows_to_select_manually_df):#if we ware adding to the rows to select manually df then we want to plot all of them because its important to check them even if none of them are duplicates
                #grab the data for this unique combination of columns from the combined_data df
                data_for_plotting = combined_data.loc[index_row]

                ##CREATE USER INPUT TEXT NOW
                unique_datasets = data_for_plotting.Dataset.unique()
                user_input_options, choice_dict = create_user_input_text(unique_datasets, options_dict)



                ##PLOT            
                # #plot the user input options in text on fig_text
                # fig_text = graph_user_input_options(user_input_options, fig_text, ax_text)

                # #plot the current row on fig_row
                # fig_row = graph_current_row(data_for_plotting, index_row, fig_row, ax_row)
                #WHY DONT WE PRINT THESE THINGS OUT,. ITS NOT WORKING. WE SHOULD THEN RUN THE CODE IN CMD SO THE TEXT OUTPUT IS CLEAR
                fig_row = graph_current_row_and_input_options(data_for_plotting, user_input_options,index_row, fig_row, ax_row)
                plt.ion()
                plt.draw()
                plt.pause(4)
                ##USER INPUT
                #ask user what dataset they want to choose for each year where a decision needs to be made, and then based on what they choose, update combined_data_concordance_manual 
                combined_data_concordance_manual, user_input = manual_user_input_function(data_for_plotting, index_row, combined_data_concordance_manual, INDEX_COLS,choice_dict,options_dict)
                #clear the figures so we can plot the next row
                fig_row.clf()
                fig_text.clf()
                # plt.pause(4)


                if user_input == 'quit':
                    break
                else:
                    print('Finished with unique combination: {}'.format(index_row))
            else:
                pass#print('Unique combination: {} did not have more than 1 dataset available so no manual decision was needed'.format(index_row))
            #save the progress to csv in case anything goes wrong. If you need to access it it should be formatted just like the combined_data_concordance_manual df at the end of this script
            combined_data_concordance_manual.to_csv(progress_csv, index=True)
        if user_input == 'quit':
            print('User input was quit on unique combination {}, so quitting the script and saving the progress to a csv'.format(index_row))
            break
        print('Finished with group: {}'.format(group))
    print('Finished with manual selection of datasets')

    #close all the figures
    plt.close('all')

    #TEMP FIX START #todo make this and the above one into a shorter bit or at least a function
    #convert date back to the last day of the year instead of year integer
    combined_data_concordance_manual.reset_index(inplace=True)
    combined_data_concordance_manual.Date = combined_data_concordance_manual.Date.apply(lambda x: str(x)+'-12-31')#TODO somehow Date becomes an index.Would be good to fix this or double check that it doesnt cause any problems
    combined_data.Date = combined_data.Date.apply(lambda x: str(x)+'-12-31')
    duplicates_manual.Date = duplicates_manual.Date.apply(lambda x: str(x)+'-12-31')
    #TEMP FIX END
    #save combined_data_concordance_manual now so we can use it later if we need to#todo make into pickle!
    combined_data_concordance_manual.to_csv('intermediate_data/data_selection/{}_data_selection_manual_backup.csv'.format(FILE_DATE_ID), index=False)
    duplicates_manual.to_csv('intermediate_data/data_selection/{}_duplicates_manual_backup.csv'.format(FILE_DATE_ID), index=True)
    #= pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual_duplicates.csv'.format(FILE_DATE_ID))
    # return combined_data_concordance_manual, duplicates_manual, bad_index_rows, num_bad_index_rows
