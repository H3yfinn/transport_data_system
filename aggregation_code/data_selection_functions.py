#%%
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import sys
from PIL import Image
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import itertools
import data_formatting_functions

import logging
logger = logging.getLogger(__name__)

#%%
def data_selection_handler(grouping_cols, combined_data_concordance, combined_data, paths_dict,datasets_to_always_use=[],highlighted_datasets=[],open_dashboard=False,default_user_input=None):


    ########PREPAREATION########
    # combined_data_concordance.set_index(paths_dict['INDEX_COLS_no_year'] , inplace=True)
    # combined_data.set_index(paths_dict['INDEX_COLS_no_year'] , inplace=True)#todo is there some way to not have to do this? 

    #order data by date
    combined_data = combined_data.sort_values(by='date')
    combined_data = combined_data.sort_values(by='date')

    user_input = ''

    groups_concordance_names_files, groups_data_names_files = save_groups_to_tmp_folder(combined_data_concordance, combined_data, paths_dict,grouping_cols)

    #save a progress pkl which will just be the combined_data_concordance. It will be updated as the user progresses through the selection process
    combined_data_concordance.to_pickle(paths_dict['selection_progress_pkl'])

    #remove combined_data and combined_data_concordance from memory
    del combined_data_concordance
    del combined_data

    rows_to_ignore = []# pd.read_pickle(paths_dict['rows_to_ignore_pkl'])
    ########SELECTION BEGINS########
    # iterate through every group
    for group_concordance_name_file, group_data_name_file in zip(groups_concordance_names_files, groups_data_names_files):
        #load the data
        group_concordance = pd.read_pickle(group_concordance_name_file)
        group_data = pd.read_pickle(group_data_name_file)
        
        #open dashboard
        if open_dashboard:
            open_plotly_group_dashboard(group_data_name_file, group_data, paths_dict)

        #set index to INDEX_COLS_no_year
        group_concordance.set_index(paths_dict['INDEX_COLS_no_year'] , inplace=True)
        group_data.set_index(paths_dict['INDEX_COLS_no_year'] , inplace=True)

        # iterate through each index col
        for index_row_no_year in group_concordance.index.unique():
            current_index_row_no_year = group_concordance.loc[index_row_no_year]
            #data_to_select_from will be used as the data from which the user will select the preferred data
            data_to_select_from = group_data.loc[index_row_no_year]

            if not check_for_multiple_datapoints(current_index_row_no_year):
                #this means there must be only one datapoint for all years. So we should just use that datapoint.
                group_concordance = set_concordance_for_single_datapoint(current_index_row_no_year, index_row_no_year,data_to_select_from, group_concordance,paths_dict)
                continue           

            #plot data and save plot as png to tmp folder
            plot_timeseries(data_to_select_from, paths_dict,index_row_no_year,highlighted_datasets)

            #now pass the group_concordance df to the user input handler which will run through each year and ask the user to select the preferred data. The preferred data will be recorded in the concordance df.
            group_concordance, user_input = manual_user_input_function(data_to_select_from, index_row_no_year, group_concordance, paths_dict, datasets_to_always_use, default_user_input)

            if user_input == 'quit':
                break
            else:
                logging.info('Finished with unique combination: {}'.format(index_row_no_year))
                #close timeseries
                
        save_progress(group_concordance,grouping_cols, paths_dict)

        if user_input == 'quit':
            logging.info('User input was quit on group %s, so quitting the script and saving the progress to a csv', group_concordance_name_file)

            break
        else:
            logging.info('Finished with group: {}'.format(group_concordance_name_file))
    logging.info('Finished with manual selection of datasets')


    # combined_data = load_and_merge_groups_from_tmp_folder(groups_data_names_files)#not sure if usefule

    #if the user_input was not quit then load progress and return it as the new combined_data_concordance
    if user_input != 'quit':
        combined_data_concordance = pd.read_pickle(paths_dict['selection_progress_pkl'])
    else:
        combined_data_concordance = 'quit'
    remove_groups_from_tmp_folder(groups_concordance_names_files, groups_data_names_files)

    return combined_data_concordance


def open_plotly_group_dashboard(group_data_name_file, group_data, paths_dict):
    #plot a dashboard using plotly and open it in the user's default browser

    #create an empty figure in the user's default browser. we will add subplots to this figure
    fig_dash = go.Figure()
    #make a shape for the set of subplots that will be as close to a square as possible
    shape = (int(np.ceil(np.sqrt(len(group_data.measure.unique())))), int(np.ceil(len(group_data.measure.unique())/np.ceil(np.sqrt(len(group_data.measure.unique()))))))
    #specify subplots using shape
    fig_dash = make_subplots(rows=shape[0], cols=shape[1], shared_xaxes=True, subplot_titles=group_data.measure.unique())
    #create a combination of every possible row and col for the subplots
    row_col_combinations = list(itertools.product(range(1,shape[0]+1), range(1,shape[1]+1)))
    i=0
    for measure in group_data.measure.unique():
        #get data for this measure
        measure_data = group_data.loc[group_data.measure == measure]
        row = row_col_combinations[i][0]
        col = row_col_combinations[i][1]
        i+=1

        for dataset in group_data['dataset'].unique():
            #plot data for this measure and this daTASET
            fig_dash.add_trace(go.Scatter(x=measure_data.loc[measure_data.dataset == dataset].date, y=measure_data.loc[measure_data.dataset == dataset].value, name=dataset, mode='lines+markers'), row=row, col=col)

    group_name = group_data_name_file.split('/')[-1].split('.')[0]
    fig_dash.update_layout(title_text=group_name)
    #save the figure as temp html file and open it in the user's default browser
    fig_dash.write_html(paths_dict['plotting_paths']['selection_dashboard'] + '/{}.png'.format(paths_dict['FILE_DATE_ID']), auto_open=True)
    return

def check_for_multiple_datapoints(current_index_row_no_year):
#check if there are multiple datapoints for this index row
    if type(current_index_row_no_year) == pd.core.series.Series:#can also be identifyed by len(shape) == 1
        #occurs if there is only one row in the df
        return False 
    elif all(current_index_row_no_year.num_datapoints == 1):
        #if all num_datapoints are 1 then skip
        return False
    else:
       return True

def set_concordance_for_single_datapoint(current_index_row_no_year, index_row_no_year,data_to_select_from, group_concordance,paths_dict):
    #for every year in the the current index row, set the concordances dataset, comment and value to the dataset and value for that year in data_to_select_from

    if type(current_index_row_no_year) == pd.core.series.Series:#can also be identifyed by len(shape) == 1 
        #occurs if there is only one row in the df, aka it is formatted as a series
        if type(data_to_select_from) == pd.core.series.Series:
            #this one is weird. We will get data_to_select_from as a series or a df. If it is a series then its len(shape) == 1 and we need to get the vlaue using .values, if it is a df then we need to use .values[0]
            group_concordance.loc[index_row_no_year, 'value'] = data_to_select_from.value
            group_concordance.loc[index_row_no_year, 'dataset'] = data_to_select_from.dataset
            group_concordance.loc[index_row_no_year, 'comment'] = data_to_select_from.comment
            group_concordance.loc[index_row_no_year, 'dataset_selection_method'] = 'single_datapoint'
        elif type(data_to_select_from) == pd.core.frame.DataFrame:
            group_concordance.loc[index_row_no_year, 'value'] = data_to_select_from.value[0]
            group_concordance.loc[index_row_no_year, 'dataset'] = data_to_select_from.dataset[0]
            group_concordance.loc[index_row_no_year, 'comment'] = data_to_select_from.comment[0]
            group_concordance.loc[index_row_no_year, 'dataset_selection_method'] = 'single_datapoint'
        else:
            logging.error(f'ERROR: not a series or df: {data_to_select_from}')
            raise ValueError
        
    elif type(data_to_select_from) == pd.core.frame.DataFrame:
        #if all num_datapoints are 1 but there are multiple years of data we will have a df
        group_concordance = group_concordance.reset_index().set_index(paths_dict['INDEX_COLS'])
        data_to_select_from = data_to_select_from.reset_index().set_index(paths_dict['INDEX_COLS'])
        for rows_to_change in data_to_select_from.index:
            group_concordance.loc[rows_to_change, 'value'] = data_to_select_from.loc[rows_to_change,'value']
            group_concordance.loc[rows_to_change, 'dataset'] = data_to_select_from.loc[rows_to_change,'dataset']
            group_concordance.loc[rows_to_change, 'comment'] = data_to_select_from.loc[rows_to_change,'comment']
            group_concordance.loc[rows_to_change, 'dataset_selection_method'] = 'single_datapoint'
    else:
        logging.error(f'ERROR: not a series or df: {data_to_select_from}')
        raise ValueError

    group_concordance = group_concordance.reset_index().set_index(paths_dict['INDEX_COLS_no_year'])

    return group_concordance

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
        group_name = os.path.join(paths_dict['selection_groups_folder'],'group_concordance_{}.pkl'.format('_'.join(group_name)))
        #save group concordance to tmp folder
        group_concordance.to_pickle(group_name)
        #add groupname to lsit
        groups_concordance_names_files.append(group_name)

    for group_name, group_data in groups_data:
        group_name = os.path.join(paths_dict['selection_groups_folder'],'group_data_{}.pkl'.format('_'.join(group_name)))
        #save group concordance to tmp folder
        group_data.to_pickle(group_name)
        #add groupname to lsit
        groups_data_names_files.append(group_name)

    return groups_concordance_names_files, groups_data_names_files

def load_and_merge_groups_from_tmp_folder(groups_data_names_files):#groups_concordance_names_files
        
    #load groups from tmp folder
    # groups_concordance = {}
    groups_data = {}
    # for group_name in groups_concordance_names_files:
    #     groups_concordance[group_name] = pd.read_pickle(group_name)
    for group_name in groups_data_names_files:
        groups_data[group_name] = pd.read_pickle(group_name)

    # #concat all groups concordance into one dataframe
    # combined_data_concordance = pd.concat(groups_concordance)
    combined_data = pd.concat(groups_data)

    return combined_data

def remove_groups_from_tmp_folder(groups_concordance_names_files, groups_data_names_files):
    #remove groups from tmp folder
    for group_name in groups_concordance_names_files:
        os.remove(group_name)
    for group_name in groups_data_names_files:
        os.remove(group_name)

def save_progress(group_concordance,grouping_cols, paths_dict):
    #load in progress pickle and add this group to it
    progress = pd.read_pickle(paths_dict['selection_progress_pkl'])

    group_concordance = group_concordance.reset_index()
    group_index = group_concordance[grouping_cols].iloc[0]

    #remove the rows which match group index from the progress df by using group_index.items() to get a list of tuples of the group_index keys and values
    group_index_keys = [value[0] for value in group_index.items()]
    group_index_values = [value[1] for value in group_index.items()]
    progress = progress[~progress[group_index_keys].isin(group_index_values).all(axis=1)]

    #add the group_concordance to the progress df
    progress = pd.concat([progress, group_concordance])
    progress.to_pickle(paths_dict['selection_progress_pkl'])

    #remove progress from memory
    del progress

def print_user_input_options(choice_dict):
    """Print the options for the user to choose from. The options are the datasets that are available for the given year."""
    logging.info('When asked for user input, supply a number for one of these options, if it is applicable:\n\n') 
    for value in choice_dict.values():
        
        logging.info(value[2])

def ask_and_parse_user_input(year, choice_dict):
    """If the user enters an invalid number, the function will print an error message and prompt the user to try again. If the user enters the same invalid number twice, the function will exit and save the user's progress. If the user enters a valid number, the function will check if the number is in a dictionary of choices, and if it is, the function will return True. If the number is not in the dictionary, the function will return False."""
    input_correct = False
    user_input = None

    print_user_input_options(choice_dict)

    valid_options = list(choice_dict.keys())
    user_input_question = '\nFor the year {} please choose a number from the options provided: {}'.format(year,valid_options)
    while input_correct == False:
        try:    
            user_input = input(user_input_question)
            user_input = int(user_input)
        except ValueError:
            user_input_question += 'Please enter a valid number from the options above. If you enter the same number again the program will exit, but your progress will be saved'
            print('Please enter a valid number from the options above. If you enter the same number again the program will exit, but your progress will be saved')
            old_user_input = user_input
            #create a try except to catch the error if the user enters the same number again
            try:
                user_input_question += '\nFor the year {} please choose a number from the options above: {}'.format(year,valid_options)
                user_input = input(user_input_question)
                user_input = int(user_input)
            except ValueError:
                if user_input == old_user_input:
                    logging.info('Exiting function and saving progress')
                    # sys.exit()#used for test
                    return 'quit'
                else:
                    logging.info('You entered a different value but not the right value. Please enter a valid number from the options above')
            input_correct = False
            continue#jump back to beginning of while loop
        if user_input == 0:
            #skip this year
            logging.info('Your input was {} which is a valid number from the options above.\nYear {} skipped'.format(user_input,year))
            input_correct = True
        elif user_input =='b':
            #go back one year
            logging.info('Your input was {} which is a valid number from the options above.\nGoing back one year'.format(user_input))
            input_correct = True
        elif user_input in choice_dict.keys():
            logging.info('Your input was {} which is a valid number from the options'.format(user_input))
            input_correct = True
        else:
            #jsut in case the user enters a number that is not in the dictionary, send them back to the start of the while loop
            logging.info('You entered an invalid number. Please enter a valid number from the options above')
            input_correct = False
    return user_input


def skip_year(group_concordance, year, index_row_no_year, paths_dict):
    #skip this year but set the dataset_selection_method for this year and index row to manual so that we know that this year was skipped
    group_concordance = group_concordance.reset_index().set_index(paths_dict['INDEX_COLS'])
    index_row_with_date = (year, *index_row_no_year)
    group_concordance.loc[index_row_with_date, 'dataset_selection_method'] = 'skipped'
    logging.info('Skipping year {} for index row {} and index row with date {}'.format(year, index_row_no_year, index_row_with_date))
    logging.info('group_concordance.loc[index_row_with_date, dataset_selection_method] = {}'.format(group_concordance.loc[index_row_with_date, 'dataset_selection_method']))
    group_concordance = group_concordance.reset_index().set_index(paths_dict['INDEX_COLS_no_year'])
    return group_concordance
   
def create_user_choice_dict(unique_datasets):

    """create options for the user to choose by looping through the options list and the dataset list"""

    options = ['Keep the dataset "{}" for all years that the chosen dataset is available', 'Keep the dataset "{}" for all consecutive years that the same combination of datasets is available','Keep the dataset "{}" only for that year']
    #create an options dictwith keys rather than indexes to return the correct option. This will allow us to include more info in the options dict as well.
    options_dict = dict()
    options_dict['Keep_for_all_years'] = options[0]
    options_dict['Keep_for_all_consecutive_years'] = options[1]
    options_dict['Keep_for_this_year'] = options[2]

    ###############################################################

    i=1
    choice_dict = {}
    choice_dict[0] = ['skip',None,'\n0: Skip this year']
    for option_key,option_text in options_dict.items():
        for dataset in unique_datasets:
            #add the option, dataset and options text to a dictionary so that we can refer to it with the number the user inputs later
            choice_dict[i] = [option_key, dataset, '\n{}: {}'.format(i, option_text.format(dataset))]
            i+=1
    #add option to go back one year
    choice_dict['b'] = ['back',None,"\nb: Revert previous year's selection"]

    return choice_dict

def find_default_dataset(datasets_to_always_use, unique_datasets, choice_dict):
    #find if we have set a default dataset for this year

    #ask the user to input a number and then check that it is a valid number. Use a fucntion for this to reduce lines in this function
    if len(datasets_to_always_use) > 0:
        #check if any datasets in  unique_datasets is in the list of datasets to always use, if so, if there is only one then find that dataset in the choice_dict and set the user_input to that number
        datasets_to_always_use_in_unique_datasets = [dataset for dataset in unique_datasets if dataset in datasets_to_always_use]
        if len(datasets_to_always_use_in_unique_datasets) == 1:
            #find the dataset in the choice_dict and set the user_input to that number
            for choice_dict_key, choice_dict_value in choice_dict.items():
                if choice_dict_value[1] == datasets_to_always_use_in_unique_datasets[0] and choice_dict_value[0] == 'Keep_for_this_year':
                    user_input = choice_dict_key
                    logging.info('Default dataset found for this year: {}'.format(datasets_to_always_use_in_unique_datasets[0]))
                    return user_input
        else:
            return None
    else:
        return None
    
def manual_user_input_function(data_to_select_from, index_row_no_year,  group_concordance, paths_dict,datasets_to_always_use, default_user_input): 
    timeseries_png =None
    user_input = None
    years_to_ignore = []
    #iterate through each year using an index i and a while loop, so that we can skip years and even go back to previous years
    year_index_i = 0
    while year_index_i < len(data_to_select_from.date.unique()):
        year = data_to_select_from.date.unique()[year_index_i]
        if year in years_to_ignore:
            year_index_i += 1
            continue


        #filter for only the current year 
        data_to_select_from_for_this_year = data_to_select_from[data_to_select_from.date == year]

        ##DETERMINE VALID USER INPUTS FOR YEAR
        unique_datasets = data_to_select_from_for_this_year.dataset.unique()
        #create a dictionary of options for the user to choose from
        choice_dict = create_user_choice_dict(unique_datasets)
        #if year is the first then remove the option to go back one year
        if year_index_i == 0:
            choice_dict.pop('b')

        logging.info('\nFor unique combination: {}'.format(index_row_no_year))

        user_input = find_default_dataset(datasets_to_always_use, unique_datasets, choice_dict)
        if user_input is None:
            #TODO TEMP FIX:
            #SET USER INPUT TO 1 FOR NOW
            if default_user_input is not None:
                user_input = default_user_input
            else:
            #TEMP FIX END

                #if not already, open time series plot for this unique combination
                #open the timeseries png in separate window
                if timeseries_png is None:
                    timeseries_png = Image.open(os.path.join(paths_dict['plotting_paths']['selection_timeseries'], paths_dict['FILE_DATE_ID'] + '.png'))
                    # timeseries_png.show()
                    timeseries_png#think this will allow it to open in linux
                    
                user_input = ask_and_parse_user_input(year, choice_dict)
                
        if user_input == 'quit':
            return group_concordance, user_input
            #sys.exit()#for testing
        elif user_input == 0:
            group_concordance = skip_year(group_concordance, year, index_row_no_year, paths_dict)
            continue
        elif user_input == 'back':
            #go back one year
            year_index_i -= 1
            continue
        else:
            group_concordance,years_to_ignore = apply_user_input_to_data(user_input, choice_dict, group_concordance,data_to_select_from, data_to_select_from_for_this_year, paths_dict,years_to_ignore)
            #go to next year
            year_index_i += 1

    #close the timeseries png
    if timeseries_png is not None:
        timeseries_png.close()
    return group_concordance, user_input

def apply_selection_to_concordance(data_to_change, selected_dataset, group_concordance,paths_dict,years_to_ignore):
    #make date a part of the index
    group_concordance = group_concordance.reset_index().set_index(paths_dict['INDEX_COLS'])

    #and then find those rows in group_concordance 
    for rows_to_change in data_to_change.index:
        #update the dataset column to be the chosen dataset
        group_concordance.loc[rows_to_change, 'dataset'] = selected_dataset
        #set dataset_selection_method to Manual too
        group_concordance.loc[rows_to_change,'dataset_selection_method'] = 'Manual'
        
        #set value using the value in data_to_change
        group_concordance.loc[rows_to_change,'value'] = data_to_change.loc[rows_to_change, 'value']
        group_concordance.loc[rows_to_change,'comment'] = data_to_change.loc[rows_to_change, 'comment']
        
        # #set number of Num_datapoints to the same value as Num_datapoints in data_to_change
        # group_concordance.loc[rows_to_change, 'Num_datapoints'] = data_to_change.loc[rows_to_change, 'Count']
        
    #reset the index so that date is a column again
    group_concordance = group_concordance.reset_index().set_index(paths_dict['INDEX_COLS_no_year'])

    #Finally, since we may have set the data for years that will come up in the next loop, we will add these to a list of years ahead that have already been set (years_to_ignore)
    years_to_ignore = np.append(years_to_ignore, data_to_change.reset_index().date.unique())
    years_to_ignore = years_to_ignore.astype(int)
    years_to_ignore = np.unique(years_to_ignore)

    return group_concordance,years_to_ignore

def apply_user_input_to_data(user_input, choice_dict, group_concordance, data_to_select_from, data_to_select_from_for_this_year, paths_dict,years_to_ignore):

    """Find the input in the choiuces dictionary and then find the option and dataset that the user chose then apply it"""

    #find the option and dataset that the user chose
    option_key = choice_dict[user_input][0]
    selected_dataset = choice_dict[user_input][1]
    
    #which matches what the user wants to change. Then by using that as an index, find the matching rows in our concordance datyaset, set the dataset, data_selection method and value columns.
    if option_key == 'Keep_for_all_consecutive_years':
        data_to_change = user_input_keep_for_all_consecutive_years(selected_dataset, data_to_select_from, data_to_select_from_for_this_year, paths_dict['INDEX_COLS'])
        group_concordance,years_to_ignore = apply_selection_to_concordance(data_to_change, selected_dataset, group_concordance, paths_dict,years_to_ignore)

    elif option_key == 'Keep_for_all_years':
        # 'Keep the dataset "{}" for all years that the chosen dataset is available'
        data_to_change = user_input_keep_for_all_years(selected_dataset, data_to_select_from, data_to_select_from_for_this_year, paths_dict)
        group_concordance,years_to_ignore = apply_selection_to_concordance(data_to_change, selected_dataset, group_concordance, paths_dict,years_to_ignore)

    elif option_key == 'Keep_for_this_year':
        #'Keep the dataset "{}" only for that year']
        data_to_change = user_input_keep_for_this_year(selected_dataset, data_to_select_from, data_to_select_from_for_this_year, paths_dict)
        group_concordance,years_to_ignore = apply_selection_to_concordance(data_to_change, selected_dataset, group_concordance, paths_dict,years_to_ignore)
    else:
        logging.error("ERROR: option_key not found, please check the code")
        sys.exit()
    
    return group_concordance,years_to_ignore

def user_input_keep_for_all_consecutive_years(selected_dataset, data_to_select_from, data_to_select_from_for_this_year, paths_dict):
    #'Keep the dataset {} for all consecutive years that the same combination of datasets is available'
    
    #use datasets column to find all years with the same set of datasets. 
    set_of_datasets_in_this_year = data_to_select_from_for_this_year.datasets[0]
    data_to_change = data_to_select_from.copy()

    #filter for only the chosen dataset in the dataset column
    data_to_change = data_to_change[data_to_change.dataset == selected_dataset]
    #filter for years after the current year
    data_to_change = data_to_change[data_to_change.date >= data_to_select_from_for_this_year.date[0]]
            
    #filter for only the rows where the set of datasets is the same as the set of datasets for the current year.
    data_to_change = data_to_change[data_to_change.datasets.isin([set_of_datasets_in_this_year])]
            
    #keep only years that are consecutive from the current year
    #sort by year and reset the index
    data_to_change = data_to_change.reset_index().sort_values(by = ['date'])           
    # create a new column that represents the difference between each year and the previous year
    data_to_change['Year_diff'] = data_to_change['date'].diff()
    # shift the values in the year column down by one row
    data_to_change['Year_shifted'] = data_to_change['date'].shift()
    #find minimum Year where Year_diff is not 1 and the Year is not year
    min_year = data_to_change.loc[(data_to_change['Year_diff'] != 1) & (data_to_change['date'] != data_to_select_from_for_this_year.date[0]), 'date'].min()
    #if year is nan then set it to the max year +1 since that means all years are applicable.
    if pd.isna(min_year):
        min_year = data_to_change['date'].astype(int).max() + 1
        logging.info(min_year, ' min_year')
    #filter for only the rows where the year is less than the min_year
    data_to_change = data_to_change.loc[data_to_change['date'] < min_year]
    # drop the columns we created
    data_to_change = data_to_change.drop(columns = ['Year_diff', 'Year_shifted'])

    #make Year a part of the index
    data_to_change = data_to_change.set_index(paths_dict['INDEX_COLS'])

    return data_to_change

def user_input_keep_for_all_years(selected_dataset, data_to_select_from, data_to_select_from_for_this_year, paths_dict):
    #find the rows in data_to_select_from where the datasets are the same as for the chosen dataset
    data_to_change = data_to_select_from[data_to_select_from.dataset == selected_dataset]

    #filter for years after the current year
    data_to_change = data_to_change[data_to_change.date >= data_to_select_from_for_this_year.date[0]]
    #make Year a part of the index
    data_to_change = data_to_change.reset_index().set_index(paths_dict['INDEX_COLS'])
    return data_to_change

def user_input_keep_for_this_year(selected_dataset, data_to_select_from, data_to_select_from_for_this_year, paths_dict):
    #we can just use data_to_select_from_for_this_year here
    data_to_change = data_to_select_from_for_this_year[data_to_select_from_for_this_year.dataset == selected_dataset]
    
    #make Year a part of the index
    data_to_change = data_to_change.reset_index().set_index(paths_dict['INDEX_COLS'])
    return data_to_change


def plot_timeseries(data_to_select_from, paths_dict,index_row_no_year, highlighted_datasets):
        #graph data in a line graph using the index from the unique comb ination to graph data out of the combined dataframe
        ##PLOT
        fig, ax = plt.subplots()
               
        #loop through the datasets and plot each one as a line
        #but because we might end up with two lines on top of each other, make each successive line a little bit more transparent and thicker
        alpha_start = 1
        alpha_step = 1 / (len(data_to_select_from.dataset.unique())+5)
        thickness_start = 3 * (len(data_to_select_from.dataset.unique())+2)
        thickness_step = 3
        for dataset in data_to_select_from.dataset.unique():
                # pick color
                color = next(ax._get_lines.prop_cycler)['color']

                #filter for only the current dataset
                dataset_data = data_to_select_from[data_to_select_from.dataset == dataset]
                #join 'comment' columns to 'dataset' column if they're not nan
                dataset_data['dataset'] = dataset_data.apply(lambda row: row['dataset'] + '-' + row['comment'] if row['comment'] != 'no_comment' else row['dataset'], axis=1)

                #plot the data
                ax.plot(dataset_data.date, dataset_data.value, label=dataset, color=color)

                if dataset in highlighted_datasets:
                    #add a highlight to the line
                    shadow_effect = [pe.Stroke(linewidth=thickness_start+(3*thickness_step), foreground='yellow'), pe.Normal()]
                    ax.lines[-1].set_path_effects(shadow_effect)

                #make the line a little bit more transparent
                alpha_start -= alpha_step
                ax.lines[-1].set_alpha(alpha_start)
                # #make the line a little bit thicker
                # thickness_start -= thickness_step
                # ax.lines[-1].set_linewidth(thickness_start)

                #plot markers for the datapoints as well
                ax.plot(dataset_data.date, dataset_data.value, 'o', label=dataset, color=color)
                #make the markers a little bit more transparent
                ax.lines[-1].set_alpha(alpha_start)
                #make the markers a little bit thicker
                thickness_start -= thickness_step
                ax.lines[-1].set_markersize(thickness_start)

        #create a 0 line so its clear if the value is 0 or not
        ax.axhline(y=0, color='black', linestyle='--')

        #finalise the plot by adding a legend, titles and subtitles and showing it in advance of user input
        ax.legend()
        #make title all the values in index_row_no_year, separated by commas
        ax.set_title('{}'.format(', '.join([str(x) for x in index_row_no_year])))
        #grab unit from the index of the data_to_select_from dataframe
        ax.set_ylabel('value {}'.format(data_to_select_from.index.get_level_values('unit')[0]))
        #set background color to white in case someone has a dark theme
        ax.set_facecolor('white')

        #make the graph fig a bit bigger 
        fig.set_size_inches(10, 10)
        
        fig.savefig(paths_dict['plotting_paths']['selection_timeseries'] + '/{}.png'.format(paths_dict['FILE_DATE_ID']))
        plt.close()#although we arent opening any plots, it seems we need to close them, at least when using jupytyer interactive
        

def close_timeseries(timeseries_png):
    #close the timeseries png
    timeseries_png.close()
    plt.close()


def prepare_data_for_selection(combined_data_concordance,combined_data,paths_dict,sorting_cols):
    """This function will take in the combined data and combined data concordance dataframes and prepare them for the manual selection process. It will filter the data to only include data from the years we are interested in. 
    #TODO this previously would' remove any duplicate data for the 8th edition transport model carbon neutrality scenario.'. Need to replace that. somewhere else"""#TODO double check that that is true, it came from ai

    combined_data_concordance = data_formatting_functions.ensure_column_types_are_correct(combined_data_concordance)
    combined_data = data_formatting_functions.ensure_column_types_are_correct(combined_data)

    data_formatting_functions.test_identify_erroneous_duplicates(combined_data,paths_dict)

    combined_data_concordance = identify_duplicated_datapoints_to_select_for(combined_data,combined_data_concordance, paths_dict['INDEX_COLS'])

    combined_data_concordance,combined_data = data_formatting_functions.filter_for_years_of_interest(combined_data_concordance,combined_data,paths_dict)

    combined_data_concordance['dataset'] = None
    combined_data_concordance['value'] = None
    combined_data_concordance['dataset_selection_method'] = None
    combined_data_concordance['comment'] = None

    #sort data
    combined_data_concordance = combined_data_concordance.sort_values(sorting_cols)
    combined_data = combined_data.sort_values(sorting_cols)
    
    return combined_data_concordance, combined_data


def test_identify_duplicated_datapoints_to_select_for(combined_data_concordance,new_combined_data_concordance):
    #Check the new concordance has the same amount of rows as the combined_data_concordance and the same index, and two extra columns called 'potential_datapoints' and 'num_datapoints'
    if len(new_combined_data_concordance) != len(combined_data_concordance):
        logging.error('The new combined data concordance has a different number of rows than the old one. This should not happen')
        raise Exception('The new combined data concordance has a different number of rows than the old one. This should not happen')
    if not new_combined_data_concordance.index.equals(combined_data_concordance.index):
        logging.error('The new combined data concordance has a different index than the old one. This should not happen')
        raise Exception('The new combined data concordance has a different index than the old one. This should not happen')
    if not 'potential_datapoints' in new_combined_data_concordance.columns:
        logging.error('The new combined data concordance does not have the column potential_datapoints. This should not happen')
        raise Exception('The new combined data concordance does not have the column potential_datapoints. This should not happen')
    if not 'num_datapoints' in new_combined_data_concordance.columns:
        logging.error('The new combined data concordance does not have the column num_datapoints. This should not happen')
        raise Exception('The new combined data concordance does not have the column num_datapoints. This should not happen')
    #CHECK NUMBER OF cols
    if len(new_combined_data_concordance.columns) != len(combined_data_concordance.columns) + 2:
        logging.error('The new combined data concordance has the wrong number of columns. This should not happen')
        raise Exception('The new combined data concordance has the wrong number of columns. This should not happen')
    return

def identify_duplicated_datapoints_to_select_for(combined_data,combined_data_concordance, INDEX_COLS):
    """ 
    This function will take in the combined dataset and create a df which essentially summarises the set of datapoints we ahve for each unique index row (including the date). It will create a list of the datasets for which data is available, a count of those datasets as well as the option to conisder the sum of the vlaue, which allows the user to accruately understand if any values for the index row have changed, since the sum of vlaues would very likely have changed too. This is utilised in the import_previous_runs_progress_to_manual() and pickup_incomplete_manual_progress() functions, if the user includes that column during that part of the process.
    """
    #todo, since we are importing deleted datasets later, we should consider wehther that will rem,ove any dupclaites?
    ###########################################################
    #create dataframe of duplicates with list of datasets and count of datasets
    duplicates = combined_data.copy()
    duplicates =  duplicates.groupby(INDEX_COLS,dropna=False).agg({'dataset': lambda x: list(x), 'value': lambda x: sum(x.dropna())}).reset_index()

    #make sure the lists are sorted so that the order is consistent
    duplicates['potential_datapoints'] = duplicates['dataset'].apply(lambda x: sorted(x))
    #create count column
    duplicates['num_datapoints'] = duplicates['dataset'].apply(lambda x: len(x))
    
    #drop dataset column and value column
    duplicates.drop(columns=['dataset','value'],    inplace=True)
    
    #join onto combined_data_concordance
    new_combined_data_concordance = duplicates.merge(combined_data_concordance, on=INDEX_COLS, how='left')#todo does this result in what we'd like? are there any issues with not using .copy)( on anythiing
    # new_combined_data = duplicates.merge(combined_data, on=INDEX_COLS, how='left')#todo, do we need to use [['datasets', 'num_datapoints']] here?

    #TODO this below didnt seem useful, but maybe it is?
    # test_identify_duplicated_datapoints_to_select_for(combined_data,combined_data_concordance,new_combined_data_concordance,INDEX_COLS)

    return new_combined_data_concordance#, new_combined_data


# def import_previous_selections(previous_selections, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,paths_dict,update_skipped_rows=False):
#     """
#     #todo we should make this way way more ismple by changing the inhernet way we do it
#     Please note this is quite a complicated process. 
#     #WARNING THERES POTENTIALLY AN ISSUE WHEN YOU UPdate THE INPUT DATA SO IT INCLUDES ANOTHER DATAPOINT AND YOU LOAD THIS IN, THE MANUAL CONCORDANCE WILL END UP AHVING TWO ROWS FOR THE SAME DATAPOINT? #cHECK IT LATER
#     """
#     #IMPORT PREVIOUS SELECTIONS
#     #Previous_selections can be the previous_combined_data_concordance_manual or the progress_csv depending on if the user wants to import completed manual selections or the progress of some manual selections
   
#     #This allows the user to import manual data selections from perveious runs to avoid having to do it again (can replace any rows where the dataset_selection_method is na with where they are Manual in the imported csv)
#     ##########################################################
#     #We will make sure there are no index rows in the previous dataframes that are not in the current dataframes
#     #first the duplicates
#     previous_duplicates_manual.set_index(paths_dict['INDEX_COLS'], inplace=True)
#     duplicates_manual.set_index(paths_dict['INDEX_COLS'], inplace=True)
#     #remove the rows that are in the previous duplicates but not in the current duplicates
#     index_diff = previous_duplicates_manual.index.difference(duplicates_manual.index)
#     previous_duplicates_manual.drop(index_diff, inplace=True)
#     #reset the index
#     previous_duplicates_manual.reset_index(inplace=True)
#     duplicates_manual.reset_index(inplace=True)

#     #now for previous_selections and combined_data_concordance_manual
#     previous_selections.set_index(paths_dict['INDEX_COLS'], inplace=True)
#     combined_data_concordance_manual.set_index(paths_dict['INDEX_COLS'], inplace=True)
#     #remove the rows that are in the previous duplicates but not in the current duplicates
#     index_diff = previous_selections.index.difference(combined_data_concordance_manual.index)
#     previous_selections.drop(index_diff, inplace=True)
#     #reset the index
#     previous_selections.reset_index(inplace=True)
#     combined_data_concordance_manual.reset_index(inplace=True)
#     ##########################################################

#     ##There is a chance that some index_rows have had new data added to them, so we will compare the previous duplicates index_rows to the current duplicates and see where thier values are different, make sure that we iterate over them in the manual data selection process
#     #so find different rows in the duplicates:
#     #first make the datasets col a string so it can be compared
#     a = previous_duplicates_manual.copy()
#     a.datasets = a.datasets.astype(str)
#     b = duplicates_manual.copy()
#     b.datasets = b.datasets.astype(str)
#     # a.set_index(INDEX_COLS_no_year,inplace=True)
#     # b.set_index(INDEX_COLS_no_year,inplace=True)
#     duplicates_diff = pd.concat([b, a])
#     #set Vlaue to int, because we want to see if the value is the same or not and if float there might be floating point errors (computer science thing)
#     duplicates_diff.value = duplicates_diff.value.astype(int)
#     #drop duplicates
#     duplicates_diff = duplicates_diff.drop_duplicates(keep=False)
#     ##########################################################

#     ##Update the iterator: We will remove rows where the user doesnt need to select a dataset. This will be done using the previous combined_data_concordance_manual.
#     rows_to_skip = previous_selections.copy()
#     rows_to_skip.set_index(paths_dict['INDEX_COLS_no_year'], inplace=True)
#     #we have to state wat rows we want to remove rather than keep because there will be some in the iterator that are not in the previous_selections df, and we will want to keep them.

#     #First remove rows that are in duplicates diff as we want to make sure the user selects for them since they have some detail which is different to what it was before. We do this using index_no_year to cover all the rows that have the same index but different years
#     duplicates_diff.set_index(paths_dict['INDEX_COLS_no_year'], inplace=True)
#     rows_to_skip = rows_to_skip[~rows_to_skip.index.isin(duplicates_diff.index)]

#     #In case we are using the progress csv we will only remove rows where all years data has been selected for that index row. This will cover the way prvious_combined_data_concordance_manual df is formatted as well 
#     #first remove where num_datapoints is na or 0. 
#     rows_to_skip = rows_to_skip[~((rows_to_skip.num_datapoints.isna()) | (rows_to_skip.num_datapoints==0))]
#     #now find rows where there is data but the user hasnt selected a dataset for it. We will remove these rows from rows_to_skip, as we want to make sure the user selects for them (in the future this may cuase issues if we add more selection methods to the previous selections) #note this currently only occurs in the case where an error occurs during sleection
#     rows_to_remove_from_rows_to_skip = rows_to_skip[rows_to_skip.dataset_selection_method.isna()]

#     if update_skipped_rows:
#         #if we are updating the skipped rows then we will also remove from rows to skip the rows where the value and dataset is NaN where the selection method is manual. This will be the case where the user has skipped selection so the selection emthod is manual but there are no values or datasets selected
#         rows_to_remove_from_rows_to_skip2 = rows_to_skip[(rows_to_skip.value.isna()) & (rows_to_skip.dataset.isna()) & (rows_to_skip.dataset_selection_method=='Manual')]

#     #now remove those rows from rows_to_skip using index_no_year. This will remove any cases where an index row has one or two datapoints that werent chosen, but the rest were.
#     rows_to_skip = rows_to_skip[~rows_to_skip.index.isin(rows_to_remove_from_rows_to_skip.index)]

#     if update_skipped_rows:
#         rows_to_skip = rows_to_skip[~rows_to_skip.index.isin(rows_to_remove_from_rows_to_skip2.index)]

#     #KEEP only rows we dont want to skip in the iterator by finding the index rows in both dfs 
#     iterator = iterator[~iterator.index.isin(rows_to_skip.index)]

#     ###############
#     ##And now update the combined_data_concordance_manual:
#     #we can just add in rows from rows_to_skip to combined_data_concordance_manual, as well as the rows from duplicates_diff, as these are the rows that are different between old and new. But make sure to remove those rows from combined_data_concordance_manual first, as we dont want to have duplicates.
#     #first set indexes to Index_col (with year)
#     rows_to_skip.reset_index(inplace=True)
#     rows_to_skip.set_index(paths_dict['INDEX_COLS'], inplace=True)
#     combined_data_concordance_manual.set_index(paths_dict['INDEX_COLS'], inplace=True)
#     duplicates_diff.reset_index(inplace=True)
#     duplicates_diff.set_index(paths_dict['INDEX_COLS'], inplace=True)

#     #remove the rows from combined_data_concordance_manual
#     combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(rows_to_skip.index)]
#     combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(duplicates_diff.index)]

#     #now add in the rows from rows_to_skip and duplicates_diff
#     combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, rows_to_skip, duplicates_diff])

#     #remove date from index
#     combined_data_concordance_manual.reset_index(inplace=True)

#     return combined_data_concordance_manual, iterator


