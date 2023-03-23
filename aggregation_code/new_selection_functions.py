import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
from PIL import Image
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import itertools


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

    user_input = ''

    groups_concordance_names_files, groups_data_names_files = save_groups_to_tmp_folder(combined_data_concordance, combined_data, paths_dict,grouping_cols)

    #save a progress pkl which will just be the combined_data_concordance. It will be updated as the user progresses through the selection process
    combined_data_concordance.to_pickle(paths_dict['selection_progress_pkl'])

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
        open_plotly_group_dashboard(group_data_name_file, group_data)

        #set index to INDEX_COLS_no_year
        group_concordance.set_index(paths_dict['INDEX_COLS_no_year'] , inplace=True)
        group_data.set_index(paths_dict['INDEX_COLS_no_year'] , inplace=True)

        # iterate through each index col
        for index_row_no_year in group_concordance.index:
            current_index_row_no_year = group_concordance.loc[index_row_no_year]

            if current_index_row_no_year.num_datapoints <= 1:
                continue
            
            #data_to_select_from will be used as the data from which the user will select the preferred data
            data_to_select_from = group_data.loc[index_row_no_year]

            #plot data
            timeseries_png = plot_timeseries(data_to_select_from, paths_dict,index_row_no_year)

            #now pass the group_concordance df to the user input handler which will run through each year and ask the user to select the preferred data. The preferred data will be recorded in the concordance df.
            group_concordance, user_input = manual_user_input_function(data_to_select_from, index_row_no_year, group_concordance, paths_dict)

            close_timeseries(timeseries_png)#doesnt seem to work
            if user_input == 'quit':
                break
            else:
                print('Finished with unique combination: {}'.format(index_row_no_year))  
                #close timeseries
                
        save_progress(group_concordance,grouping_cols, paths_dict)

        if user_input == 'quit':
            print('User input was quit on group {}, so quitting the script and saving the progress to a csv'.format(group_concordance_name_file))
            break
        else:
            print('Finished with group: {}'.format(group_concordance_name_file))

    print('Finished with manual selection of datasets')

    combined_data = load_and_merge_groups_from_tmp_folder(groups_data_names_files)#not sure if usefule

    #if the user_input was not quit then load progress and return it as the new combined_data_concordance
    if user_input != 'quit':
        combined_data_concordance = pd.read_pickle(paths_dict['selection_progress_pkl'])
    else:
        combined_data_concordance = 'quit'
    remove_groups_from_tmp_folder(groups_concordance_names_files, groups_data_names_files)

    return combined_data_concordance


def open_plotly_group_dashboard(group_data_name_file, group_data):
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
    fig_dash.write_html('plotting_output/manual_data_selection/dashboard_{}.html'.format(group_name), auto_open=True)
    return



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
        group_name = os.path.join(paths_dict['tmp_selection_groups_folder'],'group_concordance_{}.pkl'.format('_'.join(group_name)))
        #save group concordance to tmp folder
        group_concordance.to_pickle(group_name)
        #add groupname to lsit
        groups_concordance_names_files.append(group_name)

    for group_name, group_data in groups_data:
        group_name = os.path.join(paths_dict['tmp_selection_groups_folder'],'group_data_{}.pkl'.format('_'.join(group_name)))
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
    #remove the rows which match group index from the progress df by using group_index.items()
    for key, value in group_index.items():
        progress = progress[progress[key] != value]
    #add the group_concordance to the progress df
    progress = pd.concat([progress, group_concordance])
    progress.to_pickle(paths_dict['selection_progress_pkl'])

    #remove progress from memory
    del progress

def print_user_input_options(choice_dict):
    """Print the options for the user to choose from. The options are the datasets that are available for the given year."""
    print('\n\nWhen asked for user input, supply a number for one of these options, if it is applicable:\n\n')
    for value in choice_dict.values():
        print('{}'.format(value[2]))

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
                    print('Exiting function and saving progress')
                    # sys.exit()#used for test
                    return 'quit'
                else:
                    print('You entered a different value but not the right value. Please enter a valid number from the options above')
            input_correct = False
            continue#jump back to beginning of while loop
        if user_input == 0:
            #skip this year
            print('Your input was {} which is a valid number from the options above.\nYear {} skipped'.format(user_input,year))
            input_correct = True
        elif user_input =='b':
            #go back one year
            print('Your input was {} which is a valid number from the options above.\nGoing back one year'.format(user_input))
            input_correct = True
        elif user_input in choice_dict.keys():
            print('Your input was {} which is a valid number from the options'.format(user_input))
            input_correct = True
        else:
            #jsut in case the user enters a number that is not in the dictionary, send them back to the start of the while loop
            print('You entered an invalid number. Please enter a valid number from the options above')
            input_correct = False
    return user_input


def skip_year(group_concordance, year, index_row_no_year, paths_dict):
    #skip this year but set the dataset_selection_method for this year and index row to manual so that we know that this year was skipped
    group_concordance = group_concordance.reset_index().set_index(paths_dict['INDEX_COLS'])
    index_row_with_date = (year, *index_row_no_year)
    group_concordance.loc[index_row_with_date, 'dataset_selection_method'] = 'skipped'
    print('Skipping year {} for index row {} and index row with date {}'.format(year, index_row_no_year, index_row_with_date))
    print('group_concordance.loc[index_row_with_date, dataset_selection_method] = {}'.format(group_concordance.loc[index_row_with_date, 'dataset_selection_method']))
    group_concordance = group_concordance.reset_index().set_index(paths_dict['INDEX_COLS_no_year'])
    return group_concordance
   
def create_user_choice_dict(unique_datasets):

    """create options for the user to choose by looping through the options list and the dataset list"""

    options = ['Keep the dataset "{}" for all years that the chosen dataset is available', 'Keep the dataset "{}" for all consecutive years that the same combination of datasets is available','Keep the dataset "{}" only for that year', 'Delete the value for dataset "{}" for this year']
    #create an options dictwith keys rather than indexes to return the correct option. This will allow us to include more info in the options dict as well.
    options_dict = dict()
    options_dict['Keep_for_all_years'] = options[0]
    options_dict['Keep_for_all_consecutive_years'] = options[1]
    options_dict['Keep_for_this_year'] = options[2]
    options_dict['Delete'] = options[3]

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

def manual_user_input_function(data_to_select_from, index_row_no_year,  group_concordance, paths_dict): 
    
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

        #ask the user to input a number and then check that it is a valid number. Use a fucntion for this to reduce lines in this function
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

    # elif option_key == 'Delete':
    #     #set dataset_selection_method to 'Delete' in ?combined dsata ?#TODO
    #     user_input_remove_value_from_selections_permanently(selected_dataset, data_to_select_from, data_to_select_from_for_this_year, INDEX_COLS)
    #     pass
    else:
        print("ERROR: option_key not found, please check the code")
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
        print(min_year, ' min_year')
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


def plot_timeseries(data_to_select_from, paths_dict,index_row_no_year):
        #graph data in a line graph using the index from the unique comb ination to graph data out of the combined dataframe
        ##PLOT
        fig, ax = plt.subplots()
        #join 'comment' columns to 'dataset' column if they're not nan
        data_to_select_from['dataset'] = data_to_select_from.apply(lambda row: row['dataset'] + '-' + row['comment'] if row['comment'] != 'no_comment' else row['dataset'], axis=1)

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
                #plot the data
                ax.plot(dataset_data.date, dataset_data.value, label=dataset, color=color)
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
        
        fig.savefig(paths_dict['timeseries_png'])
        
        #open the timeseries png in separate window
        timeseries_png = Image.open(paths_dict['timeseries_png'])
        timeseries_png.show()
        return timeseries_png

def close_timeseries(timeseries_png):
    #close the timeseries png
    timeseries_png.close()
