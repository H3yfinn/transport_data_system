


#%%
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
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import itertools

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False

#ignore by message
warnings.filterwarnings("ignore", message="indexing past lexsort depth may impact performance")
#ignore by message
warnings.filterwarnings("ignore", message="invalid value encountered in cast")


def manual_ask_and_parse_user_input(year, choice_dict):
    """If the user enters an invalid number, the function will print an error message and prompt the user to try again. If the user enters the same invalid number twice, the function will exit and save the user's progress. If the user enters a valid number, the function will check if the number is in a dictionary of choices, and if it is, the function will return True. If the number is not in the dictionary, the function will return False."""
    input_correct = False
    user_input = None
    valid_options = list(choice_dict.keys())
    user_input_question = '\nFor the year {} please choose a number from the options provided: {}'.format(year,valid_options)
    print(user_input_question)
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
        if user_input in choice_dict.keys():
            print('Your input was {} which is a valid number from the options'.format(user_input))
            input_correct = True
        elif user_input == 0:
            #skip this year
            print('Your input was {} which is a valid number from the options above.\nYear {} skipped'.format(user_input,year))
            input_correct = True
        else:
            #jsut in case the user enters a number that is not in the dictionary, send them back to the start of the while loop
            print('You entered an invalid number. Please enter a valid number from the options above')
            input_correct = False
    return user_input

def create_user_input_text(unique_datasets, options_dict):

    """print options for the user to choose by looping through the options list and the dataset list"""
    i=1
    choice_dict = {}
    user_input_options = '' #this is the question that will be asked to the user. Just tack on all that is printed to this variable
    # print('\n\n##############################\n\nFor the year {} and the combination of columns {} choose a number from the options below:\n\n'.format(year, index_row))
    user_input_options += '\n\nWhen asked for user input, supply a number for one of these options, if it is applicable:\n\n'#, index_row)
    user_input_options += '\n0: Skip this year'
    # print('0: Skip this year')
    for option_key,option_text in options_dict.items():
        for dataset in unique_datasets:
            # print('{}: {}'.format(i, option.format(dataset)))
            user_input_options += '\n{}: {}'.format(i, option_text.format(dataset))
            #add the option and dataset to a dictionary so that we can refer to it with the number the user inputs later
            choice_dict[i] = [option_key, dataset]
            i+=1
    return user_input_options, choice_dict
    

def user_input_keep_for_all_consecutive_years(dataset, data_for_plotting, year_data, INDEX_COLS):
    #'Keep the dataset {} for all consecutive years that the same combination of datasets is available'
    
    #use datasets column to find all years with the same set of datasets. 
    set_of_datasets_in_this_year = year_data.Datasets[0]
    data_to_change = data_for_plotting.copy()

    #filter for only the chosen dataset in the dataset column
    data_to_change = data_to_change[data_to_change.Dataset == dataset]
    #filter for years after the current year
    data_to_change = data_to_change[data_to_change.Date >= year_data.Date[0]]
            

    #filter for only the rows where the set of datasets is the same as the set of datasets for the current year.
    data_to_change = data_to_change[data_to_change.Datasets.isin([set_of_datasets_in_this_year])]
            
    #keep only years that are consecutive from the current year
    #sort by year and reset the index
    data_to_change = data_to_change.reset_index().sort_values(by = ['Date'])           
    # create a new column that represents the difference between each year and the previous year
    data_to_change['Year_diff'] = data_to_change['Date'].diff()
    # shift the values in the year column down by one row
    data_to_change['Year_shifted'] = data_to_change['Date'].shift()
    #find minimum Year where Year_diff is not 1 and the Year is not year
    min_year = data_to_change.loc[(data_to_change['Year_diff'] != 1) & (data_to_change['Date'] != year_data.Date[0]), 'Date'].min()
    #if year is nan then set it to the max year +1 since that means all years are applicable.
    if pd.isna(min_year):
        min_year = data_to_change['Date'].astype(int).max() + 1
        print(min_year, ' min_year')
    #filter for only the rows where the year is less than the min_year
    data_to_change = data_to_change.loc[data_to_change['Date'] < min_year]
    # drop the columns we created
    data_to_change = data_to_change.drop(columns = ['Year_diff', 'Year_shifted'])

    #make Year a part of the index
    data_to_change = data_to_change.set_index(INDEX_COLS)

    return data_to_change

def user_input_keep_for_all_years(dataset, data_for_plotting, year_data, INDEX_COLS):
    #find the rows in data_for_plotting where the datasets are the same as for the chosen dataset
    data_to_change = data_for_plotting[data_for_plotting.Dataset == dataset]

    #filter for years after the current year
    data_to_change = data_to_change[data_to_change.Date >= year_data.Date[0]]
    #make Year a part of the index
    data_to_change = data_to_change.reset_index().set_index(INDEX_COLS)
    return data_to_change

def user_input_keep_for_this_year(dataset, data_for_plotting, year_data, INDEX_COLS):
    #we can just use year_data here
    data_to_change = year_data[year_data.Dataset == dataset]
    
    #make Year a part of the index
    data_to_change = data_to_change.reset_index().set_index(INDEX_COLS)
    return data_to_change

def user_input_remove_value_from_selections_permanently(dataset, data_for_plotting, year_data, INDEX_COLS):
    pass

def apply_selection_to_concordance(data_to_change, dataset, combined_data_concordance_manual, years_to_ignore,INDEX_COLS):
    #make Date a part of the index
    combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(INDEX_COLS)

    #and then find those rows in combined_data_concordance_manual 
    for rows_to_change in data_to_change.index:
        #update the dataset column to be the chosen dataset
        combined_data_concordance_manual.loc[rows_to_change, 'Dataset'] = dataset
        #set Dataset_selection_method to Manual too
        combined_data_concordance_manual.loc[rows_to_change,'Dataset_selection_method'] = 'Manual'
        
        #set Value using the value in data_to_change
        combined_data_concordance_manual.loc[rows_to_change,'Value'] = data_to_change.loc[rows_to_change, 'Value']
        combined_data_concordance_manual.loc[rows_to_change,'Comments'] = data_to_change.loc[rows_to_change, 'Comments']
        
        # #set number of Num_datapoints to the same value as Num_datapoints in data_to_change
        # combined_data_concordance_manual.loc[rows_to_change, 'Num_datapoints'] = data_to_change.loc[rows_to_change, 'Count']
        
    #reset the index so that Date is a column again
    combined_data_concordance_manual = combined_data_concordance_manual.reset_index(level=['Date'])

    #Finally, since we may have set the data for years that will come up in the next loop, we will add these to a list of years to ignore
    years_to_ignore = np.append(years_to_ignore, data_to_change.reset_index().Date.unique())
    years_to_ignore = years_to_ignore.astype(int)
    years_to_ignore = np.unique(years_to_ignore)

    return combined_data_concordance_manual,years_to_ignore

def apply_user_input_to_data(user_input, choice_dict, options_dict, combined_data_concordance_manual, years_to_ignore, data_for_plotting, year_data, INDEX_COLS):

    """Find the input in the choiuces dictionary and then find the option and dataset that the user chose then apply it"""

    #find the option and dataset that the user chose
    option_key = choice_dict[user_input][0]
    dataset = choice_dict[user_input][1]
    
    #which matches what the user wants to change. Then by using that as an index, find the matching rows in our concordance datyaset, set the dataset, data_selection method and value columns.
    if option_key == 'Keep_for_all_consecutive_years':
        data_to_change = user_input_keep_for_all_consecutive_years( dataset, data_for_plotting, year_data, INDEX_COLS)
        apply_selection_to_combined_data_concordance_manual(data_to_change, dataset, combined_data_concordance_manual, years_to_ignore,INDEX_COLS)

    elif option_key == 'Keep_for_all_years':
        # 'Keep the dataset "{}" for all years that the chosen dataset is available'
        data_to_change = user_input_keep_for_all_years(dataset, data_for_plotting, year_data, INDEX_COLS)
        apply_selection_to_combined_data_concordance_manual(data_to_change, dataset, combined_data_concordance_manual, years_to_ignore,INDEX_COLS)

    elif option_key == 'Keep_for_this_year':
        #'Keep the dataset "{}" only for that year']
        data_to_change = user_input_keep_for_this_year(dataset, data_for_plotting, year_data, INDEX_COLS)
        apply_selection_to_combined_data_concordance_manual(data_to_change, dataset, combined_data_concordance_manual,years_to_ignore, INDEX_COLS)

    elif option_key == 'Delete':
        #set Dataset_selection_method to 'Delete' in ?combined dsata ?#TODO
        user_input_remove_value_from_selections_permanently(dataset, data_for_plotting, year_data, INDEX_COLS)
        pass
    else:
        print("ERROR: option_key not found, please check the code")
        sys.exit()
    
    return combined_data_concordance_manual, years_to_ignore

def manual_user_input_function(data_for_plotting, index_row,  combined_data_concordance_manual, INDEX_COLS,choice_dict, options_dict): 
    
    years_to_ignore = []#todo IS THERE ANY POINT IN THIS VARIABLE HAVE WE USED IT?
    user_input = None
    for year in data_for_plotting.Date.unique():
        #double check year is not in years_to_ignore
        if year in years_to_ignore:
            continue

        #filter for only the current year
        year_data = data_for_plotting[data_for_plotting.Date == year]

        ##DETERMINE VALID USER INPUTS FOR YEAR
        unique_datasets = year_data.Dataset.unique()
        #filter choice_dict for entries where dataset is in unique_datasets
        valid_choice_dict = {k:v for k,v in choice_dict.items() if v[1] in unique_datasets}
        #ask the user to input a number and then check that it is a valid number. Use a fucntion for this to reduce lines in this function
        user_input = manual_ask_and_parse_user_input(year, valid_choice_dict)

        if user_input == 'quit':
            return combined_data_concordance_manual, user_input
            #sys.exit()#for testing
        elif user_input == 0:
            #skip this year but set the Dataset_selection_method for this year and index row to manual so that we know that this year was skipped
            #make date a part of the index so that we can set the Dataset_selection_method for this year
            #TODO it would be good if this wasnt setting selection method to manul. perhaps we could set it to 'skip_manual' or something.
            combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(INDEX_COLS)
            index_row_with_date = (year, *index_row)#todo are we sure this is the right way to do this?
            combined_data_concordance_manual.loc[index_row_with_date, 'Dataset_selection_method'] = 'Manual'
            #hopefully thats enough and that weorks!
            print('Skipping year {} for index row {} and indexrow with date {}'.format(year, index_row, index_row_with_date))
            print('combined_data_concordance_manual.loc[index_row_with_date, Dataset_selection_method] = {}'.format(combined_data_concordance_manual.loc[index_row_with_date, 'Dataset_selection_method']))
            continue
        elif user_input == 'back':#intend to find a way to go back to the previous year or index row. #todo probably just need to sort everything useing the order the index is provided in. Then we can just go back to the previous index row. Probably will then need to make the for loop a while loop and retrieve rows via a numbered index system??
            pass
        else:
            combined_data_concordance_manual, years_to_ignore = apply_user_input_to_data(user_input, valid_choice_dict, options_dict, combined_data_concordance_manual, years_to_ignore,data_for_plotting, year_data, INDEX_COLS)

    return combined_data_concordance_manual, user_input



def select_best_data_manual_by_group(combined_data_concordance_iterator,iterator,combined_data_concordance_manual,combined_data,duplicates_manual,INDEX_COLS,selection_set,sorting_cols,FILE_DATE_ID=''):
    #todo find a way to explain this function better. It is a bit confusing. i think its because all of a sudden the plotting process is specific to the data itself. eg. we are plotting the dash board for the unique set of vehicel type, transport type, measure and etc. instaed of any random index row.
    #perhaps at least just make it work in teh saem way as if we werent doing it by group and with dashbaord etc. (eg. modularise)


    # create a new folder named for the selection set in intermediate data/data selection/manual_data_selection_sets/
    #in this folder we will store the finished progress of the manual data selection for this set, named by the group id. Then if we ever want to import previous manual data selection we can just import the data from this folder, concatenate it all and then apply it to the combined data concordance and so on.
    #this way we can save effective checkpoints of the manual data selection process. 

    #first we will design the process that will allow us to import previous manual data selection, then we will design the process that will allow us to save checkpoints of the manual data selection process.


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

def graph_user_input_options(user_input_options, fig_text, ax_text):
    #estimate width and height of text box
    num_lines = len(user_input_options.splitlines())
    num_chars = max([len(line) for line in user_input_options.splitlines()])
    width = num_chars*0.1
    height = num_lines*0.2
    #increase the size of the figure to fit the text box
    fig_text.set_size_inches(width, height)
    
    ax_text.get_yaxis().get_major_formatter().set_useOffset(False)
    ax_text.clear()
    ax_text.axis('off')
    ax_text.text(0, 0, user_input_options, fontsize=10)
    # plt.pause()#needed to give the script time to show the plot before asking for user input
    
    return fig_text

def graph_current_row_and_input_options(data_for_plotting, user_input_options,index_row, fig_row, ax_row):
    
    ax_row.get_yaxis().get_major_formatter().set_useOffset(False)
    #join 'Comments' columns to 'Dataset' column if they're  not nan
    data_for_plotting['Dataset'] = data_for_plotting['Dataset'].astype(str)
    data_for_plotting['Comments'] = data_for_plotting['Comments'].astype(str)
    data_for_plotting['Dataset'] = data_for_plotting.apply(lambda row: row['Dataset'] + '-' + row['Comments'] if row['Comments'] != 'nan' else row['Dataset'], axis=1)
    #loop through the datasets and plot each one as a line
    #but because we might end up with two lines on top of each other, make each successive line a little bit more transparent and thicker
    alpha_start = 1
    alpha_step = 1 / (len(data_for_plotting.Dataset.unique())+5)
    thickness_start = 3 * (len(data_for_plotting.Dataset.unique())+2)
    thickness_step = 3
    for dataset in data_for_plotting.Dataset.unique():
        # pick color
        color = next(ax_row._get_lines.prop_cycler)['color']
        #filter for only the current dataset
        dataset_data = data_for_plotting[data_for_plotting.Dataset == dataset]
        #plot the data
        ax_row.plot(dataset_data.Date, dataset_data.Value, label=dataset, color=color)
        #make the line a little bit more transparent
        alpha_start -= alpha_step
        ax_row.lines[-1].set_alpha(alpha_start)
        # #make the line a little bit thicker
        # thickness_start -= thickness_step
        # ax_row.lines[-1].set_linewidth(thickness_start)

        #plot markers for the datapoints as well
        ax_row.plot(dataset_data.Date, dataset_data.Value, 'o', label=dataset, color=color)
        #make the markers a little bit more transparent
        ax_row.lines[-1].set_alpha(alpha_start)
        #make the markers a little bit thicker
        thickness_start -= thickness_step
        ax_row.lines[-1].set_markersize(thickness_start)

    #create a 0 line so its clear if the value is 0 or not
    ax_row.axhline(y=0, color='black', linestyle='--')
    
    #finalise the plot by adding a legend, titles and subtitles and showing it in advance of user input
    ax_row.legend()
    ax_row.set_title('{}: {}\n - {}: {}, {}, {}, {}'.format(index_row[0], index_row[1], index_row[2], index_row[3], index_row[4], index_row[5], index_row[6]))
    ax_row.set_xlabel('Date')
    ax_row.set_ylabel('Value')
    #set background color to white so we can see title and subtitles
    ax_row.set_facecolor('white')


    #TODO it seems we are going to need to plot this data in the same plot as the other gr4aph to speed things up https://stackoverflow.com/questions/32742511/displaying-a-line-of-text-outside-of-a-plot
    # #now plot the user input options in a text box outside the main plot and size the plot using tight_layout
    # ax_row.text(0, 0, user_input_options, fontsize=10)
    # fig_row.tight_layout()
    return fig_row, ax_row

def graph_current_row(data_for_plotting, index_row, fig_row, ax_row):
    ax_row.get_yaxis().get_major_formatter().set_useOffset(False)
    #join 'Comments' columns to 'Dataset' column if they're  not nan
    data_for_plotting['Dataset'] = data_for_plotting['Dataset'].astype(str)
    data_for_plotting['Comments'] = data_for_plotting['Comments'].astype(str)
    data_for_plotting['Dataset'] = data_for_plotting.apply(lambda row: row['Dataset'] + '-' + row['Comments'] if row['Comments'] != 'nan' else row['Dataset'], axis=1)
    #loop through the datasets and plot each one as a line
    #but because we might end up with two lines on top of each other, make each successive line a little bit more transparent and thicker
    alpha_start = 1
    alpha_step = 1 / (len(data_for_plotting.Dataset.unique())+5)
    thickness_start = 3 * (len(data_for_plotting.Dataset.unique())+2)
    thickness_step = 3
    for dataset in data_for_plotting.Dataset.unique():
        # pick color
        color = next(ax_row._get_lines.prop_cycler)['color']
        #filter for only the current dataset
        dataset_data = data_for_plotting[data_for_plotting.Dataset == dataset]
        #plot the data
        ax_row.plot(dataset_data.Date, dataset_data.Value, label=dataset, color=color)
        #make the line a little bit more transparent
        alpha_start -= alpha_step
        ax_row.lines[-1].set_alpha(alpha_start)
        # #make the line a little bit thicker
        # thickness_start -= thickness_step
        # ax_row.lines[-1].set_linewidth(thickness_start)

        #plot markers for the datapoints as well
        ax_row.plot(dataset_data.Date, dataset_data.Value, 'o', label=dataset, color=color)
        #make the markers a little bit more transparent
        ax_row.lines[-1].set_alpha(alpha_start)
        #make the markers a little bit thicker
        thickness_start -= thickness_step
        ax_row.lines[-1].set_markersize(thickness_start)

    #create a 0 line so its clear if the value is 0 or not
    ax_row.axhline(y=0, color='black', linestyle='--')
    
    #finalise the plot by adding a legend, titles and subtitles and showing it in advance of user input
    ax_row.legend()
    ax_row.set_title('{}: {}\n - {}: {}, {}, {}, {}'.format(index_row[0], index_row[1], index_row[2], index_row[3], index_row[4], index_row[5], index_row[6]))
    ax_row.set_xlabel('Date')
    ax_row.set_ylabel('Value')
    #set background color to white so we can see title and subtitles
    ax_row.set_facecolor('white')
    return fig_row


def plotly_dashboard(combined_data_group,group):
    #plot a dashboard using plotly and open it in the user's default browser

    #create an empty figure in the user's default browser. we will add subplots to this figure
    fig_dash = go.Figure()
    #make a shape for the set of subplots that will be as close to a square as possible
    shape = (int(np.ceil(np.sqrt(len(combined_data_group.Measure.unique())))), int(np.ceil(len(combined_data_group.Measure.unique())/np.ceil(np.sqrt(len(combined_data_group.Measure.unique()))))))
    #specify subplots using shape
    fig_dash = make_subplots(rows=shape[0], cols=shape[1], shared_xaxes=True, subplot_titles=combined_data_group.Measure.unique())
    #create a combination of every possible row and col for the subplots
    row_col_combinations = list(itertools.product(range(1,shape[0]+1), range(1,shape[1]+1)))
    i=0
    for measure in combined_data_group.Measure.unique():
        #get data for this measure
        measure_data = combined_data_group.loc[combined_data_group.Measure == measure]
        row = row_col_combinations[i][0]
        col = row_col_combinations[i][1]
        i+=1

        for dataset in combined_data_group['Dataset'].unique():
            #plot data for this measure and this daTASET
            fig_dash.add_trace(go.Scatter(x=measure_data.loc[measure_data.Dataset == dataset].Date, y=measure_data.loc[measure_data.Dataset == dataset].Value, name=dataset, mode='lines+markers'), row=row, col=col)
    #add a title to the figure as the data in group sep by spaces
    fig_dash.update_layout(title_text=', '.join(group))
    #save the figure as temp html file
    fig_dash.write_html('plotting_output/data_selection_dashboards/dashboard_{}.html'.format('_'.join(group)), auto_open=True)
    return




# def graph_manual_data(data_for_plotting, index_row):
#     #graph data in a line graph using the index from the unique comb ination to graph data out of the combined dataframe
#     ##PLOT
#     #close any existing plots
#     plt.close('all')
#     fig, ax = plt.subplots()
#     plt.ion()
#     plt.show()
#     ax.get_yaxis().get_major_formatter().set_useOffset(False)
#     #join 'Comments' columns to 'Dataset' column if they're  not nan
#     data_for_plotting['Dataset'] = data_for_plotting['Dataset'].astype(str)
#     data_for_plotting['Comments'] = data_for_plotting['Comments'].astype(str)
#     data_for_plotting['Dataset'] = data_for_plotting.apply(lambda row: row['Dataset'] + '-' + row['Comments'] if row['Comments'] != 'nan' else row['Dataset'], axis=1)
#     #loop through the datasets and plot each one as a line
#     #but because we might end up with two lines on top of each other, make each successive line a little bit more transparent and thicker
#     alpha_start = 1
#     alpha_step = 1 / (len(data_for_plotting.Dataset.unique())+5)
#     thickness_start = 3 * (len(data_for_plotting.Dataset.unique())+2)
#     thickness_step = 3
#     for dataset in data_for_plotting.Dataset.unique():
#         # pick color
#         color = next(ax._get_lines.prop_cycler)['color']
#         #filter for only the current dataset
#         dataset_data = data_for_plotting[data_for_plotting.Dataset == dataset]
#         #plot the data
#         ax.plot(dataset_data.Date, dataset_data.Value, label=dataset, color=color)
#         #make the line a little bit more transparent
#         alpha_start -= alpha_step
#         ax.lines[-1].set_alpha(alpha_start)
#         # #make the line a little bit thicker
#         # thickness_start -= thickness_step
#         # ax.lines[-1].set_linewidth(thickness_start)

#         #plot markers for the datapoints as well
#         ax.plot(dataset_data.Date, dataset_data.Value, 'o', label=dataset, color=color)
#         #make the markers a little bit more transparent
#         ax.lines[-1].set_alpha(alpha_start)
#         #make the markers a little bit thicker
#         thickness_start -= thickness_step
#         ax.lines[-1].set_markersize(thickness_start)

#     #create a 0 line so its clear if the value is 0 or not
#     ax.axhline(y=0, color='black', linestyle='--')
    
#     #finalise the plot by adding a legend, titles and subtitles and showing it in advance of user input
#     ax.legend()
#     ax.set_title('{}: {}\n - {}: {}, {}, {}, {}'.format(index_row[0], index_row[1], index_row[2], index_row[3], index_row[4], index_row[5], index_row[6]))
#     ax.set_xlabel('Date')
#     ax.set_ylabel('Value')
#     #set background color to white so we can see title and subtitles
#     ax.set_facecolor('white')
#     # fig.savefig('./plotting_output/manual_data_selection/{}_{}.png'.format(FILE_DATE_ID, row.name))#what is row.name i wonder
#     # plt.close(fig)
#     plt.draw()#show(block=True)#False)
#     plt.pause(1)#needed to give the script time to show the plot before asking for user input
#     return fig