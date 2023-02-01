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

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False

#ignore by message
warnings.filterwarnings("ignore", message="indexing past lexsort depth may impact performance")
#%%
def automatic_method(combined_data_automatic, combined_data_concordance_automatic,duplicates_auto,duplicates_auto_with_year_index, datasets_to_always_choose=[],std_out_file=None):
        """#AUTOMATIC METHOD
        #in the automatic method we will use the following rules in an order of priority from 1 being the highest priority to n being the lowest priority:
        #1. if there are two or more datapoints for a given row and one is from the same dataset as the previous year for that same unique row then use that one
        #2. if there are two or more datapoints for a given row and all but one dataset is missing in the next year for that same unique row then use the one that is not missing
        #3. if there are two or more datapoints for a given row and one is closer and within 25% of the previous year, then use that one
        #4 if none of the above apply then ask the user to manually select the best datapoint for each row, if it hasn't been done already in the past
        
        Note that this method will tend to select datasets which have data avaialble for earlier years because of option 1."""
        if std_out_file is None:
                std_out_file = sys.stdout
        else:
                std_out_file = open(std_out_file, 'w')
        y=0
        #create an empty df from combined_data_concordance_automatic which will be the rows that we will need to select manually
        rows_to_select_manually = []
        for row_index in duplicates_auto.index.unique():
                rows = duplicates_auto.loc[row_index]

                #if rows is only one row, structure it as a dataframe
                if type(rows) == pd.core.series.Series:
                        rows = pd.DataFrame(rows).T
                #loop through the years in rows
                year_list = rows.Date.unique().tolist()

                for row in rows.itertuples():
                        #get the number of datapoints
                        num_datapoints = row.Count
                        #get the datasets from the lists inside the Datasets column
                        datasets = row.Datasets
                        #extract year from the index
                        year = row.Date
                        #get previous Date from year_list if there is one, else set to None
                        previous_year = year_list[year_list.index(year)-1] if year_list.index(year) > 0 else None
                        #get the next Date
                        next_year = year_list[year_list.index(year)+1] if year_list.index(year) < len(year_list)-1 else None

                        row_index_with_year = (year,*row_index)
                        row_index_with_previous_year = (previous_year,*row_index)
                        row_index_with_next_year = (next_year,*row_index)

                        #DEFAULT:
                        #if there is only one datapoint for a given row then use that one
                        if num_datapoints == 1:
                                combined_data_concordance_automatic.loc[row_index_with_year, 'Dataset'] = datasets[0]
                                combined_data_concordance_automatic.loc[row_index_with_year, 'Dataset_selection_method'] = 'Automatic'
                                combined_data_concordance_automatic.loc[row_index_with_year, 'Num_datapoints'] = num_datapoints
                                #set value
                                row_index_with_year_and_dataset = (*row_index_with_year, datasets[0])
                                #get values we want to select from the combined_data_automatic_measure df 
                                #note that sometimes what we get back is a series sometimes just a value, so we need to be careful of that weird behaviour
                                temp_row = combined_data_automatic.loc[row_index_with_year_and_dataset]

                                if type(temp_row.Value) == pd.core.series.Series:
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Value'] = temp_row.Value.squeeze()
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Comments'] = temp_row.Comments.squeeze()
                                else:
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Value'] = temp_row.Value
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Comments'] = temp_row.Comments
                                        y+=1
                                continue
                        # Option 0
                        # if there is a dataset that is in our chosen dataset list then use that. This is a manual override
                        for dataset in datasets:
                                if dataset in datasets_to_always_choose:
                                        #TO DO include option to choose dataset according to which comes first in list of datasets to choose
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Dataset'] = dataset
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Dataset_selection_method'] = 'Automatic'
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Num_datapoints'] = num_datapoints
                                        #set values
                                        row_index_with_year_and_dataset = (*row_index_with_year, dataset)
                                        temp_row = combined_data_automatic.loc[row_index_with_year_and_dataset]

                                        if type(temp_row.Value) == pd.core.series.Series:
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Value'] = temp_row.Value.squeeze()
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Comments'] = temp_row.Comments.squeeze()
                                        else:
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Value'] = temp_row.Value
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Comments'] = temp_row.Comments
                                        continue

                        #OPTION 1
                        #1. if there are two or more datapoints for a given row and one is from the same dataset as the previous year for that same unique row then use that one
                        if previous_year is not None:
                                #get the dataset we set for the previous year
                                #DOING what is previous_year_dataset
                                previous_year_dataset = combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Dataset']
                                #if the previous year dataset is available for this year:
                                if previous_year_dataset in datasets:
                                        #set the value in combined_data_concordance_automatic for this year to the value for the same dataset used in the previous year
                                        combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Dataset'] = previous_year_dataset
                                        combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Dataset_selection_method'] = 'Automatic'
                                        combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Num_datapoints'] = num_datapoints
                                        #using combined data, extrasct the value using the row index + the chosen dataset
                                        row_index_with_previous_year_and_dataset = (*row_index_with_previous_year, previous_year_dataset)
                                        temp_row = combined_data_automatic.loc[row_index_with_previous_year_and_dataset]
                                        if type(temp_row.Value) == pd.core.series.Series:
                                                combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Value'] = temp_row.Value.squeeze()
                                                combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Comments'] = temp_row.Comments.squeeze()
                                        else:
                                                combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Value'] = temp_row.Value
                                                combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Comments'] = temp_row.Comments
                                        #let the user know what we are doing, including detailing the row index 
                                        print('Using automatic option 1. use the same dataset as the previous year for the same unique row. The row is: {}'.format(row_index_with_year), file=std_out_file)
                                        continue
                        #OPTION 2
                        #2. if there are two or more datapoints for a given row and all but one dataset is missing in the next year for that same unique row then use the one that is not missing
                        if next_year is not None:
                                #get the datasets we set for the next year
                                next_year_datasets = duplicates_auto_with_year_index.loc[row_index_with_next_year, 'Datasets']
                                #count number of datasets that are missing in the next year
                                num_datasets_missing = 0
                                for dataset in datasets:
                                        if dataset not in next_year_datasets:
                                                num_datasets_missing += 1
                                        else:
                                                not_missing_dataset = dataset
                                #if all but one dataset is missing in the next year
                                if num_datasets_missing == len(datasets) - 1:
                                        #set the value in combined_data_concordance_automatic for this year to the value for the same dataset used in the previous year
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Dataset'] = not_missing_dataset
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Dataset_selection_method'] = 'Automatic'
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Num_datapoints'] = num_datapoints
                                        #using combined data, extrasct the value using the row index + the chosen dataset
                                        row_index_with_year_and_dataset = (*row_index_with_year, not_missing_dataset)
                                        temp_row = combined_data_automatic.loc[row_index_with_year_and_dataset]
                                        if type(temp_row.Value) == pd.core.series.Series:
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Value'] = temp_row.Value.squeeze()
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Comments'] = temp_row.Comments.squeeze()
                                        else:
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Value'] = temp_row.Value
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Comments'] = temp_row.Comments
                                        #let the user know what we are doing, including detailing the row index 
                                        print('Using automatic option 2. Use the dataset that is not missing in the next year for the same unique row. The row is: {}'.format(row_index_with_year), file=std_out_file)
                                        continue
                        #OPTION 3
                        #3. if there are two or more datapoints for a given row and one is closer and within 25% of the previous year, then use that one. As long as the previous year is not missing
                        #if there is a previous year row index
                        if previous_year is not None:
                                #get the value for the previous year
                                temp_row = combined_data_concordance_automatic.loc[row_index_with_previous_year]
                                if type(temp_row.Value) == pd.core.series.Series:
                                        previous_year_value = temp_row.Value.squeeze()
                                else:
                                        previous_year_value = temp_row.Value
                                # previous_year_value = combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Value']
                                if previous_year_value == None:
                                        pass
                                else:
                                        percent_diff_previous_year_list = []
                                        for dataset in datasets:
                                                #get the value for the current row and dataset
                                                row_index_with_year_and_dataset = (*row_index_with_year, not_missing_dataset)
                                                # temp_row = combined_data_automatic.loc[row_index_with_year_and_dataset]
                                                if type(temp_row.Value) == pd.core.series.Series:
                                                        current_value = temp_row.Value.squeeze()
                                                else:
                                                        current_value = temp_row.Value
                                                #get the difference between the current value and the previous year value
                                                diff_previous_year = abs(current_value - previous_year_value)
                                                #get the percentage difference between the current value and the previous year value
                                                percent_diff_previous_year = diff_previous_year/previous_year_value
                                                #add it to the list
                                                percent_diff_previous_year_list.append(percent_diff_previous_year)
                                        #check list of percentage differences, grab the min, check it is below 25% and if so, use that dataset
                                        percent_diff_previous_year = min(percent_diff_previous_year_list)

                                        if percent_diff_previous_year < 0.25:
                                                #using the index of the value in the list, get the dataset
                                                dataset = datasets[percent_diff_previous_year_list.index(percent_diff_previous_year)]
                                                #if there are more than one datasets in dataset then we have two values that are the same. In this case, we will use the first one

                                                #set the value in combined_data_concordance_automatic for this year to the value for the same dataset used in the previous year
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Dataset'] = dataset
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Dataset_selection_method'] = 'Automatic'
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Num_datapoints'] = num_datapoints
                                                #using combined data, extrasct the value using the row index + the chosen dataset
                                                row_index_with_year_and_dataset = (*row_index_with_year, dataset)
                                                temp_row = combined_data_automatic.loc[row_index_with_year_and_dataset]
                                                if type(temp_row.Value) == pd.core.series.Series:
                                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Value'] = temp_row.Value.squeeze()
                                                else:
                                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Value'] = temp_row.Value
                                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Comments'] = temp_row.Comments
                                                #let the user know what we are doing, including detailing the row index 
                                                print('Using automatic option 3. Use the dataset that is within 25% of the previous year and closer to the previous year than the other value in for the same unique row. The row is: {}'.format(row_index_with_year), file=std_out_file)
                                                continue
                                        if percent_diff_previous_year < 0.5:
                                                print('OPTION 3. The closest dataset was within {}% of the previous year. The row is: {}'.format(round(percent_diff_previous_year*100,2), row_index_with_year), file=std_out_file)
                        #OPTION 4
                        #4 if none of the above apply then ask the user to manually select the best datapoint for each row, if it hasn't been done already in the past
                        #print('No automatic option available. Please manually select the best datapoint for each row. The row is: ', row_index)
                        #add row index to rows_to_select_manually 
                        rows_to_select_manually.append(row_index)
        print('y is', y)
        return combined_data_concordance_automatic, rows_to_select_manually


########################################################################################################################################################
# 
#    
def graph_manual_data(data_for_plotting, index_row):
        #graph data in a line graph using the index from the unique comb ination to graph data out of the combined dataframe
        ##PLOT
        #close any existing plots
        plt.close('all')
        fig, ax = plt.subplots()
        plt.ion()
        plt.show()
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
                color = next(ax._get_lines.prop_cycler)['color']
                #filter for only the current dataset
                dataset_data = data_for_plotting[data_for_plotting.Dataset == dataset]
                #plot the data
                ax.plot(dataset_data.Date, dataset_data.Value, label=dataset, color=color)
                #make the line a little bit more transparent
                alpha_start -= alpha_step
                ax.lines[-1].set_alpha(alpha_start)
                # #make the line a little bit thicker
                # thickness_start -= thickness_step
                # ax.lines[-1].set_linewidth(thickness_start)

                #plot markers for the datapoints as well
                ax.plot(dataset_data.Date, dataset_data.Value, 'o', label=dataset, color=color)
                #make the markers a little bit more transparent
                ax.lines[-1].set_alpha(alpha_start)
                #make the markers a little bit thicker
                thickness_start -= thickness_step
                ax.lines[-1].set_markersize(thickness_start)


        #finalise the plot by adding a legend, titles and subtitles and showing it in advance of user input
        ax.legend()
        ax.set_title('{}: {}\n - {}: {}, {}, {}, {}'.format(index_row[0], index_row[1], index_row[2], index_row[3], index_row[4], index_row[5], index_row[6]))
        ax.set_xlabel('Date')
        ax.set_ylabel('Value')
        #set background color to white so we can see title and subtitles
        ax.set_facecolor('white')
        # fig.savefig('./plotting_output/manual_data_selection/{}_{}.png'.format(FILE_DATE_ID, row.name))#what is row.name i wonder
        # plt.close(fig)
        plt.draw()#show(block=True)#False)
        plt.pause(3)#needed to give the script time to show the plot before asking for user input
        return fig
########################################################################################################################################################
#%%

def manual_user_input_function(data_for_plotting, index_row,  combined_data_concordance_manual, INDEX_COLS):       
        #ask the user what they want to choose by looping through the options list and the dataset list and asking the user to choose a number
        options = ['Keep the dataset "{}" for all years that the chosen dataset is available', 'Keep the dataset "{}" for all consecutive years that the same combination of datasets is available','Keep the dataset "{}" only for that year']
        
        years_to_ignore = []
        user_input = None
        for year in data_for_plotting.Date.unique():
                #double check year is not in years_to_ignore
                if year in years_to_ignore:
                        continue

                #filter for only the current year
                year_data = data_for_plotting[data_for_plotting.Date == year]
                
                #print options for the user to choose by looping through the options list and the dataset list
                i=1
                choice_dict = {}
                user_input_question = '' #this is the question that will be asked to the user. Just tack on all that is printed to this variable
                # print('\n\n##############################\n\nFor the year {} and the combination of columns {} choose a number from the options below:\n\n'.format(year, index_row))
                user_input_question += '\n\nFor year {} choose:\n\n'.format(year)#, index_row)
                user_input_question += '\n0: Skip this year'
                # print('0: Skip this year')
                for option in options:
                        for dataset in year_data.Dataset.unique():
                                # print('{}: {}'.format(i, option.format(dataset)))
                                user_input_question += '\n{}: {}'.format(i, option.format(dataset))
                                #add the option and dataset to a dictionary so that we can refer to it with the number the user inputs later
                                choice_dict[i] = [option, dataset]
                                i+=1
      
                #ask the user to input a number and then check that it is a valid number. Use a fucntion for this to reduce lines in this function
                user_input = manual_ask_and_parse_user_input(year, choice_dict,user_input_question)
                
                if user_input == 'quit':
                        return combined_data_concordance_manual, user_input
                        #sys.exit()#for testing
                elif user_input == 0:
                        #skip this year but set the Dataset_selection_method for this year and index row to manual so that we know that this year was skipped
                        #make date a part of the index so that we can set the Dataset_selection_method for this year
                        combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(INDEX_COLS)
                        index_row_with_date = (year, *index_row)
                        combined_data_concordance_manual.loc[index_row_with_date, 'Dataset_selection_method'] = 'Manual'
                        #hopefully thats enough and that weorks!
                        print('Skipping year {} for index row {} and indexrow with date {}'.format(year, index_row, index_row_with_date))
                        print('combined_data_concordance_manual.loc[index_row_with_date, Dataset_selection_method] = {}'.format(combined_data_concordance_manual.loc[index_row_with_date, 'Dataset_selection_method']))
                        continue
                else:
                        combined_data_concordance_manual, years_to_ignore = manual_apply_user_input_to_data(user_input, choice_dict, options, combined_data_concordance_manual, years_to_ignore,data_for_plotting, year_data, INDEX_COLS)

        return combined_data_concordance_manual, user_input

def manual_ask_and_parse_user_input(year, choice_dict,user_input_question):
        """If the user enters an invalid number, the function will print an error message and prompt the user to try again. If the user enters the same invalid number twice, the function will exit and save the user's progress. If the user enters a valid number, the function will check if the number is in a dictionary of choices, and if it is, the function will return True. If the number is not in the dictionary, the function will return False."""
        input_correct = False
        user_input = None
        print(user_input_question)
        while input_correct == False:
                try:    
                        user_input_question += '\nFor the year {} please choose a number from the options above: '.format(year)
                        user_input = input(user_input_question)
                        user_input = int(user_input)
                except ValueError:
                        user_input_question += 'Please enter a valid number from the options above. If you enter the same number again the program will exit, but your progress will be saved'
                        print('Please enter a valid number from the options above. If you enter the same number again the program will exit, but your progress will be saved')
                        old_user_input = user_input
                        #create a try except to catch the error if the user enters the same number again
                        try:
                                user_input_question += '\nFor the year {} please choose a number from the options above: '.format(year)
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
                        print('Your input was {} which is a valid number from the options above'.format(user_input))
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

def manual_apply_user_input_to_data(user_input, choice_dict, options, combined_data_concordance_manual, years_to_ignore, data_for_plotting, year_data, INDEX_COLS):

        #Find the input in the choiuces dictionary and then find the option and dataset that the user chose

        #find the option and dataset that the user chose
        option = choice_dict[user_input][0]
        dataset = choice_dict[user_input][1]
        
        #which matches what the user wants to change. Then by using that as an index, find the matching rows in our concordance datyaset, set the dataset, data_selection method and value columns.
        if option == options[1]:
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

        elif option == options[0]:
                # 'Keep the dataset "{}" for all years that the chosen dataset is available'
                #find the rows in data_for_plotting where the datasets are the same as for the chosen dataset
                data_to_change = data_for_plotting[data_for_plotting.Dataset == dataset]

                #filter for years after the current year
                data_to_change = data_to_change[data_to_change.Date >= year_data.Date[0]]
                #make Year a part of the index
                data_to_change = data_to_change.reset_index().set_index(INDEX_COLS)

        elif option == options[2]:
                #'Keep the dataset "{}" only for that year']
                #we can just use year_data here
                data_to_change = year_data[year_data.Dataset == dataset]
                
                #make Year a part of the index
                data_to_change = data_to_change.reset_index().set_index(INDEX_COLS)

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
                
                #set number of Num_datapoints to the same value as Count in data_to_change
                combined_data_concordance_manual.loc[rows_to_change, 'Num_datapoints'] = data_to_change.loc[rows_to_change, 'Count']
                
        #reset the index so that Date is a column again
        combined_data_concordance_manual = combined_data_concordance_manual.reset_index(level=['Date'])
        
        #Finally, since we may have set the data for years that will come up in the next loop, we will add these to a list of years to ignore
        years_to_ignore = np.append(years_to_ignore, data_to_change.reset_index().Date.unique())
        years_to_ignore = years_to_ignore.astype(int)
        years_to_ignore = np.unique(years_to_ignore)
        
        return combined_data_concordance_manual, years_to_ignore
#%%

def remove_duplicate_transport_8th_data(combined_data, duplicates):
       #prepare a copy of our dataframes so we can check that output from follkowing is what we expect
       #To make things faster in the manual dataseelection process, for any rows in the eighth edition dataset where the data for both the carbon neutral and reference scenarios (in source column) is the same, we will remove the carbon neutral scenario data, as we would always choose the reference data anyways.
       #we should be able to do this by filtering for Dataset == '8th edition transport model $ Reference' and '8th edition transport model $ Carbon neutrality' and then filtering for rows where all other columns have the same values. Then remove the Reference data from those duplicates. We will then remove that data from the combined data and combined data concordance as well.
       duplicates_8th_edition_transport_model = combined_data[combined_data['Dataset'].isin(['8th edition transport model $ Carbon neutrality', '8th edition transport model $ Reference'])]
       #find duplicates
       duplicates_8th_edition_transport_model = duplicates_8th_edition_transport_model[duplicates_8th_edition_transport_model.duplicated(subset = ['Measure', 'Medium', 'Value', 'Unit', 'Economy', 'Transport Type', 'Vehicle Type', 'Fuel_Type', 'Comments', 'Scope', 'Frequency', 'Date', 'Drive'], keep=False)]
       #grab only rows we want to remove
       duplicates_8th_edition_transport_model = duplicates_8th_edition_transport_model[duplicates_8th_edition_transport_model['Dataset'] != '8th edition transport model $ Reference']
       #now remove that data from the combined data and duplicates (for duplicates will need to rmeove the carbon neutral dataset from the datasets list and reduce count by 1)
       #First remove from combined data:
       #set all cols to indexes
       combined_data = combined_data.set_index(combined_data.columns.tolist())
       duplicates_8th_edition_transport_model = duplicates_8th_edition_transport_model.set_index(duplicates_8th_edition_transport_model.columns.tolist())
       #now remove the data where the rows are the same
       combined_data = combined_data.drop(duplicates_8th_edition_transport_model.index)
       combined_data = combined_data.reset_index()
       #Now for duplicates:
       #and for the duplicates df, make the index = all cols indexes except: count, datasets. Then we will do a left join with indicator = True
       duplicates_8th_edition_transport_model = duplicates_8th_edition_transport_model.reset_index()
       index_list = duplicates.columns.tolist()
       index_list.remove('Count')
       index_list.remove('Datasets')
       #drop any cols not in duplicates
       duplicates_8th_edition_transport_model = duplicates_8th_edition_transport_model.drop([i for i in duplicates_8th_edition_transport_model.columns.tolist() if i not in index_list], axis=1)
       duplicates_8th_edition_transport_model = duplicates_8th_edition_transport_model.set_index(index_list)
       duplicates = duplicates.set_index(index_list)
       duplicates_new = duplicates.merge(duplicates_8th_edition_transport_model, how='left', indicator=True, on=index_list)
       #NOW WHERE _merge is both, we will remove "8th edition transport model $ Carbon neutrality" from the datasets list and reduce count by 1
       duplicates_new = duplicates_new.reset_index()
       duplicates_new1 = duplicates_new[duplicates_new['_merge'] == 'both']
       duplicates_new2 = duplicates_new[duplicates_new['_merge'] != 'both']
       duplicates_new1['Datasets'] = duplicates_new1['Datasets'].apply(lambda x: [i for i in x if i != '8th edition transport model $ Carbon neutrality'])
       duplicates_new1['Count'] = duplicates_new1['Count'] - 1
       #now remove the _merge column
       duplicates_new1 = duplicates_new1.drop(columns=['_merge'])
       duplicates_new2 = duplicates_new2.drop(columns=['_merge'])
       #concat
       duplicates = pd.concat([duplicates_new1, duplicates_new2])
       return combined_data, duplicates
       #WOULD like to double check the above worked but mind is blanking on how to do this. Will come back to it later.

#%%

###########################################################################################################################################################################################################################################################




def identify_duplicates(combined_dataset, INDEX_COLS):
    #first chekc for duplicated rows when we ignore the vlaue column
    duplicates = combined_dataset.copy()
    duplicates = duplicates.drop(columns=['Value'])
    duplicates = duplicates[duplicates.duplicated(keep=False)]
    if len(duplicates) > 0:
       print('There are duplicate rows in the dataset with different Values. Please fix them before continuing. You will probably want to split them into different datasets. The duplicates are: ')
       print(duplicates)

       #extrasct the rows with duplicates and sabve them to a csv so we can import them into a spreadsheet to look at them
       duplicates = combined_dataset.copy()
       col_no_value = [col for col in duplicates.columns if col != 'Value']
       duplicates = duplicates[duplicates.duplicated(subset=col_no_value,keep=False)]
       duplicates.to_csv('intermediate_data/testing/erroneus_duplicates.csv', index=False)

       raise Exception('There are duplicate rows in the dataset. Please fix them before continuing')

    ###########################################################
    #now recreate duplicates but this time with the value column
    duplicates = combined_dataset.copy()
    duplicates =  duplicates.groupby(INDEX_COLS,dropna=False).agg({'Dataset': lambda x: list(x)}).reset_index()
    #make sure the lists are sorted so that the order is consistent
    duplicates['Dataset'] = duplicates['Dataset'].apply(lambda x: sorted(x))
    #create count column
    duplicates['Count'] = duplicates['Dataset'].apply(lambda x: len(x))
    #rename dataset to datasets
    duplicates.rename(columns={'Dataset':'Datasets'}, inplace=True)

    return duplicates

##############################################################################

def prepare_data_for_selection(combined_data_concordance,combined_data,duplicates,INDEX_COLS,EARLIEST_YEAR = "2010-01-01", LATEST_YEAR = '2020-01-01'):
    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('Date')
    
    #filter data where year is less than our earliest year
    combined_data = combined_data[combined_data['Date'] >= EARLIEST_YEAR]
    combined_data_concordance = combined_data_concordance[combined_data_concordance['Date'] >= EARLIEST_YEAR]
    duplicates = duplicates[duplicates['Date'] >= EARLIEST_YEAR]
    #and also only data where year is less than the latest year
    combined_data = combined_data[combined_data['Date'] < LATEST_YEAR]
    combined_data_concordance = combined_data_concordance[combined_data_concordance['Date'] < LATEST_YEAR]
    duplicates = duplicates[duplicates['Date'] < LATEST_YEAR]

    #######################################################################
    #STARTING SPECIFIC FIX, FEEL FREE TO IGNORE IT
    #To make things faster in the manual dataseelection process, for any rows in the eighth edition dataset where the data for both the carbon neutral and reference scenarios (in source column) is the same, we will remove the carbon neutral scenario data, as we would always choose the reference data anyways.
    #Unfortunately the code to do this is a bit long and messy so i removed it from this function and put it in data_selection_functions.py
    combined_data, duplicates = remove_duplicate_transport_8th_data(combined_data, duplicates)
    do_it_other_way = False
    if do_it_other_way:
        #just remove any data for the Carbon neutrality scenario since it is just another scenario and we dont want to have to deal with it
        combined_data = combined_data[combined_data['Dataset'] != '8th edition transport model $ Carbon neutrality']
        #to remove it fromduplicates we need to remove it from the datasets lists column and reduce count by 1
        duplicates['Datasets'] = duplicates['Datasets'].apply(lambda x: [i for i in x if i != '8th edition transport model $ Carbon neutrality'])
        #double check count is correct
        duplicates['Count'] = duplicates['Datasets'].apply(lambda x: len(x))
    #######################################################################
    
    #we need a dataframe which replicates the final dataframe but with no values in the dataset, value and duplicate columns (This dataframe is created in aggregation_code\1_aggregate_cleaned_datasets.py so we can just import that as combined_data_concordance)
    #In the folowing scripts we will fill that df with the dataset that we choose to use for each row. Any rows where we dont have the dataset to use we will leave blank and that will end up as an NA
    combined_data_concordance['Dataset'] = None
    combined_data_concordance['Num_datapoints'] = None
    combined_data_concordance['Value'] = None
    combined_data_concordance['Dataset_selection_method'] = None
    combined_data_concordance['Comments'] = None

    #add Datasets and Count columns from duplicates_manual to combined_data for use in setting values
    combined_data = combined_data.merge(duplicates.reset_index().set_index(INDEX_COLS)[['Datasets', 'Count']], how='left', left_on=INDEX_COLS, right_on=INDEX_COLS)

    #set index of all the dfs we will use MANUAL METHODS, using the INDEX_COLs:

    #AUTOMATIC data prep
    combined_data_concordance_automatic = combined_data_concordance.set_index(INDEX_COLS)
    duplicates_auto = duplicates.set_index(INDEX_COLS_no_year)
    duplicates_auto_with_year_index = duplicates.set_index(INDEX_COLS)
    combined_data_automatic = combined_data.set_index(INDEX_COLS+['Dataset'])

    #MANUAL data prep
    combined_data_concordance_iterator = combined_data_concordance[INDEX_COLS_no_year].drop_duplicates().set_index(INDEX_COLS_no_year)#TODO i think this is correct
    combined_data_concordance_manual = combined_data_concordance.set_index(INDEX_COLS_no_year)
    duplicates_manual = duplicates.set_index(INDEX_COLS_no_year)
    combined_data = combined_data.set_index(INDEX_COLS_no_year)

    return combined_data_concordance_automatic,combined_data_concordance_manual,combined_data_concordance_iterator,duplicates_auto,duplicates_auto_with_year_index,combined_data_automatic,duplicates_manual,combined_data

##############################################################################
#%%
def automatic_selection(combined_data_concordance_automatic,combined_data_automatic,duplicates_auto,duplicates_auto_with_year_index,INDEX_COLS, datasets_to_always_choose=[],FILE_DATE_ID=''):

    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('Date')

    #AUTOMATIC METHOD

    # datasets_to_always_choose #I DONT THINK THIS IS WORKING TBH
    checkpoints_1 = []
    checkpoints_2 = []

    #start a timer to see how long it takes to run the automatic method
    start_time = time.time()

    #run the automatic method, one measure at a time
    for measure in combined_data_concordance_automatic.index.get_level_values('Measure').unique():
        #filter the combined_data_concordance_automatic df to only include the current measure
        combined_data_concordance_automatic_measure = combined_data_concordance_automatic[combined_data_concordance_automatic.index.get_level_values('Measure')==measure]
        combined_data_automatic_measure = combined_data_automatic [combined_data_automatic.index.get_level_values('Measure')==measure]
        duplicates_auto_measure = duplicates_auto[duplicates_auto.index.get_level_values('Measure')==measure]
        duplicates_auto_with_year_index_measure = duplicates_auto_with_year_index[duplicates_auto_with_year_index.index.get_level_values('Measure')==measure]

        print('Measure: {}'.format(measure))
        print('Number of rows: {}'.format(len(combined_data_concordance_automatic_measure)))
        print('Time taken so far: {} seconds'.format(time.time()-start_time))
        print('\n\n')
        
        #RUN THE AUTOMATIC METHOD
        combined_data_concordance_automatic_measure, rows_to_select_manually_measure = automatic_method(combined_data_automatic_measure, combined_data_concordance_automatic_measure,duplicates_auto_measure,duplicates_auto_with_year_index_measure,datasets_to_always_choose,std_out_file = 'intermediate_data/data_selection/{}automatic_method.txt'.format(FILE_DATE_ID))

        #save the data to a csv as checkpoint
        filename = 'intermediate_data/data_selection/checkpoints/{}{}_combined_data_concordance_automatic.csv'.format(measure,FILE_DATE_ID)
        combined_data_concordance_automatic_measure.to_csv(filename, index=True)
        checkpoints_1.append(filename)
        filename = 'intermediate_data/data_selection/checkpoints/{}rows_to_select_manually_{}.csv'.format(measure,FILE_DATE_ID)
        rows_to_select_manually_measure_df = pd.DataFrame(rows_to_select_manually_measure, columns=INDEX_COLS_no_year)

        #remove duplicates from rows_to_select_manually_measure_df
        rows_to_select_manually_measure_df = rows_to_select_manually_measure_df.drop_duplicates()
        rows_to_select_manually_measure_df.to_csv(filename, index=False)
        checkpoints_2.append(filename)

    #take in all the checkpoints and combine them into one df
    combined_data_concordance_automatic = pd.concat([pd.read_csv(checkpoint, index_col=INDEX_COLS) for checkpoint in checkpoints_1])
    rows_to_select_manually_df = pd.concat([pd.read_csv(checkpoint) for checkpoint in checkpoints_2])

    return combined_data_concordance_automatic, rows_to_select_manually_df

##############################################################################

def create_manual_data_iterator(combined_data_concordance_iterator,INDEX_COLS,combined_data_concordance_manual,rows_to_select_manually_df=None, pick_up_where_left_off=False, import_previous_selection=False,run_only_on_rows_to_select_manually=False,manually_chosen_rows_to_select=None,user_edited_combined_data_concordance_iterator=None,previous_combined_data_concordance_manual=None, duplicates_manual=None, previous_duplicates_manual=None, progress_csv=None):
    """
    manually_chosen_rows_to_select: set to true if you want to manually choose the rows to select using user_edited_combined_data_concordance_iterator
    user_edited_combined_data_concordance_iterator: a manually chosen dataframe with the rows to select. This allows user to define what they want to select manually (eg. all stocks)
    
    duplicates_manual & previous_duplicates_manual need to be available if you want to use either pick_up_where_left_off or import_previous_selection. progress_csv should also be available if you want to use pick_up_where_left_off
    """
    #Remove year from the current cols without removing it from original list, and set it as a new list
    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('Date')

    #CREATE ITERATOR 
    #if we want to add to the rows_to_select_manually_df to check specific rows then set the below to True
    if run_only_on_rows_to_select_manually:
        #if this, only run the manual process on index rows where we couldnt find a dataset to use automatically for some year
        #since the automatic method is relatively strict there should be a large amount of rows to select manually
        #note that if any one year cannot be chosen automatically then we will have to choose the dataset manually for all years of that row
        iterator = rows_to_select_manually_df.copy()
        iterator.set_index(INDEX_COLS_no_year, inplace=True)
        iterator.drop_duplicates(inplace=True)#TEMP get rid of this later
    elif manually_chosen_rows_to_select:
        #we can add rows form the combined_data_concordance_iterator as edited by the user themselves. 
        iterator = user_edited_combined_data_concordance_iterator.copy()
        #since user changed the data we will jsut reset index and set again
        iterator.reset_index(inplace=True)
        iterator.set_index(INDEX_COLS_no_year, inplace=True)

        #for this example we will add all Stocks data (for the purpoose of betterunderstanding our stocks data!) and remove all the other data. But this is just an example of what the user could do to select specific rows
        use_example = False
        if use_example:
            iterator.reset_index(inplace=True)
            iterator = iterator[iterator['Measure']=='Stocks']
            #set the index to the index cols
            iterator.set_index(INDEX_COLS_no_year, inplace=True)
    else:
        iterator = combined_data_concordance_iterator.copy()

    #now determine whether we want to import previous progress or not:
    if import_previous_selection:
        iterator, combined_data_concordance_manual = import_previous_runs_progress_to_manual(previous_combined_data_concordance_manual, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS)
    
    if pick_up_where_left_off:
        iterator, combined_data_concordance_manual = pickup_incomplete_manual_progress(progress_csv, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS)

    return iterator, combined_data_concordance_manual

##############################################################################


def select_best_data_manual(combined_data_concordance_iterator,iterator,combined_data_concordance_manual,combined_data,duplicates_manual,INDEX_COLS,FILE_DATE_ID=''):
        
        ##################################

        #START MANUAL DATA SELECTION

        ##################################
        INDEX_COLS_no_year = INDEX_COLS.copy()
        INDEX_COLS_no_year.remove('Date')
        #loop through the unique combinations, plot a timeseries for each one and then ask the user what dataset they want to choose for that instance and then update the dataset column in combined_data for that row to be the chosen dataset
        # %matplotlib qt 
        import matplotlib
        matplotlib.use('TkAgg')

        combined_data_concordance_manual.set_index(INDEX_COLS_no_year, inplace=True)
        duplicates_manual.set_index(INDEX_COLS_no_year, inplace=True)

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
        for index_row in iterator.index:
                
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
                
                #identify how many datasets there are for each year by looking at the Count column
                #if there are rows where count is greater than 1 then we will plot and ask user to choose which dataset to use
                if (current_row_filter.Count.max() > 1):# | (add_to_rows_to_select_manually_df):#if we ware adding to the rows to select manually df then we want to plot all of them because its important to check them even if none of them are duplicates
                        #grab the data for this unique combination of columns from the combined_data df
                        data_for_plotting = combined_data.loc[index_row]

                        ##PLOT
                        #plot data for this unique index row
                        fig = graph_manual_data(data_for_plotting, index_row)
                        
                        ##USER INPUT
                        #ask user what dataset they want to choose for each year where a decision needs to be made, and then based on what they choose, update combined_data_concordance_manual 
                        combined_data_concordance_manual, user_input = manual_user_input_function(data_for_plotting, index_row,  combined_data_concordance_manual, INDEX_COLS)

                        plt.close(fig)
                        if user_input == 'quit':
                                print('User input was quit on unique combination {}, so quitting the script and saving the progress to a csv'.format(index_row))
                                break
                        else:
                                print('Finished with unique combination: {}'.format(index_row))
                else:
                        pass#print('Unique combination: {} did not have more than 1 dataset available so no manual decision was needed'.format(index_row))
                #save the progress to csv in case anything goes wrong. If you need to access it it should be formatted just like the combined_data_concordance_manual df at the end of this script
                combined_data_concordance_manual.to_csv(progress_csv, index=True)

        print('Finished with manual selection of datasets')

        #TEMP FIX START
        #convert date back to the last day of the year instead of year integer
        combined_data_concordance_manual.reset_index(inplace=True)
        combined_data_concordance_manual.Date = combined_data_concordance_manual.Date.apply(lambda x: str(x)+'-12-31')#TODO somehow Date becomes an index.Would be good to fix this or double check that it doesnt cause any problems
        combined_data.Date = combined_data.Date.apply(lambda x: str(x)+'-12-31')
        duplicates_manual.Date = duplicates_manual.Date.apply(lambda x: str(x)+'-12-31')
        #TEMP FIX END
        #save combined_data_concordance_manual now so we can use it later if we need to
        combined_data_concordance_manual.to_csv('intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID), index=False)
        duplicates_manual.to_csv('intermediate_data/data_selection/{}_duplicates_manual.csv'.format(FILE_DATE_ID), index=True)
        #= pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual_duplicates.csv'.format(FILE_DATE_ID))
        return combined_data_concordance_manual, duplicates_manual, bad_index_rows, num_bad_index_rows


    
#%%
##############################################################################
#%%


def import_previous_runs_progress_to_manual(previous_combined_data_concordance_manual, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS):
    #IMPORT PREVIOUS RUNS PROGRESS
    #create option to import manual data selection from perveious runs to avoid having to do it again (can replace any rows where the Final_dataset_selection_method is na with where they are Manual in the imported csv)
    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('Date')
    ##########################################################
    #We will make sure there are no index rows in the previous dataframes that are not in the current dataframes
    #first the duplicates
    previous_duplicates_manual.set_index(INDEX_COLS, inplace=True)
    duplicates_manual.set_index(INDEX_COLS, inplace=True)
    #remove the rows that are in the previous duplicates but not in the current duplicates
    index_diff = previous_duplicates_manual.index.difference(duplicates_manual.index)
    previous_duplicates_manual.drop(index_diff, inplace=True)
    #reset the index
    previous_duplicates_manual.reset_index(inplace=True)
    duplicates_manual.reset_index(inplace=True)

    #now for previous_combined_data_concordance_manual and combined_data_concordance_manual
    previous_combined_data_concordance_manual.set_index(INDEX_COLS, inplace=True)
    combined_data_concordance_manual.set_index(INDEX_COLS, inplace=True)
    #remove the rows that are in the previous duplicates but not in the current duplicates
    index_diff = previous_combined_data_concordance_manual.index.difference(combined_data_concordance_manual.index)
    previous_combined_data_concordance_manual.drop(index_diff, inplace=True)
    #reset the index
    previous_combined_data_concordance_manual.reset_index(inplace=True)
    combined_data_concordance_manual.reset_index(inplace=True)
    ##########################################################

    ##There is a chance that some index_rows have had new data added to them, so we will compare the previous duplicates index_rows to the current duplicates and see where thier values are different, make sure that we iterate over them in the manual data selection process
    #so find different rows in the duplicates:
    #first make the Datasets col a string so it can be compared
    a = previous_duplicates_manual.copy()
    a.Datasets = a.Datasets.astype(str)
    b = duplicates_manual.copy()
    b.Datasets = b.Datasets.astype(str)
    a.set_index(INDEX_COLS_no_year,inplace=True)
    b.set_index(INDEX_COLS_no_year,inplace=True)
    duplicates_diff = pd.concat([b, a]).drop_duplicates(keep=False)

    ##First update the iterator:
    #get the rows where the Dataselection method is manual
    manual_index_rows = previous_combined_data_concordance_manual.copy()

    #create a version where we rmeove Date
    manual_index_rows_no_date = manual_index_rows.copy()
    manual_index_rows_no_date.drop('Date', axis=1, inplace=True)
    #remove duplicates
    manual_index_rows_no_date.drop_duplicates(inplace=True)
    #now we want to remove any rows where the Dataselection method is manual so we dont overwrite them in selection process
    manual_index_rows_no_date_no_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method!='Manual']
    #but note that there are some rows where because we are missing any data for certain years then their index will be added to the iterator as well, so we need to remove these rows by searching for them:
    manual_index_rows_no_date_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method=='Manual']
    #now set index to same as iterator, so there is no Date col. 
    manual_index_rows_no_date_manual.set_index(INDEX_COLS_no_year, inplace=True)
    manual_index_rows_no_date_no_manual.set_index(INDEX_COLS_no_year, inplace=True)

    #remove rows that have changed in teh duplcuicates dfs from manual_index_rows_no_date_manual so they dont get removed from the iterator:
    manual_index_rows_no_date_manual = manual_index_rows_no_date_manual[~manual_index_rows_no_date_manual.index.isin(duplicates_diff.index)]

    #make sure theres no rows in no_manual that are in manual (this will remove all rows, regardless of date where one of teh rows has been selected manually)
    manual_index_rows_no_date_no_manual = manual_index_rows_no_date_no_manual[~manual_index_rows_no_date_no_manual.index.isin(manual_index_rows_no_date_manual.index)]

    #KEEP only these rows in the iterator by finding the index rows in both dfs 
    iterator = iterator[iterator.index.isin(manual_index_rows_no_date_no_manual.index)]

    ##And now update the combined_data_concordance_manual that we were orignially using:
    #find the index_rows that we have already set in previous_combined_data_concordance_manual and remove them from combined_data_concordance_manual, then replace them with the rows from previous_combined_data_concordance_manual.
    previous_combined_data_concordance_manual.set_index(INDEX_COLS_no_year,inplace=True)
    combined_data_concordance_manual.set_index(INDEX_COLS_no_year,inplace=True)

    #remove the different rows in the duplicates from the index_rows we are about to remove from combined_data_concordance_manual, so we dont miss them and instead go over any index_rows we have new data for
    previous_combined_data_concordance_manual = previous_combined_data_concordance_manual[~previous_combined_data_concordance_manual.index.isin(duplicates_diff.index)]

    #now remove these index_rows from combined_data_concordance_manual
    combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(previous_combined_data_concordance_manual.index)]

    #replace these rows in combined_data_concordance_manual by using concat
    combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, previous_combined_data_concordance_manual])

    #reset index
    combined_data_concordance_manual.reset_index(inplace=True)

    return iterator, combined_data_concordance_manual


##########################################################################################


def pickup_incomplete_manual_progress(progress_csv, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS):
    ##PICKUP LATEST PROGRESS
    #we want to save the state the user was last at so they can pick up on where they last left off. So load in the data from progress_csv, see what values have had their Dataselection method set to manual and remove them from the iterator.
    #we will then replace those rows in combined_data_concordance_manual
    #there is one subtle part to this, in that an index row will only be removed from the iterator if all the years of that index row have been set to manual. So if the user has set some years to manual but not all, for example by quitting halfway through choosing all the values for a chart, then we will not remove that index row from the iterator and the user should redo it. BUT if during the selection process the user skips rows then this will save that (they can be identified as rows where the dataselection method is manual but the value and num datapoints are NaN - they will be interpolated later)
    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('Date')
    #make the date column a datetime object
    progress_csv.Date = progress_csv.Date.apply(lambda x: str(x) + '-12-31')
    
    ##########################################################
    #We will make sure there are no index rows in the previous dataframes that are not in the current dataframes
    #first the duplicates
    previous_duplicates_manual.set_index(INDEX_COLS, inplace=True)
    duplicates_manual.set_index(INDEX_COLS, inplace=True)
    #remove the rows that are in the previous duplicates but not in the current duplicates
    index_diff = previous_duplicates_manual.index.difference(duplicates_manual.index)
    previous_duplicates_manual.drop(index_diff, inplace=True)
    #reset the index
    previous_duplicates_manual.reset_index(inplace=True)
    duplicates_manual.reset_index(inplace=True)

    #now for previous_combined_data_concordance_manual and combined_data_concordance_manual
    progress_csv.set_index(INDEX_COLS, inplace=True)
    combined_data_concordance_manual.set_index(INDEX_COLS, inplace=True)
    #remove the rows that are in the previous duplicates but not in the current duplicates
    index_diff = progress_csv.index.difference(combined_data_concordance_manual.index)
    progress_csv.drop(index_diff, inplace=True)
    #reset the index
    progress_csv.reset_index(inplace=True)
    combined_data_concordance_manual.reset_index(inplace=True)
    ##########################################################

    progress_csv.set_index(INDEX_COLS, inplace=True)

    ##There is a chance that some index_rows have had new data added to them, so we will compare the previous duplicates index_rows to the current duplicates and see where thier values are different, make sure that we iterate over them in the manual data selection process
    #so find different rows in the duplicates:
    #first make the Datasets col a string so it can be compared
    a = previous_duplicates_manual.copy()
    a.Datasets = a.Datasets.astype(str)
    b = duplicates_manual.copy()
    b.Datasets = b.Datasets.astype(str)
    a.set_index(INDEX_COLS_no_year,inplace=True)
    b.set_index(INDEX_COLS_no_year,inplace=True)
    duplicates_diff = pd.concat([b, a]).drop_duplicates(keep=False)
    
    #First update the iterator:
    #get the rows where the Dataselection method is manual
    manual_index_rows = progress_csv.copy()
    #create a version where we rmeove Date
    manual_index_rows_no_date = manual_index_rows.copy()
    manual_index_rows_no_date.reset_index(inplace=True)
    manual_index_rows_no_date.drop('Date', axis=1, inplace=True)
    #remove duplicates
    manual_index_rows_no_date.drop_duplicates(inplace=True)
    #now we want to remove any rows where the Dataselection method is manual 
    manual_index_rows_no_date_no_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method!='Manual']
    #but note that there are some rows where because we are missing any data for certain years then their index will be added to the iterator as well, so we need to remove these rows by searching for them:
    manual_index_rows_no_date_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method=='Manual']
    #now set index to same as iterator
    manual_index_rows_no_date_manual.set_index(INDEX_COLS_no_year, inplace=True)
    manual_index_rows_no_date_no_manual.set_index(INDEX_COLS_no_year, inplace=True)
    
    #remove rows that have changed in teh duplcuicates dfs from manual_index_rows_no_date_manual so they dont get removed from the iterator:
    manual_index_rows_no_date_manual = manual_index_rows_no_date_manual[~manual_index_rows_no_date_manual.index.isin(duplicates_diff.index)]

    #make sure theres no rows in no_manual that are in manual
    manual_index_rows_no_date_no_manual = manual_index_rows_no_date_no_manual[~manual_index_rows_no_date_no_manual.index.isin(manual_index_rows_no_date_manual.index)]
    #KEEP only these rows in the iterator by finding the index rows in both dfs 
    iterator = iterator[iterator.index.isin(manual_index_rows_no_date_no_manual.index)]

    ##And now update the combined_data_concordance_manual:
    #find the rows that we have already set in combined_data_concordance_manual and remove them, then replace them with the new rows
    manual_index_rows = manual_index_rows[manual_index_rows.Dataset_selection_method=='Manual']

    #remove the different rows in the duplicates from the index_rows we are about to remove from combined_data_concordance_manual, so we dont miss them and instead go over any index_rows we have new data for
    #set index to index_cols_no_year
    manual_index_rows.reset_index(inplace=True)
    manual_index_rows.set_index(INDEX_COLS_no_year, inplace=True)
    manual_index_rows = manual_index_rows[~manual_index_rows.index.isin(duplicates_diff.index)]
    manual_index_rows.reset_index(inplace=True)
    #make date a part of the index in combined_data_concordance_manual
    combined_data_concordance_manual.set_index(INDEX_COLS,inplace=True)
    manual_index_rows.set_index(INDEX_COLS,inplace=True)

    #now remove these rows from combined_data_concordance_manual
    combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(manual_index_rows.index)]
    #replace these rows in combined_data_concordance_manual by using concat
    combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, manual_index_rows])
    #remove Date from index
    combined_data_concordance_manual.reset_index(inplace=True)

    return iterator, combined_data_concordance_manual

##########################################################################################

def combine_manual_and_automatic_output(combined_data_concordance_automatic,combined_data_concordance_manual,INDEX_COLS):

    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('Date')

    #COMBINE MANUAL AND AUTOMATIC DATA SELECTION OUTPUT DFs
    #join the automatic and manual datasets so we can compare the dataset  columns from both the manual dataset automatic dataset
    #create a final_dataset column. This will be filled with the dataset in the automatic column, except where the values in the manual and automatic dataset columns are different, for which we will use the value in the manual dataset.

    #reset and set index of both dfs to INDEX_COLS
    combined_data_concordance_manual = combined_data_concordance_manual.set_index(INDEX_COLS)
    combined_data_concordance_automatic = combined_data_concordance_automatic.reset_index().set_index(INDEX_COLS)

    #join manual and automatic data selection dfs
    final_combined_data_concordance = combined_data_concordance_manual.merge(combined_data_concordance_automatic, how='outer', left_index=True, right_index=True, suffixes=('_manual', '_auto'))

    #we will either have dataset names or nan values in the manual and automatic dataset columns. We want to use the manual dataset column if it is not nan, otherwise use the automatic dataset column:
    #first set the dataset_selection_method column based on that criteria, and then use that to set other columns
    final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_auto'].notnull(), 'Final_dataset_selection_method'] = 'Automatic'
    #if the manual dataset column is not nan then use that instead
    final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_manual'].notnull(), 'Final_dataset_selection_method'] = 'Manual'

    #Now depending on the value of the final_dataset_selection_method column, we can set final_value and final_dataset columns
    #if the final_dataset_selection_method is manual then use the manual dataset column
    final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Manual', 'Value'] = final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Manual','Value_manual']
    final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Manual', 'Dataset'] = final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Manual','Dataset_manual']
    #if the final_dataset_selection_method is automatic then use the automatic dataset column
    final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Automatic', 'Dataset'] = final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Automatic','Dataset_auto']
    final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Automatic', 'Value'] = final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Automatic','Value_auto']

    #drop cols ending in _manual and _auto
    final_combined_data_concordance.drop(columns=[col for col in final_combined_data_concordance.columns if col.endswith('_manual') or col.endswith('_auto')], inplace=True)

    return final_combined_data_concordance



############################################



def interpolate_missing_values(final_combined_data_concordance,INDEX_COLS,automatic_interpolation_method = 'linear', automatic_interpolation = True, FILE_DATE_ID='',percent_of_values_needed_to_interpolate=0.7, INTERPOLATION_LIMIT=3):
    # #TEMPORARY
    # #remove data from dates after 2019 (have to format it as it is currently a string)
    # final_combined_data_concordance['Date'] = pd.to_datetime(final_combined_data_concordance['Date'])
    # final_combined_data_concordance = final_combined_data_concordance.loc[final_combined_data_concordance.Date < '2020-01-01']

    final_combined_data_concordance = final_combined_data_concordance.reset_index()
    #Remove year from the current cols without removing it from original list, and set it as a new list
    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('Date')

    #set interpoaltuion methods. If the method is one that requires an order then make sure to add the order as a number right after the method name
    interpolation_methods = ['linear', 'spline2', 'spline4'] 
    #start a timer
    start_time = datetime.datetime.now()

    ########################################################
    print('Starting interpolation, this may take a few minutes...')
    #load progress
    filename ='intermediate_data/interpolation/{}_progress.csv'.format(FILE_DATE_ID)
    progress = pd.read_csv(filename)
    #set index rows
    progress = progress.set_index(INDEX_COLS_no_year)
    final_combined_data_concordance = final_combined_data_concordance.set_index(INDEX_COLS_no_year)
    #check for any cols called 'Unnamed' and remove them (this error occurs often here so we will just remove them if they exist)
    progress = progress.loc[:, ~progress.columns.str.contains('Unnamed')]
    print('Unnamed cols removed from progress dataframe in interpolation')
    # in case the user wants to stop the code running and come back to it later, we will save the data after each measure is done. This will allow the user to pick up where they left off. So here we will load in the data from the last saved file and update the final_combined_data_concordance df so any of the users changes are kept. This will also identify if the data currently set in combined data concordance for that index row has changed since last time. If it has then we will not overwrite it an the user will ahve to inrerpolate it again.
    #so first identify where there are either 'interpolation' or 'interpolation skipped' in the final_dataselection_method column:
    previous_progress = progress.loc[progress.Final_dataset_selection_method.isin(['interpolation', 'interpolation skipped'])]
    previous_progress_no_interpolation = progress.loc[~progress.Final_dataset_selection_method.isin(['interpolation', 'interpolation skipped'])]
    #for each index row in previous_progress, check if teh sum of Value col in previous_progress_no_interpolation, matches that for final_combined_data_concordance. If not then some data point has changed and we will need to reinterpolate the data, otherwiose we can just replace the data in final_combined_data_concordance with the data in previous_progress
    for index_row in previous_progress_no_interpolation.index.unique():
        if index_row in final_combined_data_concordance.index.unique():
            #calc sum of values for index_row
            previous_progress_no_interpolation_value_sum = previous_progress_no_interpolation.loc[index_row].Value.sum()
            final_combined_data_concordance_value_sum = final_combined_data_concordance.loc[index_row].Value.sum()

            if previous_progress_no_interpolation_value_sum != final_combined_data_concordance_value_sum:
                #if the values are different then we need to reinterpolate the data (so leave the data in final_combined_data_concordance as it is)
                # previous_progress = previous_progress.drop(index=index_row)
                # print('The data for {} has changed since last time, so the interpolation will need to be redone'.format(index_row))
                pass

            else:
                #if vlkauyes are the same we want to replace all rows for this index row in final_combined_data_concordance with the rows in progress so we can pick up where we left off
                final_combined_data_concordance = final_combined_data_concordance.drop(index=index_row)
                final_combined_data_concordance = pd.concat([final_combined_data_concordance, progress.loc[index_row]])

    #reset index
    final_combined_data_concordance = final_combined_data_concordance.reset_index()
    print('Time taken so far: {}'.format(datetime.datetime.now() - start_time))
    print('Previous progress loaded, interpolation will now start')
    ########################################################
    
    #chekc for dsuplicates in final_combined_data_concordance
    #if there are then we should return them asnd ask the user to remove them
    duplicates = final_combined_data_concordance[final_combined_data_concordance.duplicated()]
    if duplicates.shape[0] > 0:
        print('Duplicates found in final_combined_data_concordance. Please remove them and try again. Returning them now.')
        return duplicates, final_combined_data_concordance

    #split the data into measures to make the dataset smaller and so faster to work on
    skipped_rows = []
    import matplotlib
    matplotlib.use('TkAgg')
    
    measures = final_combined_data_concordance.Measure.unique().tolist()
    measures.sort()
    for measure in measures:
        break_loop = False
        final_combined_data_concordance_measure = final_combined_data_concordance.loc[final_combined_data_concordance.Measure == measure]
        #set indexes
        final_combined_data_concordance_measure = final_combined_data_concordance_measure.set_index(INDEX_COLS_no_year)
        #get the unique index rows for the measure
        unique_index_rows = final_combined_data_concordance_measure.index.unique()
        #get the number of iterations for the measure
        number_of_iterations = len(unique_index_rows)
        #tell the user how many iterations there are for the measure
        print('There are {} iterations for the measure {}'.format(number_of_iterations, measure))
        #print the time taken so far
        print('Time taken so far: {}'.format(datetime.datetime.now() - start_time))

        ##############################################################

        #interpolate missing values by iterating through the unique index rows
        for index_row in unique_index_rows:
            #get the data for the current index row
            current_data = final_combined_data_concordance_measure.loc[index_row]
            
            #if data is only one row long then we can't interpolate so we will skip it
            if (len(current_data.shape) == 1):
                #if value is NaN then we can just set Final_dataset_selection_method to 'not enough values to interpolate' in final_combined_data_concordance_measure, else we will leave it as it is
                skipped_rows.append(index_row)
                temp = final_combined_data_concordance_measure.loc[index_row]
                if np.isnan(temp.Value):
                    temp.Final_dataset_selection_method ='not enough values to interpolate'
                final_combined_data_concordance_measure.loc[index_row] = temp
                continue

            elif current_data.shape[0] == 1:
                #same thing but for some reason the shape is (1, 4) instead of (4,). this means we need to do it differently
                skipped_rows.append(index_row)
                temp = final_combined_data_concordance_measure.loc[index_row]
                if np.isnan(temp.Value[0]):
                    temp.Final_dataset_selection_method ='not enough values to interpolate'
                #set final_combined_data_concordance_measure to temp (this will only change the Final_dataset_selection_method column where the Value is NA to indicate that we skipped it)
                final_combined_data_concordance_measure.loc[index_row] = temp
                continue

            #prepare intepolation using a spline and a linear method (could ask chat gpt how to choose the method since there is probably some mathemetical basis for it)
            #so we will create a value column for each interpolation method and fill it with the values in the current data. Then run each interpoaltion on that column and fill in the missing values. Then we will plot the data and ask the user to choose which method to use
            #create a new dataframe to hold the data for the current index row

            #filter for only the index row
            current_data_interpolation = current_data.loc[index_row]
            #reset index and sort by year
            current_data_interpolation = current_data_interpolation.reset_index().sort_values(by='Date')
        
            #if tehre are no na values in the Values column where Final_dataset_selection_method is not == 'interpolation skipped' then we will not interpolate because there is no data to interpolate
            if current_data_interpolation.loc[current_data_interpolation.Final_dataset_selection_method != 'interpolation skipped', 'Value'].isnull().sum() == 0:
                continue
            #doulbe check that less than 70% of the data is missing. If not then we will not interpolate
            if current_data_interpolation['Value'].isnull().sum() / len(current_data_interpolation) > percent_of_values_needed_to_interpolate:
                skipped_rows.append(index_row)#we can check on this later to see if we should have interpolated these rows
                #also set Final_dataset_selection_method where it is NA to 'not enough values to interpolate'
                current_data_interpolation.loc[current_data_interpolation['Value'].isnull(), 'Final_dataset_selection_method'] = 'not enough values to interpolate'
                #set final_combined_data_concordance_measure to the current data df (this will only change the Final_dataset_selection_method column where the Value is NA to indicate that we skipped it)
                current_data_interpolation = current_data_interpolation.set_index(INDEX_COLS_no_year)
                #drop the index row from final_combined_data_concordance_measure
                final_combined_data_concordance_measure = final_combined_data_concordance_measure.drop(index_row)
                #then add the current data df to final_combined_data_concordance_measure
                final_combined_data_concordance_measure = pd.concat([final_combined_data_concordance_measure, current_data_interpolation.loc[index_row]])  
                #reset index
                current_data_interpolation = current_data_interpolation.reset_index()
                continue

            ########################################################################################################################################################

            if not automatic_interpolation:
                #MANUAL INTERPOLATION
                #set up plot axes and such
                plt.close('all')
                fig, ax = plt.subplots()
                plt.ion()
                plt.show()
                interpolation_methods_current = interpolation_methods.copy()
                #create a new column for each interpolation method
                for interpolation_method in interpolation_methods:
                    #if the method is one that requires an order then it will have  a number as well so check if the value has a number in it
                    if re.search(r'\d', interpolation_method):
                        #get the order of the polynomial
                        order = int(re.search(r'\d', interpolation_method).group())
                        #set the interpolation method to the string when you remove the number
                        interpolation_method_string = re.sub(r'\d', '', interpolation_method)
                        #try the interpolation but it couyld fail ebcause the order is too high
                        try:
                            #interpolate the values for each interpolation method
                            current_data_interpolation[interpolation_method] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string, order=order, limit_direction='both', limit=INTERPOLATION_LIMIT)
                            #set up line in plot
                            ax.plot(current_data_interpolation['Date'], current_data_interpolation[interpolation_method], label=interpolation_method)
                            print('Polynomial interpolation succeeded for {} with method {} and order {}'.format(index_row, interpolation_method_string, order))
                        except:
                            #if the interpolation fails then notify the user and skip this method
                            print('Polynomial interpolation failed for {} with method {} and order {}'.format(index_row, interpolation_method_string, order))
                            #print error
                            print('Polynomial interpolation failure error printout: ', sys.exc_info())#TESTIONG
                            #remove teh method from the list of methods for this #it seems like something went wrong!!!  loop
                            interpolation_methods_current.remove(interpolation_method)
                            continue
                    else:
                        current_data_interpolation[interpolation_method] = current_data_interpolation['Value']
                        #interpolate the values for each interpolation method
                        current_data_interpolation[interpolation_method] = current_data_interpolation['Value'].interpolate(method=interpolation_method, limit_direction='both', limit=INTERPOLATION_LIMIT)
                        #set up line in plot
                        ax.plot(current_data_interpolation['Date'], current_data_interpolation[interpolation_method], label=interpolation_method)
                
                #plot original line as well but using a different marker
                ax.plot(current_data_interpolation['Date'], current_data_interpolation['Value'], label='original', marker='o')

                #set up legend
                ax.legend()
                #set up title
                ax.set_title('Interpolation methods for {}'.format(index_row))
                #set up x axis label
                ax.set_xlabel('Date')
                #slightly slant the x axis labels
                for tick in ax.get_xticklabels():
                    tick.set_rotation(45)
                #set up y axis label
                ax.set_ylabel('Value')
                #set background as white
                ax.set_facecolor('white')
                plt.draw()#show(block=True)#False)
                plt.pause(3)#needed to give the script time to show the plot before asking for user input # plt.show(block=False)

                #ask the user to choose which method to use
                print('{}: {}'.format('0', 'Skip this row'))
                for i in range(len(interpolation_methods_current)):
                    print('{}: {}'.format(i+1, interpolation_methods_current[i]))

                user_input_correct = False
                wrong_user_input = None
                user_input = None
                interpolation_method = None
                while user_input_correct == False:
                    user_input = input('Choose interpolation method: ')
                    if user_input == '0':
                        user_input_correct = True#user skipped. 
                        continue
                    try:
                        interpolation_method = interpolation_methods_current[int(user_input)-1]
                        #set interpolated value to that column and remove all interpoaltion columns
                        current_data_interpolation['interpolated_value'] = current_data_interpolation[interpolation_method]
                        Final_dataset_selection_method = 'interpolation'
                        current_data_interpolation = current_data_interpolation.drop(columns=interpolation_methods_current)
                        user_input_correct = True
                    except:
                        if wrong_user_input == user_input:
                            print('You have input the same incorrect value twice. Quitting program to save progress')
                            #need to quit the process but not create an error, so the stuff that has been done is saved
                            break_loop = True
                            user_input_correct = True#i think i can jsut rbeak the while loop too
                            #sys.exit()#change this
                        else:
                            print('Invalid input. Try again, if you input the same incorrect value again we will quit the program')
                            wrong_user_input = user_input
                if user_input == '0':
                    #if this happens then we will set the Final_dataset_selection_method col to 'interpolation skipped'
                    Final_dataset_selection_method = 'interpolation skipped'
                    #double check that that worked #TODO
                    print('Final_dataset_selection_method for {} is {}'.format(index_row, final_combined_data_concordance_measure.loc[index_row, 'Final_dataset_selection_method']))
                if break_loop:
                    plt.close('all')
                    break#this occurs if the user inputs the same incorrect value twice, which will probvably occur if they want to quit current process.
            ########################################################################################################################################################
            else:
                #AUTOMATIC INTERPOLATION
                #Here, if the order is too high for the spline method then it will fail so we will try the spline method with a lower order and if that fails then we will try the linear method

                #set the interpolation method to whatever was determined as the best method before.
                interpolation_method = automatic_interpolation_method
                #if the method is one that requires an order then it will have  a number as well so check if the value has a number in it
                if re.search(r'\d', interpolation_method):
                    order = int(re.search(r'\d', interpolation_method).group())
                    interpolation_method_string = re.sub(r'\d', '', interpolation_method)
                    try:
                        #interpolate the values for the interpolation method
                        current_data_interpolation['interpolated_value'] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string, order=order, limit_direction='both', limit=INTERPOLATION_LIMIT)
                        Final_dataset_selection_method = 'interpolation'
                    except:
                        #spline method order is too high, try lower order and if that fails then try linear method
                        interpolated = False
                        for order in range(1, order, -1):
                            try:
                                #interpolate the values for the interpolation method
                                current_data_interpolation['interpolated_value'] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string, order=order, limit_direction='both', limit=INTERPOLATION_LIMIT)
                                interpolated = True
                                Final_dataset_selection_method = 'interpolation'
                                break
                            except:
                                continue

                        #if the spline method failed then try the linear method
                        if interpolated == False:
                            try:#this shouldnt fail but just in case
                                interpolation_method_string = 'linear'
                                #interpolate the values for the interpolation method
                                current_data_interpolation['interpolated_value'] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string, limit_direction='both', limit=INTERPOLATION_LIMIT)
                                Final_dataset_selection_method = 'interpolation'
                            except:
                                #if the linear method fails then set the Final_dataset_selection_method to 'interpolation skipped'
                                Final_dataset_selection_method = 'interpolation skipped'
                                print('interpolation failed for {}, so it has been recorded as "interpolation skipped"'.format(index_row))
                else:   
                    #interpolate the values for the interpolation method that doesnt    require an order
                    try:#this shouldnt fail but just in case
                        interpolation_method_string = interpolation_method
                        #interpolate the values for the interpolation method
                        current_data_interpolation['interpolated_value'] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string, limit_direction='both', limit=INTERPOLATION_LIMIT)
                        Final_dataset_selection_method = 'interpolation'
                    except:
                        #if the linear method fails then set the Final_dataset_selection_method to 'interpolation skipped'
                        Final_dataset_selection_method = 'interpolation skipped'
                        print('interpolation failed for {}, so it has been recorded as "interpolation skipped"'.format(index_row))

            ########################################################################################################################################################
            ##FINALIZE INTERPOLATION
            #where Value is NaN set Value to the interpolation value and set Final_dataset_selection_method to 'interpolated'
            current_data_interpolation.loc[current_data_interpolation['Value'].isna(), 'Final_dataset_selection_method'] = Final_dataset_selection_method

            if Final_dataset_selection_method == 'interpolation':
                current_data_interpolation.loc[current_data_interpolation['Value'].isna() & current_data_interpolation['interpolated_value'].notna(), 'Value'] = current_data_interpolation['interpolated_value']
                #testing 
                #check if there are any values where interpolated_value is nan as well as Value. this doesnt seem to occur but best to be safe
                if current_data_interpolation.loc[current_data_interpolation['Value'].isna() & current_data_interpolation['interpolated_value'].isna()].shape[0] > 0:
                    print('there are values where interpolated_value is nan as well as Value')
                    print(current_data_interpolation.loc[current_data_interpolation['Value'].isna() & current_data_interpolation['interpolated_value'].isna()])
                    #throw error
                    raise ValueError('there are values where interpolated_value is nan as well as Value')
            elif Final_dataset_selection_method == 'interpolation skipped':
                pass
            else:
                print('something is wrong with the Final_dataset_selection_method for {}'.format(index_row))
                #throw error
                raise ValueError('something is wrong with the Final_dataset_selection_method for {}'.format(index_row))

            #set the index to the original index#.reset_index()
            current_data_interpolation = current_data_interpolation.set_index(INDEX_COLS_no_year)
            #all done with the current index row, so set the data for the current index row to the current_data_interpolation dataframe
            #drop the index row from final_combined_data_concordance_measure
            final_combined_data_concordance_measure = final_combined_data_concordance_measure.drop(index_row)
            #then add the current data df to final_combined_data_concordance_measure
            final_combined_data_concordance_measure = pd.concat([final_combined_data_concordance_measure, current_data_interpolation.loc[index_row]])

        #make the changes for this measure to the original dataframe and then save that dataframe as csv file to checkpoitn our progress
        final_combined_data_concordance_measure = final_combined_data_concordance_measure.reset_index()#Date is nA? why?
        #set the index to the original index
        final_combined_data_concordance_measure = final_combined_data_concordance_measure.set_index(INDEX_COLS)

        final_combined_data_concordance.set_index(INDEX_COLS, inplace=True)

        #repalce the original data with the new data where the index rows amtch
        final_combined_data_concordance.loc[final_combined_data_concordance_measure.index] = final_combined_data_concordance_measure.loc[final_combined_data_concordance_measure.index]

        final_combined_data_concordance.reset_index(inplace=True)

        #now save progress
        filename ='intermediate_data/interpolation/{}_progress.csv'.format(FILE_DATE_ID)
        final_combined_data_concordance.to_csv(filename)
        if break_loop:
            break#this occurs if the user inputs the same incorrect value twice, which will probvably occur if they want to quit current process.
    plt.close('all')
    print('Finished all interpolation')
    #print time it took to run the program
    print('Time taken to run program: {}'.format(datetime.datetime.now() - start_time))
    
    #and just quickly separate the dataset and source coplumns by splitting the value on any $ signs
    final_combined_data_concordance['Source'] = final_combined_data_concordance['Dataset'].str.split('$').str[1]
    #remove any spaces
    final_combined_data_concordance['Source'] = final_combined_data_concordance['Source'].str.strip()

    final_combined_data_concordance['Dataset'] = final_combined_data_concordance['Dataset'].str.split('$').str[0]
    #remove any spaces
    final_combined_data_concordance['Dataset'] = final_combined_data_concordance['Dataset'].str.strip()

    #and make new_final_combined_data by removing any NA values
    new_final_combined_data = final_combined_data_concordance.dropna(subset=['Value'])

    return new_final_combined_data,final_combined_data_concordance




