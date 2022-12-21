#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import pickle
import matplotlib.pyplot as plt
import warnings

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False

#ignore by message
warnings.filterwarnings("ignore", message="indexing past lexsort depth may impact performance")
#%%
def automatic_method(combined_data_automatic, combined_data_concordance_automatic,duplicates_auto,duplicates_auto_with_year_index, datasets_to_always_choose=[]):
        """#AUTOMATIC METHOD
        #in the automatic method we will use the following rules in an order of priority from 1 being the highest priority to n being the lowest priority:
        #1. if there are two or more datapoints for a given row and one is from the same dataset as the previous year for that same unique row then use that one
        #2. if there are two or more datapoints for a given row and all but one dataset is missing in the next year for that same unique row then use the one that is not missing
        #3. if there are two or more datapoints for a given row and one is closer and within 25% of the previous year, then use that one
        #4 if none of the above apply then ask the user to manually select the best datapoint for each row, if it hasn't been done already in the past
        
        Note that this method will tend to select datasets which have data avaialble for earlier years because of option 1."""
        #create an empty df from combined_data_concordance_automatic which will be the rows that we will need to select manually
        rows_to_select_manually = []
        for row_index in duplicates_auto.index:
                rows = duplicates_auto.loc[row_index]
                #loop through the years in rows
                year_list = rows.Year.unique()
                # if row_index== ('01_AUS',
                #        'Activity',
                #        '2w',
                #        'thousand_passenger_km',
                #        'road',
                #        'passenger',
                #        'bev'):
                #        break
                for row in rows.itertuples():
                        #get the number of datapoints
                        num_datapoints = row.Count
                        #get the datasets from the lists inside the Datasets column
                        datasets = row.Datasets
                        #extract year from the index
                        year = row.Year
                        #get previous year
                        previous_year = year - 1
                        #get the next year
                        next_year = year + 1

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
                                combined_data_concordance_automatic.loc[row_index_with_year, 'Value'] = combined_data_automatic.loc[row_index_with_year_and_dataset, 'Value']
                                continue

                        # Option 0
                        # if there is a dataset that is in our chosen dataset list then use that. This is a manual override
                        for dataset in datasets:
                                if dataset in datasets_to_always_choose:
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Dataset'] = dataset
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Dataset_selection_method'] = 'Automatic'
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Num_datapoints'] = num_datapoints
                                        #set value
                                        row_index_with_year_and_dataset = (*row_index_with_year, dataset)
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Value'] = combined_data_automatic.loc[row_index_with_year_and_dataset, 'Value']
                                        continue

                        #OPTION 1
                        #1. if there are two or more datapoints for a given row and one is from the same dataset as the previous year for that same unique row then use that one
                        #if there is a previous year row index
                        if previous_year in year_list:
                                #get the dataset we set for the previous year
                                previous_year_dataset = combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Dataset']
                                #if the previous year dataset is available for this year:
                                if previous_year_dataset in datasets:
                                        #set the value in combined_data_concordance_automatic for this year to the value for the same dataset used in the previous year
                                        combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Dataset'] = previous_year_dataset
                                        combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Dataset_selection_method'] = 'Automatic'
                                        combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Num_datapoints'] = num_datapoints
                                        #using combined data, extrasct the value using the row index + the chosen dataset
                                        row_index_with_previous_year_and_dataset = (*row_index_with_previous_year, previous_year_dataset)
                                        combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Value'] = combined_data_automatic.loc[row_index_with_previous_year_and_dataset, 'Value'].values[0]

                                        #let the user know what we are doing, including detailing the row index 
                                        print('Using automatic option 1. use the same dataset as the previous year for the same unique row. The row is: ', row_index)
                                        continue
                        #OPTION 2
                        #2. if there are two or more datapoints for a given row and all but one dataset is missing in the next year for that same unique row then use the one that is not missing
                        if next_year in year_list:
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
                                        combined_data_concordance_automatic.loc[row_index_with_year, 'Value'] = combined_data_automatic.loc[row_index_with_year_and_dataset, 'Value'].values[0]

                                        #let the user know what we are doing, including detailing the row index 
                                        print('Using automatic option 2. Use the dataset that is not missing in the next year for the same unique row. The row is: ', row_index)
                                        continue
                        #OPTION 3
                        #3. if there are two or more datapoints for a given row and one is closer and within 25% of the previous year, then use that one. As long as the previous year is not missing
                        #if there is a previous year row index
                        if previous_year in year_list:
                                #get the value for the previous year
                                previous_year_value = combined_data_concordance_automatic.loc[row_index_with_previous_year, 'Value']
                                if previous_year_value == None:
                                        pass
                                else:
                                        percent_diff_previous_year_list = []
                                        for dataset in datasets:
                                                #get the value for the current row and dataset
                                                row_index_with_year_and_dataset = (*row_index_with_year, not_missing_dataset)
                                                current_value = combined_data_automatic.loc[row_index_with_year_and_dataset, 'Value'].values[0]
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
                                                #set the value in combined_data_concordance_automatic for this year to the value for the same dataset used in the previous year
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Dataset'] = dataset
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Dataset_selection_method'] = 'Automatic'
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Num_datapoints'] = num_datapoints
                                                #using combined data, extrasct the value using the row index + the chosen dataset
                                                row_index_with_year_and_dataset = (*row_index_with_year, dataset)
                                                combined_data_concordance_automatic.loc[row_index_with_year, 'Value'] = combined_data_automatic.loc[row_index_with_year_and_dataset, 'Value'].values[0]

                                                #let the user know what we are doing, including detailing the row index 
                                                print('Using automatic option 3. Use the dataset that is within 25% of the previous year and closer to the previous year than the other value in for the same unique row. The row is: ', row_index)
                                                continue
                                        if percent_diff_previous_year < 0.5:
                                                print('OPTION 3. The closest dataset was within {}% of the previous year. The row is: '.format(round(percent_diff_previous_year*100,2), row_index))
                        #OPTION 4
                        #4 if none of the above apply then ask the user to manually select the best datapoint for each row, if it hasn't been done already in the past
                        #print('No automatic option available. Please manually select the best datapoint for each row. The row is: ', row_index)
                        #add row index to rows_to_select_manually 
                        rows_to_select_manually.append(row_index)
                
        return combined_data_concordance_automatic, rows_to_select_manually


########################################################################################################################################################
# 
#    
def graph_manual_data(data_for_plotting, index_row):
        #graph data in a line graph using the index from the unique comb ination to graph data out of the combined dataframe
        ##PLOT
        fig, ax = plt.subplots()
        #loop through the datasets and plot each one as a line
        for dataset in data_for_plotting.Dataset.unique():
                #filter for only the current dataset
                dataset_data = data_for_plotting[data_for_plotting.Dataset == dataset]
                #plot the data
                ax.plot(dataset_data.Year, dataset_data.Value, label=dataset)

        #finalise the plot by adding a legend, titles and subtitles and showing it in advance of user input
        ax.legend()
        ax.set_title('{}: {}\n - {}: {}, {}, {}, {}'.format(index_row[0], index_row[1], index_row[2], index_row[3], index_row[4], index_row[5], index_row[6]))
        ax.set_xlabel('Year')
        ax.set_ylabel('Value')
        #set background color to white so we can see title and subtitles
        ax.set_facecolor('white')
        # fig.savefig('./plotting_output/manual_data_selection/{}_{}.png'.format(FILE_DATE_ID, row.name))#what is row.name i wonder
        # plt.close(fig)
        plt.show(block=False)
        return fig
########################################################################################################################################################
#%%

def manual_user_input_function(data_for_plotting, index_row,  combined_data_concordance_manual, INDEX_COLS):       
        #ask the user what they want to choose by looping through the options list and the dataset list and asking the user to choose a number
        options = ['Keep the dataset "{}" for all consecutive years that the same combination of datasets is available', 'Keep the dataset "{}" for all years that the chosen dataset is available', 'Keep the dataset "{}" only for that year']
        
        years_to_ignore = []
        user_input = None
        for year in data_for_plotting.Year.unique():
                #double check year is not in years_to_ignore
                if year in years_to_ignore:
                        continue

                #filter for only the current year
                year_data = data_for_plotting[data_for_plotting.Year == year]
                
                #print options for the user to choose by looping through the options list and the dataset list
                i=1
                choice_dict = {}
                print('\n\n##############################\n\nFor the year {} and the combination of columns {} choose a number from the options below:\n\n'.format(year, index_row))
                print('0: Skip this year')
                for option in options:
                        for dataset in year_data.Dataset.unique():
                                print('{}: {}'.format(i, option.format(dataset)))
                                #add the option and dataset to a dictionary so that we can refer to it with the number the user inputs later
                                choice_dict[i] = [option, dataset]
                                i+=1
      
                #ask the user to input a number and then check that it is a valid number. Use a fucntion for this to reduce lines in this function
                user_input = manual_ask_and_parse_user_input(year, choice_dict)

                if user_input == 'quit':
                        return combined_data_concordance_manual, user_input
                        #sys.exit()#for testing
                elif user_input == 0:
                        #skip this year
                        continue
                else:
                        combined_data_concordance_manual, years_to_ignore = manual_apply_user_input_to_data(user_input, choice_dict, options, combined_data_concordance_manual, years_to_ignore,data_for_plotting, year_data, INDEX_COLS)

        return combined_data_concordance_manual, user_input

def manual_ask_and_parse_user_input(year, choice_dict):
        """If the user enters an invalid number, the function will print an error message and prompt the user to try again. If the user enters the same invalid number twice, the function will exit and save the user's progress. If the user enters a valid number, the function will check if the number is in a dictionary of choices, and if it is, the function will return True. If the number is not in the dictionary, the function will return False."""
        input_correct = False
        user_input = None
        while input_correct == False:
                try:
                        user_input = input('For the year {} please choose a number from the options above: '.format(year))
                        user_input = int(user_input)
                except ValueError:
                        print('Please enter a valid number from the options above. If you enter the same number again the program will exit, but your progress will be saved')
                        old_user_input = user_input
                        #create a try except to catch the error if the user enters the same number again
                        try:
                                user_input =  input('For the year {} please choose a number from the options above: '.format(year))
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
        if option == options[0]:
                #'Keep the dataset {} for all consecutive years that the same combination of datasets is available'
        
                #use datasets column to find all years with the same set of datasets. 
                set_of_datasets_in_this_year = year_data.Datasets[0]
                data_to_change = data_for_plotting.copy()

                #filter for only the chosen dataset in the dataset column
                data_to_change = data_to_change[data_to_change.Dataset == dataset]
                #filter for years after the current year
                data_to_change = data_to_change[data_to_change.Year >= year_data.Year[0]]
                         

                #filter for only the rows where the set of datasets is the same as the set of datasets for the current year.
                data_to_change = data_to_change[data_to_change.Datasets.isin([set_of_datasets_in_this_year])]
                       
                #keep only years that are consecutive from the current year
                #sort by year and reset the index
                data_to_change = data_to_change.reset_index().sort_values(by = ['Year'])               
                # create a new column that represents the difference between each year and the previous year
                data_to_change['Year_diff'] = data_to_change['Year'].diff()
                # shift the values in the year column down by one row
                data_to_change['Year_shifted'] = data_to_change['Year'].shift()
                #find minimum Year where Year_diff is not 1 and the Year is not year
                min_year = data_to_change.loc[(data_to_change['Year_diff'] != 1) & (data_to_change['Year'] != year_data.Year[0]), 'Year'].min()
                #if year is nan then set it to the max year +1 since that means all years are applicable.
                if pd.isna(min_year):
                        min_year = data_to_change['Year'].max()+1
                #filter for only the rows where the year is less than the min_year
                data_to_change = data_to_change.loc[data_to_change['Year'] < min_year]
                # drop the columns we created
                data_to_change = data_to_change.drop(columns = ['Year_diff', 'Year_shifted'])

                #make Year a part of the index
                data_to_change = data_to_change.set_index(INDEX_COLS)

        elif option == options[1]:
                # 'Keep the dataset "{}" for all years that the chosen dataset is available'
                #find the rows in data_for_plotting where the datasets are the same as for the chosen dataset
                data_to_change = data_for_plotting[data_for_plotting.Dataset == dataset]

                #filter for years after the current year
                data_to_change = data_to_change[data_to_change.Year >= year_data.Year[0]]
                #make Year a part of the index
                data_to_change = data_to_change.reset_index().set_index(INDEX_COLS)

        elif option == options[2]:
                #'Keep the dataset "{}" only for that year']
                #we can just use year_data here
                data_to_change = year_data[year_data.Dataset == dataset]
                
                #make Year a part of the index
                data_to_change = data_to_change.reset_index().set_index(INDEX_COLS)
                
        #and then find those rows in combined_data_concordance_manual 
        for rows_to_change in data_to_change.index:
                #make year a part of the index so we can use it to find the rows for those years in combined_data_concordance_manual
                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(INDEX_COLS)
                #update the dataset column to be the chosen dataset
                combined_data_concordance_manual.loc[rows_to_change].Dataset = dataset
                #set Dataset_selection_method to Manual too
                combined_data_concordance_manual.loc[rows_to_change].Dataset_selection_method = 'Manual'
                #set Value using the value in data_to_change
                combined_data_concordance_manual.loc[rows_to_change].Value = data_to_change.loc[rows_to_change].Value
                #set number of Num_datapoints to the same value as Count in data_to_change
                combined_data_concordance_manual.loc[rows_to_change].Num_datapoints = data_to_change.loc[rows_to_change].Count

        #Finally, since we may have set the data for years that will come up in the next loop, we will add these to a list of years to ignore
        years_to_ignore = np.append(years_to_ignore, data_to_change.reset_index().Year.unique())
        years_to_ignore = years_to_ignore.astype(int)
        years_to_ignore = np.unique(years_to_ignore)
        
        return combined_data_concordance_manual, years_to_ignore
#%%


















################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################




















test = False
if test:
        #CURRENT ISSUE. IT SEEMS THAT VALUES IN combined_data_concordance_manual ARE NOT IN combined_data
        #I would guess this is because we are creating the concordance manual using false information that combined data has thatn data for that dataset?
        #or is it because we are adding false information in the below function to the concordacne df via false information inn the duplkicates df?
        #  i GUESS THIS IS BECAUSE OF THE WAY WE CREAATE CONCORDANCE. THE PROBLEM IS WE WANT THE FUNCTION TO BE ABLE TO IGNORE THESE SITUATIONS OR EVEN NOT REACH TYHEM IN FIRSTN PLACE? 
        INDEX_COLS = ['Year', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type','Drive']
        #set the values we would normally import in the function call:
        #then load the values using pickle
        #create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
        file_date = datetime.datetime.now().strftime("%Y%m%d")
        FILE_DATE_ID = 'DATE{}'.format(file_date)
        FILE_DATE_ID = 'DATE20221209'
        with open('./intermediate_data/data_selection/{}_data_selection_manual.pickle'.format(FILE_DATE_ID), 'rb') as f:
                data_for_plotting, index_row,  combined_data_concordance_manual, INDEX_COLS = pickle.load(f)
        #ask the user what they want to choose by looping through the options list and the dataset list and asking the user to choose a number
        options = ['Keep the dataset "{}" for all years that the same combination of datasets is available', 'Keep the dataset "{}" for all years that the chosen dataset is available', 'Keep the dataset "{}" only for that year']
        
        years_to_ignore = []
        for year in data_for_plotting.Year.unique():
                #double check year is not in years_to_ignore
                if year in years_to_ignore:
                        continue

                #filter for only the current year
                year_data = data_for_plotting[data_for_plotting.Year == year]
                
                #ask the user what they want to choose by looping through the options list and the dataset list and asking the user to choose a number
                i=1
                choice_dict = {}
                choice_dict[0] = 'SKIP'
                print('\n\n##############################\n\nFor the year {} and the combination of columns {} choose a number from the options below:\n\n'.format(year, index_row))
                print('0: Skip this year')
                for option in options:
                        for dataset in year_data.Dataset.unique():
                                print('{}: {}'.format(i, option.format(dataset)))
                                #add the option and dataset to a dictionary so that we can refer to it with the number the user inputs later
                                choice_dict[i] = [option, dataset]
                                i+=1
      
                input_correct = False
                while input_correct == False:
                        try:
                                user_input = input('For the year {} please choose a number from the options above: '.format(year))
                                user_input = int(user_input)
                        except ValueError:
                                print('Please enter a valid number from the options above. If you enter the same number again the program will exit, but your progress will be saved')
                                old_user_input = user_input
                                #create a try except to catch the error if the user enters the same number again
                                try:
                                        user_input =  input('For the year {} please choose a number from the options above: '.format(year))
                                        user_input = int(user_input)
                                except ValueError:
                                        if user_input == old_user_input:
                                                print('Exiting function and saving progress')
                                                sys.exit()#used for test
                                                # return 'quit'
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

                #Find the input in the choiuces dictionary and then find the option and dataset that the user chose
                if user_input == 'quit':
                        # return combined_data_concordance_manual
                        sys.exit()#for testing
                elif user_input == 0:
                        #skip this year
                        continue
                else:
                        #find the option and dataset that the user chose
                        option = choice_dict[user_input][0]
                        dataset = choice_dict[user_input][1]
                        
                        #which matches what the user wants to change. Then by using that as an index, find the matching rows in our concordance datyaset, set the dataset, data_selection method and value columns.
                        if option == options[0]:
                                #'Keep the dataset {} for all consecutive years that the same combination of datasets is available'
                                
                                #use datasets column to find all years with the same set of datasets. 
                                set_of_datasets_in_this_year = year_data.Datasets[0]
                                data_to_change = data_for_plotting.copy()

                                #filter for only the chosen dataset in the dataset column
                                data_to_change = data_to_change[data_to_change.Dataset == dataset]
                                #filter for years after the current year
                                data_to_change = data_to_change[data_to_change.Year >= year]

                                test_x = False
                                if test_x:
                                        #create a test case where we remove rows where year is 2020 and dataset is'8th edition transport model (forecasted reference scenario)'
                                        data_to_change = data_to_change[~((data_to_change.Year == 2020) & (data_to_change.Dataset == '8th edition transport model (forecasted reference scenario)'))]

                                #filter for only the rows where the set of datasets is the same as the list of datasets for the current year. 
                                data_to_change = data_to_change[data_to_change.Datasets.isin([set_of_datasets_in_this_year])]
                                #keep only years that are consecutive from the current year
                                #sort by year and reset the index
                                data_to_change = data_to_change.reset_index().sort_values(by = ['Year'])               
                                # create a new column that represents the difference between each year and the previous year
                                data_to_change['Year_diff'] = data_to_change['Year'].diff()
                                # shift the values in the year column down by one row
                                data_to_change['Year_shifted'] = data_to_change['Year'].shift()
                                #find minimum Year where Year_diff is not 1 and the Year is not year
                                min_year = data_to_change.loc[(data_to_change['Year_diff'] != 1) & (data_to_change['Year'] != year), 'Year'].min()
                                #if year is nan then set it to the max year +1 since that means all years are applicable.
                                if pd.isna(min_year):
                                        min_year = data_to_change['Year'].max()+1
                                #filter for only the rows where the year is less than the min_year
                                data_to_change = data_to_change.loc[data_to_change['Year'] < min_year]
                                # drop the columns we created
                                data_to_change = data_to_change.drop(columns = ['Year_diff', 'Year_shifted'])

                                #make Year a part of the index
                                data_to_change = data_to_change.set_index(INDEX_COLS)


                        elif option == options[1]:
                                # 'Keep the dataset "{}" for all years that the chosen dataset is available'
                                #find the rows in data_for_plotting where the datasets are the same as for the chosen dataset
                                data_to_change = data_for_plotting[data_for_plotting.Dataset == dataset]

                                #filter for years after the current year
                                data_to_change = data_to_change[data_to_change.Year >= year]
                                #make Year a part of the index
                                data_to_change = data_to_change.reset_index().set_index(INDEX_COLS)

                        elif option == options[2]:
                                #'Keep the dataset "{}" only for that year']
                                #we can just use year_data here
                                data_to_change = year_data[year_data.Dataset == dataset]
                                
                                #make Year a part of the index
                                data_to_change = data_to_change.reset_index().set_index(INDEX_COLS)
                                
                        #and then find those rows in combined_data_concordance_manual 
                        for rows_to_change in data_to_change.index:
                                #make year a part of the index so we can use it to find the rows for those years in combined_data_concordance_manual
                                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(INDEX_COLS)
                                #update the dataset column to be the chosen dataset
                                combined_data_concordance_manual.loc[rows_to_change].Dataset = dataset
                                #set Dataset_selection_method to Manual too
                                combined_data_concordance_manual.loc[rows_to_change].Dataset_selection_method = 'Manual'
                                #set Value using the value in data_to_change
                                combined_data_concordance_manual.loc[rows_to_change].Value = data_to_change.loc[rows_to_change].Value
                                #set number of Num_datapoints to the same value as Count in data_to_change
                                combined_data_concordance_manual.loc[rows_to_change].Num_datapoints = data_to_change.loc[rows_to_change].Count
                                
                        #Finally, since we may have set the data for years that will come up in the next loop, we will add these to a list of years to ignore
                        years_to_ignore = np.append(years_to_ignore, data_to_change.reset_index().Year.unique())
                        print('Adding years {} to the list of years to ignore'.format(years_to_ignore))
                        years_to_ignore = years_to_ignore.astype(int)
                        years_to_ignore = np.unique(years_to_ignore)
                        

#%%
