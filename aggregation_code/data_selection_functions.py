#%%
import datetime
import pandas as pd
import numpy as np
import os
import re
import pickle
# import plotly.express as px
# pd.options.plotting.backend = "plotly"#set pandas backend to plotly plotting instead of matplotlib

# import plotly.io as pio
# pio.renderers.default = "browser"#allow plotting of graphs in the interactive notebook in vscode #or set to notebook
# import plotly.graph_objects as go
# import plotly
import matplotlib.pyplot as plt
#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False
#%%
def automatic_method(duplicates,combined_data_concordance_automatic):
    #AUTOMATIC METHOD
    #in the automatic method we will use the following rules in an order of priority from 1 being the highest priority to n being the lowest priority:
    #1. if there are two or more datapoints for a given row and one is from the same dataset as the previous year for that same unique row then use that one
    #2. if there are two or more datapoints for a given row and one is from the same dataset as the next year for that same unique row then use that one
    #3. if there are two or more datapoints for a given row and one is within 25% of the previous year and closer to the previous year than the other value in for that same unique row then use that one
    #4 if none of the above apply then raise an error and ask the user to manually select the best datapoint for each row, if it hasn't been done already in the past
    for row in duplicates.itertuples():
        #get the row index
        row_index = row.Index
        #get the number of datapoints
        num_datapoints = row.Count
        #get the datasets from the lists inside the Datasets column
        datasets = row.Datasets
        #get the year
        year = row.Year
        #get the economy
        economy = row.Economy
        #get the measure
        measure = row.Measure
        #get the vehicle type
        vehicle_type = row['Vehicle Type']
        #get the unit
        unit = row.Unit
        #get the medium
        medium = row.Medium
        #get the transport type
        transport_type = row['Transport Type']
        #get the drive
        drive = row.Drive
        #get the previous year
        previous_year = year - 1
        #get the next year
        next_year = year + 1

        #first check the manual methods dataframe to see if the user has already manually selected the best datapoint for this row:
        #get the row from the manual methods dataframe#TODOTODOTODO

        #get the row index for the previous year
        previous_year_row_index = combined_data_concordance_automatic[(combined_data_concordance_automatic['Year']==previous_year) & (combined_data_concordance_automatic['Economy']==economy) & (combined_data_concordance_automatic['Measure']==measure) & (combined_data_concordance_automatic['Vehicle Type']==vehicle_type) & (combined_data_concordance_automatic['Unit']==unit) & (combined_data_concordance_automatic['Medium']==medium) & (combined_data_concordance_automatic['Transport Type']==transport_type) & (combined_data_concordance_automatic['Drive']==drive)].index
        #get the row index for the next year
        next_year_row_index = combined_data_concordance_automatic[(combined_data_concordance_automatic['Year']==next_year) & (combined_data_concordance_automatic['Economy']==economy) & (combined_data_concordance_automatic['Measure']==measure) & (combined_data_concordance_automatic['Vehicle Type']==vehicle_type) & (combined_data_concordance_automatic['Unit']==unit) & (combined_data_concordance_automatic['Medium']==medium) & (combined_data_concordance_automatic['Transport Type']==transport_type) & (combined_data_concordance_automatic['Drive']==drive)].index

        #OPTION 1
        #1. if there are two or more datapoints for a given row and one is from the same dataset as the previous year for that same unique row then use that one
        #if there is a previous year row index
        if len(previous_year_row_index) > 0:
                #get the dataset for the previous year
                previous_year_dataset = combined_data_concordance_automatic.loc[previous_year_row_index, 'Dataset'].values[0]
                #if the previous year dataset is available for this year:
                if previous_year_dataset in datasets:
                        #set the value in combined_data_concordance_automatic for this year to the value for the same dataset used in the previous year
                        combined_data_concordance_automatic.loc[row_index, 'Dataset'] = previous_year_dataset
                        combined_data_concordance_automatic.loc[row_index, 'Dataset_selection_method'] = 'Automatic'
                        combined_data_concordance_automatic.loc[row_index, 'Num_datapoints'] = num_datapoints
                        #let the user know what we are doing, including detailing all the values for this row
                        print('Using automatic option 1. use the same dataset as the previous year for the same unique row. The row is: Year: {}, Economy: {}, Measure: {}, Vehicle Type: {}, Unit: {}, Medium: {}, Transport Type: {}, Drive: {}'.format(year, economy, measure, vehicle_type, unit, medium, transport_type, drive))
                        continue
        #OPTION 2
        #2. if there are two or more datapoints for a given row and one is from the same dataset as the next year for that same unique row then use that one
        #if there is a next year row index
        if len(next_year_row_index) > 0:
                #get the dataset for the next year
                next_year_dataset = combined_data_concordance_automatic.loc[next_year_row_index, 'Dataset'].values[0]
                #if the next year dataset is available for this year:
                if next_year_dataset in datasets:
                        #set the value in combined_data_concordance_automatic for this year to the value for the same dataset used in the next year
                        combined_data_concordance_automatic.loc[row_index, 'Dataset'] = next_year_dataset
                        combined_data_concordance_automatic.loc[row_index, 'Dataset_selection_method'] = 'Automatic'
                        combined_data_concordance_automatic.loc[row_index, 'Num_datapoints'] = num_datapoints
                        #let the user know what we are doing, including detailing all the values for this row
                        print('Using automatic option 2. use the same dataset as the next year for the same unique row. The row is: Year: {}, Economy: {}, Measure: {}, Vehicle Type: {}, Unit: {}, Medium: {}, Transport Type: {}, Drive: {}'.format(year, economy, measure, vehicle_type, unit, medium, transport_type, drive))
                        continue
        #OPTION 3
        #3. if there are two or more datapoints for a given row and one is within 25% of the previous year and closer to the previous year than the other value in for that same unique row then use that one
        #if there is a previous year row index
        if len(previous_year_row_index) > 0:
                diff_previous_year_dict = {}
                for dataset in datasets:
                        #get the value for the previous year
                        previous_year_value = combined_data_concordance_automatic.loc[previous_year_row_index, 'Value'].values[0]
                        #get the value for the current row
                        current_value = row.Value
                        #get the difference between the current value and the previous year value
                        diff_previous_year = abs(current_value - previous_year_value)
                        #find abs % difference between the current value and the previous year value
                        percent_diff_previous_year = abs(diff_previous_year / previous_year_value)
                        #put it in a dictionary with the dataset as the key, and the difference as the value
                        diff_previous_year_dict[dataset] = percent_diff_previous_year
                #get the dataset with the smallest difference
                dataset = min(diff_previous_year_dict, key=diff_previous_year_dict.get)
                #if the value is <25% bigger or smaller, set the value in combined_data_concordance_automatic for this year to that value.
                if diff_previous_year_dict[dataset] < 0.25:
                        combined_data_concordance_automatic.loc[row_index, 'Dataset'] = dataset
                        combined_data_concordance_automatic.loc[row_index, 'Dataset_selection_method'] = 'Automatic'
                        combined_data_concordance_automatic.loc[row_index, 'Num_datapoints'] = num_datapoints
                        #let the user know what we are doing, including detailing all the values for this row
                        print('Using automatic option 3. use the dataset with the smallest difference to the previous year for the same unique row. The row is: Year: {}, Economy: {}, Measure: {}, Vehicle Type: {}, Unit: {}, Medium: {}, Transport Type: {}, Drive: {}'.format(year, economy, measure, vehicle_type, unit, medium, transport_type, drive))
                        continue

        #OPTION 4
        #4 if none of the above apply then raise an error and ask the user to manually select the best datapoint for each row, if it hasn't been done already in the past
        print('No automatic option available. Please manually select the best datapoint for each row. The row is: Year: {}, Economy: {}, Measure: {}, Vehicle Type: {}, Unit: {}, Medium: {}, Transport Type: {}, Drive: {}'.format(year, economy, measure, vehicle_type, unit, medium, transport_type, drive))
        #TODO I guess we just flow onto the manual method now but maybe not?
        
        automatically_picked_data = combined_data_concordance_automatic.copy()
        return automatically_picked_data

# #create manual methods object since we will have many functions within it
# class manual_methods:
#         def __init__(self):
#                pass

#         def manual_selection(self, combined_data_concordance_automatic, combined_data_concordance_manual, year, economy, measure, vehicle_type, unit, medium, transport_type, drive):

########################################################################################################################################################
# 
#    
def graph_manual_data(data_for_plotting, unique_combination):
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
        ax.set_title('{}: {}\n - {}: {}, {}, {}, {}'.format(unique_combination[0], unique_combination[1], unique_combination[2], unique_combination[3], unique_combination[4], unique_combination[5], unique_combination[6]))
        ax.set_xlabel('Year')
        ax.set_ylabel('Value')
        #set background color to white so we can see title and subtitles
        fig.patch.set_facecolor('white')
        # fig.savefig('./plotting_output/manual_data_selection/{}_{}.png'.format(FILE_DATE_ID, row.name))#what is row.name i wonder
        # plt.close(fig)
        plt.show()

########################################################################################################################################################
#%%

def user_input_function(options, data_for_plotting, unique_combination,  combined_data_concordance_manual, combined_data_for_value_extraction, current_cols_dataset, current_cols_no_year, current_cols):       
        #ask the user what they want to choose by looping through the options list and the dataset list and asking the user to choose a number
        for year in data_for_plotting.Year.unique():
                #filter for only the current year
                year_data = data_for_plotting[data_for_plotting.Year == year]
                
                #ask the user what they want to choose by looping through the options list and the dataset list and asking the user to choose a number
                i=1
                choice_dict = {}
                choice_dict[0] = 'SKIP'
                print('\n\n##############################\n\nFor the year {} and the combination of columns {} choose a number from the options below:\n\n'.format(year, unique_combination))
                print('0: Skip this year')
                for option in options:
                        for dataset in year_data.Dataset.unique():
                                print('{}: {}'.format(i, option.format(dataset)))
                                #add the option and dataset to a dictionary so that we can refer to it with the number the user inputs later
                                choice_dict[i] = [option, dataset]
                                i+=1
                run_this = False
                if run_this:
                                
                        input_correct = False
                        while input_correct == False:
                                user_choice = int(input('Please choose a number from the options above: '))
                                if user_choice in choice_dict.keys():
                                        input_correct = False
                                elif user_choice == 0:
                                        #skip this year
                                        print('Year {} skipped'.format(year))
                                        input_correct = True#TODO not 100% where the programe goes to after continue
                                else:
                                        print('Please enter a valid number from the options above')
                                        input_correct = False
                else:
                        user_choice = int(input('Please choose a number from the options above: '))

                #Find the input in the choiuces dictionary and then as long as the input is correct, proceed:
                if user_choice in choice_dict.keys():
                        if user_choice == 0:
                                #skip this year
                                continue
                        else:
                                #find the option and dataset that the user chose
                                option = choice_dict[user_choice][0]
                                dataset = choice_dict[user_choice][1]

                                if option == options[0]:
                                        #'Keep the dataset {} for all years that the same combination of datasets is available'

                                        #find the rows in data_for_plotting where the datasets are the same as for the current year
                                        same_datasets = data_for_plotting[data_for_plotting.Dataset.isin(year_data.Dataset.unique())]

                                        #and then find those rows in combined_data_concordance_manual and update the dataset column to be the chosen dataset
                                        for rows_to_change in same_datasets.index:
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset = dataset
                                                #set Dataset_selection_method to Manual
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset_selection_method = 'Manual'
                                                
                                                #extract this set of rows from combined_data_concordance_manual and make every column an index, from which we will look up those values in the combined_data_for_value_extraction df. 
                                                #First Use MultiIndex.get_locs to get the index of the rows we want to change
                                                index_rows = combined_data_concordance_manual.index.get_locs(rows_to_change)
                                                #Then use .iloc to get the rows we want to change using the index we got from .get_locs
                                                combined_data_concordance_manual_for_value_extraction = combined_data_concordance_manual.iloc[index_rows].reset_index().set_index(current_cols_dataset)
                                                
                                                #reset and set index of combined_data_concordance_manual so we can easily set the value column in combined_data_concordance_manual to be the value in combined_data_for_value_extraction
                                                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(current_cols_dataset)

                                                #find rows we want to set values for in combined_data_concordance_manual, and find those rows in combined_data_for_value_extraction, from which we'll grab the values
                                                for row in combined_data_concordance_manual_for_value_extraction.index:
                                                        #and then update the value column in combined_data_concordance_manual to be the value in combined_data_for_value_extraction
                                                        combined_data_concordance_manual.loc[row].Value = combined_data_for_value_extraction.loc[row].Value
                                                        
                                                #set the index back to the original
                                                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(current_cols_no_year)

                                        #remove those years from the data_for_plotting df so that we don't ask the user to choose again for those years
                                        data_for_plotting = data_for_plotting[~data_for_plotting.Year.isin(same_datasets.Year)]#TODO i dont think this works. 

                                elif option == options[1]:
                                        #'Keep the dataset {} for all years that the chosen dataset is available'

                                        #find the rows in data_for_plotting where the datasets are the same as for the chosen dataset
                                        same_datasets = data_for_plotting[data_for_plotting.Dataset.contains(dataset)]

                                        #and then find those rows in combined_data_concordance_manual and update the dataset column to be the chosen dataset
                                        for rows_to_change in same_datasets.index:
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset = dataset
                                                #set Dataset_selection_method to Manual
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset_selection_method = 'Manual'
                                                
                                                #extract this set of rows from combined_data_concordance_manual and make every column an index, from which we will look up those values in the combined_data_for_value_extraction df
                                                combined_data_concordance_manual_for_value_extraction = combined_data_concordance_manual.loc[rows_to_change].copy().reset_index().set_index(current_cols_dataset)

                                                #reset and set index of combined_data_concordance_manual so we can easily set the value column in combined_data_concordance_manual to be the value in combined_data_for_value_extraction
                                                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(current_cols_dataset)

                                                #find rows we want to set values for in combined_data_concordance_manual, and find those rows in combined_data_for_value_extraction, from which we'll grab the values
                                                for row in combined_data_concordance_manual_for_value_extraction.index:
                                                        #and then update the value column in combined_data_concordance_manual to be the value in combined_data_for_value_extraction
                                                        combined_data_concordance_manual.loc[row].Value = combined_data_for_value_extraction.loc[row].Value
                                                        
                                                #set the index back to the original
                                                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(current_cols_no_year)

                                        #remove those years from the data_for_plotting df so that we don't ask the user to choose again for those years
                                        data_for_plotting = data_for_plotting[~data_for_plotting.Year.isin(same_datasets.index)]

                                elif option == options[2]:
                                        #'Keep the dataset {} only for that year'
                                        #we can just use year_data here                                    
                                        
                                        #and then find those rows in combined_data_concordance_manual and update the dataset column to be the chosen dataset
                                        for rows_to_change in year_data.index:
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset = dataset
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset = dataset
                                                #set Dataset_selection_method to Manual
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset_selection_method = 'Manual'
                                                
                                                #extract this set of rows from combined_data_concordance_manual and make every column an index, from which we will look up those values in the combined_data_for_value_extraction df
                                                combined_data_concordance_manual_for_value_extraction = combined_data_concordance_manual.loc[rows_to_change].copy().reset_index().set_index(current_cols_dataset)

                                                #reset and set index of combined_data_concordance_manual so we can easily set the value column in combined_data_concordance_manual to be the value in combined_data_for_value_extraction
                                                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(current_cols_dataset)

                                                #find rows we want to set values for in combined_data_concordance_manual, and find those rows in combined_data_for_value_extraction, from which we'll grab the values
                                                for row in combined_data_concordance_manual_for_value_extraction.index:
                                                        #and then update the value column in combined_data_concordance_manual to be the value in combined_data_for_value_extraction
                                                        combined_data_concordance_manual.loc[row].Value = combined_data_for_value_extraction.loc[row].Value
                                                        
                                                #set the index back to the original
                                                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(current_cols_no_year)
                                else:
                                        print('Something went wrong for index {} and year {}'.format(data_for_plotting.index, year))
                                        
        return combined_data_concordance_manual


#%%
##########################################################################################
test = True
if test:
        #CURRENT ISSUE. IT SEEMS THAT VALUES IN combined_data_concordance_manual ARE NOT IN combined_data. i GUESS THIS IS BECAUSE OF THE WAY WE CREAATE CONCORDANCE. THE PROBLEM IS WE WANT THE FUNCTION TO BE ABLE TO IGNORE THESE SITUATIONS OR EVEN NOT REACH TYHEM IN FIRSTN PLACE? 
        current_cols = ['Year', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type','Drive']
        #Remove year from the current cols without removing it from original list, and set it as a new list
        current_cols_no_year = current_cols.copy()
        current_cols_no_year.remove('Year')
        #add the dataset column to the current cols without removing it from original list, and set it as a new list
        current_cols_dataset = current_cols.copy()
        current_cols_dataset.append('Dataset')
        #set the values we would normally import in the function call:
        #then load the values using pickle
        #create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
        file_date = datetime.datetime.now().strftime("%Y%m%d")
        FILE_DATE_ID = 'DATE{}'.format(file_date)
        FILE_DATE_ID = 'DATE20221206'
        with open('./intermediate_data/data_selection/{}_data_selection_manual.pickle'.format(FILE_DATE_ID), 'rb') as f:
                options, data_for_plotting, unique_combination,  combined_data_concordance_manual, combined_data_for_value_extraction = pickle.load(f)
        #ask the user what they want to choose by looping through the options list and the dataset list and asking the user to choose a number
        for year in data_for_plotting.Year.unique():
                #filter for only the current year
                year_data = data_for_plotting[data_for_plotting.Year == year]
                
                #ask the user what they want to choose by looping through the options list and the dataset list and asking the user to choose a number
                i=1
                choice_dict = {}
                choice_dict[0] = 'SKIP'
                print('\n\n##############################\n\nFor the year {} and the combination of columns {} choose a number from the options below:\n\n'.format(year, unique_combination))
                print('0: Skip this year')
                for option in options:
                        for dataset in year_data.Dataset.unique():
                                print('{}: {}'.format(i, option.format(dataset)))
                                #add the option and dataset to a dictionary so that we can refer to it with the number the user inputs later
                                choice_dict[i] = [option, dataset]
                                i+=1
                run_this = False
                if run_this:
                                
                        input_correct = False
                        while input_correct == False:
                                user_choice = int(input('Please choose a number from the options above: '))
                                if user_choice in choice_dict.keys():
                                        input_correct = False
                                elif user_choice == 0:
                                        #skip this year
                                        print('Year {} skipped'.format(year))
                                        input_correct = True#TODO not 100% where the programe goes to after continue
                                else:
                                        print('Please enter a valid number from the options above')
                                        input_correct = False
                else:
                        user_choice = int(input('Please choose a number from the options above: '))

                #Find the input in the choiuces dictionary and then as long as the input is correct, proceed:
                if user_choice in choice_dict.keys():
                        if user_choice == 0:
                                #skip this year
                                continue
                        else:
                                #find the option and dataset that the user chose
                                option = choice_dict[user_choice][0]
                                dataset = choice_dict[user_choice][1]

                                if option == options[0]:
                                        #'Keep the dataset {} for all years that the same combination of datasets is available'

                                        #find the rows in data_for_plotting where the datasets are the same as for the current year
                                        same_datasets = data_for_plotting[data_for_plotting.Dataset.isin(year_data.Dataset.unique())]
                                        
                                        # #make Year a part of the index so we can use it to find the rows in combined_data_concordance_manual
                                        # same_datasets = same_datasets.reset_index().set_index(current_cols)

                                        #and then find those rows in combined_data_concordance_manual and update the dataset column to be the chosen dataset
                                        for rows_to_change in same_datasets.index:
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset = dataset
                                                #set Dataset_selection_method to Manual
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset_selection_method = 'Manual'
                                                
                                                #extract this set of rows from combined_data_concordance_manual and make every column an index, from which we will look up those values in the combined_data_for_value_extraction df. 
                                                #First Use MultiIndex.get_locs to get the index of the rows we want to change
                                                index_rows = combined_data_concordance_manual.index.get_locs(rows_to_change)
                                                #Then use .iloc to get the rows we want to change using the index we got from .get_locs
                                                combined_data_concordance_manual_for_value_extraction = combined_data_concordance_manual.iloc[index_rows].reset_index().set_index(current_cols_dataset)
                                                
                                                #reset and set index of combined_data_concordance_manual so we can easily set the value column in combined_data_concordance_manual to be the value in combined_data_for_value_extraction
                                                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(current_cols_dataset)

                                                #find rows we want to set values for in combined_data_concordance_manual, and find those rows in combined_data_for_value_extraction, from which we'll grab the values
                                                for row in combined_data_concordance_manual_for_value_extraction.index:
                                                        #and then update the value column in combined_data_concordance_manual to be the value in combined_data_for_value_extraction
                                                        combined_data_concordance_manual.loc[row].Value = combined_data_for_value_extraction.loc[row].Value
                                                        
                                                #set the index back to the original
                                                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(current_cols)

                                        #remove those years from the data_for_plotting df so that we don't ask the user to choose again for those years
                                        data_for_plotting = data_for_plotting[~data_for_plotting.Year.isin(same_datasets.Year)]#TODO i dont think this works. 

                                elif option == options[1]:
                                        #'Keep the dataset {} for all years that the chosen dataset is available'

                                        #find the rows in data_for_plotting where the datasets are the same as for the chosen dataset
                                        same_datasets = data_for_plotting[data_for_plotting.Dataset.contains(dataset)]

                                        #and then find those rows in combined_data_concordance_manual and update the dataset column to be the chosen dataset
                                        for rows_to_change in same_datasets.index:
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset = dataset
                                                #set Dataset_selection_method to Manual
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset_selection_method = 'Manual'
                                                
                                                #extract this set of rows from combined_data_concordance_manual and make every column an index, from which we will look up those values in the combined_data_for_value_extraction df
                                                combined_data_concordance_manual_for_value_extraction = combined_data_concordance_manual.loc[rows_to_change].copy().reset_index().set_index(current_cols_dataset)

                                                #reset and set index of combined_data_concordance_manual so we can easily set the value column in combined_data_concordance_manual to be the value in combined_data_for_value_extraction
                                                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(current_cols_dataset)

                                                #find rows we want to set values for in combined_data_concordance_manual, and find those rows in combined_data_for_value_extraction, from which we'll grab the values
                                                for row in combined_data_concordance_manual_for_value_extraction.index:
                                                        #and then update the value column in combined_data_concordance_manual to be the value in combined_data_for_value_extraction
                                                        combined_data_concordance_manual.loc[row].Value = combined_data_for_value_extraction.loc[row].Value
                                                        
                                                #set the index back to the original
                                                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(current_cols_no_year)

                                        #remove those years from the data_for_plotting df so that we don't ask the user to choose again for those years
                                        data_for_plotting = data_for_plotting[~data_for_plotting.Year.isin(same_datasets.index)]

                                elif option == options[2]:
                                        #'Keep the dataset {} only for that year'
                                        #we can just use year_data here                                    
                                        
                                        #and then find those rows in combined_data_concordance_manual and update the dataset column to be the chosen dataset
                                        for rows_to_change in year_data.index:
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset = dataset
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset = dataset
                                                #set Dataset_selection_method to Manual
                                                combined_data_concordance_manual.loc[rows_to_change].Dataset_selection_method = 'Manual'
                                                
                                                #extract this set of rows from combined_data_concordance_manual and make every column an index, from which we will look up those values in the combined_data_for_value_extraction df
                                                combined_data_concordance_manual_for_value_extraction = combined_data_concordance_manual.loc[rows_to_change].copy().reset_index().set_index(current_cols_dataset)

                                                #reset and set index of combined_data_concordance_manual so we can easily set the value column in combined_data_concordance_manual to be the value in combined_data_for_value_extraction
                                                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(current_cols_dataset)

                                                #find rows we want to set values for in combined_data_concordance_manual, and find those rows in combined_data_for_value_extraction, from which we'll grab the values
                                                for row in combined_data_concordance_manual_for_value_extraction.index:
                                                        #and then update the value column in combined_data_concordance_manual to be the value in combined_data_for_value_extraction
                                                        combined_data_concordance_manual.loc[row].Value = combined_data_for_value_extraction.loc[row].Value
                                                        
                                                #set the index back to the original
                                                combined_data_concordance_manual = combined_data_concordance_manual.reset_index().set_index(current_cols_no_year)
                                else:
                                        print('Something went wrong for index {} and year {}'.format(data_for_plotting.index, year))
                                        
# %%
