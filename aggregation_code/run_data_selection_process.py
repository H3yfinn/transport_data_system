#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import sys
import re
import data_selection_functions as data_selection_functions
import utility_functions as utility_functions
import matplotlib.pyplot as plt

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')
sys.path.append('./aggregation_code')
#%%
#create code to run the baove functions
INDEX_COLS = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium','Transport Type','Drive','Fuel_Type','Frequency', 'Scope']

#MANUAL DATA SELECTION VARIABLES
pick_up_where_left_off = True
import_previous_selection = True
run_only_on_rows_to_select_manually = False

#load data
# FILE_DATE_ID = 'DATE20221205'
use_all_data = False
use_9th_edition_set =True 
if use_all_data:
    #run aggreagtion code file
    exec(open("./aggregation_code/1_aggregate_cleaned_datasets.py").read())
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    combined_dataset = pd.read_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))
    combined_data_concordance= pd.read_csv('intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
elif use_9th_edition_set:
    exec(open("./aggregation_code/1_aggregate_cleaned_dataset_9th_edition.py").read())
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/9th_dataset/', 'combined_dataset_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    combined_dataset = pd.read_csv('intermediate_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID))
    combined_data_concordance= pd.read_csv('intermediate_data/9th_dataset/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
else:
    print('Please set use_all_data or use_9th_edition_set to True')

##############################################################################
#%%
FILE_DATE_ID = ''

#%%
duplicates = data_selection_functions.identify_duplicates(combined_dataset, INDEX_COLS)

combined_data_concordance_automatic,combined_data_concordance_manual,combined_data_concordance_iterator,duplicates_auto,duplicates_auto_with_year_index,combined_data_automatic,duplicates_manual,combined_data = data_selection_functions.prepare_data_for_selection(combined_data_concordance,combined_dataset,duplicates,INDEX_COLS,EARLIEST_YEAR = "2010-01-01",    LATEST_YEAR = '2020-01-01')
#%%
run_automatic =True
if run_automatic:
    combined_data_concordance_automatic, rows_to_select_manually_df = data_selection_functions.automatic_selection(combined_data_concordance_automatic,combined_data_automatic,duplicates_auto,duplicates_auto_with_year_index,INDEX_COLS, datasets_to_always_choose=[])
#     a = rows_to_select_manually_df.copy()#TODO REMOVE THIS
# rows_to_select_manually_df = a.copy()#TODO REMOVE THIS
#%%
#load combined_data_concordance_manual now so we can use it later if we need to
previous_combined_data_concordance_manual = pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID))
previous_duplicates_manual = pd.read_csv('intermediate_data/data_selection/{}_duplicates_manual.csv'.format(FILE_DATE_ID))
progress_csv = pd.read_csv('intermediate_data/data_selection/{}_progress.csv'.format(FILE_DATE_ID))
# previous_duplicates_manual = duplicates_manual.copy()#TODO REMOVE THIS
#reset duplicates_manual, combined_data_concordance_manual index
duplicates_manual = duplicates_manual.reset_index()
combined_data_concordance_manual = combined_data_concordance_manual.reset_index()
#%%

#########################SET ME TO SET VARIABLES FOR FUNCTION
pick_up_where_left_off=True
import_previous_selection=True
run_only_on_rows_to_select_manually=True
manually_chosen_rows_to_select=None
user_edited_combined_data_concordance_iterator=None
previous_combined_data_concordance_manual= previous_combined_data_concordance_manual
duplicates_manual=duplicates_manual
previous_duplicates_manual=previous_duplicates_manual
progress_csv=progress_csv
#########################
#%%
iterator, combined_data_concordance_manual = data_selection_functions.create_manual_data_iterator(combined_data_concordance_iterator,
INDEX_COLS,combined_data_concordance_manual,
rows_to_select_manually_df,
pick_up_where_left_off, 
import_previous_selection,run_only_on_rows_to_select_manually,
manually_chosen_rows_to_select,
user_edited_combined_data_concordance_iterator,
previous_combined_data_concordance_manual, 
duplicates_manual,
previous_duplicates_manual, progress_csv)

#%%
combined_data_concordance_manual, duplicates_manual, bad_index_rows, num_bad_index_rows = data_selection_functions.select_best_data_manual(combined_data_concordance_iterator,iterator,combined_data_concordance_manual,combined_data,duplicates_manual,INDEX_COLS,FILE_DATE_ID=FILE_DATE_ID)
#%%
final_combined_data_concordance = data_selection_functions.combine_manual_and_automatic_output(combined_data_concordance_automatic,combined_data_concordance_manual,INDEX_COLS)
#%%
#do interpolation:
new_final_combined_data,final_combined_data_concordance = data_selection_functions.interpolate_missing_values(final_combined_data_concordance,INDEX_COLS,automatic_interpolation_method = 'linear', automatic_interpolation = True,FILE_DATE_ID=FILE_DATE_ID,percent_of_values_needed_to_interpolate=0.7)

if use_all_data:
    final_combined_data_concordance.to_csv('output_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID), index=False)
    new_final_combined_data.to_csv('output_data/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)
elif use_9th_edition_set:
    final_combined_data_concordance.to_csv('output_data/9th_dataset/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID), index=False)
    new_final_combined_data.to_csv('output_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)
#%%







#throw error so we don't accidentally run this
# raise Exception('STOPPED HERE')











#%%



run = True
if run:
    automatic_interpolation_method = 'linear'
    automatic_interpolation = True
    FILE_DATE_ID=FILE_DATE_ID
    percent_of_values_needed_to_interpolate=0.7
    #FOR SOME REASON ALL OF VALUE IS NA
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
    # in case the user wants to stop the code running and come back to it later, we will save the data after each measure is done. This will allow the user to pick up where they left off. So here we will load in the data from the last saved file and update the final_combined_data_concordance df so any of the users changes are kept. This will also identify if the data currently set in combined data concordance for that index row has changed since last time. If it has then we will not overwrite it an the user will ahve to inrerpolate it again.
    #so first identify where there are either 'interpolation' or 'interpolation skipped' in the final_dataselection_method column:
    previous_progress_no_interpolation = progress.loc[~progress.Final_dataset_selection_method.isin(['interpolation', 'interpolation skipped'])]
    #for each index row in previous_progress_no_interpolation, check if teh sum of Value col  matches that for final_combined_data_concordance (this is possible because the would-be interpolated values would be NA in final_combined_data_concordance, if nothing had changed). If sum of value cols for that index row dont match then some data point has changed and we will need to reinterpolate the data. If they match we can just replace the data in final_combined_data_concordance with the data in previous_progress
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
        #return duplicates, final_combined_data_concordance

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
            
            #prepare intepolation using a spline and a linear method (could ask chat gpt how to choose the method since there is probably some mathemetical basis for it)
            #so we will create a value column for each interpolation method and fill it with the values in the current data. Then run each interpoaltion on that column and fill in the missing values. Then we will plot the data and ask the user to choose which method to use
            #create a new dataframe to hold the data for the current index row
            current_data_interpolation = current_data.copy()
            #filter for only the index row
            # current_data_interpolation = current_data_interpolation.loc[index_row]
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
                final_combined_data_concordance_measure.loc[index_row] = current_data_interpolation.loc[index_row]
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
                            current_data_interpolation[interpolation_method] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string, order=order, limit_direction='both')
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
                        current_data_interpolation[interpolation_method] = current_data_interpolation['Value'].interpolate(method=interpolation_method, limit_direction='both')
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
                        current_data_interpolation['interpolated_value'] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string, order=order, limit_direction='both')
                        Final_dataset_selection_method = 'interpolation'
                    except:
                        #spline method order is too high, try lower order and if that fails then try linear method
                        interpolated = False
                        for order in range(1, order, -1):
                            try:
                                #interpolate the values for the interpolation method
                                current_data_interpolation['interpolated_value'] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string, order=order, limit_direction='both')
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
                                current_data_interpolation['interpolated_value'] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string, limit_direction='both')
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
                        current_data_interpolation['interpolated_value'] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string, limit_direction='both')
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

            #set the index to the original index
            current_data_interpolation = current_data_interpolation.reset_index().set_index(INDEX_COLS_no_year)
            #all done with the current index row, so set the data for the current index row to the current_data_interpolation dataframe
            final_combined_data_concordance_measure.loc[index_row] = current_data_interpolation.loc[index_row]

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
        final_combined_data_concordance.to_csv(filename, index=False)
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

        # return new_final_combined_data,final_combined_data_concordance
















#%%
datasets_to_always_choose = []
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
    if measure == 'Stocks':
        break
    #RUN THE AUTOMATIC METHOD
    combined_data_concordance_automatic_measure, rows_to_select_manually_measure = data_selection_functions.automatic_method(combined_data_automatic_measure, combined_data_concordance_automatic_measure,duplicates_auto_measure,duplicates_auto_with_year_index_measure,datasets_to_always_choose,std_out_file = 'intermediate_data/data_selection/{}automatic_method.txt'.format(FILE_DATE_ID))

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

# %%





############################################################################################################################################################################









datasets_to_always_choose=[]


# def automatic_method(combined_data_automatic, combined_data_concordance_automatic,duplicates_auto,duplicates_auto_with_year_index, datasets_to_always_choose=[],std_out_file=None):
# # """#AUTOMATIC METHOD
# # #in the automatic method we will use the following rules in an order of priority from 1 being the highest priority to n being the lowest priority:
# # #1. if there are two or more datapoints for a given row and one is from the same dataset as the previous year for that same unique row then use that one
# # #2. if there are two or more datapoints for a given row and all but one dataset is missing in the next year for that same unique row then use the one that is not missing
# # #3. if there are two or more datapoints for a given row and one is closer and within 25% of the previous year, then use that one
# # #4 if none of the above apply then ask the user to manually select the best datapoint for each row, if it hasn't been done already in the past

# # Note that this method will tend to select datasets which have data avaialble for earlier years because of option 1."""
std_out_file = 'intermediate_data/data_selection/{}automatic_method.txt'.format(FILE_DATE_ID)
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
# return combined_data_concordance_automatic, rows_to_select_manually

# %%








##################################








#%%
#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import sys
import re
import data_selection_functions as data_selection_functions
import utility_functions as utility_functions
import matplotlib.pyplot as plt

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')
sys.path.append('./aggregation_code')

#create code to run the baove functions
INDEX_COLS = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium','Transport Type','Drive','Fuel_Type','Frequency', 'Scope']

#MANUAL DATA SELECTION VARIABLES
pick_up_where_left_off = True
import_previous_selection = True
run_only_on_rows_to_select_manually = False

#load data
# FILE_DATE_ID = 'DATE20221205'
use_all_data = False
use_9th_edition_set =True 
if use_all_data:
    #run aggreagtion code file
    exec(open("./aggregation_code/1_aggregate_cleaned_datasets.py").read())
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    combined_dataset = pd.read_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))
    combined_data_concordance= pd.read_csv('intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
elif use_9th_edition_set:
    exec(open("./aggregation_code/1_aggregate_cleaned_dataset_9th_edition.py").read())
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/9th_dataset/', 'combined_dataset_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    combined_dataset = pd.read_csv('intermediate_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID))
    combined_data_concordance= pd.read_csv('intermediate_data/9th_dataset/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
else:
    print('Please set use_all_data or use_9th_edition_set to True')

##############################################################################
FILE_DATE_ID = ''

duplicates = data_selection_functions.identify_duplicates(combined_dataset, INDEX_COLS)

combined_data_concordance_automatic,combined_data_concordance_manual,combined_data_concordance_iterator,duplicates_auto,duplicates_auto_with_year_index,combined_data_automatic,duplicates_manual,combined_data = data_selection_functions.prepare_data_for_selection(combined_data_concordance,combined_dataset,duplicates,INDEX_COLS,EARLIEST_YEAR = "2010-01-01",    LATEST_YEAR = '2020-01-01')
#%%
run_automatic =True
if run_automatic:
    combined_data_concordance_automatic, rows_to_select_manually_df = data_selection_functions.automatic_selection(combined_data_concordance_automatic,combined_data_automatic,duplicates_auto,duplicates_auto_with_year_index,INDEX_COLS, datasets_to_always_choose=[])
#     a = rows_to_select_manually_df.copy()#TODO REMOVE THIS
# rows_to_select_manually_df = a.copy()#TODO REMOVE THIS

#load combined_data_concordance_manual now so we can use it later if we need to
previous_combined_data_concordance_manual = pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID))
previous_duplicates_manual = pd.read_csv('intermediate_data/data_selection/{}_duplicates_manual.csv'.format(FILE_DATE_ID))
progress_csv = pd.read_csv('intermediate_data/data_selection/{}_progress.csv'.format(FILE_DATE_ID))
# previous_duplicates_manual = duplicates_manual.copy()#TODO REMOVE THIS
#reset duplicates_manual, combined_data_concordance_manual index
duplicates_manual = duplicates_manual.reset_index()
combined_data_concordance_manual = combined_data_concordance_manual.reset_index()

#########################SET ME TO SET VARIABLES FOR FUNCTION
pick_up_where_left_off=True
import_previous_selection=False
run_only_on_rows_to_select_manually=True
manually_chosen_rows_to_select=None
user_edited_combined_data_concordance_iterator=None
previous_combined_data_concordance_manual= previous_combined_data_concordance_manual
duplicates_manual=duplicates_manual
previous_duplicates_manual=previous_duplicates_manual
progress_csv=progress_csv
#########################

#create_manual_data_iterator
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
#%%
#now determine whether we want to import previous progress or not:
if import_previous_selection:
    # iterator, combined_data_concordance_manual = import_previous_runs_progress_to_manual(previous_combined_data_concordance_manual, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS)
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
#%%
pick_up_where_left_off = True
if pick_up_where_left_off:
    # iterator, combined_data_concordance_manual = pickup_incomplete_manual_progress(progress_csv, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS)
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


































# %%
