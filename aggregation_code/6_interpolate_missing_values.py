#This will intepolate missing values in the data when the missing value is between two values. There is an option for the user to pick the inteprolation method eg. spline and what order spline it is. The manual method will alsso show the user what the intpoaltion will look like. The method will record in the dataframe that the datapoint was interpolated.

#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import sys
import matplotlib.pyplot as plt

#if using jupyter notebook then set the backend to inline so that the graphs are displayed in the notebook instead of in a new window

# %matplotlib inline

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

import data_selection_functions as data_selection_functions

PRINT_GRAPHS_AND_STATS = False

#%%
#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./output_data/', '_final_combined_data_concordance.csv')
FILE_DATE_ID = 'DATE{}'.format(file_date)
# FILE_DATE_ID = 'DATE20221212'

# final_combined_data_concordance.to_csv('./output_data/{}_final_combined_data_concordance.csv'.format(FILE_DATE_ID))
final_combined_data_concordance = pd.read_csv('./output_data/{}_final_combined_data_concordance.csv'.format(FILE_DATE_ID))

#TEMP if there is a col called 'index' then remove it
if 'index' in final_combined_data_concordance.columns:
    final_combined_data_concordance = final_combined_data_concordance.drop(columns=['index'])#it must be happening in the dataaselection process
#%%
#TEMPORARY
#remove data from dates after 2019 (have to format it as it is currently a string)
final_combined_data_concordance['Date'] = pd.to_datetime(final_combined_data_concordance['Date'])
final_combined_data_concordance = final_combined_data_concordance.loc[final_combined_data_concordance.Date < '2020-01-01']
#%%
INDEX_COLS = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium','Transport Type','Drive','Fuel_Type','Scope']
#Remove year from the current cols without removing it from original list, and set it as a new list
INDEX_COLS_no_year = INDEX_COLS.copy()
INDEX_COLS_no_year.remove('Date')
#%%
#set indexes
# final_combined_data_concordance = final_combined_data_concordance.reset_index().set_index(INDEX_COLS_no_year)
# unique_index_rows = final_combined_data_concordance.index.unique()
# #order by unique rows 
# final_combined_data_concordance = final_combined_data_concordance.sort_index()

#%%
#set interpoaltuion methods. If the method is one that requires an order then make sure to add the order as a number right after the method name
interpolation_methods = ['linear', 'spline2', 'spline4']
automatic_interpolation_method = 'linear'
automatic_interpolation = True
#%%
#because this was taking a long time to run we will set up some code to save the data after checkpoints are reached, and also provide the user with some inidcation of progress via timing. #TBH its not that logn nbow
#count number of iterations:
number_of_measures = len(final_combined_data_concordance.Measure.unique())
#start a timer
start_time = datetime.datetime.now()

#%%
# in case the user wants to stop the code running and come back to it later, we will save the data after each measure is done. This will allow the user to pick up where they left off. So here we will load in the data from the last saved file and update the final_combined_data_concordance df so it 
#%%
#chekc for dsuplicates in final_combined_data_concordance
final_combined_data_concordance.duplicated().sum()
print('There are {} duplicates in the final_combined_data_concordance dataframe'.format(final_combined_data_concordance.duplicated().sum()))
duplicates = final_combined_data_concordance[final_combined_data_concordance.duplicated()]
#TODO oragnise the above/


#%% 
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
        current_data_interpolation = current_data_interpolation.loc[index_row]
        #reset index and sort by year
        current_data_interpolation = current_data_interpolation.reset_index().sort_values(by='Date')

        #if tehre are no na values in the Values column where Final_dataset_selection_method is not == 'interpolation skipped' then we will not interpolate because there is no data to interpolate
        if current_data_interpolation.loc[current_data_interpolation.Final_dataset_selection_method != 'interpolation skipped', 'Value'].isnull().sum() == 0:
            continue
        #doulbe check that less than 70% of the data is missing. If not then we will not interpolate
        if current_data_interpolation['Value'].isnull().sum() / len(current_data_interpolation) > 0.7:
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
    final_combined_data_concordance.to_csv(filename)
    if break_loop:
        break#this occurs if the user inputs the same incorrect value twice, which will probvably occur if they want to quit current process.
plt.close('all')
print('Finished all interpolation')
#PLEASE NOTE I DONT THINK THE ABOVE IS WORKING THERES QUITE A FEW DUPLICATES!

#IT SEEMS THERE ARE SOME VALUES THAT ARE NA IN THE OUTPUT EVEN THOUGHT THE DATA SELEECTION METHOD IS INTERPOLATION. 
#Actually it seems like its ok?? GReat!
#%%
#print time it took to run the program
print('Time taken to run program: {}'.format(datetime.datetime.now() - start_time))

#%%
#and just quickly separate the dataset and source coplumns by splitting the value on any $ signs
final_combined_data_concordance['Source'] = final_combined_data_concordance['Dataset'].str.split('$').str[1]
#remove any spaces
final_combined_data_concordance['Source'] = final_combined_data_concordance['Source'].str.strip()

final_combined_data_concordance['Dataset'] = final_combined_data_concordance['Dataset'].str.split('$').str[0]
#remove any spaces
final_combined_data_concordance['Dataset'] = final_combined_data_concordance['Dataset'].str.strip()
#%%
# #save the final combined data concordance to a csv file with date id 
# file_date = datetime.datetime.now().strftime("%Y%m%d_%H%M")
# FILE_DATE_ID = 'DATE{}'.format(file_date)
final_combined_data_concordance.to_csv('output_data/{}_interpolated_combined_data_condordance.csv'.format(FILE_DATE_ID), index=False)

#and make new_final_combined_data by removing any NA values
new_final_combined_data = final_combined_data_concordance.dropna(subset=['Value'])
#save as our final dataset
new_final_combined_data.to_csv('output_data/{}_interpolated_combined_data.csv'.format(FILE_DATE_ID), index=False)

#%%
