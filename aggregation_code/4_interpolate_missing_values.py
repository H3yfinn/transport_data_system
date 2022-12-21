#interpolate missing values. This should fill in the value in data_selection_method column where we end up interpolating the value. But only interpolate the vlaue , not fill on either side.
#this is best here rather than other places as it allows us to have more understanding of the effects and be clear on what values in what datasets are interpolated or not

#using the interpolate method from pandas we will go through each index row and interpolate the missing values. when we interpolate we will provide an option for the user to choose the method of interpolation using the same graphing, input methods as used in the data selection manual method.
#once done we will plot the data

#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import pickle
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
FILE_DATE_ID = 'DATE{}'.format(file_date)
# FILE_DATE_ID = 'DATE20221212'

#%%
# final_combined_data_concordance.to_csv('./output_data/{}_final_combined_data_concordance.csv'.format(FILE_DATE_ID))
final_combined_data_concordance = pd.read_csv('./output_data/{}_final_combined_data_concordance.csv'.format(FILE_DATE_ID), index_col=0)

#%%
INDEX_COLS = ['Year', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type','Drive']
#Remove year from the current cols without removing it from original list, and set it as a new list
INDEX_COLS_no_year = INDEX_COLS.copy()
INDEX_COLS_no_year.remove('Year')

#%%
#set indexes
final_combined_data_concordance = final_combined_data_concordance.reset_index().set_index(INDEX_COLS_no_year)

#set interpoaltuion methods. If the method is one that requires an order then make sure to add the order as a number right after the method name
interpolation_methods = ['linear', 'spline2', 'spline4']
automatic_interpolation_method = 'linear'
automatic_interpolation = True
#%%
#interpolate missing values
for index_row in final_combined_data_concordance.index:
    #get the data for the current index row
    current_data = final_combined_data_concordance.loc[index_row]
    #prepare intepolation using a spline and a linear method (could ask chat gpt how to choose the method since there is probably some mathemetical basis for it)
    #so we will create a value column for each interpolation method and fill it with the values in the current data. Then run each interpoaltion on that column and fill in the missing values. Then we will plot the data and ask the user to choose which method to use
    #create a new dataframe to hold the data for the current index row
    current_data_interpolation = current_data.copy()
    #filter for only the index row
    current_data_interpolation = current_data_interpolation.loc[index_row]
    #reset index and sort by year
    current_data_interpolation = current_data_interpolation.reset_index().sort_values(by='Year')

    #doulbe check that less than 70% of the data is missing. If not then we will not interpolate
    if current_data_interpolation['Value'].isnull().sum() / len(current_data_interpolation) > 0.7:
        continue

    ########################################################################################################################################################

    if not automatic_interpolation:
        #MANUAL INTERPOLATION
        #set up plot axes and such
        fig, ax = plt.subplots()
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
                    current_data_interpolation[interpolation_method] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string, order=order)
                    #set up line in plot
                    ax.plot(current_data_interpolation['Year'], current_data_interpolation[interpolation_method], label=interpolation_method)
                except:
                    #if the interpolation fails then notify the user and skip this method
                    # print('Interpolation failed for {} with method {} and order {}'.format(index_row, interpolation_method_string, order))
                    #print error
                    print('ERROR printout: ', sys.exc_info()[0])
                    #remove teh method from the list of methods for this loop
                    interpolation_methods_current.remove(interpolation_method)
                    continue
            else:
                current_data_interpolation[interpolation_method] = current_data_interpolation['Value']
                #interpolate the values for each interpolation method
                current_data_interpolation[interpolation_method] = current_data_interpolation['Value'].interpolate(method=interpolation_method)
                #set up line in plot
                ax.plot(current_data_interpolation['Year'], current_data_interpolation[interpolation_method], label=interpolation_method)
        
        #plot original line as well but using a different marker
        ax.plot(current_data_interpolation['Year'], current_data_interpolation['Value'], label='original', marker='o')

        #set up legend
        ax.legend()
        #set up title
        ax.set_title('Interpolation methods for {}'.format(index_row))
        #set up x axis label
        ax.set_xlabel('Year')
        #set up y axis label
        ax.set_ylabel('Value')
        #set background as white
        ax.set_facecolor('white')
        plt.show(block=False)

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
                user_input_correct = True
                continue
            try:
                interpolation_method = interpolation_methods_current[int(user_input)-1]
                #set interpolated value to that column and remove all interpoaltion columns
                current_data_interpolation['interpolated_value'] = current_data_interpolation[interpolation_method]
                #drop all the columns that were used for interpolation
                current_data_interpolation = current_data_interpolation.drop(columns=interpolation_methods_current)
                user_input_correct = True
            except:
                if wrong_user_input == user_input:
                    print('You have input the same incorrect value twice. Quitting program')
                    sys.exit()
                else:
                    print('Invalid input. Try again, if you input the same incorrect value again we will quit the program')
                    wrong_user_input = user_input
        if user_input == '0':
            continue



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
                current_data_interpolation['interpolated_value'] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string, order=order)
            except:
                #spline method order is too high, try lower order and if that fails then try linear method
                interpolated = False
                for order in range(1, order, -1):
                    try:
                        #interpolate the values for the interpolation method
                        current_data_interpolation['interpolated_value'] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string, order=order)
                        interpolated = True
                        break
                    except:
                        continue

                #if the spline method failed then try the linear method
                if interpolated == False:
                    interpolation_method_string = 'linear'
                    #interpolate the values for the interpolation method
                    current_data_interpolation['interpolated_value'] = current_data_interpolation['Value'].interpolate(method=interpolation_method_string)
        else:   
            #interpolate the values for the interpolation method that doesnt    require an order
            current_data_interpolation['interpolated_value'] = current_data_interpolation['Value'].interpolate(method=interpolation_method)

    ########################################################################################################################################################
    ##FINALIZE INTERPOLATION
    #where Value is NaN and interpoaltion value is not NaN, set Value to the interpolation value and set Final_dataset_selection_method to 'interpolated'
    current_data_interpolation.loc[current_data_interpolation['Value'].isna() & current_data_interpolation['interpolated_value'].notna(), 'Final_dataset_selection_method'] = 'interpolated'
    current_data_interpolation.loc[current_data_interpolation['Value'].isna() & current_data_interpolation['interpolated_value'].notna(), 'Value'] = current_data_interpolation['interpolated_value']
    #et the index to the original index
    current_data_interpolation = current_data_interpolation.reset_index().set_index(INDEX_COLS_no_year)

    #all done with the current index row, so set the data for the current index row to the current_data_interpolation dataframe
    final_combined_data_concordance.loc[index_row] = current_data_interpolation#will thise work?


#%%

# #save the final combined data concordance to a csv file with date id including the hour and mins so that we can use it later. this also allows us to utilise half completed interpolations.
# file_date = datetime.datetime.now().strftime("%Y%m%d_%H%M")
# FILE_DATE_ID = 'DATE{}'.format(file_date)
final_combined_data_concordance.to_csv('output_data/{}_interpolated_combined_data_concordance.csv'.format(FILE_DATE_ID))

#%%