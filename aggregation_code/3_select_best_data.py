
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
# FILE_DATE_ID = 'DATE20221206'

#%%
#this is for determining what are the best datpoints to use when there are two or more datapoints for a given time period

#load data
# FILE_DATE_ID = 'DATE20221205'
combined_data_concordance= pd.read_csv('intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
combined_data = pd.read_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))
#%%
#visualise what data points we have multiple data for by creating a table that shows what rows have duplicates when we ignore the value and dataset columns:

#But first things first we need to make sure there are no unexpected values in the dataset, otehrwsie they could be values which are miscategorised and therefore we might not realise when we have more than one of a given datapoint. We will do this by printing a strongly worded statement to the user to double check that there are no unexpected values in the dataset!
print('Dear User this is a strongly worded statement to check that there are no unexpected values in the dataset. Thank you for your time. ')

#%%

#open the csv file with duplicates in it
duplicates = pd.read_csv('intermediate_data/duplicates{}.csv'.format(FILE_DATE_ID))

#datasets column is being converted to a string for some reason, we will convert it back to a list
duplicates['Datasets'] = duplicates['Datasets'].apply(lambda x: eval(x))

#%%
INDEX_COLS = ['Year', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type','Drive']
#Remove year from the current cols without removing it from original list, and set it as a new list
INDEX_COLS_no_year = INDEX_COLS.copy()
INDEX_COLS_no_year.remove('Year')

#%%

#now for the rows that have duplicates we will find the best datapoint to use for each row:
#INTRO
#Instead of setting values we will be setting the value of the dataset columnn where there are two or more datapoints for a given row to the dataset that we should use for that row.
#we will provide an automatic way of doing this and then also allow the user to manually select the best datapoint for each row.


#%%
#STEP 1
#So first of all we need a dataframe which replicates the final dataframe but with no values in the dataset, value and duplicate columns (This dataframe is created in aggregation_code\1_aggregate_cleaned_datasets.py so we can just import that as combined_data_concordance)
#In the folowing scripts we will fill that df with the dataset that we choose to use for each row. Any rows where we dont have the dataset to use we will leave blank and that will end up as an NA

#we will also create a column to indicate if the value was manually selected or automatically selected and a column to indicate how many datapoints there were for that row
#later on we can fill the value column with the value from the dataset that we choose to use for each row, but for now we will leave it blank
combined_data_concordance['Dataset'] = None
combined_data_concordance['Num_datapoints'] = None
combined_data_concordance['Value'] = None
combined_data_concordance['Dataset_selection_method'] = None

#add Datasets and Count columns from duplicates_manual to combined_data for use in setting values
combined_data = combined_data.merge(duplicates.reset_index().set_index(INDEX_COLS)[['Datasets', 'Count']], how='left', left_on=INDEX_COLS, right_on=INDEX_COLS)

#set index of all the dfs we will use for AUTOMATIC AND MANUAL METHODS, using the INDEX_COLs:
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

#%%

#STEP 2
#AUTOMATIC METHOD
run_automatic = True
datasets_to_always_choose = []
if run_automatic:
       #open txt file to send all the printed output to while using the next function, instead of printing to the console
       with open('intermediate_data/data_selection/automatic_method{}.txt'.format(FILE_DATE_ID), 'w') as f:
              old_stdout = sys.stdout
              sys.stdout = f
              #run the automatic method
              combined_data_concordance_automatic, rows_to_select_manually = data_selection_functions.automatic_method(combined_data_automatic, combined_data_concordance_automatic,duplicates_auto,duplicates_auto_with_year_index,datasets_to_always_choose)
              
              sys.stdout = old_stdout

       #save
       combined_data_concordance_automatic.to_csv('intermediate_data/data_selection/{}_data_selection_automatic.csv'.format(FILE_DATE_ID))

       #convert list of tuples rows_to_select_manually to a dataframe with the same titles as INDEX_COLS
       rows_to_select_manually_df = pd.DataFrame(rows_to_select_manually, columns=INDEX_COLS_no_year)
       rows_to_select_manually_df.to_csv('intermediate_data/data_selection/{}_rows_to_select_manually.csv'.format(FILE_DATE_ID), index=False)
       

#save
#%%
#MANUAL METHOD
#This is a script to make it easier for the user to do the manual selection of datasets, especailly at the start of choosing the datasets as it will otehrwise be very timeconsuming to create all the spreadsheets.

# 1a.find the rows that have duplicates and then plot a timeseries for the full series which the duplicates are from
#so find the unique combinations of the following columns: Economy, Measure, Vehicle Type, Unit, Medium, Transport Type, Drive, then if any years of that combination have more than one dataset of data, then plot a timeseries for that unique combination of columns.

#%%
run_only_on_rows_to_select_manually = True
if run_only_on_rows_to_select_manually:
       #if this, only run the manual process on index rows where we couldnt find a dataset to use automatically for some year
       #since the automatic method is relatively strict there should be a large amount of rows to select manually
       #note that if any one year cannot be chosen automatically then we will have to choose the dataset manually for all years of that row
       if not 'rows_to_select_manually_df' in locals():
              rows_to_select_manually_df = pd.read_csv('intermediate_data/data_selection/{}_rows_to_select_manually.csv'.format(FILE_DATE_ID))
       iterator = rows_to_select_manually_df.reset_index().set_index(INDEX_COLS_no_year).drop_duplicates()
else:
       iterator = combined_data_concordance_iterator
#Create bad_index_rows as a empty df with the same columns as index_rows
bad_index_rows = pd.DataFrame(columns=combined_data_concordance_iterator.columns)
num_bad_index_rows = 0
#%%
#loop through the unique combinations, plot a timeseries for each one and then ask the user what dataset they want to choose for that instance and then update the dataset column in combined_data for that row to be the chosen dataset
for index_row in iterator.index:
       #filter for only the current row in our duplicates_manual dataset by using the row as an index
       try:
              current_row_filter = duplicates_manual.loc[duplicates_manual.index.isin([index_row])]
       except KeyError:
              num_bad_index_rows += 1
              print('Bad unique combination {}: {}. Please check it to find out what the error was'.format(num_bad_index_rows,index_row))
              bad_index_rows = bad_index_rows.append(combined_data_concordance_iterator.loc[index_row])
              #if the row is not in the duplicates_manual dataset then continue to the next row
              continue
       
       #identify how many datasets there are for each year by looking at the Count column
       #if there are rows where count is greater than 1 then we will plot and ask user to choose which dataset to use
       if current_row_filter.Count.max() > 1:
              #grab the data for this unique combination of columns from the combined_data df
              data_for_plotting = combined_data.loc[combined_data.index.isin([index_row])]

              ##PLOT
              #plot data for this unique index row
              fig = data_selection_functions.graph_manual_data(data_for_plotting, index_row)
              
              ##USER INPUT
              #ask user what dataset they want to choose for each year where a decision needs to be made, and then based on what they choose, update combined_data_concordance_manual 
              combined_data_concordance_manual, user_input = data_selection_functions.manual_user_input_function(data_for_plotting, index_row,  combined_data_concordance_manual, INDEX_COLS)
              
              plt.close(fig)
              if user_input == 'quit':
                     print('User input was quit on unique combination {}, so quitting the script and saving the progress to a csv'.format(index_row))
                     break
              else:
                     print('Finished with unique combination: {}'.format(index_row))
       else:
              pass#print('Unique combination: {} did not have more than 1 dataset available so no manual decision was needed'.format(index_row))
       
print('Finished with manual selection of datasets')
#%%
#save the combined_data_concordance_manual df to a csv
combined_data_concordance_manual.to_csv('./intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID))

# #save bad_index_rows to a csv
bad_index_rows.to_csv('./intermediate_data/data_selection/{}_bad_index_rows.csv'.format(FILE_DATE_ID))

#%%
#Some issues we may come across:
#if there is only one datapoint for a given row but interpolating between other dataset/s is obviously a better method
#if a single datapoint is obviously wrong and should be removed (could fix with havign a NONE dataset that is used for this purpose?)
#BIGGEST ISSUE is that we are probably going to have very few datapoints that are duplicates because we will get lots of datapoints for lots of different categories that dont match exactly. eg. ato data is not split into different vehicle or engine types so there are no duplicates between that and 8th edition activity data.
#ALSO with the manual sheet, if you dont know what years we have values for then it will eb hard to use. so

#EXTRA MANUAL METHOD: PLEASE NOTE THAT IF YOU WANT TO CHOOSE THE DATA MANUALLY THEN JJUST SET THE DATA WITHIN THE CSV {}_data_selection_manual.csv'.format(FILE_DATE_ID)), THIS WILL BE USED INSTEAD OF WHAT IS OUTPUT FROM THE MANUAL METHOD ABOVE
#%%
#COMBINE MANUAL AND AUTOMATIC DATA SELECTION OUTPUT DFs
#join the automatic and manual datasets so we can compare the dataset  columns from both the manual dataset automatic dataset
#create a final_dataset column. This will be filled with the dataset in the automatic column, except where the values in the manual and automatic dataset columns are different, for which we will use the value in the manual dataset.
test=True
if test:
       #load in the manual data selection df and automatic data selection df
       combined_data_concordance_manual = pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID), index_col=INDEX_COLS)
       combined_data_concordance_automatic = pd.read_csv('intermediate_data/data_selection/{}_data_selection_automatic.csv'.format(FILE_DATE_ID), index_col=INDEX_COLS)

do_this = True
if do_this:
       #join manual and automatic data selection dfs
       final_combined_data_concordance = combined_data_concordance_manual.merge(combined_data_concordance_automatic, how='outer', left_index=True, right_index=True, suffixes=('_manual', '_auto'))

       #we will either have dataset names or nan values in the manual and automatic dataset columns. We want to use the manual dataset column if it is not nan, otherwise use the automatic dataset column:
       #first set the dataset_selection_method column based on that criteria, and then use that to set other columns
       final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_auto'].notnull(), 'Final_dataset_selection_method'] = 'Automatic'
       #if the manual dataset column is not nan then use that instead
       final_combined_data_concordance.loc[final_combined_data_concordance['Dataset_manual'].notnull(), 'Final_dataset_selection_method'] = 'Manual'

       #Now depending on the value of the final_dataset_selection_method column, we can set final_value and final_dataset columns
       #if the final_dataset_selection_method is manual then use the manual dataset column
       final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Manual', 'Value'] = final_combined_data_concordance['Value_manual']
       final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Manual', 'Dataset'] = final_combined_data_concordance['Dataset_manual']
       #if the final_dataset_selection_method is automatic then use the automatic dataset column
       final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Automatic', 'Dataset'] = final_combined_data_concordance['Dataset_auto']
       final_combined_data_concordance.loc[final_combined_data_concordance['Final_dataset_selection_method'] == 'Automatic', 'Value'] = final_combined_data_concordance['Value_auto']

       #drop cols ending in _manual and _auto
       final_combined_data_concordance.drop(columns=[col for col in final_combined_data_concordance.columns if col.endswith('_manual') or col.endswith('_auto')], inplace=True)
             
       #save the final_combined_data_concordance df to a csv
       final_combined_data_concordance.to_csv('./output_data/{}_final_combined_data_concordance.csv'.format(FILE_DATE_ID))


#%%
