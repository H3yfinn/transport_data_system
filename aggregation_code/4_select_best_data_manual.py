#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import time
import sys
import matplotlib.pyplot as plt
import pickle
import data_selection_functions as data_selection_functions
#%%
combined_data_concordance_iterator = pd.read_pickle('intermediate_data/data_selection/combined_data_concordance_iterator.pkl')
combined_data_concordance_manual = pd.read_pickle('intermediate_data/data_selection/combined_data_concordance_manual.pkl')
combined_data = pd.read_pickle('intermediate_data/data_selection/combined_data.pkl')
duplicates_manual = pd.read_pickle('intermediate_data/data_selection/duplicates_manual.pkl')
#%%
INDEX_COLS = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type','Drive','Fuel_Type','Frequency','Scope']
#Remove year from the current cols without removing it from original list, and set it as a new list
INDEX_COLS_no_year = INDEX_COLS.copy()
INDEX_COLS_no_year.remove('Date')

FILE_DATE_ID = 'DATE' + str(datetime.datetime.now().strftime('%Y%m%d'))
#%%
#MANUAL METHOD
#This is a script to make it easier for the user to do the manual selection of datasets, especailly at the start of choosing the datasets as it will otehrwise be very timeconsuming to create all the spreadsheets.

# 1a.find the rows that have duplicates and then plot a timeseries for the full series which the duplicates are from
#so find the unique combinations of the following columns: Economy, Measure, Vehicle Type, Unit, Medium, Transport Type, Drive, then if any years of that combination have more than one dataset of data, then plot a timeseries for that unique combination of columns.

#%%
#CREATE ITERATOR 
#if we want to add to the rows_to_select_manually_df to check specific rows then set the below to True
add_to_rows_to_select_manually_df = False
run_only_on_rows_to_select_manually = True
if run_only_on_rows_to_select_manually:
       #if this, only run the manual process on index rows where we couldnt find a dataset to use automatically for some year
       #since the automatic method is relatively strict there should be a large amount of rows to select manually
       #note that if any one year cannot be chosen automatically then we will have to choose the dataset manually for all years of that row
       rows_to_select_manually_df = pd.read_csv('intermediate_data/data_selection/rows_to_select_manually.csv')
       iterator = rows_to_select_manually_df.copy()
       iterator.set_index(INDEX_COLS_no_year, inplace=True)
       iterator.drop_duplicates(inplace=True)#TEMP get rid of this later
elif add_to_rows_to_select_manually_df:
       #we can add rows form the combined_data_concordance_iterator to the rows_to_select_manually_df
       #for this example we will add all Stocks data (for the purpoose of betterunderstanding our stocks data!) and remove all the other data
       iterator = combined_data_concordance_iterator.reset_index()
       iterator = iterator[iterator['Measure']=='Stocks']
       #set the index to the index cols
       iterator.set_index(INDEX_COLS_no_year, inplace=True)
else:
       iterator = combined_data_concordance_iterator
#Create bad_index_rows as a empty df with the same columns as index_rows
bad_index_rows = pd.DataFrame(columns=combined_data_concordance_iterator.columns)
num_bad_index_rows = 0

#%%
#IMPORT PREVIOUS RUNS PROGRESS
#create option to import manual data selection from perveious runs to avoid having to do it again (can replace any rows where the Final_dataset_selection_method is na with where they are Manual in the imported csv)
import_manual_data_selection = True
if import_manual_data_selection:
       import_complete_FILE_DATE_ID = 'DATE20230126'#FILE_DATE_ID#
       final_combined_data_concordance_manual = pd.read_csv('./intermediate_data/data_selection/{}_data_selection_manual.csv'.format(import_complete_FILE_DATE_ID))
       # final_combined_data_concordance_manual = pd.read_csv('./intermediate_data/data_selection/DATE20230124_data_selection_manual CHECKPOINT.csv')
       
       # final_combined_data_concordance = final_combined_data_concordance.set_index(INDEX_COLS_no_year)

       #First update the iterator:
       #get the rows where the Dataselection method is manual
       manual_index_rows = final_combined_data_concordance_manual.copy()#[progress_csv.Dataset_selection_method=='Manual']
       #create a version where we rmeove Date
       manual_index_rows_no_date = manual_index_rows.copy()
       manual_index_rows_no_date.drop('Date', axis=1, inplace=True)
       #remove duplicates
       manual_index_rows_no_date.drop_duplicates(inplace=True)
       #now we want to remove any rows where the Dataselection method is manual so we dont overwrite them in selection process
       manual_index_rows_no_date_no_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method!='Manual']
       #but note that there are some rows where because we are missing any data for certain years then their index will be added to the iterator as well, so we need to remove these rows by searching for them:
       manual_index_rows_no_date_manual = manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method=='Manual']
       #now set index to same as iterator
       manual_index_rows_no_date_manual.set_index(INDEX_COLS_no_year, inplace=True)
       manual_index_rows_no_date_no_manual.set_index(INDEX_COLS_no_year, inplace=True)
       #make sure theres no rows in no_manual that are in manual
       manual_index_rows_no_date_no_manual = manual_index_rows_no_date_no_manual[~manual_index_rows_no_date_no_manual.index.isin(manual_index_rows_no_date_manual.index)]
       #KEEP only these rows in the iterator by finding the index rows in both dfs 
       iterator = iterator[iterator.index.isin(manual_index_rows_no_date_no_manual.index)]

       #and now update the combined_data_concordance_manual:
       #find the rows that we have already set in final_combined_data_concordance_manual and remove them from combined_data_concordance_manual, then replace them with the new rows
       #this means we can keep any updates made to the combined data concordance earlier #PLEASE NOTE THAT IF ANY ROWS HAVE BEEN ADDED FOR THE ALREADY CHOSEN ROWS THEN THIS WILL NOT UPDATE ACCORDING TO THAT
       manual_index_rows = final_combined_data_concordance_manual[final_combined_data_concordance_manual.Dataset_selection_method=='Manual']

       final_combined_data_concordance_manual.set_index(INDEX_COLS_no_year,inplace=True)
       combined_data_concordance_manual.set_index(INDEX_COLS_no_year,inplace=True)

       #now remove these rows from combined_data_concordance_manual
       combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(final_combined_data_concordance_manual.index)]
       #replace these rows in combined_data_concordance_manual by using concat
       combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, final_combined_data_concordance_manual])
       #reset index
       combined_data_concordance_manual.reset_index(inplace=True)

#%%
#PICKUP LATEST PROGRESS
#we want to save the state the user was last at so they can pick up on where they last left off. So load in the data from progress_csv, see what values have had their Dataselection method set to manual and remove them from the iterator.
#we will then replace those rwos in combined_data_concordance_manual
#there is one subtle part to this, in that an index row will only be removed from the iterator if all the years of that index row have been set to manual. So if the user has set some years to manual but not all, for example by quitting halfway through choosing all the values for a chart, then we will not remove that index row from the iterator and the user should redo it. BUT if the user skips rows trhen this will save that (they can be identified as rows where the dataselection method is manual but the value and num datapoints are NaN -  will need to double check hjow the interpoaltion affects this)
pick_up_where_left_off = False
if pick_up_where_left_off:
       progress_FILE_DATE_ID = 'DATE20230124'
       progress_csv = pd.read_csv('intermediate_data/data_selection/{}_progress.csv'.format(progress_FILE_DATE_ID))
       #make the date column a datetime object
       progress_csv.Date = progress_csv.Date.apply(lambda x: str(x) + '-12-31')
       #make date part of index 
       progress_csv.set_index(INDEX_COLS, inplace=True)

       #First update the iterator:
       #get the rows where the Dataselection method is manual
       manual_index_rows = progress_csv.copy()#[progress_csv.Dataset_selection_method=='Manual']
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
       #make sure theres no rows in no_manual that are in manual
       manual_index_rows_no_date_no_manual = manual_index_rows_no_date_no_manual[~manual_index_rows_no_date_no_manual.index.isin(manual_index_rows_no_date_manual.index)]
       #KEEP only these rows in the iterator by finding the index rows in both dfs 
       iterator = iterator[iterator.index.isin(manual_index_rows_no_date.index)]

       #and now update the combined_data_concordance_manual:
       #find the rows that we have already set in combined_data_concordance_manual and remove them, then replace them with the new rows
       manual_index_rows = manual_index_rows[manual_index_rows.Dataset_selection_method=='Manual']
       #make date a part of the index in combined_data_concordance_manual
       combined_data_concordance_manual.set_index(INDEX_COLS,inplace=True)
       #now remove these rows from combined_data_concordance_manual
       combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(manual_index_rows.index)]
       #replace these rows in combined_data_concordance_manual by using concat
       combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, manual_index_rows])
       #remove Date from index
       combined_data_concordance_manual.reset_index(inplace=True)


##################################

#START MANUAL DATA SELECTION

##################################
#%%
#loop through the unique combinations, plot a timeseries for each one and then ask the user what dataset they want to choose for that instance and then update the dataset column in combined_data for that row to be the chosen dataset
# %matplotlib qt 
import matplotlib
matplotlib.use('TkAgg')

combined_data_concordance_manual.set_index(INDEX_COLS_no_year, inplace=True)
# %matplotlib qt
#TEMP FIX START
#NOTE MAKING THIS ONLY WORK FOR YEARLY DATA, AS IT WOULD BE COMPLICATED TO DO IT OTEHRWISE. lATER ON WE CAN TO IT COMPLETELY BY CHANGING LINES THAT ADD 1 TO THE DATE IN THE DATA SLECTION FUNCTIONS TO ADD ONE UNIT OF WHATEVER THE FREQUENCY IS. 
#change dataframes date col to be just the year in that date eg. 2022-12-31 = 2022 by using string slicing
combined_data_concordance_manual.Date = combined_data_concordance_manual.Date.apply(lambda x: x[:4]).astype(int)
combined_data.Date = combined_data.Date.apply(lambda x: x[:4]).astype(int)
duplicates_manual.Date = duplicates_manual.Date.apply(lambda x: x[:4]).astype(int)#todo CHECK WHY WE ARE GETTING FLOAT YEARS
#%%
#order data by date
combined_data_concordance_manual = combined_data_concordance_manual.sort_values(by='Date')
combined_data = combined_data.sort_values(by='Date')
duplicates_manual = duplicates_manual.sort_values(by='Date')
#TEMP FIX END

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
       #save the progress to csv in case anything goes wrong. If you need to access it it should be formatted just like the combined_data_concordance_manual df at the end of this script
       combined_data_concordance_manual.to_csv(progress_csv, index=True)

print('Finished with manual selection of datasets')

#TEMP FIX START
#convert date back to the last day of the year instead of year integer
combined_data_concordance_manual.reset_index(inplace=True)
combined_data_concordance_manual.Date = combined_data_concordance_manual.Date.apply(lambda x: str(x)+'-12-31')#TODO somehow Date becomes an index., check where that ahppens jsut in case
combined_data.Date = combined_data.Date.apply(lambda x: str(x)+'-12-31')
duplicates_manual.Date = duplicates_manual.Date.apply(lambda x: str(x)+'-12-31')
#TEMP FIX END


       
#%%
#save the combined_data_concordance_manual df to a csv
combined_data_concordance_manual.to_csv('./intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID), index=False)

# #save bad_index_rows to a csv
bad_index_rows.to_csv('./intermediate_data/data_selection/{}_bad_index_rows.csv'.format(FILE_DATE_ID), index=True)


#%%
#Some issues we may come across:
#if there is only one datapoint for a given row but interpolating between other dataset/s is obviously a better method
#if a single datapoint is obviously wrong and should be removed (could fix with havign a NONE dataset that is used for this purpose?)
#BIGGEST ISSUE is that we are probably going to have very few datapoints that are duplicates because we will get lots of datapoints for lots of different categories that dont match exactly. eg. ato data is not split into different vehicle or engine types so there are no duplicates between that and 8th edition activity data.
#ALSO with the manual sheet, if you dont know what years we have values for then it will eb hard to use. so
