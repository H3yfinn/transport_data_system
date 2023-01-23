#The intention is to guide the user through picking the best dataset to use for each datapoint where there are multiple. To speed things up it also uses an automatic method if the user is happy with it. If this is done then the user will only end up having to use the manual method for perhaps a few if no datapoints that were too tricky for the automatic method. The manual method will use the python prompt method to ask what datapoint the user wants to sue, as well as showing a graph of the different datapoints in the whole time series.
#The automatic method currently has these steps:
#in the automatic method we will use the following rules in an order of priority from 1 being the highest priority to n being the lowest priority:
#1. if there are two or more datapoints for a given row and one is from the same dataset as the previous year for that same unique row then use that one
#2. if there are two or more datapoints for a given row and all but one dataset is missing in the next year for that same unique row then use the one that is not missing
#3. if there are two or more datapoints for a given row and one is closer and within 25% of the previous year, then use that one
#4 if none of the above apply then ask the user to manually select the best datapoint for each row, if it hasn't been done already in the past
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
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
# FILE_DATE_ID = 'DATE20221206'

#%%
#this is for determining what are the best datpoints to use when there are two or more datapoints for a given time period

#load data
# FILE_DATE_ID = 'DATE20221205'
combined_data_concordance= pd.read_csv('intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
combined_data = pd.read_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))

# #repalce nan in comments col with nan string #this solves a lot of issues down the line
# combined_data['Comments'] = combined_data['Comments'].fillna('nan')
#%%
#visualise what data points we have multiple data for by creating a table that shows what rows have duplicates when we ignore the value and dataset columns:

#But first things first we need to make sure there are no unexpected values in the dataset, otehrwsie they could be values which are miscategorised and therefore we might not realise when we have more than one of a given datapoint. We will do this by printing a strongly worded statement to the user to double check that there are no unexpected values in the dataset!
print('Dear User this is a strongly worded statement to check that there are no unexpected values in the dataset. Thank you for your time. ')

#%%
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'duplicates')
FILE_DATE_ID = 'DATE{}'.format(file_date)

#open the csv file with duplicates in it
duplicates = pd.read_csv('intermediate_data/duplicates{}.csv'.format(FILE_DATE_ID))

#datasets column is being converted to a string for some reason, we will convert it back to a list
duplicates['Datasets'] = duplicates['Datasets'].apply(lambda x: eval(x))

#%%
INDEX_COLS = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type','Drive','Fuel_Type','Frequency','Scope']
#Remove year from the current cols without removing it from original list, and set it as a new list
INDEX_COLS_no_year = INDEX_COLS.copy()
INDEX_COLS_no_year.remove('Date')


#%%
#filter for the years we want to bother selecting data for
EARLIEST_YEAR = "2010-01-01"
LATEST_YEAR = '2020-01-01'#datetime.datetime.now().year
#filter data where year is less than 2010
combined_data = combined_data[combined_data['Date'] >= EARLIEST_YEAR]
combined_data_concordance = combined_data_concordance[combined_data_concordance['Date'] >= EARLIEST_YEAR]
duplicates = duplicates[duplicates['Date'] >= EARLIEST_YEAR]
#and also only data where year is less than the current year
combined_data = combined_data[combined_data['Date'] < LATEST_YEAR]
combined_data_concordance = combined_data_concordance[combined_data_concordance['Date'] < LATEST_YEAR]
duplicates = duplicates[duplicates['Date'] < LATEST_YEAR]

#%%
#######################################################################
#STARTING SPECIFIC FIX, FEEL FREE TO IGNORE IT
#I have made this into a function to reduce the code in this aprticular file. it is in data_selection_functions.py and reduces the code in this file by about 40 lines
combined_data, duplicates = data_selection_functions.remove_duplicate_transport_8th_data(combined_data, duplicates)
#######################################################################
#but actually temporarily we are going to just remove any data for the Carbon neutrality scenario since it is just another scenario and we dont want to have to deal with it
combined_data = combined_data[combined_data['Dataset'] != '8th edition transport model $ Carbon neutrality']
#to remove it fromduplicates we need to remove it from the datasets lists column and reduce count by 1
duplicates['Datasets'] = duplicates['Datasets'].apply(lambda x: [i for i in x if i != '8th edition transport model $ Carbon neutrality'])
#double check count is correct
duplicates['Count'] = duplicates['Datasets'].apply(lambda x: len(x))


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
combined_data_concordance['Comments'] = None

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
run_automatic = False
datasets_to_always_choose = ["ATO $ Country Official Statistics"]
checkpoints_1 = []
checkpoints_2 = []
if run_automatic:
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
              combined_data_concordance_automatic_measure, rows_to_select_manually_measure = data_selection_functions.automatic_method(combined_data_automatic_measure, combined_data_concordance_automatic_measure,duplicates_auto_measure,duplicates_auto_with_year_index_measure,datasets_to_always_choose,std_out_file = 'intermediate_data/data_selection/automatic_method{}.txt'.format(FILE_DATE_ID))
              #save the data to a csv
              filename = 'intermediate_data/data_selection/checkpoints/{}_combined_data_concordance_automatic_{}.csv'.format(FILE_DATE_ID, measure)
              combined_data_concordance_automatic_measure.to_csv(filename, index=True)
              checkpoints_1.append(filename)
              filename = 'intermediate_data/data_selection/checkpoints/{}_rows_to_select_manually_{}.csv'.format(FILE_DATE_ID, measure)
              rows_to_select_manually_measure_df = pd.DataFrame(rows_to_select_manually_measure, columns=INDEX_COLS_no_year)
              #remove duplicates from rows_to_select_manually_measure_df
              rows_to_select_manually_measure_df = rows_to_select_manually_measure_df.drop_duplicates()
              rows_to_select_manually_measure_df.to_csv(filename, index=False)
              checkpoints_2.append(filename)

       #take in all the checkpoints and combine them into one df
       combined_data_concordance_automatic = pd.concat([pd.read_csv(checkpoint, index_col=INDEX_COLS) for checkpoint in checkpoints_1])
       rows_to_select_manually_df = pd.concat([pd.read_csv(checkpoint) for checkpoint in checkpoints_2])
       
       #save
       combined_data_concordance_automatic.to_csv('intermediate_data/data_selection/{}_data_selection_automatic.csv'.format(FILE_DATE_ID), index=True)
       rows_to_select_manually_df.to_csv('intermediate_data/data_selection/{}_rows_to_select_manually.csv'.format(FILE_DATE_ID), index=False)
       
#%%
#MANUAL METHOD
#This is a script to make it easier for the user to do the manual selection of datasets, especailly at the start of choosing the datasets as it will otehrwise be very timeconsuming to create all the spreadsheets.

# 1a.find the rows that have duplicates and then plot a timeseries for the full series which the duplicates are from
#so find the unique combinations of the following columns: Economy, Measure, Vehicle Type, Unit, Medium, Transport Type, Drive, then if any years of that combination have more than one dataset of data, then plot a timeseries for that unique combination of columns.

#%%~
#if we want to add to the rows_to_select_manually_df to check specific rows then set the below to True
add_to_rows_to_select_manually_df = False
run_only_on_rows_to_select_manually = False
if run_only_on_rows_to_select_manually:
       #if this, only run the manual process on index rows where we couldnt find a dataset to use automatically for some year
       #since the automatic method is relatively strict there should be a large amount of rows to select manually
       #note that if any one year cannot be chosen automatically then we will have to choose the dataset manually for all years of that row
       if not 'rows_to_select_manually_df' in locals():
              rows_to_select_manually_df = pd.read_csv('intermediate_data/data_selection/{}_rows_to_select_manually.csv'.format(FILE_DATE_ID))
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
##%%
#PICKUP LATEST PROGRESS
#we want to save the state the user was last at so they can pick up on where they last left off. So load in the data from progress_csv, see what values have had their Dataselection method set to manual and remove them from the iterator.
#we will then replace those rwos in combined_data_concordance_manual
#there is one subtle part to this, in that an index row will only be removed from the iterator if all the years of that index row have been set to manual. So if the user has set some years to manual but not all, for example by quitting halfway through choosing all the values for a chart, then we will not remove that index row from the iterator and the user should redo it. BUT if the user skips rows trhen this will save that (they can be identified as rows where the dataselection method is manual but the value and num datapoints are NaN -  will need to double check hjow the interpoaltion affects this)
pick_up_where_left_off = True
if pick_up_where_left_off:
       progress_csv = pd.read_csv('intermediate_data/data_selection/{}_progress.csv'.format(FILE_DATE_ID), index_col=INDEX_COLS_no_year)
       #make the date column a datetime object
       progress_csv.Date = progress_csv.Date.apply(lambda x: str(x) + '-12-31')
       #make date part of index 
       progress_csv.reset_index(inplace=True)
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
       manual_index_rows_no_date =          manual_index_rows_no_date[manual_index_rows_no_date.Dataset_selection_method!='Manual']
       #now set index to same as iterator
       manual_index_rows_no_date.set_index(INDEX_COLS_no_year, inplace=True)
       #KEEP only these rows in the iterator by finding the index rows in both dfs 
       iterator = iterator[iterator.index.isin(manual_index_rows_no_date.index)]

       #and now update the combined_data_concordance_manual:
       #find the rows that we have already set in combined_data_concordance_manual and remove them, then replace them with the new rows
       manual_index_rows = manual_index_rows[manual_index_rows.Dataset_selection_method=='Manual']
       #make date a part of the index in combined_data_concordance_manual
       combined_data_concordance_manual.reset_index(inplace=True)
       combined_data_concordance_manual.set_index(INDEX_COLS,inplace=True)
       #now remove these rows from combined_data_concordance_manual
       combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(manual_index_rows.index)]
       #replace these rows in combined_data_concordance_manual by using concat
       combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, manual_index_rows])
       #remove Date from index
       combined_data_concordance_manual.reset_index(inplace=True)
       combined_data_concordance_manual.set_index(INDEX_COLS_no_year, inplace=True)
#%%
#loop through the unique combinations, plot a timeseries for each one and then ask the user what dataset they want to choose for that instance and then update the dataset column in combined_data for that row to be the chosen dataset
# %matplotlib qt 
import matplotlib
matplotlib.use('TkAgg')
# %matplotlib qt
#TEMP FIX START
#NOTE MAKING THIS ONLY WORK FOR YEARLY DATA, AS IT WOULD BE COMPLICATED TO DO IT OTEHRWISE. lATER ON WE CAN TO IT COMPLETELY BY CHANGING LINES THAT ADD 1 TO THE DATE IN THE DATA SLECTION FUNCTIONS TO ADD ONE UNIT OF WHATEVER THE FREQUENCY IS. 
#change dataframes date col to be just the year in that date eg. 2022-12-31 = 2022 by using string slicing
combined_data_concordance_manual.Date = combined_data_concordance_manual.Date.apply(lambda x: x[:4]).astype(int)
combined_data.Date = combined_data.Date.apply(lambda x: x[:4]).astype(int)
duplicates_manual.Date = duplicates_manual.Date.apply(lambda x: x[:4]).astype(int)
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
#create option to import manual data selection from perveious runs to avoid having to do it again (can replace any rows where the Final_dataset_selection_method is na with where they are Manual in the imported csv)
import_manual_data_selection = False
if import_manual_data_selection:
       FILE_DATE_ID = 'DATE20230120'
       final_combined_data_concordance = pd.read_csv('./output_data/{}_final_combined_data_concordance.csv'.format(FILE_DATE_ID))
       final_combined_data_concordance = final_combined_data_concordance.set_index(INDEX_COLS)
       final_combined_data_concordance = final_combined_data_concordance[final_combined_data_concordance.Final_dataset_selection_method == 'Manual']
       combined_data_concordance_manual = combined_data_concordance_manual.combine_first(final_combined_data_concordance)#this SHOULD work because the index is the same for both dfs and the only difference is that the final_combined_data_concordance df has some rows with Final_dataset_selection_method = Manual and the combined_data_concordance_manual df has some rows with Final_dataset_selection_method = na
       
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

#EXTRA MANUAL METHOD: PLEASE NOTE THAT IF YOU WANT TO CHOOSE THE DATA MANUALLY THEN JJUST SET THE DATA WITHIN THE CSV {}_data_selection_manual.csv'.format(FILE_DATE_ID)), THIS WILL BE USED INSTEAD OF WHAT IS OUTPUT FROM THE MANUAL METHOD ABOVE
#%%
#PLEASE keep this cell here as it makes it easy to inspect the output of both medthods
test=True
if test:
       #load in the manual data selection df and automatic data selection df
       combined_data_concordance_manual = pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID))#, index_col=INDEX_COLS)
       combined_data_concordance_automatic = pd.read_csv('intermediate_data/data_selection/{}_data_selection_automatic.csv'.format(FILE_DATE_ID), index_col=INDEX_COLS)
#%%
#COMBINE MANUAL AND AUTOMATIC DATA SELECTION OUTPUT DFs
#join the automatic and manual datasets so we can compare the dataset  columns from both the manual dataset automatic dataset
#create a final_dataset column. This will be filled with the dataset in the automatic column, except where the values in the manual and automatic dataset columns are different, for which we will use the value in the manual dataset.

#%%
do_this = True
if do_this:
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
             
       #save the final_combined_data_concordance df to a csv
       final_combined_data_concordance.to_csv('./output_data/{}_final_combined_data_concordance.csv'.format(FILE_DATE_ID), index=True)


#%%
