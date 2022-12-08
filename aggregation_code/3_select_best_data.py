
#%%
import datetime
import pandas as pd
import numpy as np
import os
import re
# import plotly.express as px
# pd.options.plotting.backend = "plotly"#set pandas backend to plotly plotting instead of matplotlib

# import plotly.io as pio
# pio.renderers.default = "browser"#allow plotting of graphs in the interactive notebook in vscode #or set to notebook
# import plotly.graph_objects as go
# import plotly
import matplotlib.pyplot as plt
import data_selection_functions
#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False

#%%
#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
FILE_DATE_ID = 'DATE20221206'

#%%
#this is for determining what are the best datpoints to use when there are two or more datapoints for a given time period

#load data
# FILE_DATE_ID = 'DATE20221205'
combined_dataset = pd.read_csv('output_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))
combined_data_concordance= pd.read_csv('output_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
combined_data = pd.read_csv('output_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))
#%%
#visualise what data points we have multiple data for by creating a table that shows what rows have duplicates when we ignore the value and dataset columns:

#But first things first we need to make sure there are no unexpected values in the dataset, otehrwsie they could be values which are miscategorised and therefore we might not realise when we have more than one of a given datapoint. We will do this by printing a strongly worded statement to the user to double check that there are no unexpected values in the dataset!
print('Dear User this is a strongly worded statement to check that there are no unexpected values in the dataset. Thank you for your time. ')

#%%

# #open the csv file with duplicates in it
duplicates = pd.read_csv('output_data/duplicates{}.csv'.format(FILE_DATE_ID))
#%%

#now for the rows that have duplicates we will find the best datapoint to use for each row:
#INTRO
#Instead of setting values we will be setting the value of the dataset columnn where there are two or more datapoints for a given row to the dataset that we should use for that row.
#we will provide an automatic way of doing this and then also allow the user to manually select the best datapoint for each row.
#ABOUT MANUAL METHOD
#In the manual method, the way that we will display what dataset to use for each datapoint is by creating a dataset for each measure where we will have a column for each year and then the value for each year, for each combination of categories (which are columns) will be the dataset that we should use for that year. Then we will save that as an xlsx with each sheet a different measure
#Then the user can easily go into the xlsx and adjust the dataset that is used for each year manually if they want to. This way it will be easier for the user to see what is going on.
#ABOUT AUTOMATIC METHOD
#The automatic is detailed in that section.

#FINAL OUTPUT
#The final output will be a large, tall dataframe with one row for each unique combination of categories,  a years column, a vlaues column, a dataset column and a column to indicate if the value was manually selected or automatically selected and a column to indicate how many datapoints there were for that row

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

combined_data_concordance_automatic = combined_data_concordance.copy()
duplicates_auto = duplicates.copy()

#create dummy spreadsheet for testing. this can be a copy of the automatic. perhaps this line can be deleted later TODO
combined_data_concordance_manual = combined_data_concordance.copy()
duplicates_manual = duplicates.copy()
#%%
##
#ignore this cell i think
#Sort by Year in both combined_data_concordance and duplicates. This is so we can more easily use index to find previous and next years for certain rows in the dfs
# combined_data_concordance = combined_data_concordance.sort_values(by='Year')
# duplicates = duplicates.sort_values(by='Year')
run = False
if run:
       automatically_picked_data = data_selection_functions.automatic_method(duplicates_auto,combined_data_concordance_automatic)

#IN THE ABOVE NEED TO APPLY METHOD TO MAKE SURE THE DIFFERENCE IS LESS THAN 25% OTEHRWISE WE USE THE NEXT OPTION. ALSO MIGHJT NEED TO MAKE THE CONCORDANCE CREATION METHOD IN THE AGGREGATION METHOD FASTER.
       # #OPTION 5
       # #if there is a previous year row index
       # if len(previous_year_row_index) > 0:
       #        #get the value for the previous year
       #        previous_year_value = combined_data_concordance.loc[previous_year_row_index, 'Value'].values[0]
       #        #get the value for the current row
       #        current_value = row.Value
       #        #get the difference between the current value and the previous year value
       #        diff_previous_year = abs(current_value - previous_year_value)
       #        #if the difference between the current value and the previous year value is less than the threshold
       #        if diff_previous_year < threshold:
       #               #set the value in combined_data_concordance for this year to the value for the same dataset
#%%
#MANUAL METHOD
#in the manual method we will take in the spreadsheet created by the user. We will turn it into a long format with all the measures concatenated so there is one measure column.
#then join the automatic and manual datasets so we can compare the dataset  columns from both the manual dataset automatic dataset
#create a final_dataset column. This will be filled with the dataset in the automatic column, except where the values in the manual and automatic dataset columns are different, for which we will use the value in the manual dataset.
#for the sake of tracking what has been done we will also create a column called dataset_selection_method which will be filled with the name of the column that the dataset was selected from. So if the dataset was selected from the automatic column then it will be filled with automatic and if it was selected from the manual column then it will be filled with manual.
do_this = False#I realised that this may be too difficult and the use of the tool in next cell may be better. also thought of the idea that user may need help knowing what years and rows to fill in with datasets we may have.
if do_this:
       #create dummy spreadsheet for testing. this can be a copy of the automatic
       combined_data_concordance_manual = combined_data_concordance.copy()
       #remove the dataset_selection_method,Num_datapoints, and Value columns
       combined_data_concordance_manual = combined_data_concordance_manual.drop(columns=['Dataset_selection_method', 'Num_datapoints', 'Value'])
       #make it wide so the years are columns
       combined_data_concordance_manual = combined_data_concordance_manual.pivot_table(index=['Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium', 'Transport Type', 'Drive'], columns='Year', values='Dataset')
       #reset the index
       combined_data_concordance_manual = combined_data_concordance_manual.reset_index()
       # #make the years columns into integers not strings
       # combined_data_concordance_manual.columns = combined_data_concordance_manual.columns.astype(int)
       #now jsut quickly we will save the combined_data_concordance_manual to an xlsx file so we can manually edit it and load it in as if it was the manual dataset
       #So for each measure in the combined_data_concordance_manual dataset create a sheet in the saved xlsx below, and set the name of the sheet to the measure
       for measure in combined_data_concordance_manual.Measure.unique():
              #filter for only the current measure
              measure_rows = combined_data_concordance_manual[combined_data_concordance_manual.Measure == measure]
              #save those values in a sheet with the measure name as the sheet name
              # measure_rows.to_excel('./output_data/concordance_manual_{}.xlsx'.format(FILE_DATE_ID), index=False)
#%%
#We will also create a series of helpful scripts to make it easier for the user to do the manual selection of datasets, especailly at the start of choosing the datasets as it will otehrwise be very timeconsuming to create all the spreadsheets. These will be:
#1. a script that will find the rows that have duplicates and then plot a timeseries for the full series which the duplicates are from, with the line a different color for each dataset. This way the user can easily see which dataset is the best one to use for each row.
#b. i. this will also ask the user what dataset they want to choose for that instance and then update the dataset column for that row to be the chosen dataset. this can be done using the input() function and then using the .loc function to update the dataset column for that row, based on whether the user chooses 1, 2, 3, etc. for the dataset they want to use.
#b. ii. since this will be a bit timeconsuming to do for every value in a timeseries, the user can choose options that will set that dataset as the default for all rows where that dataset is available. This can be done by just asking the user a second question after their input for the first question. This will be a yes or no question asking if they want to set that dataset as the default for all rows where that dataset is available. If they say yes then we will use the .loc function to update the dataset column for all rows in the same timeseries where that dataset is available to be the chosen dataset. If they say no then we will just update the dataset column for that row to be the chosen dataset.

#create that script now:
# 1a.find the rows that have duplicates and then plot a timeseries for the full series which the duplicates are from
#so find the unique combinations of the following columns: Economy, Measure, Vehicle Type, Unit, Medium, Transport Type, Drive, then if any years of that combination have more than one dataset of data, then plot a timeseries for that unique combination of columns.
#find the unique combinations of the following columns: Economy, Measure, Vehicle Type, Unit, Medium, Transport Type, Drive
combined_data_concordance_manual_not_indexed = combined_data_concordance_manual.copy()
duplicates_manual_not_indexed = duplicates_manual.copy()
combined_data_not_indexed = combined_data.copy()

current_cols = ['Year', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type','Drive']
#Remove year from the current cols without removing it from original list, and set it as a new list
current_cols_no_year = current_cols.copy()
current_cols_no_year.remove('Year')
#add the dataset column to the current cols without removing it from original list, and set it as a new list
current_cols_dataset = current_cols.copy()
current_cols_dataset.append('Dataset')

#%%
#set index of all the dfs we will use to be the above cols
combined_data_concordance_manual = combined_data_concordance_manual_not_indexed.set_index(current_cols_no_year)

duplicates_manual=duplicates_manual_not_indexed.set_index(current_cols_no_year)
combined_data = combined_data_not_indexed.set_index(current_cols_no_year)#set year as index so we can retrieve the data by year in this dataset

combined_data_for_value_extraction = combined_data_not_indexed.copy().set_index(current_cols_dataset)

unique_combinations = combined_data_concordance_manual_not_indexed[current_cols_no_year].drop_duplicates()

unique_combinations.set_index(current_cols_no_year, inplace=True)

#TODO create bad_unique_combinations as a empty df with the same columns as unique_combinations
bad_unique_combinations = pd.DataFrame(columns=unique_combinations.columns)
num_bad_unique_combinations = 0
#%%
#loop through the unique combinations, plot a timeseries for each one and then ask the user what dataset they want to choose for that instance and then update the dataset column in combined_data for that row to be the chosen dataset
for unique_combination in unique_combinations.index:
       #filter for only the current row in our duplicates_manual dataset by using the row as an index
       #TODO we are going to try using this but if it doesnt work then add the unqiue combination as a row to a lsit and we will check them outside the loop
       try:
              current_row_filter = duplicates_manual.loc[duplicates_manual.index.isin([unique_combination])]
       except KeyError:
              num_bad_unique_combinations += 1
              print('bad unique combination {}: {}'.format(num_bad_unique_combinations,unique_combination))
              bad_unique_combinations = bad_unique_combinations.append(unique_combinations.loc[unique_combination])
              #if the row is not in the duplicates_manual dataset then continue to the next row
              continue
       
       #identify how many datasets there are for each year by looking at the Count column
       #if there are rows where count is greater than 1 then we will plot and ask user to choose
       if current_row_filter.Count.max() > 1:
              #grab the data for this unique combination of columns from the combined_data df
              data_for_plotting = combined_data.loc[combined_data.index.isin([unique_combination])]

              ##PLOT
              data_selection_functions.graph_manual_data(data_for_plotting, unique_combination)
              
              ##USER INPUT

              #ask user what dataset they want to choose for each year where a decision needs to be made
              options = ['Keep the dataset "{}" for all years that the same combination of datasets is available', 'Keep the dataset "{}" for all years that the chosen dataset is available', 'Keep the dataset "{}" only for that year']
              
              #Then based on what they choose, update the dataset column in  combined_data_concordance_manual for that row to be the chosen dataset
              #quickly save the values using pickle: options, data_for_plotting, unique_combination,  combined_data_concordance_manual, combined_data_for_value_extraction
              import pickle
              with open('./intermediate_data/data_selection/{}_data_selection_manual.pickle'.format(FILE_DATE_ID), 'wb') as f:
                     pickle.dump([options, data_for_plotting, unique_combination,  combined_data_concordance_manual, combined_data_for_value_extraction], f)

              combined_data_concordance_manual = data_selection_functions.user_input_function(options, data_for_plotting, unique_combination,  combined_data_concordance_manual, combined_data_for_value_extraction)
                                   
#save the combined_data_concordance_manual df to a csv
combined_data_concordance_manual.to_csv('./intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID))

#save bad_unique_combinations to a csv
bad_unique_combinations.to_csv('./intermediate_data/data_selection/{}_bad_unique_combinations.csv'.format(FILE_DATE_ID))

#%%
#Some issues we may come across:
#if there is only one datapoint for a given row but interpolating between other dataset/s is obviously a better method
#if a single datapoint is obviously wrong and should be removed (could fix with havign a NONE dataset that is used for this purpose?)
#BIGGEST ISSUE is that we are probably going to have very few datapoints that are duplicates because we will get lots of datapoints for lots of different categories that dont match exactly. eg. ato data is not split into different vehicle or engine types so there are no duplicates between that and 8th edition activity data.
#ALSO with the manual sheet, if you dont know what years we have values for then it will eb hard to use. so


