#%%


#to do:
#remove the duplicates df and instead hold duplicatedm list in comnbined data concordance?
#remove the iterator df and instead run over the combined data concordance? We can label the rows according to whether we want to select them manually or not.
#implement a strict ordering system throughout so that we can then easily identify previously selected rows and reselect them.
#save the progress in pickles so that we dont need to do so much setting and resetting of indexes. 
#label variables according to their index setting.

# Save and open and delete pngs rather than gui
# Make it so data can be labelled so that it doesnt appear in future selections by by marking it with data selection method = delete
#make it easier to make changes by modularising everyuthing way more.

#this will help to remove the issue with index rows where we search for values but nans stuff it up.


# so we will 


# Save and open and delete pngs rather than gui
# Delete data by marking it with data selection method = delete






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

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')
sys.path.append('./aggregation_code')
# sys.path.append('../../')

import data_selection_functions as data_selection_functions
import data_formatting_functions as data_formatting_functions
import utility_functions as utility_functions
import data_selection_functions_test as data_selection_functions_test

#%%
#create code to run the baove functions. If you add more columns to the data then you need to make sure they are added to the list below
INDEX_COLS = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium','Transport Type','Drive','Fuel_Type','Frequency', 'Scope']

#MANUAL DATA SELECTION VARIABLES
pick_up_where_left_off = True
import_previous_selection = True
run_only_on_rows_to_select_manually = False

#load data
# FILE_DATE_ID = 'DATE20221205'
use_all_data = False#False
use_9th_edition_set =True#False#True 
run_data_aggregation = False
#%%
if use_all_data:
    #run aggreagtion code file
    if run_data_aggregation:
       exec(open("./aggregation_code/1_aggregate_cleaned_datasets.py").read())
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    combined_dataset = pd.read_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))
    combined_data_concordance= pd.read_csv('intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))

elif use_9th_edition_set:
    if run_data_aggregation:
        exec(open("./aggregation_code/1_aggregate_cleaned_dataset_9th_edition.py").read())
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/9th_dataset/', 'combined_dataset_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    combined_dataset = pd.read_csv('intermediate_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID))
    combined_data_concordance= pd.read_csv('intermediate_data/9th_dataset/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
else:
    print('Please set use_all_data or use_9th_edition_set to True')

#since we dont expect to run the data selection process that often we will just save the data in a dated folder in intermediate_data/data_selection_process/FILE_DATE_ID/

##############################################################################
#%%
FILE_DATE_ID = 'DATE' + str(datetime.datetime.now().strftime('%Y%m%d'))

#%%
# #filter for just one economy for testing purposes:
combined_dataset = combined_dataset[combined_dataset['Economy']=='12_NZ']
combined_data_concordance = combined_data_concordance[combined_data_concordance['Economy']=='12_NZ']

#%%

#%%
EARLIEST_YEAR = "2010-01-01"
LATEST_YEAR = '2023-01-01'
#%%
combined_data_concordance_automatic,combined_data_concordance_manual,combined_data_concordance_iterator,duplicates_auto,duplicates_auto_with_year_index,combined_data_automatic,duplicates_manual,combined_data = data_formatting_functions.prepare_data_for_selection(combined_data_concordance,combined_dataset,INDEX_COLS,EARLIEST_YEAR, LATEST_YEAR)

rows_to_select_manually_df = []

#%%
duplicates_manual = duplicates_manual.reset_index()
combined_data_concordance_manual = combined_data_concordance_manual.reset_index()
#%%

#########################SET ME TO SET VARIABLES FOR FUNCTION
pick_up_where_left_off=True#this is for if there is an error while running. If you exit out properly then you should use the import_previous_selection variable
#please note i havent tested yet because i ahvent had the chance
import_previous_manual_concordance=False

run_only_on_rows_to_select_manually=False
manually_chosen_rows_to_select=None
user_edited_combined_data_concordance_iterator=None
update_skipped_rows =False# True
consider_value_in_duplicates = True
#LEAVE THESE AS N0NE, THEY WILL GET SET BASED ON THE ABOVE VARIABLES
previous_combined_data_concordance_manual=None
previous_duplicates_manual=None
progress_csv=None

if import_previous_manual_concordance:
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/data_selection/', '_data_selection_manual')
    FILE_DATE_ID2 = 'DATE{}'.format(file_date)
    previous_selections = pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID2))
    # previous_selections = pd.read_csv('intermediate_data/data_selection/DATE20230214_data_selection_manual - Copy (2).csv')
elif pick_up_where_left_off:
    # file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/data_selection/', '_duplicates_manual')
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/data_selection/', '_progress.csv')
    FILE_DATE_ID2 = 'DATE{}'.format(file_date)

    previous_selections = pd.read_csv('intermediate_data/data_selection/{}_progress.csv'.format(FILE_DATE_ID2))
else:
    previous_selections = None
if pick_up_where_left_off or import_previous_selection:
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/data_selection/', '_duplicates_manual')
    FILE_DATE_ID2 = 'DATE{}'.format(file_date)
    #load duplicates_manual now so we can use it later if we need to
    previous_duplicates_manual = pd.read_csv('intermediate_data/data_selection/{}_duplicates_manual.csv'.format(FILE_DATE_ID2))

    if not consider_value_in_duplicates:
        #if consider_value_in_duplicates is set to True here then keep the value col in the duplicates_manual df, else drop it. This is because value is used to determine if there are new values or not, but if we dont want to consider value then we dont need it
        previous_duplicates_manual = previous_duplicates_manual.drop(columns=['Value'])
        duplicates_manual = duplicates_manual.drop(columns=['Value'])

#%%
iterator, combined_data_concordance_manual = data_formatting_functions.create_manual_data_iterator(
combined_data_concordance_iterator,
INDEX_COLS,
combined_data_concordance_manual,
duplicates_manual,
rows_to_select_manually_df,
run_only_on_rows_to_select_manually,
manually_chosen_rows_to_select,
user_edited_combined_data_concordance_iterator,
previous_selections,
previous_duplicates_manual,
update_skipped_rows)#

print('There are {} rows in the iterator'.format(len(iterator)))

#%%


#CURRENT ISSUES:
# 2018  min_year : printed during:

# For the year 2017 please choose a number from the options provided: [1, 2, 4, 5, 7, 8]
# Your input was 4 which is a valid number from the options
# 2018  min_year

# For the year 2018 please choose a number from the options provided: [1, 3, 4, 6, 7, 9]

#skipping now goes to the next group i think

selection_set = ['Economy', 'Transport Type','Vehicle Type','Drive']
sorting_cols = ['Economy','Transport Type','Vehicle Type','Date', 'Drive', 'Measure', 'Unit', 'Scope']
data_selection_functions_test.select_best_data_manual_by_group(combined_data_concordance_iterator,iterator,combined_data_concordance_manual,combined_data,duplicates_manual,INDEX_COLS,selection_set,sorting_cols,FILE_DATE_ID='test')
 #%%

#filter for only these rows in the index cols ['Economy',
#  'Measure',
#  'Vehicle Type',
#  'Unit',
#  'Medium',
#  'Transport Type',
#  'Drive',
#  'Fuel_Type',
#  'Frequency',
#  'Scope']
# ('12_NZ', 'freight_tonne_km', nan, 'freight_tonne_km', 'air', 'freight', nan, nan, 'Yearly', 'National')
# ('12_NZ', 'Energy', nan, 'PJ', 'air', 'freight', nan, nan, 'Yearly', 'National')
do_this = False
if do_this:
        
    iterator = iterator.reset_index()
    iterator = iterator[iterator['Economy']=='12_NZ']
    iterator = iterator[iterator['Transport Type']=='freight']
    iterator = iterator[iterator['Scope']=='National']
    iterator = iterator[iterator['Medium']=='air']
    iterator = iterator[iterator['Frequency']=='Yearly']
    iterator = iterator[iterator['Drive'].isnull()]
    iterator = iterator[iterator['Fuel_Type'].isnull()]
    iterator = iterator[iterator['Vehicle Type'].isnull()]
    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('Date')
    iterator = iterator.set_index(INDEX_COLS_no_year)

#%%

#%%


#%%



#%%




# # def select_best_data_manual_by_group(combined_data_concordance_iterator,iterator,combined_data_concordance_manual,combined_data,duplicates_manual,INDEX_COLS,selection_set,sorting_cols,FILE_DATE_ID=''):
# # create a new folder named for the selection set in intermediate data/data selection/manual_data_selection_sets/
# #in this folder we will store the finished progress of the manual data selection for this set, named by the group id. Then if we ever want to import previous manual data selection we can just import the data from this folder, concatenate it all and then apply it to the combined data concordance and so on.
# #this way we can save effective checkpoints of the manual data selection process. 

# #first we will design the process that will allow us to import previous manual data selection, then we will design the process that will allow us to save checkpoints of the manual data selection process.


# ##################################

# #START MANUAL DATA SELECTION

# ##################################
# INDEX_COLS_no_year = INDEX_COLS.copy()
# INDEX_COLS_no_year.remove('Date')
# #if Date is in sorting_cols then remove it
# if 'Date' in sorting_cols:
#     sorting_cols.remove('Date')
# #loop through the unique combinations, plot a timeseries for each one and then ask the user what dataset they want to choose for that instance and then update the dataset column in combined_data for that row to be the chosen dataset
# # %matplotlib qt 
# import matplotlib
# matplotlib.use('TkAgg')

# combined_data_concordance_manual.set_index(INDEX_COLS_no_year, inplace=True)
# duplicates_manual.set_index(INDEX_COLS_no_year, inplace=True)

# # %matplotlib qt
# #TEMP FIX START
# #NOTE MAKING THIS ONLY WORK FOR YEARLY DATA, AS IT WOULD BE COMPLICATED TO DO IT OTEHRWISE. lATER ON WE CAN TO IT COMPLETELY BY CHANGING LINES THAT ADD 1 TO THE DATE IN THE DATA SLECTION FUNCTIONS TO ADD ONE UNIT OF WHATEVER THE FREQUENCY IS. 
# #change dataframes date col to be just the year in that date eg. 2022-12-31 = 2022 by using string slicing
# combined_data_concordance_manual.Date = combined_data_concordance_manual.Date.apply(lambda x: x[:4]).astype(int)
# combined_data.Date = combined_data.Date.apply(lambda x: x[:4]).astype(int)
# duplicates_manual.Date = duplicates_manual.Date.apply(lambda x: x[:4]).astype(int)#todo CHECK WHY WE ARE GETTING FLOAT YEARS

# #order data by date
# combined_data_concordance_manual = combined_data_concordance_manual.sort_values(by='Date')
# combined_data = combined_data.sort_values(by='Date')
# duplicates_manual = duplicates_manual.sort_values(by='Date')
# #TEMP FIX END
# #Create bad_index_rows as a empty df with the same columns as index_rows
# bad_index_rows = pd.DataFrame(columns=combined_data_concordance_iterator.columns)
# num_bad_index_rows = 0
# #create progresss csv so we can add lines to it as we go
# progress_csv = 'intermediate_data/data_selection/{}_progress.csv'.format(FILE_DATE_ID)

# ####################################
# #split data into groups based on the selection set so that the data selection process can be done for each group separately
# options = ['Keep the dataset "{}" for all years that the chosen dataset is available', 'Keep the dataset "{}" for all consecutive years that the same combination of datasets is available','Keep the dataset "{}" only for that year']
# #make a var called iterator_group and repalce nas with 'nan' so that we can group by it
# iterator_group = iterator.reset_index().copy()
# iterator_group = iterator_group.fillna('nan')
# iterator_group = iterator_group.groupby(selection_set)

# #create set of 2 one for plotting the current row and one for plotting the user options in text. 
# fig_row, ax_row = plt.subplots()
# fig_text, ax_text = plt.subplots()

# # combined_data_concordance_manual_group = combined_data_concordance_manual.groupby(selection_set)
# # combined_data_group = combined_data.groupby(selection_set)
# # duplicates_manual_group = duplicates_manual.groupby(selection_set)
# for group in iterator_group.groups:
#     iterator_for_group = iterator_group.get_group(group)
#     #now replace any 'nan' values with np.nan so that we can use the isna() function
#     iterator_for_group = iterator_for_group.replace('nan',np.nan)
#     #sort iterator_for_group by set of index columns so the way the data comes in is easy to understand
#     iterator_for_group = iterator_for_group.sort_values(by=sorting_cols)
#     #set index of iterator_for_group to be the index columns
#     iterator_for_group.set_index(INDEX_COLS_no_year, inplace=True)

#     #extract the data in iterator for group from combined data using the index
#     combined_data_group = combined_data.loc[iterator_for_group.index].reset_index()

#     # plotly_dashboard(combined_data_group,group)
    
#     fig_dash = go.Figure()
#     #make a shape for the set of subplots that will be as close to a square as possible
#     shape = (int(np.ceil(np.sqrt(len(combined_data_group.Measure.unique())))), int(np.ceil(len(combined_data_group.Measure.unique())/np.ceil(np.sqrt(len(combined_data_group.Measure.unique()))))))
#     #specify subplots using shape
#     fig_dash = make_subplots(rows=shape[0], cols=shape[1], shared_xaxes=True, subplot_titles=combined_data_group.Measure.unique())
#     #create a combination of every possible row and col for the subplots
#     row_col_combinations = list(itertools.product(range(1,shape[0]+1), range(1,shape[1]+1)))
#     i=0
#     for measure in combined_data_group.Measure.unique():
#         #get data for this measure
#         measure_data = combined_data_group.loc[combined_data_group.Measure == measure]
#         row = row_col_combinations[i][0]
#         col = row_col_combinations[i][1]
#         i+=1

#         for dataset in combined_data_group['Dataset'].unique():
#             #plot data for this measure and this daTASET
#             fig_dash.add_trace(go.Scatter(x=measure_data.loc[measure_data.Dataset == dataset].Date, y=measure_data.loc[measure_data.Dataset == dataset].Value, name=dataset, mode='lines+markers'), row=row, col=col)
#     #add a title to the figure as the data in group sep by spaces
#     fig_dash.update_layout(title_text=', '.join(group))
#     #save the figure as temp html file
#     fig_dash.write_html('plotting_output/data_selection_dashboards/dashboard_{}.html'.format('_'.join(group)), auto_open=True)
    
#     break
# #%%
#     user_input = ''
#     for index_row in iterator_for_group.index:
        
#         if 'Yearly' not in index_row:
#             print('Skipping row {} as it is not yearly data'.format(index_row))
#             continue

#         #filter for only the current row in our duplicates_manual dataset by using the row as an index
#         try:
#             current_row_filter = duplicates_manual.loc[index_row]
#         except KeyError:
#             num_bad_index_rows += 1
#             print('Bad unique combination {}: {}. Please check it to find out what the error was'.format(num_bad_index_rows,index_row))
#             bad_index_rows = bad_index_rows.append(combined_data_concordance_iterator.loc[index_row])
#             #if the row is not in the duplicates_manual dataset then continue to the next row
#             continue
        
#         #identify how many datasets there are for each year by looking at the Num_datapoints column
#         #if there are rows where count is greater than 1 then we will plot and ask user to choose which dataset to use
#         if (current_row_filter.Num_datapoints.max() > 1):# | (add_to_rows_to_select_manually_df):#if we ware adding to the rows to select manually df then we want to plot all of them because its important to check them even if none of them are duplicates
#             #grab the data for this unique combination of columns from the combined_data df
#             data_for_plotting = combined_data.loc[index_row]

#             ##CREATE USER INPUT TEXT NOW
#             unique_datasets = data_for_plotting.Dataset.unique()
#             user_input_options, choice_dict = create_user_input_text(unique_datasets, options)

#             ##PLOT            
#             #plot the user input options in text on fig_text
#             fig_text = graph_user_input_options(user_input_options, fig_text, ax_text)
            
#             #plot the current row on fig_row
#             fig_row = graph_current_row(data_for_plotting, index_row, fig_row, ax_row)

#             ##USER INPUT
#             #ask user what dataset they want to choose for each year where a decision needs to be made, and then based on what they choose, update combined_data_concordance_manual 
#             combined_data_concordance_manual, user_input = manual_user_input_function(data_for_plotting, index_row, combined_data_concordance_manual, INDEX_COLS,choice_dict,options)

#             #now the user made their decision, clear the figures so we can plot the next row
#             fig_row.clf()
#             fig_text.clf()

#             if user_input == 'quit':
#                 break
#             else:
#                 print('Finished with unique combination: {}'.format(index_row))
#         else:
#             pass#print('Unique combination: {} did not have more than 1 dataset available so no manual decision was needed'.format(index_row))
#         #save the progress to csv in case anything goes wrong. If you need to access it it should be formatted just like the combined_data_concordance_manual df at the end of this script
#         combined_data_concordance_manual.to_csv(progress_csv, index=True)
#     if user_input == 'quit':
#         print('User input was quit on unique combination {}, so quitting the script and saving the progress to a csv'.format(index_row))
#         break
#     print('Finished with group: {}'.format(group))
# print('Finished with manual selection of datasets')

# #close all the figures
# plt.close('all')

# #TEMP FIX START
# #convert date back to the last day of the year instead of year integer
# combined_data_concordance_manual.reset_index(inplace=True)
# combined_data_concordance_manual.Date = combined_data_concordance_manual.Date.apply(lambda x: str(x)+'-12-31')#TODO somehow Date becomes an index.Would be good to fix this or double check that it doesnt cause any problems
# combined_data.Date = combined_data.Date.apply(lambda x: str(x)+'-12-31')
# duplicates_manual.Date = duplicates_manual.Date.apply(lambda x: str(x)+'-12-31')
# #TEMP FIX END
# #save combined_data_concordance_manual now so we can use it later if we need to
# combined_data_concordance_manual.to_csv('intermediate_data/data_selection/{}_data_selection_manual_backup.csv'.format(FILE_DATE_ID), index=False)
# duplicates_manual.to_csv('intermediate_data/data_selection/{}_duplicates_manual_backup.csv'.format(FILE_DATE_ID), index=True)
# #= pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual_duplicates.csv'.format(FILE_DATE_ID))
# # return combined_data_concordance_manual, duplicates_manual, bad_index_rows, num_bad_index_rows


# def plotly_dashboard(combined_data_group,group):
#     #plot a dashboard using plotly and open it in the user's default browser

#     #create an empty figure in the user's default browser. we will add subplots to this figure
#     fig_dash = go.Figure()
#     #make a shape for the set of subplots that will be as close to a square as possible
#     shape = (int(np.ceil(np.sqrt(len(combined_data_group.Measure.unique())))), int(np.ceil(len(combined_data_group.Measure.unique())/np.ceil(np.sqrt(len(combined_data_group.Measure.unique()))))))
#     #specify subplots using shape
#     fig_dash = make_subplots(rows=shape[0], cols=shape[1], shared_xaxes=True, subplot_titles=combined_data_group.Measure.unique())
#     row=1
#     col = 1
#     i=0
#     for measure in combined_data_group.Measure.unique():
#         #get data for this measure
#         measure_data = combined_data_group.loc[combined_data_group.Measure == measure]
#         row=i//shape[1]+1
#         i+=1
#         col=i%shape[1]+1

#         for dataset in combined_data_group['Dataset'].unique():
#             #plot data for this measure and this daTASET
#             fig_dash.add_trace(go.Scatter(x=measure_data.loc[measure_data.Dataset == dataset].Date, y=measure_data.loc[measure_data.Dataset == dataset].Value, name=dataset, mode='lines+markers'), row=row, col=col)
#     #add a title to the figure as the data in group sep by spaces
#     fig_dash.update_layout(title_text=', '.join(group))
#     #save the figure as temp html file
#     fig_dash.write_html('plotting_output/data_selection_dashboards/dashboard_{}.html'.format('_'.join(group)), auto_open=True)
#     return

































#%%
final_combined_data_concordance = data_formatting_functions.combine_manual_and_automatic_output(combined_data_concordance_automatic,combined_data_concordance_manual,INDEX_COLS)



#%%
#do interpolation:
# FILE_DATE_ID = FILE_DATE_ID
load_progress = False
automatic_interpolation = True
automatic_interpolation_method = 'linear'
percent_of_values_needed_to_interpolate=0.7
load_progress=load_progress
INTERPOLATION_LIMIT=3

#%%
new_final_combined_data,final_combined_data_concordance = data_selection_functions.interpolate_missing_values(final_combined_data_concordance,INDEX_COLS,automatic_interpolation_method, automatic_interpolation,FILE_DATE_ID,percent_of_values_needed_to_interpolate, INTERPOLATION_LIMIT,load_progress)

if use_all_data:
    final_combined_data_concordance.to_csv('output_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID), index=False)
    new_final_combined_data.to_csv('o1utput_data/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)
elif use_9th_edition_set:
    final_combined_data_concordance.to_csv('output_data/9th_dataset/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID), index=False)
    new_final_combined_data.to_csv('output_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)
#%%


