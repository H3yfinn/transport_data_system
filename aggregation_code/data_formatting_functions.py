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
import yaml
#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

import utility_functions as utility_functions
PRINT_GRAPHS_AND_STATS = False

#ignore by message
warnings.filterwarnings("ignore", message="indexing past lexsort depth may impact performance")

"""
combined_dataset dataframe, a combination of all the data that has been aggregated for this selection process, including their values and the dataset they came from. 
combined_data_concordance = dataframe, a dataframe of all the unique index rows of data that has been aggregated for this selection process,but with a few extra columns to help with the selection process. This is created in aggregation_code\1_aggregate_cleaned_datasets.py by creating a unique index row for each row in the output dataset we intend to have. It will have columns such as 'dataseection_method' and 'potential_datapoints' which are used to help the user select the correct datapoint for each row. 

INDEX_COLS = list of strings, the columns which make up the users intended index of the combined dataset. This is used to group the data by these columns so the dataframe can be treated like a dicitonary bv accessing different rows by their index.

previous_combined_dataset  = dataset from previous run. This can be used to import settings made, such as rows to ignore, values already selected etc. It will be compared against the combined dataset and have its rows merged into the combined dataset if the indexes are the same.
    MATCHING MUST BE DONE USING value COLUMN TOO
 However, the index will also include the vlaue column, so if the value column has changed, the row will be treated as a new row. To make sure this works properly, the vlaue MAY NEED TO be rounded to 0 decimal places ?
 There may be cases where the selection process is exited due to a user error. it is important that the previous combined dataset can retrieve the users progress so they can continue where they left off. 
    To habdle this possibility its important that the combined dataset AND combined_Datset_concordance is updated every time the user makes a selection. 
    This is why we dont create copies of the combined dataset and combined data concordance, but instead update them directly. 
    This is also why we need to save the combined dataset and combined data concordance to a pickle file every time the user makes a selection, so that even if the program crashes, the user can still continue where they left off.
This should also be able to be implememted even if the new combined datraset and concoradance are smaller thanb previous one. EASY

datapoints_to_ignore = the datapoint that the user has decided to ignore. Can have a column that determines if the data is  being ignored permanently and a column for if hte data is being ignored temporarily.
    thios should probably be implemented right at the start opf the seleciton process so that the duplicated datasets can be defined early on.

Functions we need:
 sxomething to remove a dataset form the list of datasets for a row in the combined data concordance. This could be used when the user decides to ignore a datapoint but it seems like it would be useful to ahve generally.\
 

 order of functions:
 1. creat data concordance
 2. identify duplicated datapoints to select for

"""
def convert_string_to_snake_case(string):
    """
    Converts a string to snake case
    """
    # Convert anything to snake case, inclkuding a string with spaces
    string = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    string = re.sub('([a-z0-9])([A-Z])', r'\1_\2', string).lower()
    #repalce spaces with underscores
    string = string.replace(' ', '_')
    #replace any double underscores with single underscores
    string = string.replace('__', '_')
    return string

def setup_dataselection_process(FILE_DATE_ID,INDEX_COLS, EARLIEST_date, LATEST_date):
    #PERHAPS COULD GET ALL THIS STUFF FROM CONFIG.YML?
    #create folders to store the data, set paths, aggregate data and create the data concordance:
    intermediate_folder = 'intermediate_data/selection_process/{}/'.format(FILE_DATE_ID)
    if not os.path.exists(intermediate_folder):
        os.makedirs(intermediate_folder)

    #TODO remove this.
    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('date')
    
    INDEX_COLS_no_scope_no_fuel = INDEX_COLS.copy()
    INDEX_COLS_no_scope_no_fuel.remove('scope')
    INDEX_COLS_no_scope_no_fuel.remove('fuel')

    EARLIEST_YEAR = int(EARLIEST_date[:4])
    LATEST_YEAR = int(LATEST_date[:4])
    #create the paths dictionary
    paths_dict = dict()
    paths_dict['intermediate_folder'] = intermediate_folder
    paths_dict['INDEX_COLS_no_year'] = INDEX_COLS_no_year
    paths_dict['INDEX_COLS'] = INDEX_COLS
    paths_dict['EARLIEST_date'] = EARLIEST_date
    paths_dict['LATEST_date'] = LATEST_date
    paths_dict['EARLIEST_YEAR'] = EARLIEST_YEAR
    paths_dict['LATEST_YEAR'] = LATEST_YEAR
    paths_dict['INDEX_COLS_no_scope_no_fuel'] = INDEX_COLS_no_scope_no_fuel
    return paths_dict

def extract_latest_groomed_data():
    #open the yml and extract the datasets:
    with open('config/config.yml', 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    datasets_transport = []
    datasets_other = []
    for dataset in cfg['datasets']:
        if cfg['datasets'][dataset]['included']:
            if cfg['datasets'][dataset]['type'] == 'transport':
                datasets_transport.append([cfg['datasets'][dataset]['folder'], dataset, cfg['datasets'][dataset]['file_path']])
            else:
                datasets_other.append([cfg['datasets'][dataset]['folder'], dataset, cfg['datasets'][dataset]['file_path']])
    #now replace the FILE_DATE_ID with the latest date available for each file:
    for dataset in datasets_transport:
        #get the latest file
        latest_file = utility_functions.get_latest_date_for_data_file(dataset[0], dataset[1])
        #add date to the start of latest_file
        latest_file = 'DATE'+latest_file
        #replace the FILE_DATE_ID with the latest file date
        dataset[2] = dataset[2].replace('FILE_DATE_ID', latest_file)
    for dataset in datasets_other:
        #get the latest file
        latest_file = utility_functions.get_latest_date_for_data_file(dataset[0], dataset[1])
        #add date to the start of latest_file
        latest_file = 'DATE'+latest_file
        #replace the FILE_DATE_ID with the latest file date
        dataset[2] = dataset[2].replace('FILE_DATE_ID', latest_file)
    return datasets_transport, datasets_other

def make_quick_fixes_to_datasets(combined_data):

    #and where fuel is na, set it to 'All'
    combined_data['fuel'] = combined_data['fuel'].fillna('all')
    #if scope col is na then set it to 'national'
    combined_data['scope'] = combined_data['scope'].fillna('national')
    #if comment is NA then set it to 'No comment'
    combined_data['comment'] = combined_data['comment'].fillna('no_comment')
    #remove all na values in value column
    combined_data = combined_data[combined_data['value'].notna()]
    #where medium is not 'road' or 'Road', then set vehicle type and drive to the medium. This is because the medium is the only thing that is relevant for non-road, currently.
    combined_data.loc[combined_data['medium'].str.lower() != 'road', 'vehicle_type'] = combined_data.loc[combined_data['medium'].str.lower() != 'road', 'medium']
    combined_data.loc[combined_data['medium'].str.lower() != 'road', 'drive'] = combined_data.loc[combined_data['medium'].str.lower() != 'road', 'medium']

    #convert all values in all columns to snakecase, except economy and value
    for col in combined_data.columns:
        if col not in ['economy', 'value']:
            #if type of col is not string then tell the user
            if combined_data[col].dtype != 'object':
                print('WARNING: column {} is not a string. It is a {}'.format(col, combined_data[col].dtype))
                # print its non string values:
                print('Unique values in column {} are:'.format(combined_data[col].unique()))
            else:
                #make any nan values into strings. we will fix them later.
                combined_data[col] = combined_data[col].fillna('nan')
                combined_data[col] = combined_data[col].apply(convert_string_to_snake_case)
                #reutrn nas to nan
                combined_data[col] = combined_data[col].replace('nan', np.nan)

    #To make things faster in the manual dataseelection process, for any rows in the eighth edition dataset where the data for both the carbon neutral and reference scenarios (in source column) is the same, we will remove the carbon neutral scenario data, as we would always choose the reference data anyways.
    combined_data = combined_data[combined_data['source'] != 'carbon_neutrality']

    return combined_data

def check_dataset_for_issues(combined_data, INDEX_COLS):

    #if there are any nans in the index columns (EXCEPT FUEL) then throw an error and let the user know: 
    INDEX_COLS_no_fuel = INDEX_COLS.copy()
    INDEX_COLS_no_fuel.remove('fuel')
    if combined_data[INDEX_COLS_no_fuel].isna().any().any():
        #find the columns with nans
        cols_with_nans = combined_data[INDEX_COLS_no_fuel].isna().any()
        #print the columns with nans and how many nans there are in each
        print('The following columns have nans: ')
        for col in cols_with_nans[cols_with_nans].index:
            print('{}: {}'.format(col, combined_data[col].isna().sum()))
        raise Exception('There are nans in the index columns. Please fix this before continuing.')
    #Important step: make sure that units are the same for each measure so that they can be compared. If they are not then the measure should be different.
    #For example, if one measure is in tonnes and another is in kg then they should just be converted. But if one is in tonnes and another is in number of vehicles then they should be different measures.
    for measure in combined_data['measure'].unique():
        if len(combined_data[combined_data['measure'] == measure]['unit'].unique()) > 1:
            print(measure)
            print(combined_data[combined_data['measure'] == measure]['unit'].unique())
            raise Exception('There are multiple units for this measure. This is not allowed. Please fix this before continuing.')
    #check for any duplicates
    if len(combined_data[combined_data.duplicated()]) > 0:
        raise Exception('There are {} duplicates in the combined data. Please fix this before continuing.'.format(len(combined_data[combined_data.duplicated()])))
    
    return combined_data

def replace_bad_col_names(col):
    col = convert_string_to_snake_case(col)
    if col == 'fuel_type':
        col = 'fuel'  
    if col == 'comments':
        col = 'comment'
    return col

def combine_datasets(datasets, FILE_DATE_ID, paths_dict):

    #loop through each dataset and load it into a dataframe, then concatenate the dataframes together
    combined_data = pd.DataFrame()
    for dataset in datasets:
        # #get the latest date for the dataset
        # file_date = utility_functions.get_latest_date_for_data_file(dataset[0], dataset[1])
        # #create the file date id
        # FILE_DATE_ID = 'date{}'.format(file_date)
        # #load the dataset into a dataframe
        new_dataset = pd.read_csv(dataset[2])#.format(FILE_DATE_ID)
        #convert cols to snake case
        new_dataset.columns = [replace_bad_col_names(col) for col in new_dataset.columns]
        #concatenate the dataset to the combined data
        combined_data = pd.concat([combined_data, new_dataset], ignore_index=True)
    combined_data = make_quick_fixes_to_datasets(combined_data)
    check_dataset_for_issues(combined_data, paths_dict['INDEX_COLS'])

    #A make thigns easier in this process, we will concatenate the source and dataset columns into one column called dataset. But if source is na then we will just use the dataset column
    combined_data['dataset'] = combined_data.apply(lambda row: row['dataset'] if pd.isna(row['source']) else row['dataset'] + ' $ ' + row['source'], axis=1)
    #then drop source column
    combined_data = combined_data.drop(columns=['source'])

    ############################################################

    #SAVE DATA

    ############################################################

    #save data to pickle file in intermediate data, as well as a csv file in output data/combined datasets unselected
    combined_data.to_pickle(f"{paths_dict['intermediate_folder']}/unselected_combined_data.pkl")
    combined_data.to_csv(f"output_data/combined_datasets_unselected/combined_data_{FILE_DATE_ID}.csv", index=False)

    return combined_data


def create_concordance(combined_data, frequency = 'Yearly'):
    
    ############################################################

    #CREATE CONCORDANCE

    ############################################################
    #CREATE CONCORDANCE
    #create a concordance which contains all the unique rows in the combined data df, when you remove the dataset source and value columns.
    combined_data_concordance = combined_data.drop(columns=['dataset','comment', 'value']).drop_duplicates()#todo is this the ebst way to handle the cols
    #we will also have to split the frequency column by its type: Yearly, Quarterly, Monthly, Daily
    #YEARLY
    yearly = combined_data_concordance[combined_data_concordance['frequency'] == frequency]
    #YEARS:
    MAX = yearly['date'].max()
    MIN = yearly['date'].min()
    #using datetime creates a range of dates, separated by year with the first year being the MIN and the last year being the MAX
    if frequency == 'Yearly':
        years = pd.date_range(start=MIN, end=MAX, freq='Y')
    elif frequency == 'Monthly':#todo this means that you can ruin data selection on only one frequency at a time, whiich means the way we name files and folders needs to be changed. maybe recondier this
        #using datetime creates a range of dates, separated by month with the first month being the MIN and the last month being the MAX
        years = pd.date_range(start=MIN, end=MAX, freq='M')
    else:
        print("ERROR: frequency not recognised. You might have to update the code to include this frequency.")
        sys.exit()

    #drop date from ATO_data_years
    yearly = yearly.drop(columns=['date']).drop_duplicates()
    #now do a cross join between the concordance and the years array
    combined_data_concordance_new = yearly.merge(pd.DataFrame(years, columns=['date']), how='cross')

    return combined_data_concordance_new


def filter_for_9th_edition_data(combined_data, model_concordances_base_year_measures_file_name, paths_dict):

    ############################################################

    #FILTER FOR 9th data only

    ############################################################
    model_concordances_measures = pd.read_csv(model_concordances_base_year_measures_file_name)


    #Make sure that the model condordance has all the years in the input date range
    original_years = model_concordances_measures['date'].unique()
    for year in range(paths_dict['EARLIEST_YEAR'],paths_dict['LATEST_YEAR']):
        if year not in original_years:
            temp = model_concordances_measures.copy()
            temp['date'] = year
            model_concordances_measures = pd.concat([model_concordances_measures,temp])

    #set date
    model_concordances_measures['date'] = model_concordances_measures['date'].astype(str) + '-12-31'

    #Easiest way to do this is to loop through the unique rows in model_concordances_measures and then if there are any rows that are not in the 8th dataset then add them in with 0 values. 
    INDEX_COLS_no_scope_no_fuel=paths_dict['INDEX_COLS_no_scope_no_fuel']

    #set index#todo what happens if the index is already set?
    model_concordances_measures = model_concordances_measures.set_index(INDEX_COLS_no_scope_no_fuel)
    combined_data = combined_data.set_index(INDEX_COLS_no_scope_no_fuel)

    #Use diff to remove data that isnt in the 9th edition concordance
    extra_rows = combined_data.index.difference(model_concordances_measures.index)
    filtered_combined_data = combined_data.drop(extra_rows)

    #now see what we are missing:
    missing_rows = model_concordances_measures.index.difference(filtered_combined_data.index)
    #create a new dataframe with the missing rows
    missing_rows_df = pd.DataFrame(index=missing_rows)
    # save them to a csv
    print('Saving missing rows to /intermediate_data/9th_dataset/missing_rows.csv. There are {} missing rows'.format(len(missing_rows)))
    missing_rows_df.to_csv('./intermediate_data/9th_dataset/missing_rows.csv')

    filtered_combined_data.reset_index(inplace=True)

    ############################################################

    #CREATE ANOTHER DATAFRAME AND REMOVE THE 0'S, TO SEE WHAT IS MISSING IF WE DO THAT

    ############################################################

    model_concordances_measures_no_zeros = model_concordances_measures.copy()

    combined_data_no_zeros = combined_data[combined_data['value'] != 0]

    #Use diff to remove data that isnt in the 9th edition concordance
    extra_rows = combined_data_no_zeros.index.difference(model_concordances_measures_no_zeros.index)
    filtered_combined_data_no_zeros = combined_data_no_zeros.drop(extra_rows)


    #now see what we are missing:
    missing_rows = model_concordances_measures_no_zeros.index.difference(filtered_combined_data_no_zeros.index)
    #create a new dataframe with the missing rows
    missing_rows_df = pd.DataFrame(index=missing_rows)
    # save them to a csv
    print('Saving missing rows to /intermediate_data/9th_dataset/missing_rows_no_zeros.csv. There are {} missing rows'.format(len(missing_rows)))
    missing_rows_df.to_csv('./intermediate_data/9th_dataset/missing_rows_no_zeros.csv')

    filtered_combined_data_no_zeros.reset_index(inplace=True)

    return filtered_combined_data_no_zeros


def test_identify_erroneous_duplicates(combined_data):
    """check for duplicates in the combined dataset when you ignore the value column.
        combined_data = 
    """
    
    duplicates = combined_data.copy()
    duplicates = duplicates.drop(columns=['value'])
    duplicates = duplicates[duplicates.duplicated(keep=False)]
    if len(duplicates) > 0:
        print('There are duplicate rows in the dataset with different values. Please fix them before continuing. You will probably want to split them into different datasets. The duplicates are: ')
        print(duplicates)

        #extrasct the rows with duplicates and sabve them to a csv so we can import them into a spreadsheet to look at them
        duplicates = combined_data.copy()
        col_no_value = [col for col in duplicates.columns if col != 'value']
        duplicates = duplicates[duplicates.duplicated(subset=col_no_value,keep=False)]
        duplicates.to_csv('intermediate_data/testing/erroneus_duplicates.csv', index=False)

        raise Exception('There are duplicate rows in the dataset. Please fix them before continuing')
    
    return


def test_identify_duplicated_datapoints_to_select_for(combined_data_concordance,new_combined_data_concordance):
    #Check the new concordance has the same amount of rows as the combined_data_concordance and the same index, and two extra columns called 'potential_datapoints' and 'num_datapoints'
    if len(new_combined_data_concordance) != len(combined_data_concordance):
        raise Exception('The new combined data concordance has a different number of rows than the old one. This should not happen')
    if not new_combined_data_concordance.index.equals(combined_data_concordance.index):
        raise Exception('The new combined data concordance has a different index than the old one. This should not happen')
    if not 'potential_datapoints' in new_combined_data_concordance.columns:
        raise Exception('The new combined data concordance does not have the column potential_datapoints. This should not happen')
    if not 'num_datapoints' in new_combined_data_concordance.columns:
        raise Exception('The new combined data concordance does not have the column num_datapoints. This should not happen')
    #CHECK NUMBER OF cols
    if len(new_combined_data_concordance.columns) != len(combined_data_concordance.columns) + 2:
        raise Exception('The new combined data concordance has the wrong number of columns. This should not happen')
    return

def identify_duplicated_datapoints_to_select_for(combined_data,combined_data_concordance, INDEX_COLS):
    """ 
    This function will take in the combined dataset and create a df which essentially summarises the set of datapoints we ahve for each unique index row (including the date). It will create a list of the datasets for which data is available, a count of those datasets as well as the option to conisder the sum of the vlaue, which allows the user to accruately understand if any values for the index row have changed, since the sum of vlaues would very likely have changed too. This is utilised in the import_previous_runs_progress_to_manual() and pickup_incomplete_manual_progress() functions, if the user includes that column during that part of the process.
    """
    #todo, since we are importing deleted datasets later, we should consider wehther that will rem,ove any dupclaites?
    ###########################################################
    #create dataframe of duplicates with list of datasets and count of datasets
    duplicates = combined_data.copy()
    duplicates =  duplicates.groupby(INDEX_COLS,dropna=False).agg({'dataset': lambda x: list(x), 'value': lambda x: sum(x.dropna())}).reset_index()

    #make sure the lists are sorted so that the order is consistent
    duplicates['potential_datapoints'] = duplicates['dataset'].apply(lambda x: sorted(x))
    #create count column
    duplicates['num_datapoints'] = duplicates['dataset'].apply(lambda x: len(x))

    #join onto combined_data_concordance
    new_combined_data_concordance = duplicates.merge(combined_data_concordance, on=INDEX_COLS, how='left')#todo does this result in what we'd like? are there any issues with not using .copy)( on anythiing
    new_combined_data = duplicates.merge(combined_data, on=INDEX_COLS, how='left')#todo, do we need to use [['datasets', 'num_datapoints']] here?

    test_identify_duplicated_datapoints_to_select_for(combined_data,combined_data_concordance,new_combined_data_concordance,INDEX_COLS)

    return new_combined_data_concordance, new_combined_data

##############################################################################
# def create_data_concordance():#TODO this would be better here than in the aggregation code to make it cleaner
#     pass
# #todo do these
# def import_previous_data_concordance():
#     #load in concordance and then use merge_previous_data_concordance() to change any rows that the same.
#     #todo, take a look at the previous selctions fucntion and see if we can just use a merge to udpate it. I guess it is jsut the concordance that needs updating?
#     def merge_previous_data_concordance():
#     pass
#     pass
# def import_previous_combined_data():
#     #whats the point in this one? todo
#     pass
# def merge_previous_combined_data():
#     pass

def filter_for_years_of_interest(combined_data_concordance,combined_data,paths_dict):
   
    #filter data where year is less than our earliest year
    combined_data = combined_data[combined_data['date'] >= paths_dict['EARLIEST_YEAR']]
    combined_data_concordance = combined_data_concordance[combined_data_concordance['date'] >= paths_dict['EARLIEST_YEAR']]
    
    #and also only data where year is less than the latest year
    combined_data = combined_data[combined_data['date'] < paths_dict['LATEST_YEAR']]
    combined_data_concordance = combined_data_concordance[combined_data_concordance['date'] < paths_dict['LATEST_YEAR']]

    return combined_data_concordance,combined_data

def prepare_data_for_selection(combined_data_concordance,combined_data,paths_dict):
    """This function will take in the combined data and combined data concordance dataframes and prepare them for the manual selection process. It will filter the data to only include data from the years we are interested in. 
    #TODO this previously would' remove any duplicate data for the 8th edition transport model carbon neutrality scenario.'. Need to replace that. somewhere else"""#TODO double check that that is true, it came from ai

    test_identify_erroneous_duplicates(combined_data)

    combined_data_concordance,combined_data = identify_duplicated_datapoints_to_select_for(combined_data,combined_data_concordance, paths_dict['INDEX_COLS'])

    combined_data_concordance,combined_data = filter_for_years_of_interest(combined_data_concordance,combined_data,paths_dict)

    combined_data_concordance['dataset'] = None
    combined_data_concordance['value'] = None
    combined_data_concordance['dataset_selection_method'] = None
    combined_data_concordance['comment'] = None

    return combined_data_concordance, combined_data


#%%
def import_previous_selections(previous_selections, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,paths_dict,update_skipped_rows=False):
    """
    #todo we should make this way way more ismple by changing the inhernet way we do it
    Please note this is quite a complicated process. 
    #WARNING THERES POTENTIALLY AN ISSUE WHEN YOU UPdate THE INPUT DATA SO IT INCLUDES ANOTHER DATAPOINT AND YOU LOAD THIS IN, THE MANUAL CONCORDANCE WILL END UP AHVING TWO ROWS FOR THE SAME DATAPOINT? #cHECK IT LATER
    """
    #IMPORT PREVIOUS SELECTIONS
    #Previous_selections can be the previous_combined_data_concordance_manual or the progress_csv depending on if the user wants to import completed manual selections or the progress of some manual selections
   
    #This allows the user to import manual data selections from perveious runs to avoid having to do it again (can replace any rows where the dataset_selection_method is na with where they are Manual in the imported csv)
    ##########################################################
    #We will make sure there are no index rows in the previous dataframes that are not in the current dataframes
    #first the duplicates
    previous_duplicates_manual.set_index(paths_dict['INDEX_COLS'], inplace=True)
    duplicates_manual.set_index(paths_dict['INDEX_COLS'], inplace=True)
    #remove the rows that are in the previous duplicates but not in the current duplicates
    index_diff = previous_duplicates_manual.index.difference(duplicates_manual.index)
    previous_duplicates_manual.drop(index_diff, inplace=True)
    #reset the index
    previous_duplicates_manual.reset_index(inplace=True)
    duplicates_manual.reset_index(inplace=True)

    #now for previous_selections and combined_data_concordance_manual
    previous_selections.set_index(paths_dict['INDEX_COLS'], inplace=True)
    combined_data_concordance_manual.set_index(paths_dict['INDEX_COLS'], inplace=True)
    #remove the rows that are in the previous duplicates but not in the current duplicates
    index_diff = previous_selections.index.difference(combined_data_concordance_manual.index)
    previous_selections.drop(index_diff, inplace=True)
    #reset the index
    previous_selections.reset_index(inplace=True)
    combined_data_concordance_manual.reset_index(inplace=True)
    ##########################################################

    ##There is a chance that some index_rows have had new data added to them, so we will compare the previous duplicates index_rows to the current duplicates and see where thier values are different, make sure that we iterate over them in the manual data selection process
    #so find different rows in the duplicates:
    #first make the datasets col a string so it can be compared
    a = previous_duplicates_manual.copy()
    a.datasets = a.datasets.astype(str)
    b = duplicates_manual.copy()
    b.datasets = b.datasets.astype(str)
    # a.set_index(INDEX_COLS_no_year,inplace=True)
    # b.set_index(INDEX_COLS_no_year,inplace=True)
    duplicates_diff = pd.concat([b, a])
    #set Vlaue to int, because we want to see if the value is the same or not and if float there might be floating point errors (computer science thing)
    duplicates_diff.value = duplicates_diff.value.astype(int)
    #drop duplicates
    duplicates_diff = duplicates_diff.drop_duplicates(keep=False)
    ##########################################################

    ##Update the iterator: We will remove rows where the user doesnt need to select a dataset. This will be done using the previous combined_data_concordance_manual.
    rows_to_skip = previous_selections.copy()
    rows_to_skip.set_index(paths_dict['INDEX_COLS_no_year'], inplace=True)
    #we have to state wat rows we want to remove rather than keep because there will be some in the iterator that are not in the previous_selections df, and we will want to keep them.

    #First remove rows that are in duplicates diff as we want to make sure the user selects for them since they have some detail which is different to what it was before. We do this using index_no_year to cover all the rows that have the same index but different years
    duplicates_diff.set_index(paths_dict['INDEX_COLS_no_year'], inplace=True)
    rows_to_skip = rows_to_skip[~rows_to_skip.index.isin(duplicates_diff.index)]

    #In case we are using the progress csv we will only remove rows where all years data has been selected for that index row. This will cover the way prvious_combined_data_concordance_manual df is formatted as well 
    #first remove where num_datapoints is na or 0. 
    rows_to_skip = rows_to_skip[~((rows_to_skip.num_datapoints.isna()) | (rows_to_skip.num_datapoints==0))]
    #now find rows where there is data but the user hasnt selected a dataset for it. We will remove these rows from rows_to_skip, as we want to make sure the user selects for them (in the future this may cuase issues if we add more selection methods to the previous selections) #note this currently only occurs in the case where an error occurs during sleection
    rows_to_remove_from_rows_to_skip = rows_to_skip[rows_to_skip.dataset_selection_method.isna()]

    if update_skipped_rows:
        #if we are updating the skipped rows then we will also remove from rows to skip the rows where the value and dataset is NaN where the selection method is manual. This will be the case where the user has skipped selection so the selection emthod is manual but there are no values or datasets selected
        rows_to_remove_from_rows_to_skip2 = rows_to_skip[(rows_to_skip.value.isna()) & (rows_to_skip.dataset.isna()) & (rows_to_skip.dataset_selection_method=='Manual')]

    #now remove those rows from rows_to_skip using index_no_year. This will remove any cases where an index row has one or two datapoints that werent chosen, but the rest were.
    rows_to_skip = rows_to_skip[~rows_to_skip.index.isin(rows_to_remove_from_rows_to_skip.index)]

    if update_skipped_rows:
        rows_to_skip = rows_to_skip[~rows_to_skip.index.isin(rows_to_remove_from_rows_to_skip2.index)]

    #KEEP only rows we dont want to skip in the iterator by finding the index rows in both dfs 
    iterator = iterator[~iterator.index.isin(rows_to_skip.index)]

    ###############
    ##And now update the combined_data_concordance_manual:
    #we can just add in rows from rows_to_skip to combined_data_concordance_manual, as well as the rows from duplicates_diff, as these are the rows that are different between old and new. But make sure to remove those rows from combined_data_concordance_manual first, as we dont want to have duplicates.
    #first set indexes to Index_col (with year)
    rows_to_skip.reset_index(inplace=True)
    rows_to_skip.set_index(paths_dict['INDEX_COLS'], inplace=True)
    combined_data_concordance_manual.set_index(paths_dict['INDEX_COLS'], inplace=True)
    duplicates_diff.reset_index(inplace=True)
    duplicates_diff.set_index(paths_dict['INDEX_COLS'], inplace=True)

    #remove the rows from combined_data_concordance_manual
    combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(rows_to_skip.index)]
    combined_data_concordance_manual = combined_data_concordance_manual[~combined_data_concordance_manual.index.isin(duplicates_diff.index)]

    #now add in the rows from rows_to_skip and duplicates_diff
    combined_data_concordance_manual = pd.concat([combined_data_concordance_manual, rows_to_skip, duplicates_diff])

    #remove date from index
    combined_data_concordance_manual.reset_index(inplace=True)

    return combined_data_concordance_manual, iterator



##############################################################################





def create_config_yml_file(paths_dict):
        
    datasets = [('intermediate_data/8th_edition_transport_model/', 'eigth_edition_transport_data_final_', 'intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_final_FILE_DATE_ID.csv'), ('intermediate_data/estimated/', '_8th_ATO_passenger_road_updates.csv', './intermediate_data/estimated/FILE_DATE_ID_8th_ATO_passenger_road_updates.csv'), ('intermediate_data/estimated/', '_8th_ATO_bus_update.csv', './intermediate_data/estimated/FILE_DATE_ID_8th_ATO_bus_update.csv'), ('intermediate_data/estimated/', '_8th_ATO_road_freight_tonne_km.csv', './intermediate_data/estimated/FILE_DATE_ID_8th_ATO_road_freight_tonne_km.csv'), ('intermediate_data/estimated/', '_8th_iea_ev_all_stock_updates.csv', 'intermediate_data/estimated/FILE_DATE_ID_8th_iea_ev_all_stock_updates.csv'), ('intermediate_data/estimated/', '_8th_ATO_vehicle_type_update.csv', './intermediate_data/estimated/FILE_DATE_ID_8th_ATO_vehicle_type_update.csv'), ('intermediate_data/ATO/', 'ATO_data_cleaned_', 'intermediate_data/ATO/ATO_data_cleaned_FILE_DATE_ID.csv'), ('intermediate_data/item_data/', 'item_dataset_clean_', 'intermediate_data/item_data/item_dataset_clean_FILE_DATE_ID.csv'), ('intermediate_data/estimated/', '_turnover_rate_3pct', 'intermediate_data/estimated/FILE_DATE_ID_turnover_rate_3pct.csv'),  ('intermediate_data/estimated/', 'EGEDA_merged', 'intermediate_data/estimated/EGEDA_mergedFILE_DATE_ID.csv'), ('intermediate_data/estimated/', 'ATO_revenue_pkm', 'intermediate_data/estimated/ATO_revenue_pkmFILE_DATE_ID.csv'), ('intermediate_data/estimated/', 'nearest_available_date', 'intermediate_data/estimated/nearest_available_dateFILE_DATE_ID.csv'), ('intermediate_data/IEA/', '_iea_fuel_economy.csv', 'intermediate_data/IEA/FILE_DATE_ID_iea_fuel_economy.csv'), ('intermediate_data/IEA/', '_evs.csv', 'intermediate_data/IEA/FILE_DATE_ID_evs.csv'), ('intermediate_data/estimated/filled_missing_values/', 'missing_drive_values_', 'intermediate_data/estimated/filled_missing_values/missing_drive_values_FILE_DATE_ID.csv'), ('intermediate_data/estimated/', 'occ_load_guesses', 'intermediate_data/estimated/occ_load_guessesFILE_DATE_ID.csv'), ('intermediate_data/estimated/', 'new_vehicle_efficiency_estimates_', 'intermediate_data/estimated/new_vehicle_efficiency_estimates_FILE_DATE_ID.csv'), ('intermediate_data/Macro/', 'all_macro_data_', 'intermediate_data/Macro/all_macro_data_FILE_DATE_ID.csv')]


    #saVE THESE TO config.YML 
    # #open yml
    # import yaml
    # with open('config/config.yml', 'r') as ymlfile:
    #     cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    #create a key called datassets then set it so the key is the second element of the tuple, then folder: is the first element of the tuple and file_path: is the third element of the tuple. Then we will also have a script: value which will be set to TBA
    cfg = dict()
    cfg['datasets'] = dict()
    for dataset in datasets:
        
        cfg['datasets'][dataset[1]] = {'folder': dataset[0], 'file_path': dataset[2], 'script': 'TBA'}

    #save INDEX_COLS to the yml under the key 'INDEX_COLS'
    cfg['INDEX_COLS'] = paths_dict['INDEX_COLS']
    #save yml
    with open('config/config.yml', 'w') as ymlfile:
        yaml.dump(cfg, ymlfile)
    return






















def combine_manual_and_automatic_output(combined_data_concordance_automatic,combined_data_concordance_manual,INDEX_COLS):
    #todo, i think its useful to have automatic selection but i think it needs to be a lot more functional. for example it should be able to easily implement the following:
    #if the user says a dataset should be prioirtised then it should be.
    #... anything else


    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('date')

    #COMBINE MANUAL AND AUTOMATIC DATA SELECTION OUTPUT DFs
    #join the automatic and manual datasets so we can compare the dataset  columns from both the manual dataset automatic dataset
    #create a final_dataset column. This will be filled with the dataset in the automatic column, except where the values in the manual and automatic dataset columns are different, for which we will use the value in the manual dataset.

    #reset and set index of both dfs to INDEX_COLS
    combined_data_concordance_manual = combined_data_concordance_manual.set_index(INDEX_COLS)
    combined_data_concordance_automatic = combined_data_concordance_automatic.reset_index().set_index(INDEX_COLS)

    #remove the datasets and dataset_selection_method columns from the manual df
    combined_data_concordance_manual.drop(columns=['datasets','dataset_selection_method'], inplace=True)
    #join manual and automatic data selection dfs
    final_combined_data_concordance = combined_data_concordance_manual.merge(combined_data_concordance_automatic, how='outer', left_index=True, right_index=True, suffixes=('_manual', '_auto'))

    #we will either have dataset names or nan values in the manual and automatic dataset columns. We want to use the manual dataset column if it is not nan, otherwise use the automatic dataset column:
    #first set the dataset_selection_method column based on that criteria, and then use that to set other columns
    final_combined_data_concordance.loc[final_combined_data_concordance['dataset_auto'].notnull(), 'dataset_selection_method'] = 'Automatic'
    #if the manual dataset column is not nan then use that instead
    final_combined_data_concordance.loc[final_combined_data_concordance['dataset_manual'].notnull(), 'dataset_selection_method'] = 'Manual'

    #Now depending on the value of the dataset_selection_method column, we can set final_value and final_dataset columns
    #if the dataset_selection_method is manual then use the manual dataset column
    final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Manual', 'value'] = final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Manual','value_manual']
    final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Manual', 'dataset'] = final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Manual','dataset_manual']
    #if the dataset_selection_method is automatic then use the automatic dataset column
    final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Automatic', 'dataset'] = final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Automatic','dataset_auto']
    final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Automatic', 'value'] = final_combined_data_concordance.loc[final_combined_data_concordance['dataset_selection_method'] == 'Automatic','value_auto']

    #drop cols ending in _manual and _auto
    final_combined_data_concordance.drop(columns=[col for col in final_combined_data_concordance.columns if col.endswith('_manual') or col.endswith('_auto')], inplace=True)

    return final_combined_data_concordance



##############################################################################

def create_manual_data_iterator(
combined_data_concordance_iterator,
INDEX_COLS,
combined_data_concordance_manual,
duplicates_manual,
rows_to_select_manually_df=[],
run_only_on_rows_to_select_manually=False,
manually_chosen_rows_to_select=None,
user_edited_combined_data_concordance_iterator=None,
previous_selections=None,
previous_duplicates_manual=None,
update_skipped_rows=False):
    
    """
    manually_chosen_rows_to_select: set to true if you want to manually choose the rows to select using user_edited_combined_data_concordance_iterator
    user_edited_combined_data_concordance_iterator: a manually chosen dataframe with the rows to select. This allows user to define what they want to select manually (eg. all stocks)

    duplicates_manual & previous_duplicates_manual need to be available if you want to use either pick_up_where_left_off or import_previous_selection. progress_csv should also be available if you want to use pick_up_where_left_off

    This will create an iterator which will be used to manually select the dataset to use for each row. it is the same as the iterator which is input into this fucntion but it also has had data removed from it so that it only contains rows where we need to manually select the dataset to use
    """
    #Remove year from the current cols without removing it from original list, and set it as a new list
    INDEX_COLS_no_year = paths_dict['INDEX_COLS_no_year']

    #todo what function was this really filling
    # #CREATE ITERATOR 
    # #if we want to add to the rows_to_select_manually_df to check specific rows then set the below to True
    # if run_only_on_rows_to_select_manually:
    #     #if this, only run the manual process on index rows where we couldnt find a dataset to use automatically for some year
    #     #since the automatic method is relatively strict there should be a large amount of rows to select manually
    #     #note that if any one year cannot be chosen automatically then we will have to choose the dataset manually for all years of that row
    #     iterator = rows_to_select_manually_df.copy()
    #     iterator.set_index(INDEX_COLS_no_year, inplace=True)
    #     iterator.drop_duplicates(inplace=True)#TEMP get rid of this later
    # elif manually_chosen_rows_to_select:
    #     #we can add rows form the combined_data_concordance_iterator as edited by the user themselves. 
    #     iterator = user_edited_combined_data_concordance_iterator.copy()
    #     #since user changed the data we will jsut reset index and set again
    #     iterator.reset_index(inplace=True)
    #     iterator.set_index(INDEX_COLS_no_year, inplace=True)

    #     #for this example we will add all Stocks data (for the purpoose of betterunderstanding our stocks data!) and remove all the other data. But this is just an example of what the user could do to select specific rows
    #     use_example = False
    #     if use_example:
    #         iterator.reset_index(inplace=True)
    #         iterator = iterator[iterator['measure']=='Stocks']
    #         #set the index to the index cols
    #         iterator.set_index(INDEX_COLS_no_year, inplace=True)
    # else:
    #     iterator = combined_data_concordance_iterator.copy()

    #to do this should be a default process at start of whjole process i think 
    # #now determine whether we want to import previous progress or not:
    # if previous_selections is not None:
    #     combined_data_concordance_manual,iterator = import_previous_selections(previous_selections, combined_data_concordance_manual,previous_duplicates_manual,duplicates_manual,iterator,INDEX_COLS,update_skipped_rows)
        
    return iterator, combined_data_concordance_manual



##############################################################################

