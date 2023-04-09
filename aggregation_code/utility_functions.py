
#####################################################
import os
import re
import datetime
import sys
import logging
import numpy as np
import pandas as pd
import shutil
#set cwd to the root of the project
# os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
logger = logging.getLogger(__name__)
#%%
def compare_data_to_previous_version_of_data_in_csv(df,prev_file_path):
    #compare data to the msot recent previoous version of the data, and if it is not the ssame, save it
    if os.path.exists(prev_file_path):
        df_prev = pd.read_csv(prev_file_path)
        if df.equals(df_prev):
            print('The data has not changed since the last time it was saved, not saving it again')
            return False
        else:
            return True
    else:
        return True

def make_economy_code_to_name_tall(economy_code_to_name):
    """Make the economy code to name data wide. 
    """
    #format economy code to name to name to be tall so we can access all possible econmoy names.
    #stack the Economy name, and any columns that begin with Alt_name into a single column
    alt_name_cols = [col for col in economy_code_to_name.columns if col.startswith('Alt_name')]

    economy_code_to_name = economy_code_to_name.melt(id_vars=['Economy'], value_vars=['Economy_name']+alt_name_cols, var_name='column_name', value_name='Economy name').drop(columns=['column_name'])

    #drop na values from economy name
    economy_code_to_name = economy_code_to_name.dropna(subset=['Economy name'])
    return economy_code_to_name

def setup_paths_dict(FILE_DATE_ID,INDEX_COLS, EARLIEST_date, LATEST_date, previous_FILE_DATE_ID=None,save_plotting_backups=True,previous_selections_file_path=None):
    paths_dict = dict()
    paths_dict['log_file_path'] = 'logs/{}.log'.format(FILE_DATE_ID)
    #PERHAPS COULD GET ALL THIS STUFF FROM CONFIG.YML?
    #create folders to store the data, set paths, aggregate data and create the data concordance:
    intermediate_folder = 'intermediate_data/selection_process/{}/'.format(FILE_DATE_ID)
    paths_dict['selection_groups_folder'] = os.path.join(intermediate_folder, '/groups/')
    if not os.path.exists(intermediate_folder):
        os.makedirs(intermediate_folder)
    else:
        #make copy of all data to a folder with the current time so they can be recovered if needed.
        backup_dir = os.path.join(intermediate_folder, 'backup_'+datetime.datetime.now().strftime("%H_%M_%S"))  
        os.makedirs(backup_dir)
        for file in os.listdir(intermediate_folder):
            #if folder then ignore
            if os.path.isdir(os.path.join(intermediate_folder, file)):
                continue
            shutil.copy(os.path.join(intermediate_folder, file), backup_dir)
            
    if not os.path.exists(paths_dict['selection_groups_folder']):
        os.makedirs(paths_dict['selection_groups_folder'])

    #TODO remove this.
    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('date')
    
    INDEX_COLS_no_scope_no_fuel = INDEX_COLS.copy()
    INDEX_COLS_no_scope_no_fuel.remove('scope')
    INDEX_COLS_no_scope_no_fuel.remove('fuel')

    EARLIEST_YEAR = int(EARLIEST_date[:4])
    LATEST_YEAR = int(LATEST_date[:4])

    paths_dict['intermediate_folder'] = intermediate_folder
    paths_dict['output_data_folder'] = 'output_data/'
    paths_dict['INDEX_COLS_no_year'] = INDEX_COLS_no_year
    paths_dict['INDEX_COLS'] = INDEX_COLS
    paths_dict['EARLIEST_date'] = EARLIEST_date
    paths_dict['LATEST_date'] = LATEST_date
    paths_dict['EARLIEST_YEAR'] = EARLIEST_YEAR
    paths_dict['LATEST_YEAR'] = LATEST_YEAR
    paths_dict['INDEX_COLS_no_scope_no_fuel'] = INDEX_COLS_no_scope_no_fuel
    paths_dict['FILE_DATE_ID'] = FILE_DATE_ID

    #check previous_selections_file_path exists
    if previous_selections_file_path is not None:
        if os.path.exists(previous_selections_file_path):
            paths_dict['previous_selections_file_path'] = previous_selections_file_path
        else:
            raise ValueError('The previous selections file path you provided does not exist')
    else:
        paths_dict['previous_selections_file_path'] = previous_selections_file_path
    #serach for previous_selections_file_name in intermediate_data/selection_process/
    #if it exists, then use the path as previous_selections_file_path
    #if it does not exist, then notify user and set it to noone
    # if previous_selections_file_name is not None:
    #     #walk through all files in intermediate_data/selection_process/ and find the file with the name previous_selections_file_name
    #     for root, dirs, files in os.walk('intermediate_data/selection_process/'):
    #         for file in files:
    #             if file == previous_selections_file_name:
    #                 paths_dict['previous_selections_file_path'] = os.path.join(root, file)
    #                 break
    #         else:
    #             continue
    #     if 'previous_selections_file_path' not in paths_dict.keys():
    #         raise ValueError('The previous selections file name you provided does not exist in the intermediate_data/selection_process/ folder')

    # paths_dict['measure_to_units_dict'] = {'vehicle_km': 'km', 'activity': 'passenger_km_or_freight_tonne_km', 'energy': 'pj', 'mileage': 'km_per_stock', 'stocks': 'stocks', 'occupancy_or_load': 'passengers_or_tonnes', 'new_vehicle_efficiency': 'pj_per_km'}
    #base units off the 9th edition concordance:
    measures_to_units = pd.read_csv('input_data/concordances/9th/measure_to_unit_concordance.csv')
    #make sure the columns are snake case
    measures_to_units.columns = [convert_string_to_snake_case(col) for col in measures_to_units.columns]
    #convert all values in cols to snake case
    measures_to_units = convert_all_cols_to_snake_case(measures_to_units)
    #convert to dict
    paths_dict['measure_to_units_dict'] = measures_to_units.set_index('measure').to_dict()['unit']
    

    # #get egeda data for calcualting enegry totals for non road passsenger
    # egeda_date_id = get_latest_date_for_data_file(data_folder_path='./intermediate_data/estimated/', file_name='EGEDA_merged')
    # paths_dict['egeda_data_file'] = './intermediate_data/estimated/EGEDA_merged{}.csv'.format(egeda_date_id)

    #initial dfs:
    paths_dict['unfiltered_combined_data'] = os.path.join(paths_dict['intermediate_folder'], f'unfiltered_combined_data.pkl')
    paths_dict['combined_data'] = os.path.join(paths_dict['intermediate_folder'], f'combined_data.pkl')
    paths_dict['combined_data_concordance'] = os.path.join(paths_dict['intermediate_folder'], f'combined_data_concordance.pkl')

    #stocks_mileage_occupancy_load_efficiency dfs:
    #selection:
    paths_dict['stocks_mileage_occupancy_load_efficiency_combined_data_concordance'] = os.path.join(paths_dict['intermediate_folder'],'stocks_mileage_occupancy_load_efficiency_combined_data_concordance.pkl')
    #interp:
    paths_dict['interpolated_stocks_mileage_occupancy_load_efficiency_combined_data_concordance'] = os.path.join(paths_dict['intermediate_folder'],'interpolated_stocks_mileage_occupancy_load_efficiency_combined_data_concordance.pkl')
    
    #energy_activitydfs:
    #preparation/calcualtion:
    paths_dict['calculated_activity_energy_combined_data'] = os.path.join(paths_dict['intermediate_folder'],'calculated_activity_energy_combined_data.pkl')
    #selection:
    paths_dict['all_other_combined_data_concordance'] = os.path.join(paths_dict['intermediate_folder'],'all_other_combined_data_concordance.pkl')
    #interpolation:
    paths_dict['interpolated_all_other_combined_data_concordance'] = os.path.join(paths_dict['intermediate_folder'],'interpolated_all_other_combined_data_concordance.pkl')
    #final bnefore calcualting anthing new for non road:
    paths_dict['all_selections_done_combined_data_concordance'] = os.path.join(paths_dict['intermediate_folder'],f'all_selections_done_combined_data_concordance_{FILE_DATE_ID}.pkl')
    paths_dict['all_selections_done_combined_data'] = os.path.join(paths_dict['intermediate_folder'],f'all_selections_done_combined_data_{FILE_DATE_ID}.pkl')
    #final all data dfs:
    paths_dict['final_combined_data_not_rescaled'] = os.path.join(paths_dict['intermediate_folder'],'final_combined_data_not_rescaled.pkl')
    paths_dict['final_combined_rescaled_data'] = os.path.join(paths_dict['intermediate_folder'],'final_combined_rescaled_data.pkl')
    paths_dict['final_data_csv'] = paths_dict['output_data_folder']+ 'combined_data_{}.csv'.format(paths_dict['FILE_DATE_ID'])
    ####

    #infrequenty used data formatting paths
    paths_dict['erroneus_duplicates'] =  os.path.join(paths_dict['intermediate_folder'],'erroneus_duplicates.csv')
    paths_dict['missing_rows_no_zeros'] = os.path.join(paths_dict['intermediate_folder'], 'missing_rows_no_zeros.csv')
    paths_dict['missing_rows'] = os.path.join(paths_dict['intermediate_folder'], 'missing_rows.csv')
    paths_dict['combined_data_error.pkl'] = os.path.join(paths_dict['intermediate_folder'], 'combined_data_error.pkl')
    paths_dict['unselected_combined_data'] = f"{paths_dict['intermediate_folder']}/unselected_combined_data.pkl"

    #selection
    paths_dict['selection_progress_pkl'] = os.path.join(intermediate_folder, f'selection_progress.pkl')
    #interpoaltion

    #previous data
    if previous_FILE_DATE_ID is not None:
        #we will load data from the previous file date id, althoguh we will save the data to the current file date id. This will only be created for those files that take a while to create
        paths_dict['previous_intermediate_folder'] = 'intermediate_data/selection_process/{}/'.format(previous_FILE_DATE_ID)
        #initial dfs:
        paths_dict['previous_unfiltered_combined_data'] = os.path.join(paths_dict['previous_intermediate_folder'], f'unfiltered_combined_data.pkl')
        paths_dict['previous_combined_data'] = os.path.join(paths_dict['previous_intermediate_folder'], f'combined_data.pkl')
        paths_dict['previous_combined_data_concordance'] = os.path.join(paths_dict['previous_intermediate_folder'], f'combined_data_concordance.pkl')
        #stocks_mileage_occupancy_load_efficiency
        #selection
        paths_dict['previous_stocks_mileage_occupancy_load_efficiency_combined_data_concordance'] = os.path.join(paths_dict['previous_intermediate_folder'],'stocks_mileage_occupancy_load_efficiency_combined_data_concordance.pkl')
        #interpolation
        paths_dict['previous_interpolated_stocks_mileage_occupancy_load_efficiency_combined_data_concordance'] = os.path.join(paths_dict['previous_intermediate_folder'],'interpolated_stocks_mileage_occupancy_load_efficiency_combined_data_concordance.pkl')

        #energy_activity dfs:
        #preparation/calcualtion:

        #selection:
        paths_dict['previous_all_other_combined_data_concordance'] = os.path.join(paths_dict['previous_intermediate_folder'],'all_other_combined_data_concordance.pkl')
        #interpolation:
        paths_dict['previous_interpolated_all_other_combined_data_concordance'] = os.path.join(paths_dict['previous_intermediate_folder'],'interpolated_all_other_combined_data_concordance.pkl')

        paths_dict['previous_selection_progress_pkl'] = os.path.join(paths_dict['previous_intermediate_folder'], f'selection_progress.pkl')

    paths_dict = add_plot_paths_to_paths_dict(paths_dict, save_plotting_backups)

    return paths_dict



def add_plot_paths_to_paths_dict(paths_dict, save_plotting_backups):
    #add all the paths used for plotting to the paths dict
    paths_dict['plotting_dir'] = 'plotting_output/data_selection/{}'.format(paths_dict['FILE_DATE_ID'])

    if not os.path.exists(paths_dict['plotting_dir']):
        os.makedirs(paths_dict['plotting_dir'])
    elif save_plotting_backups:
        #make copy of all data to a folder with the current time so they can be recovered if needed.
        backup_dir = os.path.join(paths_dict['plotting_dir'], 'backup_'+datetime.datetime.now().strftime("%H_%M_%S"))  
        os.makedirs(backup_dir)
        for file in os.listdir(paths_dict['plotting_dir']):
            #if folder then ignore
            if os.path.isdir(os.path.join(paths_dict['plotting_dir'], file)):
                continue
            shutil.copy(os.path.join(paths_dict['plotting_dir'], file), backup_dir)
    else:
        #delete all files in the folder
        for file in os.listdir(paths_dict['plotting_dir']):
            #if folder then ignore
            if os.path.isdir(os.path.join(paths_dict['plotting_dir'], file)):
                continue
            os.remove(os.path.join(paths_dict['plotting_dir'], file))
    
    #add paths for the plots. we will just use the name of the function which creates the plots.
    paths_dict['plotting_paths'] = {}
    paths_dict['plotting_paths']['plot_new_and_old_road_measures'] = os.path.join(paths_dict['plotting_dir'], 'plot_new_and_old_road_measures')
    paths_dict['plotting_paths']['plot_egeda_comparison'] = os.path.join(paths_dict['plotting_dir'], 'plot_egeda_comparison')
    paths_dict['plotting_paths']['plot_egeda_comparison_medium_proportions'] = os.path.join(paths_dict['plotting_dir'], 'plot_egeda_comparison_medium_proportions')
    paths_dict['plotting_paths']['plot_egeda_comparison_total_energy_use'] = os.path.join(paths_dict['plotting_dir'], 'plot_egeda_comparison_total_energy_use')
    paths_dict['plotting_paths']['plot_egeda_comparison_total_energy_use_diff'] = os.path.join(paths_dict['plotting_dir'], 'plot_egeda_comparison_total_energy_use_diff')
    paths_dict['plotting_paths']['plot_egeda_comparison_total_energy_use_diff_medium_proportions'] = os.path.join(paths_dict['plotting_dir'], 'plot_egeda_comparison_total_energy_use_diff_medium_proportions')
    paths_dict['plotting_paths']['plot_plotly_non_road_energy_estimations_by_economy'] = os.path.join(paths_dict['plotting_dir'], 'plot_plotly_non_road_energy_estimations_by_economy')
    paths_dict['plotting_paths']['graph_egeda_road_energy_use_vs_calculated'] = os.path.join(paths_dict['plotting_dir'], 'graph_egeda_road_energy_use_vs_calculated')
    paths_dict['plotting_paths']['plot_plotly_non_road_energy_estimations'] = os.path.join(paths_dict['plotting_dir'], 'plot_plotly_non_road_energy_estimations')
    paths_dict['plotting_paths']['plot_non_road_energy_use_by_transport_type'] = os.path.join(paths_dict['plotting_dir'], 'plot_non_road_energy_use_by_transport_type')
    paths_dict['plotting_paths']['plot_intensity'] = os.path.join(paths_dict['plotting_dir'], 'plot_intensity')
    paths_dict['plotting_paths']['plot_activity'] = os.path.join(paths_dict['plotting_dir'], 'plot_activity')
    paths_dict['plotting_paths']['plot_final_data_energy_activity'] = os.path.join(paths_dict['plotting_dir'], 'plot_final_data_energy_activity')

    #for selection processes
    paths_dict['plotting_paths']['interpolation_timeseries'] = os.path.join(paths_dict['plotting_dir'], 'interpolation')
    paths_dict['plotting_paths']['selection_dashboard'] = os.path.join(paths_dict['plotting_dir'], 'dashboards')
    paths_dict['plotting_paths']['selection_timeseries'] = os.path.join(paths_dict['plotting_dir'], 'timeseries')

    #now create the folders
    for path in paths_dict['plotting_paths'].values():
        if not os.path.exists(path):
            os.makedirs(path)
    
 
    
    return paths_dict



def determine_date_format(df):
    #identify if date is in yyyy or yyyy-mm-dd format
    if len(str(df['date'].max())) == 4:
        return 'yyyy'
    elif len(str(df['date'].max())) == 10:
        return 'yyyy-mm-dd'
    else:
        raise Exception('Date format not recognised')
        
def ensure_date_col_is_year(df):

    #change dataframes date col to be just the year in that date eg. 2022-12-31 = 2022 by using string slicing
    #frist check the date format is yyyy-mm-dd or yyyy
    date_format = determine_date_format(df)
    if date_format == 'yyyy':
        #if the date is just yyyy then we just need to make sure it is an int
        df.date = df.date.astype(int)
    else:
        #first makje date a object
        df.date = df.date.astype(str)
        #then slice the string to get the year
        df.date = df.date.apply(lambda x: x[:4]).astype(int)
    return df

def ensure_date_col_is_yyyy_mm_dd(df):

    #change dataframes date col to be the final day of the year in that date eg. 2022-12-31 = 2022 by using string slicing
    #frist check the date format is yyyy-mm-dd or yyyy
    date_format = determine_date_format(df)
    if date_format == 'yyyy-mm-dd':
        #if the date is just yyyy-mm-dd then great!
        pass
    else:
        #given that the other date format is yyyy, we need to convert it to yyyy-mm-dd by adding -12-31 to the end of the string
        df.date = df.date.astype(str)
        df.date = df.date.apply(lambda x: x + '-12-31')
    return df

def setup_logging(FILE_DATE_ID,paths_dict,testing=True):
    if testing:
        logging_level = logging.DEBUG
    else:
        logging_level = logging.INFO
    #set up logging now that we have the paths all set up:
    logging.basicConfig(
        handlers=[
            logging.StreamHandler(sys.stdout),#logging will print things to the console as well as to the log file
            logging.FileHandler(paths_dict['log_file_path'])
        ], encoding='utf-8', level=logging_level, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logger = logging.getLogger()
    logger.info(f"LOGGING STARTED: {FILE_DATE_ID}, being saved to {paths_dict['log_file_path']} and outputted to console")
    
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

def convert_all_cols_to_snake_case(df):
    #will convert all vlaues in cols to snake case
    for col in df.columns:
        if col not in ['economy', 'value', 'date']:
            #if type of col is not string then tell the user
            if df[col].dtype != 'object':
                logging.info('Column {} is not a string. It is a {}'.format(col, df[col].dtype))
                # print its non string values:
                logging.info('Unique values in column {} are: {}'.format(col, df[col].unique()))
            else:
                #make any nan values into strings. 
                df[col] = df[col].fillna('nan')
                df[col] = df[col].apply(convert_string_to_snake_case)
                #reutrn nas to nan
                df[col] = df[col].replace('nan', np.nan)
    return df


#%%
def get_latest_date_for_data_file(data_folder_path, file_name):
    #get list of all files in the data folder
    all_files = os.listdir(data_folder_path)
    #filter for only the files with the correct file extension
    all_files = [file for file in all_files if file_name in file]
    #drop any files with no date in the name
    all_files = [file for file in all_files if re.search(r'\d{8}', file)]
    #get the date from the file name
    all_files = [re.search(r'\d{8}', file).group() for file in all_files]
    #convert the dates to datetime objects
    all_files = [datetime.datetime.strptime(date, '%Y%m%d') for date in all_files]
    #get the latest date
    try:
        latest_date = max(all_files)
    except ValueError:
        print('No files found for ' + file_name)
        return None
    #convert the latest date to a string
    latest_date = latest_date.strftime('%Y%m%d')
    return latest_date
#%%

def move_archive_to_different_folder(new_archive_folder_path, root_folder_path):
    #move the archive files stored throughout this project to a new folder outside this project

    #get list of all files in an archive folder in this project, including their paths and all subsequent subfolders
    all_files = [os.path.join(root, file) for root, dirs, files in os.walk(root_folder_path) for file in files]
    # grab only the files that are in an archive folder
    all_files = [file for file in all_files if 'archive' in file]

    #move these files to the new_archive_folder_path. Save them with the same folder structure as they had in this project, so if the folder is missing in the new folder, create it
    for file in all_files:
        #get the folder path for the file
        folder_path = os.path.dirname(file)
        #get the new folder path for the file
        new_folder_path = folder_path.replace(root_folder_path, new_archive_folder_path)
        #if the folders for these paths dont exist, create them
        if not os.path.exists(new_folder_path):
            os.makedirs(new_folder_path)
        #move the file to the new folder path
        os.rename(file, file.replace(root_folder_path, new_archive_folder_path))
    return all_files

#%%

# root_folder_path = './'
# archive_folder_path = '../all_archived_files/'
# move_archive_to_different_folder(archive_folder_path, root_folder_path)

#%%
#use os.walk to grab all folders and files in the root folder path, including all subsequent subfolders
import os
def move_archived_files_to_different_folder(all_archived_files = 'all_archived_files', root_folder_path = './',ignored_folders_and_files = ['.vscode', '.git','env_jupyter','env_no_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__']):

    file_list = []
    for root, dirs, files in os.walk(root_folder_path):
        #remove any folders that are in the ignored folders list
        dirs[:] = [d for d in dirs if d not in ignored_folders_and_files]
        #remove any files that are in the ignored files list
        files[:] = [f for f in files if f not in ignored_folders_and_files]
        #remove all files that are
        #connect the dir to the file name with forward slashes
        files = [os.path.join(root, file) for file in files]
        #make  slashes forward slashes
        files = [file.replace('\\', '/') for file in files]
        #add the files to the file list
        file_list.extend(files)

    #remove any files that dont contain the word archive
    file_list = [file for file in file_list if 'archive' in file]

    #add the folder name for this project to the start of the file path
    #get folder name for this project
    project_folder_name = os.path.basename(os.getcwd())

    #now we have a list of all files in the project that are in an archive folder. We will move these to a new folder outside the project, and save them with the same folder structure as they had in this project, so if the folder is missing in the new folder, create it
    for file in file_list:
        #get the folder path for the file
        folder_path = os.path.dirname(file)
        #add the project name to the folder path
        folder_path = folder_path.replace('./', f'./{project_folder_name}/')
        #get the new folder path for the file
        new_folder_path = folder_path.replace('./', f'../{all_archived_files}/')
        #if the folders for these paths dont exist, create them
        if not os.path.exists(new_folder_path):
            os.makedirs(new_folder_path)

        #check if the file is already there, if so then rename that file to '_old', then move the new file to the new folder path. IF there is already a file that has _old in the name, then ask the user if they want to overwrite it
        #create copy of name
        file_name = file.replace('./', f'../{all_archived_files}/{project_folder_name}/')
        #check if the file is already there. If it is, then rename it to _old
        if os.path.exists(file_name):
            #if the file is already there, then rename it to _old
            #get base name 
            new_file_name = os.path.basename(file_name)
            # and remove extension
            extension = os.path.splitext(new_file_name)[1]
            new_file_name = os.path.splitext(new_file_name)[0]
            #  then add _old
            new_file_name = new_file_name + '_old'
            #  then add extension
            new_file_name = new_file_name + extension
            #  then add the folder path
            new_file_name = os.path.dirname(file_name) + '/' + new_file_name
            #if this new file name already exists, then ask the user if they want to overwrite it, otherwise return an error
            if os.path.exists(new_file_name):
                user_input = input(f'The file {new_file_name} already exists. Do you want to overwrite it? (y/n)')
                if user_input == 'y':
                    #rename the file
                    os.rename(file_name, new_file_name)
                else:
                    #error
                    raise Exception(f'The file {new_file_name} already exists. Please fix the issue and try again')
            else:
                #rename the file
                os.rename(file_name, new_file_name)

        #NOW move the ORIGINAL file to the new folder path   
        os.rename(file, file_name)

# move_archived_files_to_different_folder(all_archived_files = 'all_archived_files', root_folder_path = './plotting_output',ignored_folders_and_files = ['.vscode', '.git','env_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__','env_no_jupyter'])




#%%
def move_all_files_to_archive(folder,files_to_ignore = ['.vscode', '.git','env_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__', 'Readme.md']):
    #move all files in a folder to an archive folder in that folder. This will walk through all subfolders in the folder and move all files to an archive folder in each subfolder
    #get list of all files in the folder, including their paths and all subsequent subfolders
    all_files = [os.path.join(root, file) for root, dirs, files in os.walk(folder) for file in files]
    #replace double backslashes with forward slashes
    all_files = [file.replace('\\', '/') for file in all_files]
    #ignore any files that are in the archive folder
    all_files = [file for file in all_files if 'archive' not in file]
    #ignore any files that are in the files_to_ignore list
    all_files = [file for file in all_files if os.path.basename(file) not in files_to_ignore]

    #put all files in the archive folder, creating the archive folder if it doesnt exist
    for file in all_files:
        #get the folder path for the file
        folder_path = os.path.dirname(file)
        #get the new folder path for the file
        new_folder_path = os.path.join(folder_path, 'archive')
        #if new_folder_path doesnt exist, create it
        if not os.path.exists(new_folder_path):
            os.makedirs(new_folder_path)
        #replace double backslashes with forward slashes
        new_file_path = os.path.join(new_folder_path, os.path.basename(file))
        #replace double backslashes with forward slashes
        new_file_path = new_file_path.replace('\\', '/')
        #move the file to the new folder path
        os.rename(file,new_file_path )

# move_all_files_to_archive('./plotting_output', files_to_ignore = ['.vscode', '.git','env_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__', 'Readme.md'])
#%%
#Now put gitkeeps into all folders that arent archive folders
def add_gitkeep_to_all_archive_folders(folder = './',folders_to_ignore = ['.vscode', '.git','env_jupyter','env_no_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__']):
    #get list of all files in the folder, including their paths and all subsequent subfolders
    all_paths = [os.path.join(root, file) for root, dirs, files in os.walk(folder) for file in files]
    #replace double backslashes with forward slashes
    all_paths = [file.replace('\\', '/') for file in all_paths]
    #get all folders in the folder
    all_folders = [os.path.dirname(path) for path in all_paths]
    #remove any folders that contain words in the folders_to_ignore list
    all_folders = [folder for folder in all_folders if not any(word in folder for word in folders_to_ignore)]
    #remove any folders that are archive folders
    all_folders = [folder for folder in all_folders if 'archive' not in folder]

    #add gitkeeps to all folders if they dont already have a gitkeep
    for folder in all_folders:
        #if the folder doesnt have a gitkeep, add one
        if not os.path.exists(os.path.join(folder, '.gitkeep')):
            #create a gitkeep file in the new folder
            with open(os.path.join(folder, '.gitkeep'), 'w') as f:
                f.write('')

# add_gitkeep_to_all_archive_folders('./')
#%%
# for p in generate_all_files(Path("."), only_files=False,ignored_folders = ['.vscode', '.git','env_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__']):
#     print(p)




# #%%
# # all_files = [os.path.join(dirs, file) for root, dirs, files in os.walk(root_folder_path) for file in files]
# #use os.walk to grab all folders and files in the root folder path, including all subsequent subfolders
# all_files = [os.path.join(root, file) for root, dirs, files in os.walk(root_folder_path,topdown=True) for file in files]
# #get subfolders in the root folder path
# # grab only the files that are in an archive folder
# all_files = [file for file in all_files if 'archive' in file]
# #remove any that contain these folders:
# ignored_folders = ['.vscode', '.git','env_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__']
# all_files = [file for file in all_files if not any(ignored_folder in file for ignored_folder in ignored_folders)]

# #%%
# #move these files to the new_archive_folder_path. Save them with the same folder structure as they had in this project, so if the folder is missing in the new folder, create it
# for file in all_files:
#     #get the folder path for the file
#     folder_path = os.path.dirname(file)
#     #get the new folder path for the file
#     new_folder_path = folder_path.replace(root_folder_path, new_archive_folder_path)
#     #if the folders for these paths dont exist, create them
#     if not os.path.exists(new_folder_path):
#         os.makedirs(new_folder_path)
#     #move the file to the new folder path
#     os.rename(file, file.replace(root_folder_path, new_archive_folder_path))

# # %%


# import os
# import shutil

# # Set the source and destination directories
# src_dir = "/path/to/project/folder"
# dst_dir = "/path/to/other/project/folder"

# # Recursively find all /archive/ directories under src_dir
# for dirpath, dirnames, filenames in os.walk(src_dir):
#     if "archive" in dirnames:
#         archive_dir = os.path.join(dirpath, "archive")
#         # Copy the entire directory structure to dst_dir
#         shutil.copytree(archive_dir, dst_dir + archive_dir.replace(src_dir, ""))
#         dirnames.remove("archive")






# all_archived_files = 'all_archived_files' 
# root_folder_path = './'
# ignored_folders_and_files = ['.vscode', '.git','env_jupyter','env_no_jupyter','.gitattributes', '.gitignore', '.gitkeep', '__pycache__']
# file_list = []
# for root, dirs, files in os.walk(root_folder_path):
#     #remove any folders that are in the ignored folders list
#     dirs[:] = [d for d in dirs if d not in ignored_folders_and_files]
#     #remove any files that are in the ignored files list
#     files[:] = [f for f in files if f not in ignored_folders_and_files]
#     #remove all files that are
#     #connect the dir to the file name with forward slashes
#     files = [os.path.join(root, file) for file in files]
#     #make  slashes forward slashes
#     files = [file.replace('\\', '/') for file in files]
#     #add the files to the file list
#     file_list.extend(files)

# #remove any files that dont contain the word archive
# file_list = [file for file in file_list if 'archive' in file]

# #add the folder name for this project to the start of the file path
# #get folder name for this project
# project_folder_name = os.path.basename(os.getcwd())

# #now we have a list of all files in the project that are in an archive folder. We will move these to a new folder outside the project, and save them with the same folder structure as they had in this project, so if the folder is missing in the new folder, create it
# for file in file_list:
#     #get the folder path for the file
#     folder_path = os.path.dirname(file)
#     #add the project name to the folder path
#     folder_path = folder_path.replace('./', f'./{project_folder_name}/')
#     #get the new folder path for the file
#     new_folder_path = folder_path.replace('./', f'../{all_archived_files}/')
#     #if the folders for these paths dont exist, create them
#     if not os.path.exists(new_folder_path):
#         os.makedirs(new_folder_path)
#     #check if the file is already there, if so then rename that file to '_old', then move the new file to the new folder path. IF there is already a file that has _old in the name, then ask the user if they want to overwrite it
#     if os.path.exists(file.replace('./', f'../{all_archived_files}/')):
#         #move the file to the new folder path
#         os.rename(file, file.replace('./', f'../{all_archived_files}/{project_folder_name}/'))
