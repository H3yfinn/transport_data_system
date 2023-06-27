#take in data from input_data/Manually_inputted_data.xlsx. Then make sure that it has all the required columns and categories within those coloumns. Then concat and save it to intermediate_data/Manually_inputted_data_cleaned.csv

#%%
import pandas as pd
import numpy as np
import os
import datetime
import re
import yaml
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
import sys
folder_path = './aggregation_code'  # Replace with the actual path of the folder you want to add
sys.path.append(folder_path)
import utility_functions 
#%%
#take in concordances (created in the model, perhaps creation should be moved here)
with open('config/selection_config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
INDEX_COLS = cfg['INDEX_COLS']
concordances_file_path = cfg['concordances_file_path']
concordances = pd.read_csv(concordances_file_path)

#TAKE IN ROAD DATA
wb = pd.ExcelFile('./input_data/Manually_inputted_data_road.xlsx')
concat_df_road = pd.DataFrame()
for sheet in wb.sheet_names:
    df = pd.read_excel(wb,sheet)
    concat_df_road = pd.concat([concat_df_road,df],ignore_index=True)

#TAKE IN NON ROAD DATA
wb = pd.ExcelFile('./input_data/Manually_inputted_data_other.xlsx')
concat_df_other = pd.DataFrame()
for sheet in wb.sheet_names:
    df = pd.read_excel(wb,sheet)
    concat_df_other = pd.concat([concat_df_other,df],ignore_index=True)


#drop 'Value_MJ' col if it exists
if 'Value_MJ' in concat_df_road.columns:
    concat_df_road = concat_df_road.drop('Value_MJ',axis=1)
if 'Value_MJ' in concat_df_other.columns:
    concat_df_other = concat_df_other.drop('Value_MJ',axis=1)


def extend_df_for_missing_dates(df):
    #now we want to fill in with as many dates as we need. For now we will assume that is between 2010 and the current year
    
    current_year = datetime.datetime.now().year
    years = np.arange(2010,current_year+1)
    #convert years to yyyy-mm-dd by adding 12-31 to end
    years = [str(year)+'-12-31' for year in years]
    #make sure date in concat_df_road is a str
    if 'Date' in df.columns:
        df['Date'] = df['Date'].astype(str)
    else:
        df['Date'] = years[0]

    #now for every row in concat_df_road we want to add a row for each date in years. We'll use pd.repeat to do this.
    #first create version of df withno values col
    df_no_values = df.drop('Value',axis=1)
    df_no_values = df_no_values.reindex(df_no_values.index.repeat(len(years)))
    df_no_values['Date'] = years * len(df)
    df_no_values.reset_index(drop=True, inplace=True)
    #add the values column back in using merge
    df = df_no_values.merge(df,how='left',on=df_no_values.columns.tolist())

    cols=df.columns.tolist() 
    #drop ['Source', 'Comments', 'Value', 'Dataset', 'Date'] from cols
    cols = [col for col in cols if col not in ['Source', 'Comments', 'Value', 'Dataset', 'Date']]
    df['Value'] = df.groupby(cols)['Value'].apply(lambda x: x.ffill().bfill())
    return df

concat_df_road = extend_df_for_missing_dates(concat_df_road)
concat_df_other = extend_df_for_missing_dates(concat_df_other)

def fill_missing_drive_cols(df, concordances):
    #if the df is missing the drive col then just add it for every drive. this might cause issues where we want the measure to be drive non specific but it seems better this way.
    if 'Drive' not in df.columns:
        concordances[concordances.Medium=='road']
        unique_road_drives = concordances['Drive'].unique()
        #create a df with all the unique drives
        concat_df = pd.DataFrame()
        df_copy = df.copy()
        for drive in unique_road_drives:
            df_copy['Drive'] = drive
            concat_df = pd.concat([concat_df,df_copy],ignore_index=True)
        df = concat_df
    return df

concat_df_road = fill_missing_drive_cols(concat_df_road, concordances)

def separate_vehicle_type_distributions(df,concordances):
    #note that this data is used separately to the other data as it has a different format. SO take in data from that sheet and separate it from the otehr data as it will jsut be timesed by stocks data based on the Vehicle Type column at the beginning of the selection process. This will be like the function split_stocks_where_drive_is_all_into_bev_phev_and_ice in pre_selection_data_estimation_functions.data_estimation_functions
    # currently its format is with the col nbames:  Source Dataset Comments Date Medium 	Economy Transport Type Vehicle Type 	Vehicle1	Vehicle2	Vehicle3	Vehicle1_name	Vehicle2_name	Vehicle3_name 
    #It dxoesnt need any cleaning, but does need to be hcecked to make sure that the vehicle types are the same as in the concordances file.

    
    vehicle_type_distributions = df.copy()
    vehicle_type_distributions = vehicle_type_distributions[vehicle_type_distributions['Measure']=='Vehicle_type_distribution']
    df = df[df['Measure']!='Vehicle_type_distribution']

    vehicle_types = concordances['Vehicle_type'].unique()
    
    #check that the values in vehicle_name and vehicle type cols are the same as in the concordances file, and also check that all values in the vehicle cols add to 1 for each row. 
    #first check that the vehicle types are the same
    vehicle_types_in_data = vehicle_type_distributions['Vehicle Type'].unique() + vehicle_type_distributions['Vehicle1_name'].unique() + vehicle_type_distributions['Vehicle2_name'].unique() + vehicle_type_distributions['Vehicle3_name'].unique()
    if not all([vehicle_type in vehicle_types for vehicle_type in vehicle_types_in_data]):
        raise ValueError('The vehicle types in the vehicle type distribution data are not the same as in the concordances file')
    
    #check values add to 1. use regex to identify the vehicle cols
    regex = re.compile(r'Vehicle\d')
    vehicle_cols = [col for col in vehicle_type_distributions.columns if regex.match(col)]
    #check that the values in these cols add to 1. make sure ot ignore nas
    if not all(vehicle_type_distributions[vehicle_cols].fillna(0).sum(axis=1)==1):
        raise ValueError('The values in the vehicle cols do not add to 1 for each row')
    
    return df, vehicle_type_distributions

concat_df_road, vehicle_type_distributions = separate_vehicle_type_distributions(concat_df_road, concordances)

#%%
def convert_occupancy_and_load_to_occupancy_or_load(df):
    #take in the data for occupan and laod and combine them into one measure
    #note that this is likely to be use for some time.
    occ_load = df[df['Measure'].isin(['Occupancy', 'Load'])]
    occ_load['Measure'] = 'Occupancy_or_load'
    #repalce unit with passengers_or_tonnes
    occ_load['Unit'] = 'Passengers_or_tonnes'
    #drop these rows from the original df
    df = df[~df['Measure'].isin(['Occupancy', 'Load'])]
    #concat the two dfs
    df = pd.concat([df, occ_load], ignore_index=True)
    return df

concat_df_road = convert_occupancy_and_load_to_occupancy_or_load(concat_df_road)
#%%
# def create_phev_and_ice_versions_of_values(df):
#     #because we are not sure if we will introduice these aggregations for good we will itnroduce these using code. Just extract the values for phevg/phevd and g/d drive types and set their drive to phev and ice respectively, then group by all cols except value and average. then add them back into the df
#     # if 'Drive' in df.columns:
#     phev = df[df['Drive'].isin(['phev_g','phev_d'])]
#     phev['Drive'] = 'phev'
#     cols = phev.columns.tolist()
#     cols.remove('Value')
#     phev = phev.groupby(cols).mean().reset_index()

#     ice = df[df['Drive'].isin(['ice_g','ice_d'])]
#     ice['Drive'] = 'ice'
#     cols = ice.columns.tolist()
#     cols.remove('Value')
#     ice = ice.groupby(cols).mean().reset_index()

#     df = pd.concat([df, phev, ice], ignore_index=True)

#     return df

# concat_df_road = create_phev_and_ice_versions_of_values(concat_df_road)  
#%%
#dont think we need te below as i think we already have this data anyway
# def create_cng_lpg_versions_of_values(df):
#     #for now, use ice data to fill in the cng and lpg data. This is not ideal but it will do for now
#     cng = df[df['Drive'].isin(['ice'])]
#     cng['Drive'] = 'cng'
#     cols = cng.columns.tolist()
#     cols.remove('Value')
#     cng = cng.groupby(cols).mean().reset_index()

#     lpg = df[df['Drive'].isin(['ice'])]
#     lpg['Drive'] = 'lpg'
#     cols = lpg.columns.tolist()
#     cols.remove('Value')
#     lpg = lpg.groupby(cols).mean().reset_index()

#     #drop lpg and cng from df
#     df = pd.concat([df, lpg, ice], ignore_index=True)

#     return df

# concat_df_roadd = create_cng_lpg_versions_of_values(concat_df_road)
#%%
def save_df_to_csv(df,save_path, file_name_start):
    #now we want to save the data to csv. but make sure this data si different from the previous version of the data
    #get the date of the previous version of the data
    prev_file_date = utility_functions.get_latest_date_for_data_file(save_path, file_name_start)
        
    if prev_file_date == None:
        #save the data
        FILE_DATE_ID = 'DATE{}'.format(datetime.datetime.now().strftime('%Y%m%d'))
        df.to_csv(f'{save_path}/{file_name_start}{FILE_DATE_ID}.csv',index=False)
    else:
        FILE_DATE_ID_prev = 'DATE{}'.format(prev_file_date)
        prev_file_path = f'{save_path}/{file_name_start}{FILE_DATE_ID_prev}.csv'

        if utility_functions.compare_data_to_previous_version_of_data_in_csv(df,prev_file_path):
            #save the data
            FILE_DATE_ID = 'DATE{}'.format(datetime.datetime.now().strftime('%Y%m%d'))
            df.to_csv(f'{save_path}/{file_name_start}{FILE_DATE_ID}.csv',index=False)

#%%
save_df_to_csv(concat_df_road,'./intermediate_data/estimated','manually_inputted_data_cleaned_road')
save_df_to_csv(concat_df_other,'./intermediate_data/estimated','manually_inputted_data_cleaned_other')
save_df_to_csv(vehicle_type_distributions, './intermediate_data/estimated','vehicle_type_distributions')



print('Done')
#%%


#%%


# import pandas as pd
# from openpyxl import load_workbook

# # # Load your DataFrame
# # df = pd.DataFrame(your_data)

# # Define the file path of your excel file
# file_path = '../input_data/Manually_inputted_data_road.xlsx'

# # Try to load the workbook
# try:
#     book = load_workbook(file_path)
# except FileNotFoundError:
#     # If the file does not exist, simply save the DataFrame as a new file
#     # df.to_excel(file_path, sheet_name='New Sheet', index=False)
#     print('File does not exist. Saving DataFrame as a new file.')
# #%%
# # # # Check if the sheet exists
# # if 'New Sheet' in book.sheetnames:
# #     # If the sheet exists, remove it
# #     std = book.get_sheet_by_name('New Sheet')
# #     book.remove(std)
# #     book.save(file_path)
# #loop through sheets and change them 
# for sheet in book.sheetnames:
#     # Load the existing file
#     book = load_workbook(file_path)

#     #change the vehicle type from car to lcv where the transport type is freight
#     df = pd.read_excel(file_path, sheet_name=sheet)
#     df.loc[df['Transport Type'] == 'Freight', 'Vehicle Type'] = 'lcv'

#     # Create a Pandas Excel writer using XlsxWriter as the engine
#     writer = pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace')

#     # Copy the existing sheets
#     writer.book = book

#     # Add the new DataFrame to the writer
#     df.to_excel(writer, sheet_name=sheet, index=False)

#     # Close the Pandas Excel writer and output the Excel file
#     writer.save()

# # # Then append the new data
# # with pd.ExcelWriter(file_path, engine='openpyxl', mode='a') as writer:
# #     df.to_excel(writer, sheet_name='New Sheet', index=False)

# %%
