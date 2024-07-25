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
# expected columns names:
# 'Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Date', 'Economy',
# 'Frequency', 'Measure', 'Unit', 'Dataset', 'Scope', 'Comments', 'Fuel',
#    'Source', 'Vehicle type', 'comment', 'Value'
expected_cols = ['Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Date', 'Economy', 'Frequency', 'Measure', 'Unit', 'Dataset', 'Scope', 'Comments', 'Fuel', 'Source', 'Value']
#take in concordances (created in the model, perhaps creation should be moved here)
with open('config/selection_config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
INDEX_COLS = cfg['INDEX_COLS']
concordances_file_path = cfg['concordances_file_path']
concordances = pd.read_csv(concordances_file_path)

#TAKE IN ROAD DATA
wb = pd.ExcelFile('./input_data/Manually_inputted_data_road.xlsx')
concat_df_road = pd.DataFrame()
expected_sheets = ['vehicle_type_distributions',
 'Occupancy',
 'Average_age',
 'Load',
 'Mileage',
 'Turnover_rate',
 'New_vehicle_efficiency',
 'Efficiency']
for sheet in wb.sheet_names:
    df = pd.read_excel(wb,sheet)
    if sheet =='vehicle_type_distributions':
        vehicle_type_distributions = df.copy()
        continue
    elif sheet not in expected_sheets:
        continue
    elif set(df.columns) != set(expected_cols):
        #if missing col is 'drive' then just pass
        missing_cols = [col for col in expected_cols if col not in df.columns]
        extra_cols = [col for col in df.columns if col not in expected_cols]
        if len(missing_cols) == 1 and missing_cols[0] == 'Drive' and len(extra_cols) == 0:
            pass
        else:   
            raise ValueError(f'The columns in the sheet {sheet} are not the same as the expected columns')
    concat_df_road = pd.concat([concat_df_road,df],ignore_index=True)
wb.close()


#%% 

#TAKE IN NON ROAD DATA
expected_sheets = ['Intensity', 'average_age']
wb = pd.ExcelFile('./input_data/Manually_inputted_data_other.xlsx')
concat_df_other = pd.DataFrame()
for sheet in wb.sheet_names:
    df = pd.read_excel(wb,sheet)
    if sheet not in expected_sheets:
        continue
    elif set(df.columns) != set(expected_cols):
        #if the extra col is Value_MJ then pass
        missing_cols = [col for col in expected_cols if col not in df.columns]
        extra_cols = [col for col in df.columns if col not in expected_cols]
        if len(missing_cols) == 0 and len(extra_cols) == 1 and extra_cols[0] == 'Value_MJ':
            pass
        else:
            raise ValueError(f'The columns in the sheet {sheet} are not the same as the expected columns')
    concat_df_other = pd.concat([concat_df_other,df],ignore_index=True)

wb.close()
#drop 'Value_MJ' col if it exists
if 'Value_MJ' in concat_df_road.columns:
    concat_df_road = concat_df_road.drop('Value_MJ',axis=1)
if 'Value_MJ' in concat_df_other.columns:
    concat_df_other = concat_df_other.drop('Value_MJ',axis=1)

#%%
def prepare_vehicle_type_distributions(vehicle_type_distributions,concordances):
    #note that this data is used separately to the other data as it has a different format. SO take in data from that sheet and separate it from the otehr data as it will jsut be timesed by stocks data based on the Vehicle Type column at the beginning of the selection process. This will be like the function split_stocks_where_drive_is_all_into_bev_phev_and_ice in pre_selection_data_estimation_functions.data_estimation_functions
    # currently its format is with the col nbames:  Source Dataset Comments Date Medium 	Economy Transport Type Vehicle Type 	Vehicle1	Vehicle2	Vehicle3	Vehicle1_name	Vehicle2_name	Vehicle3_name 
    #It dxoesnt need any cleaning, but does need to be hcecked to make sure that the vehicle types are the same as in the concordances file.
    # # #breakpoint()
    #breakpoint()
    vehicle_types = concordances['Vehicle Type'].unique()
    #add lpv tro the vehicle types as it is an aggreation of the passenger lt,suv and car categories
    vehicle_types = np.append(vehicle_types,'lpv')
    
    #check that the values in vehicle_name and vehicle type cols are the same as in the concordances file, and also check that all values in the vehicle cols add to 1 for each row. 
    #first check that the vehicle types are the same
    vehicle_types_in_data = vehicle_type_distributions['Vehicle Type'].unique().tolist() + vehicle_type_distributions['Vehicle1_name'].unique().tolist() + vehicle_type_distributions['Vehicle2_name'].unique().tolist() + vehicle_type_distributions['Vehicle3_name'].unique().tolist()
    #drop nan
    vehicle_types_in_data = [vehicle_type for vehicle_type in vehicle_types_in_data if type(vehicle_type)==str]

    if not all([vehicle_type in vehicle_types for vehicle_type in vehicle_types_in_data]):
      raise ValueError('The vehicle types in the vehicle type distribution data are not the same as in the concordances file')

    #check values add to 1. use regex to identify the vehicle cols so they sdont have anything after the digit

    regex = re.compile(r'Vehicle\d$')
    vehicle_cols = [col for col in vehicle_type_distributions.columns if regex.match(col)]
    #check that the values in these cols add to 1. make sure ot ignore nas and use a method that 
    if not any(abs(vehicle_type_distributions[vehicle_cols].fillna(0).sum(axis=1) - 1.0) < 1e-6):

        raise ValueError('The values in the vehicle cols do not add to 1 for each row')
    
    # #drop any non nexessarey cols:'Drive',
    # # 'Frequency',
    # # 'Unit',
    # # 'Scope',
    # # 'Fuel'
    # vehicle_type_distributions = vehicle_type_distributions.drop(['Drive', 'Frequency', 'Unit', 'Scope', 'Fuel'],axis=1)
    
    return vehicle_type_distributions

vehicle_type_distributions = prepare_vehicle_type_distributions(vehicle_type_distributions, concordances)
#%%

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

    #sometimes excel will format the date so it ends with the time as well (as 00:00:00). we want to remove this
    if df['Date'].str.contains(' 00:00:00').any():
        df['Date'] = df['Date'].str.replace(' 00:00:00','')
    #and then sometimes the user might accidentally just put the year, so add 12-31 to the end of those
    dates = df['Date'].unique().tolist()
    number_dates = [date for date in dates if len(date)==4]
    if len(number_dates)>0:
        df.loc[df['Date'].isin(number_dates),'Date'] = df.loc[df['Date'].isin(number_dates),'Date'].astype(str) + '-12-31'
        
    cols=df.columns.tolist() 
    #identify value cols as cols with numbers in them. they can be floats or ints
    value_cols = [col for col in cols if df[col].dtype in ['int64','float64']]

    #now for every row in concat_df_road we want to add a row for each date in years. We'll use pd.repeat to do this.
    #first create version of df withno values col
    df_no_values = df.drop(value_cols,axis=1)
    df_no_values = df_no_values.reindex(df_no_values.index.repeat(len(years)))
    df_no_values['Date'] = years * len(df)
    #drop duplicates )dont know why they are occuring but its because of df_no_values.reindex(df_no_values.index.repeat(len(years)))
    df_no_values = df_no_values.drop_duplicates()    
    df_no_values.reset_index(drop=True, inplace=True)
    #add the values column back in using merge
    df = df_no_values.merge(df,how='left',on=df_no_values.columns.tolist())
    #then we need to add values. the best way woudl be to ffill and bfill when sported by date and the otehr cols. to have better control over thigns, iterate over groups. this will be slow but it makes it easier to debug
    #drop ['Source', 'Comments', 'Value', 'Dataset', 'Date'] from cols
    cols = [col for col in cols if col not in ['Source', 'Comments', 'Dataset', 'Date']+value_cols]
    # Sort, reset index, and fill NaN with 'nan'
    df = df.sort_values(by=cols).reset_index(drop=True)
    df[cols] = df[cols].fillna('nan')

    # Create an empty DataFrame to store the results
    result_df = pd.DataFrame()

    # Iterate over each group and apply the ffill and bfill operations
    for name, group in df.groupby(cols):
        for col in value_cols:
            group[col] = group[col].ffill().bfill()
        result_df = pd.concat([result_df, group], ignore_index=True)

    # Replace 'nan' string with np.nan
    result_df = result_df.replace('nan', np.nan)

    return result_df
#%%
concat_df_road = extend_df_for_missing_dates(concat_df_road)
#%%
concat_df_other = extend_df_for_missing_dates(concat_df_other)
vehicle_type_distributions = extend_df_for_missing_dates(vehicle_type_distributions)
#%%
def fill_missing_drive_cols(df, concordances, medium='road'):
    #if the df is missing the drive col then just add it for every drive. this might cause issues where we want the measure to be drive non specific but it seems better this way.
    #breakpoint()
    if 'Drive' not in df.columns:
        road_concordances = concordances[concordances.Medium==medium]
        unique_road_drives = road_concordances['Drive'].unique()
        #create a df with all the unique drives
        concat_df = pd.DataFrame()
        df_copy = df.copy()
        for drive in unique_road_drives:
            df_copy['Drive'] = drive
            concat_df = pd.concat([concat_df,df_copy],ignore_index=True)
        df = concat_df
    #otherwise, find where drive is na, use concordances to fill it in
    elif df.Drive.isna().any():
        na_drives = df[df.Drive.isna()].drop('Drive',axis=1).drop_duplicates()
        df = df.dropna(subset=['Drive'])
        #merge with concordances to get the drive
        road_concordances = concordances[concordances.Medium==medium][['Medium','Vehicle Type','Measure','Economy','Transport Type', 'Drive']]
        breakpoint()
        na_drives = na_drives.merge(road_concordances,how='left',on=['Medium','Vehicle Type','Measure','Economy','Transport Type'])
        breakpoint()
        #drop unneeded cols
        
        #concat the two dfs
        df = pd.concat([df, na_drives], ignore_index=True)
        
    return df
#%%
vehicle_type_distributions = fill_missing_drive_cols(vehicle_type_distributions, concordances)
#%%
concat_df_road = fill_missing_drive_cols(concat_df_road, concordances)

#%%
def set_new_drive_ages_to_one(df, concordances):
    #currently average age is set to same value drive types. we want to set it based on the drive.
    #we want average age we have in the data to be used for 'old drive types' and set to 1 for 'new drive types'. so we need to find the average age of the old drive types and the new drive types. So grab the drives form teh concordance, chekc they can be split into the new and old drive types i expectm and then set theoir average age to 1 or the average age we have in the data.
    #load in the concordance
    df_age = df[df['Measure']=='Average_age'].copy()
    df = df[df['Measure']!='Average_age'].copy()

    #grab the drive types for road:
    drive_types = concordances[(concordances['Medium']=='road')].Drive.unique()#array(['bev', 'ice_d', 'ice_g', 'cng', 'fcev', 'lpg', 'phev_d', 'phev_g'],
    new_drives = ['bev','phev_d','phev_g','fcev']
    old_drives = ['ice_d','ice_g','cng','lpg']
    #check that the drive types are in the data
    if set(drive_types) != set(new_drives+old_drives):
        breakpoint()
        raise ValueError('The drive types in the concordances file are not the same as in the data')
        
    old_drive_concordance = concordances[ (concordances['Drive'].isin(old_drives))][['Drive','Vehicle Type', 'Transport Type', 'Medium']].drop_duplicates()
    new_drive_concordance = concordances[ (concordances['Drive'].isin(new_drives))][['Drive','Vehicle Type', 'Transport Type', 'Medium']].drop_duplicates()

    #join the concordance on the data so we can replicate the average age for the new drive types
    df_age=df_age.drop(columns=['Drive']).drop_duplicates()
    df_old = df_age.merge(old_drive_concordance,on=['Vehicle Type','Transport Type', 'Medium'],how='inner').copy()
    df_new = df_age.merge(new_drive_concordance,on=['Vehicle Type','Transport Type', 'Medium'],how='inner').copy()
    df_new['Value'] = 1

    #concat the two dataframes
    df_age = pd.concat([df_old,df_new])
    
    df = pd.concat([df,df_age],ignore_index=True)
    
    return df

#%%
concat_df_road = set_new_drive_ages_to_one(concat_df_road, concordances)
    
#%%
def convert_occupancy_and_load_to_occupancy_or_load(df):
    #take in the data for occupan and laod and combine them into one measure
    #note that this is likely to be use for some time.
    occ_load = df[df['Measure'].isin(['Occupancy', 'Load'])].copy()
    occ_load.loc[:,'Measure'] = 'Occupancy_or_load'
    occ_load.loc[:,'Unit'] = 'Passengers_or_tonnes'
    #drop these rows from the original df
    df = df[~df['Measure'].isin(['Occupancy', 'Load'])].copy()
    #concat the two dfs
    df = pd.concat([df, occ_load], ignore_index=True)
    return df
#%%
concat_df_road = convert_occupancy_and_load_to_occupancy_or_load(concat_df_road)
#%%
def break_vehicle_types_into_more_specific_types(concat_df_road):
    #break ht into mt and ht, break lpv into lt,suv and car.
    #do this for the following sheets in concat_df_road:
    sheets = ['Mileage','Turnover_rate',
       'Occupancy_or_load']#NOTE THAT OCCUPANCY LOAD CURRENTLY HAS LPV ONLY, WHEREAS THE OTHER TWO HAVE LPV, SUV, CAR AND LT! I THINK ITS SOURCE OF SOME DUPLICATES
    #so for each sheet we want to break the vehicle types into more specific types
    for sheet in sheets:
        df = concat_df_road.copy()
        df = df[df['Measure']==sheet]
        lpv = df[df['Vehicle Type']=='lpv']
        car = lpv.copy()
        suv = lpv.copy()
        lt = lpv.copy()
        car['Vehicle Type'] = 'car'
        suv['Vehicle Type'] = 'suv'
        lt['Vehicle Type'] = 'lt'
        df = df[df['Vehicle Type']!='lpv']
        df = pd.concat([df,car,suv,lt],ignore_index=True)

        # ht = df[df['Vehicle Type']=='ht']
        # # #times ht avg load by 1.5
        # ht['Value'] = ht['Value']*1.5#TEMPORARY
        # mt = ht.copy()
        # mt['Vehicle Type'] = 'mt'
        # df = df[df['Vehicle Type']!='ht']
        # df = pd.concat([df,mt,ht],ignore_index=True)

        concat_df_road = concat_df_road[concat_df_road['Measure']!=sheet]
        concat_df_road = pd.concat([concat_df_road,df],ignore_index=True)
    return concat_df_road
#%%
concat_df_road = break_vehicle_types_into_more_specific_types(concat_df_road)

#%%

def identify_missing_vehicle_drive_medium_combinations_road(df, concordances):
    #loop through all the data and identify where we are missing data. let the user know so they fix it
    #jsut merge the df and concordances and check where indicator is either left or right:
    #breakpoint()
    # for column in df.columns:
    #     if df[column].dtype != concordances[column].dtype:
    #         print(f"Column '{column}' has different data types: {df[column].dtype} vs {concordances[column].dtype}")
    #set the concordances date to string then make it end with 12-31
    new_concordances = concordances.copy()
    new_concordances['Date'] = new_concordances['Date'].astype(str) + '-12-31'
    #filter for same medium in df in new_concordances
    new_concordances = new_concordances[new_concordances['Medium'].isin(df['Medium'].unique().tolist())]
    #filter for same dates in both
    new_concordances = new_concordances[new_concordances['Date'].isin(df['Date'].unique().tolist())]
    #filter for same measures in both
    new_concordances = new_concordances[new_concordances['Measure'].isin(df['Measure'].unique().tolist())]
    
    df_copy = df.copy()
    df_copy = df_copy.loc[df_copy['Date'].isin(new_concordances['Date'].unique().tolist())]
    df_copy = df_copy.loc[df_copy['Measure'].isin(new_concordances['Measure'].unique().tolist())]
    df_copy = df_copy.merge(new_concordances,how='outer',on=['Medium','Vehicle Type','Drive','Measure','Date', 'Economy','Transport Type'], indicator=True, suffixes=('','_concordances'))
    df_missing = df_copy[df_copy['_merge']!='both']
    left_only = df_missing[df_missing['_merge']!='right_only']
    right_only = df_missing[df_missing['_merge']!='left_only']

    df_missing1 = df_missing[['Medium','Vehicle Type','Drive','Measure','Transport Type']].drop_duplicates()
    
    #print them:
    print('The following combinations of medium, vehicle type, drive and measure are not in the concordances file:')
    print(left_only[['Vehicle Type','Drive','Measure', 'Transport Type']].drop_duplicates())
    
    #the right only data actually needs to be fixed. let the user know whats missing and then they can fix it
    print('The following combinations of medium, vehicle type, drive and measure are not in the data:')
    print(right_only[['Vehicle Type','Drive','Transport Type','Measure']].drop_duplicates())
    
    #create the required data. First, if tehre are any Measuress otehr than Turnover_rate or Mileage, throw an error caise we ahvbent prepared for that:
    breakpoint()
    if len(right_only)>0:
        if not all(right_only['Measure'].isin(['Turnover_rate','Mileage', 'Average_age'])): 
            raise ValueError('The above combinations of medium, vehicle type, drive and measure are not rady to be hadnled in this code. This is for the measures: {}'.format(right_only['Measure'].unique().tolist()))
        #     right_only.columns
        # Index(['Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Date', 'Economy', 'Measure', 'Dataset', 'Scope', 'Comments',
        #        'Fuel', 'Source', 'Value',  '_merge'],
        #turnover rate:
        #just  get the avg of turnover rate and fill Value with it. and drop _merge col then save in intermediate_data\archive\turnover_rate_avg.csv
        turnover_rate_avg = df[df['Measure']=='Turnover_rate'].Value.mean()
        turnover_rate_avg_df = right_only.copy()
        turnover_rate_avg_df = turnover_rate_avg_df[turnover_rate_avg_df.Measure=='Turnover_rate']
        if len(turnover_rate_avg_df)>0:
            
            turnover_rate_avg_df['Value'] = turnover_rate_avg
            turnover_rate_avg_df = turnover_rate_avg_df.drop('_merge',axis=1)
            #order cols like so: Vehicle Type	Transport Type	Drive	Value	Measure	Unit	Dataset	Date	Source	Medium	Frequency	Scope	Economy	Fuel	Comments
            turnover_rate_avg_df = turnover_rate_avg_df[['Vehicle Type','Transport Type','Drive','Value','Measure','Unit','Dataset','Date','Source','Medium','Frequency','Scope','Economy','Fuel','Comments']]
            turnover_rate_avg_df.to_csv('./intermediate_data/archive/turnover_rate_avg.csv',index=False)
        
        #and for mileage, we need to get the avg of mileage for each vehicle type  and fill the values with that. then save in intermediate_data\archive\mileage_avg.csv
        mileage_avg = df[df['Measure']=='Mileage'].groupby(['Vehicle Type','Transport Type']).Value.mean().reset_index()
        mileage_avg_df = right_only.copy()
        mileage_avg_df = mileage_avg_df[mileage_avg_df.Measure=='Mileage']
        if len(mileage_avg_df)>0:
                
            mileage_avg_df = mileage_avg_df.merge(mileage_avg,how='left',on=['Vehicle Type','Transport Type'], suffixes=('','_avg'))
            #fill the values with the avg
            mileage_avg_df['Value'] = mileage_avg_df['Value_avg']
            #
            #identify any nans. print them
            if any(mileage_avg_df['Value'].isna()):
                print('The following combinations of medium, vehicle type, drive and measure are missing from the data and cannot be filled with the average mileage:')
                print(mileage_avg_df[mileage_avg_df['Value'].isna()][['Vehicle Type','Drive','Transport Type','Measure']].drop_duplicates())
            mileage_avg_df = mileage_avg_df.drop(['_merge','Value_avg'],axis=1)
            #order cols like so: Vehicle Type	Transport Type	Value	Measure	Unit	Dataset	Date	Source	Medium	Frequency	Scope	Economy	Drive	Fuel	Comments
            mileage_avg_df = mileage_avg_df[['Vehicle Type','Transport Type','Value','Measure','Unit','Dataset','Date','Source','Medium','Frequency','Scope','Economy','Drive','Fuel','Comments']]
            mileage_avg_df.to_csv('./intermediate_data/archive/mileage_avg.csv',index=False)
            
        #do same for average_age
        average_age_avg = df[df['Measure']=='Average_age'].groupby(['Vehicle Type','Transport Type']).Value.mean().reset_index()
        average_age_avg_df = right_only.copy()
        average_age_avg_df = average_age_avg_df[average_age_avg_df.Measure=='Average_age']
        if len(average_age_avg_df)>0:
            average_age_avg_df = average_age_avg_df.merge(average_age_avg,how='left',on=['Vehicle Type','Transport Type'], suffixes=('','_avg'))
            #fill the values with the avg
            average_age_avg_df['Value'] = average_age_avg_df['Value_avg']
            #
            #identify any nans. print them
            if any(average_age_avg_df['Value'].isna()):
                print('The following combinations of medium, vehicle type, drive and measure are missing from the data and cannot be filled with the average average_age:')
                print(average_age_avg_df[average_age_avg_df['Value'].isna()][['Vehicle Type','Drive','Transport Type','Measure']].drop_duplicates())
            average_age_avg_df = average_age_avg_df.drop(['_merge','Value_avg'],axis=1)
            #order cols like so: Vehicle Type	Transport Type	Value	Measure	Unit	Dataset	Date	Source	Medium	Frequency	Scope	Economy	Drive	Fuel	Comments
            average_age_avg_df = average_age_avg_df[['Vehicle Type','Transport Type','Value','Measure','Unit','Dataset','Date','Source','Medium','Frequency','Scope','Economy','Drive','Fuel','Comments']]
            average_age_avg_df.to_csv('./intermediate_data/archive/average_age_avg.csv',index=False)       
            
        #breakpoint()
        raise ValueError('The above combinations of medium, vehicle type, drive and measure are missing from the data. Please add them to the data and run this script again')


# def identify_missing_vehicle_drive_medium_combinations_non_road(df, concordances):
#     #loop through all the data and identify where we are missing data. let the user know so they fix it
#     #jsut merge the df and concordances and check where indicator is either left or right:
#     #breakpoint()
#     # for column in df.columns:
#     #     if df[column].dtype != concordances[column].dtype:
#     #         print(f"Column '{column}' has different data types: {df[column].dtype} vs {concordances[column].dtype}")
#     #set the concordances date to string then make it end with 12-31
#     new_concordances = concordances.copy()
#     new_concordances['Date'] = new_concordances['Date'].astype(str) + '-12-31'
#     #filter for same medium in df in new_concordances
#     new_concordances = new_concordances[new_concordances['Medium'].isin(df['Medium'].unique().tolist())]
#     #filter for same dates in both
#     new_concordances = new_concordances[new_concordances['Date'].isin(df['Date'].unique().tolist())]
#     #filter for same measures in both
#     new_concordances = new_concordances[new_concordances['Measure'].isin(df['Measure'].unique().tolist())]
    
#     df_copy = df.copy()
#     df_copy = df_copy.loc[df_copy['Date'].isin(new_concordances['Date'].unique().tolist())]
#     df_copy = df_copy.loc[df_copy['Measure'].isin(new_concordances['Measure'].unique().tolist())]
#     df_copy = df_copy.merge(new_concordances,how='outer',on=['Medium','Vehicle Type','Drive','Measure','Date', 'Economy','Transport Type'], indicator=True, suffixes=('','_concordances'))
#     df_missing = df_copy[df_copy['_merge']!='both']
#     left_only = df_missing[df_missing['_merge']!='right_only']
#     right_only = df_missing[df_missing['_merge']!='left_only']

#     df_missing1 = df_missing[['Medium','Vehicle Type','Drive','Measure','Transport Type']].drop_duplicates()
    
#     #print them:
#     print('The following combinations of medium, vehicle type, drive and measure are not in the concordances file:')
#     print(left_only[['Vehicle Type','Drive','Measure']].drop_duplicates())
    
#     #the right only data actually needs to be fixed. let the user know whats missing and then they can fix it
#     print('The following combinations of medium, vehicle type, drive and measure are not in the data:')
#     print(right_only[['Vehicle Type','Drive','Transport Type','Measure']].drop_duplicates())
    
#     #create the required data. First, if tehre are any Measuress otehr than Turnover_rate or Mileage, throw an error caise we ahvbent prepared for that:
#     breakpoint()
#     if len(right_only)>0:
#         if not all(right_only['Measure'].isin(['Turnover_rate','Mileage', 'Average_age'])): 
#             raise ValueError('The above combinations of medium, vehicle type, drive and measure are not rady to be hadnled in this code. This is for the measures: {}'.format(right_only['Measure'].unique().tolist()))
#         #     right_only.columns
#         # Index(['Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Date', 'Economy', 'Measure', 'Dataset', 'Scope', 'Comments',
#         #        'Fuel', 'Source', 'Value',  '_merge'],
#         #turnover rate:
#         #just  get the avg of turnover rate and fill Value with it. and drop _merge col then save in intermediate_data\archive\turnover_rate_avg.csv
#         turnover_rate_avg = df[df['Measure']=='Turnover_rate'].Value.mean()
#         turnover_rate_avg_df = right_only.copy()
#         turnover_rate_avg_df = turnover_rate_avg_df[turnover_rate_avg_df.Measure=='Turnover_rate']
#         if len(turnover_rate_avg_df)>0:
            
#             turnover_rate_avg_df['Value'] = turnover_rate_avg
#             turnover_rate_avg_df = turnover_rate_avg_df.drop('_merge',axis=1)
#             #order cols like so: Vehicle Type	Transport Type	Drive	Value	Measure	Unit	Dataset	Date	Source	Medium	Frequency	Scope	Economy	Fuel	Comments
#             turnover_rate_avg_df = turnover_rate_avg_df[['Vehicle Type','Transport Type','Drive','Value','Measure','Unit','Dataset','Date','Source','Medium','Frequency','Scope','Economy','Fuel','Comments']]
#             turnover_rate_avg_df.to_csv('./intermediate_data/archive/turnover_rate_avg.csv',index=False)
        
#         #and for mileage, we need to get the avg of mileage for each vehicle type  and fill the values with that. then save in intermediate_data\archive\mileage_avg.csv
#         mileage_avg = df[df['Measure']=='Mileage'].groupby(['Vehicle Type','Transport Type']).Value.mean().reset_index()
#         mileage_avg_df = right_only.copy()
#         mileage_avg_df = mileage_avg_df[mileage_avg_df.Measure=='Mileage']
#         if len(mileage_avg_df)>0:
                
#             mileage_avg_df = mileage_avg_df.merge(mileage_avg,how='left',on=['Vehicle Type','Transport Type'], suffixes=('','_avg'))
#             #fill the values with the avg
#             mileage_avg_df['Value'] = mileage_avg_df['Value_avg']
#             #
#             #identify any nans. print them
#             if any(mileage_avg_df['Value'].isna()):
#                 print('The following combinations of medium, vehicle type, drive and measure are missing from the data and cannot be filled with the average mileage:')
#                 print(mileage_avg_df[mileage_avg_df['Value'].isna()][['Vehicle Type','Drive','Transport Type','Measure']].drop_duplicates())
#             mileage_avg_df = mileage_avg_df.drop(['_merge','Value_avg'],axis=1)
#             #order cols like so: Vehicle Type	Transport Type	Value	Measure	Unit	Dataset	Date	Source	Medium	Frequency	Scope	Economy	Drive	Fuel	Comments
#             mileage_avg_df = mileage_avg_df[['Vehicle Type','Transport Type','Value','Measure','Unit','Dataset','Date','Source','Medium','Frequency','Scope','Economy','Drive','Fuel','Comments']]
#             mileage_avg_df.to_csv('./intermediate_data/archive/mileage_avg.csv',index=False)
            
#         #do same for average_age
#         average_age_avg = df[df['Measure']=='Average_age'].groupby(['Vehicle Type','Transport Type']).Value.mean().reset_index()
#         average_age_avg_df = right_only.copy()
#         average_age_avg_df = average_age_avg_df[average_age_avg_df.Measure=='Average_age']
#         if len(average_age_avg_df)>0:
#             average_age_avg_df = average_age_avg_df.merge(average_age_avg,how='left',on=['Vehicle Type','Transport Type'], suffixes=('','_avg'))
#             #fill the values with the avg
#             average_age_avg_df['Value'] = average_age_avg_df['Value_avg']
#             #
#             #identify any nans. print them
#             if any(average_age_avg_df['Value'].isna()):
#                 print('The following combinations of medium, vehicle type, drive and measure are missing from the data and cannot be filled with the average average_age:')
#                 print(average_age_avg_df[average_age_avg_df['Value'].isna()][['Vehicle Type','Drive','Transport Type','Measure']].drop_duplicates())
#             average_age_avg_df = average_age_avg_df.drop(['_merge','Value_avg'],axis=1)
#             #order cols like so: Vehicle Type	Transport Type	Value	Measure	Unit	Dataset	Date	Source	Medium	Frequency	Scope	Economy	Drive	Fuel	Comments
#             average_age_avg_df = average_age_avg_df[['Vehicle Type','Transport Type','Value','Measure','Unit','Dataset','Date','Source','Medium','Frequency','Scope','Economy','Drive','Fuel','Comments']]
#             average_age_avg_df.to_csv('./intermediate_data/archive/average_age_avg.csv',index=False)       
            
#         #breakpoint()
#         raise ValueError('The above combinations of medium, vehicle type, drive and measure are missing from the data. Please add them to the data and run this script again')
    
#%%    
identify_missing_vehicle_drive_medium_combinations_road(concat_df_road, concordances)
# identify_missing_vehicle_drive_medium_combinations(concat_df_other, concordances)#for now jsut keep drive to all for non road. we can change this later if we want
#%%
#check for duplicates:
# QUICK FIX
concat_df_road = concat_df_road.drop_duplicates()
if any(concat_df_road.duplicated()):
    raise ValueError('There are duplicates in the road data {}'.format(concat_df_road[concat_df_road.duplicated()]))
if any(concat_df_other.duplicated()):
    raise ValueError('There are duplicates in the non road data, {}'.format(concat_df_other[concat_df_other.duplicated()]))
if any(vehicle_type_distributions.duplicated()):
    raise ValueError('There are duplicates in the vehicle type distributions data {}'.format(vehicle_type_distributions[vehicle_type_distributions.duplicated()]))
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



# do_this=True
# if do_this:
#     # def replace_manually_inputted_efficiency_with_usa_datapoints()
#     #pull in eff data from 
#     # fuel_economy_by_vehicle_type_new.to_csv('intermediate_data/estimated/USA_based_vehicle_efficiency_estimates_{}.csv'.format(FILE_DATE_ID),index=False)
#     fuel_economy_by_vehicle_type_new = pd.read_csv('intermediate_data/estimated/USA_based_vehicle_efficiency_estimates_DATE20230717.csv')
#     #cols:['transport_type', 'vehicle_type', 'drive', 'unit', 'medium', 'scope',
#     #    'date', 'measure', 'economy', 'frequency', 'value', 'dataset', 'Fuel']
#     fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type_new.rename(columns={'transport_type':'Transport Type','vehicle_type':'Vehicle Type','drive':'Drive','unit':'Unit','medium':'Medium','scope':'Scope','date':'Date','measure':'Measure','economy':'Economy','frequency':'Frequency','value':'Value','dataset':'Dataset','Fuel':'Fuel'})
#     #join to the data and  replace the vlaues with it
#     a = concat_df_road.merge(fuel_economy_by_vehicle_type_new,how='outer',on=['Transport Type','Vehicle Type','Drive','Unit','Medium','Date','Measure','Economy','Frequency'])
#     a = a.loc[a.Measure.isin(['Efficiency'])]

#     #drop lpv
#     a = a[a['Vehicle Type']!='lpv'] 

#     #set Dataset to 'manually_inputted_data_cleaned_road'
#     a['Dataset'] = 'manually_inputted_data_cleaned_road'
#     #set Value to Value_y
#     a['Value'] = a['Value_y']
#     #set scope_x to 'National'
#     a['Scope'] = 'National'
#     #set Comments to 'USA data'
#     a['Comments'] = 'USA data'
#     #set Fuel_x to 'all'
#     a['Fuel'] = 'all'
#     #set source to 'USA_alternative_fuels_data_center'
#     a['Source'] = 'USA_alternative_fuels_data_center'

#     #drop any cols that end with _x or _y
#     a = a.loc[:,~a.columns.str.endswith('_x')]
#     a = a.loc[:,~a.columns.str.endswith('_y')]

#     #now create replica and set measure to New_vehicle_efficiency
#     b = a.copy()
#     b['Measure'] = 'New_vehicle_efficiency'

#     concat_df_road = concat_df_road.loc[concat_df_road.Measure!='Efficiency']
#     concat_df_road = concat_df_road.loc[concat_df_road.Measure!='New_vehicle_efficiency']

#     concat_df_road = pd.concat([concat_df_road,a,b],ignore_index=True)





# def create_phev_and_ice_versions_of_values(df):
#     #because we are not sure if we will introduice these aggregations for good we will itnroduce these using code. Just extract the values for phevg/phevd and g/d drive types and set their drive to phev and ice respectively, then group by all cols except value and average. then add them back into the df
#     # if 'Drive' in df.columns:
#     phev = df[df['Drive'].isin(['phev_g','phev_d'])]
#     phev['Drive'] = 'phev'
#     cols = phev.columns.tolist()
#     cols.remove('Value')
#     phev = phev.groupby(cols).mean().reset_index()

#     ice = df[df['Drive'].isin(['ice_g','ice_d'])]
#     ice['Drive'] = 
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

# #drop freight 2w from concat_df_road. this commonly occurs in the data but we dont want it. 
# concat_df_road = concat_df_road[((concat_df_road['Transport Type']!='freight') & (concat_df_road['Vehicle Type']!='2w'))]

# # create a new Excel file
# file_name = './input_data/Manually_inputted_data_road_new.xlsx'
# writer = pd.ExcelWriter(file_name, engine='openpyxl')

# # loop over unique values in the 'Measure' column
# for measure in concat_df_road['Measure'].unique():
#     # create a sheet for each measure
#     df = concat_df_road[concat_df_road['Measure']==measure]
#     df.to_excel(writer, sheet_name=measure, index=False)

# writer.close()


# #drop freight 2w from concat_df_road. this commonly occurs in the data but we dont want it. 
# concat_df_road = concat_df_road[~((concat_df_road['Transport Type']=='freight') & (concat_df_road['Vehicle Type']=='2w'))]

# # create a new Excel file
# file_name = './input_data/Manually_inputted_data_road_new.xlsx'
# writer = pd.ExcelWriter(file_name, engine='openpyxl')

# # loop over unique values in the 'Measure' column
# for measure in concat_df_road['Measure'].unique():
#     # create a sheet for each measure
#     df = concat_df_road[concat_df_road['Measure']==measure]
#     df.to_excel(writer, sheet_name=measure, index=False)

# writer.close()

