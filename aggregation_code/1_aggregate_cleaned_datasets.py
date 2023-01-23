# Use this to gather together all non-aggregated data that is already cleaned and put it into the same dataset. Once thats been done we can pass it to the next script to select the best data for each time period. There  is some data manipulation to make quick fixes to make themany sources of data fit better together... but an efforts been made to keep most of that in the original cleaning files.
# It will also create a concordance which will essentially create rows in the dataset where you would expect them, like between missing years.

#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re


#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False

#%%

#load in the cleaned datasets here and then deal with them in a cell each
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/ATO_data/', 'ATO_data_cleaned_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
ATO_dataset_clean = pd.read_csv('intermediate_data/ATO_data/ATO_data_cleaned_{}.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/8th_edition_transport_model/', 'eigth_edition_transport_data_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
eigth_edition_transport_data = pd.read_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_{}.csv'.format(FILE_DATE_ID))

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/item_data/', 'item_dataset_clean_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
item_data_apec_tall = pd.read_csv('intermediate_data/item_data/item_dataset_clean_' + FILE_DATE_ID + '.csv')

############################################################

#%%
#handle transport model dataset
#remove all 8th edition data that is from the reference and carbon neutrality scenarios
eigth_edition_transport_data = eigth_edition_transport_data[eigth_edition_transport_data['Dataset'] == '8th edition transport model']
#create a date column with month and day set to 1
eigth_edition_transport_data['Date'] = eigth_edition_transport_data['Year'].astype(str) + '-12-31'
#make frequency column and set to yearly
eigth_edition_transport_data['Frequency'] = 'Yearly'
#remove Year column
eigth_edition_transport_data = eigth_edition_transport_data.drop(columns=['Year'])

#%%
############################################################


#handle ATO dataset
ATO_dataset_clean['Dataset'] = 'ATO'

#remove nan values in vlaue column
ATO_dataset_clean = ATO_dataset_clean[ATO_dataset_clean['Value'].notna()]
#%%
#some ato in multiple sheets have the same values in all other cols. So we will check for duplicates before removing the sheet col, throw an error if there are any, and then remove the sheet col
if len(ATO_dataset_clean[ATO_dataset_clean.duplicated()]) > 0:
    raise Exception('There are duplicates in the ATO dataset when we keep the sheet column. This is not expected.')
#drop sheet column
ATO_dataset_clean = ATO_dataset_clean.drop(columns=['Sheet'])
#now drop duplicates since they are only the ones where the saemw vlaue is in multiple sheets
ATO_dataset_clean = ATO_dataset_clean.drop_duplicates()
# #grab first duplicates and rename to ATO2
# ATO_dataset_clean.loc[ATO_dataset_clean.duplicated(subset=cols, keep='first'), 'Dataset'] = 'ATO2'
# #do it again just to be sure
# ATO_dataset_clean.loc[ATO_dataset_clean.duplicated(subset=cols, keep='first'), 'Dataset'] = 'ATO3'

############################################################


#%%
#handle item data
item_data_apec_tall['Dataset'] = 'ITEM'
#remove Source column
# item_data_apec_tall = item_data_apec_tall.drop(columns=['Source'])
#remove na values in value column
item_data_apec_tall = item_data_apec_tall[item_data_apec_tall['Value'].notna()]
#create a date column with month and day set to 1
item_data_apec_tall['Date'] = item_data_apec_tall['Year'].astype(str) + '-12-31'
#make frequency column and set to yearly
item_data_apec_tall['Frequency'] = 'Yearly'
#remove Year column
item_data_apec_tall = item_data_apec_tall.drop(columns=['Year'])


############################################################

#%%
#join data together
combined_data = ATO_dataset_clean.append(eigth_edition_transport_data, ignore_index=True)
combined_data = combined_data.append(item_data_apec_tall, ignore_index=True)

#if scope col is na then set it to 'national'
combined_data['Scope'] = combined_data['Scope'].fillna('National')

#MAKE SURE ALL COLUMNS ARE STRINGS EXCEPT FOR VALUE WHICH IS FLOAT
for col in combined_data.columns:
    if col != 'Value':
        combined_data[col] = combined_data[col].astype(str)
    else:
        combined_data[col] = combined_data[col].astype(float)

#where vehiclke type and drive is 'nan' or na, set it to the value in medium, except where medium is road, then just leave as na
combined_data['Vehicle Type'] = combined_data['Vehicle Type'].replace('nan', np.nan)
combined_data['Vehicle Type'] = combined_data['Vehicle Type'].fillna(combined_data['Medium'])
combined_data.loc[(combined_data['Vehicle Type'] == 'Road'), 'Vehicle Type'] = np.nan
#now do drive
combined_data['Drive'] = combined_data['Drive'].replace('nan', np.nan)
combined_data['Drive'] = combined_data['Drive'].fillna(combined_data['Medium'])
combined_data.loc[(combined_data['Drive'] == 'Road'), 'Drive'] = np.nan

#%%

#create a frequency column and set it to yearly if the gap bet
#%%
#Important step: make sure that units are the same for each measure so that they can be compared. If they are not then the measure should be different.
#For example, if one measure is in tonnes and another is in kg then they should just be converted. But if one is in tonnes and another is in number of vehicles then they should be different measures.
for measure in combined_data['Measure'].unique():
    if len(combined_data[combined_data['Measure'] == measure]['Unit'].unique()) > 1:
        print(measure)
        print(combined_data[combined_data['Measure'] == measure]['Unit'].unique())
        raise Exception('There are multiple units for this measure. This is not allowed. Please fix this before continuing.')

#check for any duplicates
if len(combined_data[combined_data.duplicated()]) > 0:
    raise Exception('There are duplicates in the combined data. Please fix this before continuing.')

#A fix to make thigns easier, we will concatenate the Source and Dataset columns into one column called Dataset. But if source is na then we will just use the dataset column
combined_data['Dataset'] = combined_data.apply(lambda row: row['Dataset'] if pd.isna(row['Source']) else row['Dataset'] + ' $ ' + row['Source'], axis=1)
#then drop source column
combined_data = combined_data.drop(columns=['Source'])
#%%
#CREATE CONCORDANCE
#create a concordance which contains all the unique rows in the combined data df, when you remove the Dataset source and value columns.
combined_data_concordance = combined_data.drop(columns=['Dataset','Comments', 'Value']).drop_duplicates()
#we will also have to split the frequency column by its type: Yearly, Quarterly, Monthly, Daily
#YEARLY
yearly = combined_data_concordance[combined_data_concordance['Frequency'] == 'Yearly']
#YEARS:
MAX = yearly['Date'].max()
MIN = yearly['Date'].min()
#using datetime creates a range of dates, separated by year with the first year being the MIN and the last year being the MAX
years = pd.date_range(start=MIN, end=MAX, freq='Y')
#drop date from ATO_data_years
yearly = yearly.drop(columns=['Date']).drop_duplicates()
#now do a cross join between the concordance and the years array
combined_data_concordance_new = yearly.merge(pd.DataFrame(years, columns=['Date']), how='cross')
#MONTHS:
monthly = combined_data_concordance[combined_data_concordance['Frequency'] == 'Monthly']
MAX = monthly['Date'].max()
MIN = monthly['Date'].min()
#using datetime creates a range of dates, separated by month with the first month being the MIN and the last month being the MAX
months = pd.date_range(start=MIN, end=MAX, freq='M')
#drop date from ATO_data_months
monthly = monthly.drop(columns=['Date']).drop_duplicates()
monthly = monthly.merge(pd.DataFrame(months, columns=['Date']), how='cross')

#%%
#concat the months and years concordances together
combined_data_concordance_new = pd.concat([combined_data_concordance_new, monthly], axis=0)

#%%
#save
combined_data.to_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID), index=False)
#also save combined data as 'all data' in case we need to use it later
combined_data.to_csv('output_data/all_unfiltered_data_{}.csv'.format(FILE_DATE_ID), index=False)
combined_data_concordance_new.to_csv('intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID), index=False)
#%%