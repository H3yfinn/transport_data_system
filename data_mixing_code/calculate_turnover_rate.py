#need turnover rate for the 9th edition model . we will do that now using the best data we can find (doesnt matter if it doesnt fit the rest of the data for each, say, transport type, we just want the most accurate value for turnover rate for each vehicle type we can find)

#we know we have some pretty good data from ATO for passenger ldv's, so we will try that out
#probl;em is that data doesnt have related sales so how ? or does it?


#%%
#set working directory as one folder back so that config works
import os
import re
import pandas as pd
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')
#%%
#we're oging to take in the data we have from the 8th edition transport model and convert it into LDV data so it can be compared to other datasets which are commonly reported as LDV data. 
#LDV data is efectively the sum of the lt and lv values

#we will also look inot importing data from other datasets since that might have data on lv/lt when it can be added to create ldv's

import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data = pd.read_csv('./intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))

#load datafrom 9th
# APERC\transport_data_system\intermediate_data\9th_dataset
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/9th_dataset/', 'combined_dataset_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data_9th = pd.read_csv('./intermediate_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID))
#%%
# #for now use 9th dataset. We will filter for Stocks and sales. then make wide so we have a  stock and sales col. then filter for rows where we have both stocks and sales. then we can calculate turnover rate
# combined_data_9th = combined_data_9th[combined_data_9th['Measure'].isin(['Stocks', 'Sales'])]

#for now use original dataset since 9th doesnt have sales. 
combined_data = combined_data[combined_data['Measure'].isin(['Stocks', 'Sales'])]
#%%
#unfortunately, later on, duplicates result in a pct cahnge being counted when it shouldnt be. So we will extract the duplciated rows, and then choose one of them to keep. 
INDEX_COLS = ['Measure','Date', 'Economy', 'Vehicle Type', 'Medium','Transport Type','Drive','Fuel_Type','Frequency', 'Scope']
#grab the duplicated rows
combined_data_dups = combined_data[combined_data.duplicated(subset=INDEX_COLS, keep=False)]
#now we will choose one of the duplicates to keep. First remove any datasets with est in their name because they wont be on same level as others
combined_data_dups = combined_data_dups[~combined_data_dups['Dataset'].str.contains('est')]
#then filter again for only dupes
combined_data_dups2 = combined_data_dups[combined_data_dups.duplicated(subset=INDEX_COLS, keep=False)]
#grab the datasets the sales data is from and where they are available keep them:
datasets_to_keep = combined_data[combined_data['Measure']=='Sales']['Dataset'].unique()
combined_data_dups2 = combined_data_dups2[combined_data_dups2['Dataset'].isin(datasets_to_keep)]
#then filter again for only dupes
combined_data_dups3 = combined_data_dups2[combined_data_dups2.duplicated(subset=INDEX_COLS, keep=False)]
#now just keep the first one
combined_data_dups3 = combined_data_dups3.drop_duplicates(subset=INDEX_COLS, keep='first')
#remove dupes from combined data then add back in the ones we want to keep
combined_data = combined_data[~combined_data.duplicated(subset=INDEX_COLS, keep=False)]
combined_data = pd.concat([combined_data,combined_data_dups3])

#%%
#for now we will include drive in the index col but perhaps it would be better to sum values for each vehicle tpe
INDEX_COLS_plus_dataset = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium','Transport Type','Drive','Fuel_Type','Frequency', 'Scope', 'Dataset']
INDEX_COLS_no_measure = INDEX_COLS_plus_dataset.copy()
INDEX_COLS_no_measure.remove('Measure')
#also remove unit
INDEX_COLS_no_measure.remove('Unit')
#make wide
combined_data_wide = combined_data.pivot(index=INDEX_COLS_no_measure, columns='Measure', values='Value')
combined_data_wide.reset_index(inplace=True)
#get rows with only stocks and sales
combined_data_no_nas1 = combined_data_wide[combined_data_wide['Stocks'].notna() & combined_data_wide['Sales'].notna()]

#%%
#so it looks like no rows have both stocks and sales. this would be because the datasets are not the same. It seems it might be important to have a 9th version for sales if we want to most accurately calculate turnover rate. Since iom not sure this is the ebst way to do it, i will leave that idea for now and instead calc turnover rate by:
#removing the dataset col, separating sales and stocks into two dataframes, then merging them on the index cols.
combined_data_sales = combined_data_wide.drop(columns=['Dataset', 'Stocks'])
combined_data_stocks = combined_data_wide.drop(columns=['Dataset', 'Sales'])
INDEX_COLS = ['Date', 'Economy', 'Vehicle Type', 'Medium','Transport Type','Drive','Fuel_Type','Frequency', 'Scope']
combined_data_sales_stocks = combined_data_sales.merge(combined_data_stocks, on=INDEX_COLS, how='outer')

#get rows with only stocks and sales
combined_data_no_nas = combined_data_sales_stocks[combined_data_sales_stocks['Stocks'].notna() & combined_data_sales_stocks['Sales'].notna()]
#convert date to year using first 4 digits
combined_data_no_nas['Date'] = combined_data_no_nas['Date'].str.slice(0,4).astype(int)
#ok looks like the only stuff we have sales for is bev and phev , then where drive is na. So we will calculate turnover rate for those, since we could expect that a speicifc turnover rate for bev and phev would be good, and also usefulo to comare anywayy
#%%
#now for each row, if the row above or below it is in same group, then check if the year is one less or more. if it is, set a flag to true. if it isnt, set a flag to false. then filter for rows where the flag is true
INDEX_COLS_no_date = ['Economy', 'Vehicle Type', 'Medium','Transport Type','Drive','Fuel_Type','Frequency', 'Scope']

grouped = combined_data_no_nas.groupby(INDEX_COLS_no_date)
#%%
#set NaN to 'nan' (groupby doesnt work with NaN)
combined_data_no_nas['Drive'].fillna('nan', inplace=True)
combined_data_no_nas['Fuel_Type'].fillna('nan', inplace=True)

grouped = combined_data_no_nas.groupby(INDEX_COLS_no_date)

# filter groups where there is at least one other year that is one less or one more than the current year
filtered_grouped = grouped.filter(lambda x: ((x['Date'] + 1).isin(x['Date'].values).any() 
                                          or (x['Date'] - 1).isin(x['Date'].values).any()))

#%%
#calculate stocks minus sales per year
turnover_rate = filtered_grouped.copy()
turnover_rate['Stocks_minus_sales'] = turnover_rate['Stocks'] - turnover_rate['Sales']

#calculate pct change for consecutive years
turnover_rate['Pct_change'] = turnover_rate.groupby(INDEX_COLS_no_date)['Stocks_minus_sales'].pct_change()

#order by date
turnover_rate.sort_values(by=['Date']+INDEX_COLS_no_date, inplace=True)
#%%
#plot turnover rate using scatterplots, facted by vehicle type, and coloured by economy
#first remove outliers as anything without a negative turnover rate is not possible if we are using stocks and sales
turnover_rate = turnover_rate[turnover_rate['Pct_change'] < 0]
#remove anything below -0.2
turnover_rate = turnover_rate[turnover_rate['Pct_change'] > -0.2]
#%%
import plotly.express as px
fig = px.scatter(turnover_rate, x="Date", y="Pct_change", color="Economy", facet_col="Vehicle Type")
fig.show()

#EHHH maybe just set to 0.03 for now

# %%
#so for every row in combined_data_9th, set Vlaue to 0.03, Dataset to 'Turnover_rate_est' and Measure to 'Turnover_rate'. Then Unit to % 
combined_data_9th_t = combined_data_9th.copy()
#%%
combined_data_9th_t['Value'] = 0.03
combined_data_9th_t['Dataset'] = 'Turnover_rate_est'
combined_data_9th_t['Measure'] = 'Turnover_rate'
combined_data_9th_t['Unit'] = '%'

#filter for medium = road, scope = national
combined_data_9th_t = combined_data_9th_t[combined_data_9th_t['Medium'] == 'road']
combined_data_9th_t = combined_data_9th_t[combined_data_9th_t['Scope'] == 'National']
#drop duplicates
combined_data_9th_t.drop_duplicates(inplace=True)
#%%

#save
combined_data_9th_t.to_csv('./intermediate_data/estimated/{}_turnover_rate_3pct.csv'.format(FILE_DATE_ID), index=False)
# %%
