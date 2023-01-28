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

#%%

#take in sales and stocks datas from the same source for same vehicle type in same transport type. If possible for same drive type too.


#calculate stocks minus sales per year

#calculate year on year change in stocks minus sales

#calculate turnover rate as year on year change in stocks minus sales divided by stocks minus sales

#plot turnover rate foir each economy on a line graph using left y axis, then also plot the stocks (dashed) and sales (double dashed) on the right y axis, with Date on the x axis. We can do this in different colors for each vehicle type, drive combination