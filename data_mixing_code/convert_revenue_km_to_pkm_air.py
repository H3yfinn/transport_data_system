#for now we iwll jsut assume that revenue km in the ATO data is 1:1 to passenger km , even if it is a slight underestimate ( Revenue passenger kilometers (or revenue passenger miles, in some territories) are a measure of passenger demand in a given market. They are calculated by multiplying the number of revenue-paying passengers aboard an aircraft by the distance the aircraft traveled. )
#%%
import pandas as pd
import re
#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

import utility_functions as utility_functions


file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/ATO_data/', 'ATO_data_cleaned_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
ATO_dataset_clean = pd.read_csv('intermediate_data/ATO_data/ATO_data_cleaned_{}.csv'.format(FILE_DATE_ID))

#%%
#grab revenue km data
a = ATO_dataset_clean.loc[ATO_dataset_clean.Measure == 'revenue_passenger_km']
#filter for only Scope = 'National'
a = a.loc[a.Scope == 'National']
#change Measure to passenger_km
a.Measure = 'passenger_km'
a.Unit = 'passenger_km'

#change dataset to 'Rev_pass_km' and source to 'ICCT'
a.Dataset = 'Rev_pass_km'
a.Source = 'ICCT'
#drop sheet col
a = a.drop(columns = ['Sheet'])

#save to csv
a.to_csv('intermediate_data/estimated/ATO_revenue_pkm{}.csv'.format(FILE_DATE_ID), index = False)
# %%
