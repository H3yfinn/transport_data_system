
#%%
#repalcement for estimate avg efficiency data because that wasnt resulting in very trsutworthy valeus. Instead we will use this to retreive and estimat better valus.
import pandas as pd
import numpy as np
import os
import datetime
import re
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
import sys
folder_path = './aggregation_code'  # Replace with the actual path of the folder you want to add
sys.path.append(folder_path)
import utility_functions 
#first, take in data from 
# fuel_economy_by_vehicle_type_new.to_csv('intermediate_data/USA/all_economys_fuel_economy_by_vehicle_type_{}.csv'.format(FILE_DATE_ID),index=False)
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/USA/', 'all_economys_fuel_economy_by_vehicle_type_')
fuel_economy_by_vehicle_type_new = pd.read_csv('intermediate_data/USA/all_economys_fuel_economy_by_vehicle_type_{}.csv'.format('DATE'+file_date))

#note that this data is really only for the US. but we will just assume its the same for all countries for now. Then we will times the ice value by 3.5 to find the efficiency of BEV's.
#%%
#we will also assume that cng and lpg powered vehicles are the same efficiency as ice ones. THis is because there is not much knowledge aobut these, but it is assumed that if they are more efficient its only by a few percent, and they would also lose out on the relative efficiency gains from learning.

#drop the vehicle sub type ans then average the values
# fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type_new.drop(columns=['vehicle_sub_type'])
index_cols = fuel_economy_by_vehicle_type_new.columns.tolist()
index_cols.remove('value')
fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type_new.groupby(index_cols).mean().reset_index()

#now drop vehicle_type = ridesource
fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type_new[~fuel_economy_by_vehicle_type_new['vehicle_type'].isin(['ridesource'])]

#get ice_g and ice_d data:
fuel_economy_by_vehicle_type_new_ice_g = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['drive']=='ice_g']
fuel_economy_by_vehicle_type_new_ice_d = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['drive']=='ice_d']

# #now filter for only the ice_g vehicles
fuel_economy_by_vehicle_type_new_ice = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['drive']=='ice_g']

#and now for drive in cng, lpg, create a cpoy of the df
fuel_economy_by_vehicle_type_new_cng = fuel_economy_by_vehicle_type_new_ice.copy()
fuel_economy_by_vehicle_type_new_lpg = fuel_economy_by_vehicle_type_new_ice.copy()
fuel_economy_by_vehicle_type_new_bev = fuel_economy_by_vehicle_type_new_ice.copy()
fuel_economy_by_vehicle_type_new_phev_g = fuel_economy_by_vehicle_type_new_ice_g.copy()
fuel_economy_by_vehicle_type_new_phev_d = fuel_economy_by_vehicle_type_new_ice_d.copy()
fuel_economy_by_vehicle_type_new_phev = fuel_economy_by_vehicle_type_new_ice.copy()
fuel_economy_by_vehicle_type_new_fcev = fuel_economy_by_vehicle_type_new_ice.copy()
#and then change the drive type to cng and lpg respectively
fuel_economy_by_vehicle_type_new_cng['drive'] = 'cng'
fuel_economy_by_vehicle_type_new_lpg['drive'] = 'lpg'

fuel_economy_by_vehicle_type_new_bev['drive'] = 'bev'
fuel_economy_by_vehicle_type_new_fcev['drive'] = 'fcev'

# fuel_economy_by_vehicle_type_new_phev['drive'] = 'phev'
fuel_economy_by_vehicle_type_new_phev_g['drive'] = 'phev_g'
fuel_economy_by_vehicle_type_new_phev_d['drive'] = 'phev_d'

#and now calcualte the efficiency of BEV's and fcevs (assuming they are similar)(we are using 3.5 times gasoline efficiency). this is very general, but we will use this for now.
fuel_economy_by_vehicle_type_new_bev['value'] = fuel_economy_by_vehicle_type_new_bev['value']*3.5
fuel_economy_by_vehicle_type_new_fcev['value'] = fuel_economy_by_vehicle_type_new_fcev['value']*3.5
#we can assume that phevs are about 3.5/2 times as efficient as ice vehicles (since they use both fuel and electricity and its thought that they use them about 50% of the time each)
# fuel_economy_by_vehicle_type_new_phev['value'] = fuel_economy_by_vehicle_type_new_phev['value']*(3.5/2)
fuel_economy_by_vehicle_type_new_phev_g['value'] = fuel_economy_by_vehicle_type_new_phev_g['value']*(3.5/2)
fuel_economy_by_vehicle_type_new_phev_d['value'] = fuel_economy_by_vehicle_type_new_phev_d['value']*(3.5/2)

#and then concat them to the original df
fuel_economy_by_vehicle_type_new = pd.concat([fuel_economy_by_vehicle_type_new_ice,fuel_economy_by_vehicle_type_new_cng,fuel_economy_by_vehicle_type_new_lpg, fuel_economy_by_vehicle_type_new_bev, fuel_economy_by_vehicle_type_new_fcev, 
 fuel_economy_by_vehicle_type_new_phev_g, fuel_economy_by_vehicle_type_new_phev_d, fuel_economy_by_vehicle_type_new_ice_g, fuel_economy_by_vehicle_type_new_ice_d])
# fuel_economy_by_vehicle_type_new_phev,
#and then drop the duplicates
fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type_new.drop_duplicates()

#%%
#ADJUST VEHICLE TYPE
#now adjust efficiency for cars vs suvs and ht vs mt, since they are all based on the same data
car = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['vehicle_type']=='car']
suv = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['vehicle_type']=='suv']
ht = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['vehicle_type']=='ht']
mt = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['vehicle_type']=='mt']
fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type_new[~fuel_economy_by_vehicle_type_new['vehicle_type'].isin(['car','suv','ht','mt'])]
car['value'] = car['value']*1.1
suv['value'] = suv['value']*0.9
ht['value'] = ht['value']*0.9
mt['value'] = mt['value']*1.1

#%%
#extract 2w vehicle type and replicate it for freight
two_wheeler = fuel_economy_by_vehicle_type_new[fuel_economy_by_vehicle_type_new['vehicle_type']=='2w']
two_wheeler['transport_type'] = 'freight'
fuel_economy_by_vehicle_type_new = pd.concat([fuel_economy_by_vehicle_type_new,two_wheeler])

#put thme back in:
fuel_economy_by_vehicle_type_new = pd.concat([fuel_economy_by_vehicle_type_new,car,suv,ht,mt])
#%%
#now replicate the df for every year between 2010 and 2025
fuel_economy_by_vehicle_type_new['date'] = '2010-12-31'
fuel_economy_by_vehicle_type_new_2010 = fuel_economy_by_vehicle_type_new.copy()
for year in range(2010+1, 2025):
    fuel_economy_by_vehicle_type_new_2010['date'] = str(year)+'-12-31'
    fuel_economy_by_vehicle_type_new = pd.concat([fuel_economy_by_vehicle_type_new,fuel_economy_by_vehicle_type_new_2010])

#and replicate the data again so we ahve a new df for new_vehicle_efficiency
new_fuel_economy_by_vehicle_type_new = fuel_economy_by_vehicle_type_new.copy()
new_fuel_economy_by_vehicle_type_new['measure']='new_vehicle_efficiency'

#concatenate the two dataframes
fuel_economy_by_vehicle_type_new = pd.concat([fuel_economy_by_vehicle_type_new,new_fuel_economy_by_vehicle_type_new])
#%%

#make dataset col
fuel_economy_by_vehicle_type_new['dataset'] = 'USA_alternative_fuels_data_center'

fuel_economy_by_vehicle_type_new['Fuel'] = 'all'

#%%
import datetime
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
#save it so that it can be concatenated to the transport_data_system_df_original
fuel_economy_by_vehicle_type_new.to_csv('intermediate_data/estimated/USA_based_vehicle_efficiency_estimates_{}.csv'.format(FILE_DATE_ID),index=False)

#%%

