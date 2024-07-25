#this data has the columns Exp_Country_Name	Exp_Country_ISO3	Imp_Country_Name	Imp_Country_ISO3	Vehicle_type	Year	Quantity	Source_Code
#most countries are not apec so we should later filter out the apec countries, but for now we will just use the data as is (we can add a flag to the data to indicate if the country is apec or not - it will contain either False or the economy code for the apec economy , e.g. 01_AUS for australia)
#i'd like to use this data for apec to compare to the stocks data and try work out how many'new' vehicles are used imports vs new imports in as many countries as possible. Thn we can use this to try estimate the ratio for other countries, based on their level of development, etc.

# We can use the C:\Users\finbar.maunsell\github\transport_data_system\config\economy_code_to_name.csv file to do the mapping of the economy codes to the economy names for apec economies.
# it has the cols Economy	Economy_name	Alt_name	Alt_name2	Alt_name3	iso_code	alt_aperc_name
# we will use iso_code and Economy to do the mapping

#%%

import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import re
import datetime
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')

#%%

used_vehicles_data = pd.read_csv('input_data/ITF/usedvehicles_v1.2.csv')
#drop Value and Currency cols
used_vehicles_data = used_vehicles_data.drop(columns=['Value','Currency', 'Source_Code'])
################
#create a column to indicate if the country is apec or not
economy_code_to_name = pd.read_csv('config/economy_code_to_name.csv')
used_vehicles_data = used_vehicles_data.merge(economy_code_to_name[['iso_code','Economy']], left_on='Imp_Country_ISO3', right_on='iso_code', how='left')
#%%

used_vehicles_data['import_country_apec'] = used_vehicles_data['Economy'].apply(lambda x: x if pd.notna(x) else False)

used_vehicles_data = used_vehicles_data.drop(columns=['iso_code','Economy'])
used_vehicles_data = used_vehicles_data.merge(economy_code_to_name[['iso_code','Economy']], left_on='Exp_Country_ISO3', right_on='iso_code', how='left')
used_vehicles_data['export_country_apec'] = used_vehicles_data['Economy'].apply(lambda x: x if pd.notna(x) else False)
used_vehicles_data = used_vehicles_data.drop(columns=['iso_code','Economy'])

################
#%%

#calculate totals for each country, vehicle type and year. we will do this for exports and imports. then we can set the other column (e.g. import_country_apec when summing for export country) to 'all', and concatenate the three dataframes together
#make sure to only sum the Quantity col
import_totals = used_vehicles_data.groupby(['Imp_Country_ISO3','Imp_Country_Name', 'import_country_apec', 'Vehicle_type','Year']).agg({'Quantity':'sum'}).reset_index()
export_totals = used_vehicles_data.groupby(['Exp_Country_ISO3','Exp_Country_Name','export_country_apec', 'Vehicle_type','Year']).agg({'Quantity':'sum'}).reset_index()
import_totals['export_country_apec'] = 'all'
import_totals['Exp_Country_Name'] = 'all'
import_totals['Exp_Country_ISO3'] = 'all'
export_totals['import_country_apec'] = 'all'
export_totals['Imp_Country_Name'] = 'all'
export_totals['Imp_Country_ISO3'] = 'all'

all_totals = pd.concat([import_totals, export_totals], axis=0)
#%%
used_vehicles_data_updated = pd.concat([used_vehicles_data, all_totals], axis=0)
#%%
#rename quantity to value and add in a measure column that is 'used_vehicle_transfers'
used_vehicles_data_updated = used_vehicles_data_updated.rename(columns={'Quantity':'Value'})
used_vehicles_data_updated['Measure'] = 'used_vehicle_transfers'
used_vehicles_data_updated['Dataset'] = 'ITF'
#%%

#map the Vehicle_type to what we use in our data:
mapping_vtype = {
    'Passenger Cars':'car',
}
#if there is no mapping for a value in the Vehicle_type column, we should raise an error
for vtype in used_vehicles_data_updated['Vehicle_type'].unique():
    if vtype not in mapping_vtype.keys():
        raise ValueError(f'No mapping for Vehicle_type: {vtype}')
    else:
        used_vehicles_data_updated['Vehicle_type'] = used_vehicles_data_updated['Vehicle_type'].replace(vtype, mapping_vtype[vtype])
        
#%%
#save the data

file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

used_vehicles_data_updated.to_csv(f'input_data/ITF/{FILE_DATE_ID}_usedvehicles_PROCESSED_v1.2.csv', index=False)
#%%
