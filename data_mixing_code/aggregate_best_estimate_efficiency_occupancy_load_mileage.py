#%%
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
from PIL import Image
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import itertools
import utility_functions
plotting = True#change to false to stop plots from appearing
#create datasets for use based on our previous estimates of mileage, occupancy and efficiency. However this will clean the dfs up and make them cover all possible years.
#this script should be kept so it  can grab whatever is the best dataset we have available for mileage, occupancy and efficiency.

#%%
def TEMP_retrieve_estimated_mileage():
    #grab mean mileage data from previous work done in the transport data system:
    mileage = pd.read_csv('../intermediate_data/estimated/travel_km_per_stock_estimates_DATE20230216.csv')
    #convert all cols to snake case
    mileage.columns = [utility_functions.convert_string_to_snake_case(col) for col in mileage.columns]
    #convert all values in cols to snake case
    mileage = utility_functions.convert_all_cols_to_snake_case(mileage)
    #convert date to yyyy format
    mileage = utility_functions.ensure_date_col_is_year(mileage)
    #rename measure to mileage
    mileage['measure'] = 'mileage'
    #roda
    mileage = mileage[mileage['medium'] == 'road']
    #keep only the cols we need: economy, mileage, vehicle_type, drive
    mileage = mileage[['economy','value','vehicle_type','drive', 'transport_type','measure']]
    #check for duplicates
    if mileage.duplicated().any():
        print('duplicates in mileage')
        print(mileage[mileage.duplicated()])
    return mileage

def TEMP_retrieve_estimated_efficiency():
    #grab mean efficiency data from previous work done in the transport data system:
    efficiency = pd.read_csv('../intermediate_data/estimated/new_vehicle_efficiency_estimates_DATE20230216.csv')
    #convert all cols to snake case
    efficiency.columns = [utility_functions.convert_string_to_snake_case(col) for col in efficiency.columns]
    #convert all values in cols to snake case
    efficiency = utility_functions.convert_all_cols_to_snake_case(efficiency)
    #convert date to yyyy format
    efficiency = utility_functions.ensure_date_col_is_year(efficiency)
    # efficiency['new_vehicle_efficiency'] = efficiency['value']
    #roda
    efficiency = efficiency[efficiency['medium'] == 'road']
    #keep only the cols we need: economy, efficiency, vehicle_type, drive
    efficiency = efficiency[['economy','value','vehicle_type','drive', 'transport_type','measure']]
    #check for duplicates
    if efficiency.duplicated().any():
        print('duplicates in efficiency')
        print(efficiency[efficiency.duplicated()])
    return efficiency


def TEMP_retrieve_estimated_occupancy():
    #grab mean mileage data from previous work done in the transport data system:
    occupancy = pd.read_csv('../intermediate_data/estimated/occ_load_guessesDATE20230310.csv')
    #convert all cols to snake case
    occupancy.columns = [utility_functions.convert_string_to_snake_case(col) for col in occupancy.columns]
    #convert all values in cols to snake case
    occupancy = utility_functions.convert_all_cols_to_snake_case(occupancy)
    #convert date to yyyy format
    occupancy = utility_functions.ensure_date_col_is_year(occupancy)
    #filter for occupancy only 
    occupancy = occupancy[occupancy['measure'] == 'occupancy']
    # occupancy['occupancy'] = occupancy['value']
    #filter for road passenger only
    occupancy = occupancy[occupancy['transport_type'] == 'passenger']
    #roda
    occupancy = occupancy[occupancy['medium'] == 'road']
    #keep only the cols we need: economy, occupancy, vehicle_type, drive
    occupancy = occupancy[['economy','value','vehicle_type','drive', 'transport_type','measure']]
    #check for duplicates
    if occupancy.duplicated().any():
        print('duplicates in occupancy')
        print(occupancy[occupancy.duplicated()])
    return occupancy

def TEMP_retrieve_estimated_load():
    #grab mean mileage data from previous work done in the transport data system:
    load = pd.read_csv('../intermediate_data/estimated/occ_load_guessesDATE20230310.csv')
    #convert all cols to snake case
    load.columns = [utility_functions.convert_string_to_snake_case(col) for col in load.columns]
    #convert all values in cols to snake case
    load = utility_functions.convert_all_cols_to_snake_case(load)
    #convert date to yyyy format
    load = utility_functions.ensure_date_col_is_year(load)
    #filter for load only 
    load = load[load['measure'] == 'load']
    # load['load'] = load['value']
    #filter for road freight only
    load = load[load['transport_type'] == 'freight']
    #roda
    load = load[load['medium'] == 'road']
    #keep only the cols we need: economy, load, vehicle_type, drive
    load = load[['economy','value','vehicle_type','drive', 'transport_type','measure']]
    #check for duplicates
    if load.duplicated().any():
        print('duplicates in load')
        print(load[load.duplicated()])
    return load
#%%
mileage = TEMP_retrieve_estimated_mileage()
occupancy = TEMP_retrieve_estimated_occupancy()
efficiency = TEMP_retrieve_estimated_efficiency()
load = TEMP_retrieve_estimated_load()

#concat together occupancy and load and make the measure 'occupancy_or_load'
occupancy_load= pd.concat([occupancy,load])
# #melt occ and load into one col
# occupancy_load = occupancy_load.melt(id_vars=['economy','vehicle_type','drive', 'transport_type'], value_vars=['occupancy','load'], var_name='measure', value_name='occupancy_or_load')

# #concat the dataframes
# mileage_occupancy_load_efficiency_data = pd.merge(mileage, occupancy_load, how = 'outer', on = ['economy','vehicle_type','drive', 'transport_type'])
mileage_occupancy_load_efficiency_data = pd.concat([mileage,occupancy_load, efficiency])
#%%
# mileage_occupancy_load_efficiency_data = pd.merge(mileage_occupancy_load_efficiency_data, efficiency, how = 'outer', on = ['economy','vehicle_type','drive', 'transport_type'])

#drop any nans in 
#%%
#replicate the df so we ahve one for each year between 2010 and 2025
mileage_occupancy_load_efficiency_data['date'] = 2010
mileage_occupancy_load_efficiency_data_new = mileage_occupancy_load_efficiency_data.copy()
years = list(range(2011,2026))
for year in years:
    temp = mileage_occupancy_load_efficiency_data.copy()
    temp['date'] = year
    mileage_occupancy_load_efficiency_data_new = pd.concat([mileage_occupancy_load_efficiency_data_new,temp])
#%%
#fill in columns values required for the model
# INDEX_COLS:
# - Date
# - Economy
# - Measure
# - Vehicle Type
# - Unit
# - Medium
# - Transport Type
# - Drive
# - Fuel_Type
# - Frequency
# - Scope
# mileage_occupancy_load_efficiency_data_new = mileage_occupancy_load_efficiency_data_new.melt(id_vars = ['date','economy','vehicle_type','drive', 'transport_type'], value_vars = ['mileage','occupancy_or_load','new_vehicle_efficiency'], var_name = 'measure', value_name = 'value')
# if measure is mileage, unit is km_per_year, if measure is occupancy, unit is passengers if measure is efficiency, unit is pj_per_km
#use map
mileage_occupancy_load_efficiency_data_new['unit'] = mileage_occupancy_load_efficiency_data_new['measure'].map({'mileage':'km_per_stock','occupancy_or_load':'passengers_or_tonnes','new_vehicle_efficiency':'pj_per_km'})

mileage_occupancy_load_efficiency_data_new['medium'] = 'road'
#%%
mileage_occupancy_load_efficiency_data_new['fuel_type'] = 'all'
mileage_occupancy_load_efficiency_data_new['frequency'] = 'yearly'
mileage_occupancy_load_efficiency_data_new['scope'] = 'national'
mileage_occupancy_load_efficiency_data_new['dataset'] = 'estimated_mileage_occupancy_load_efficiency'
mileage_occupancy_load_efficiency_data_new['source'] = 'transport_data_system'
#save to estimates folder
import datetime
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
mileage_occupancy_load_efficiency_data_new.to_csv('../intermediate_data/estimated/mileage_occupancy_load_efficiency_data_{}.csv'.format(FILE_DATE_ID), index = False)
#%%
plot = False #note that currentlyt this is not very interesting becaues every economy has the same data as the others
if plot:
    #plot the data in separate graphs for each measure. We will do boxplots whjich will show the x axis as the economy
    #we will plot facet cols for each vehicle type and facet rows for each drive
    import plotly.express as px
    for measure in ['mileage','occupancy','new_vehicle_efficiency']:
        fig = px.strip(mileage_occupancy_load_efficiency_data_new[mileage_occupancy_load_efficiency_data_new['measure'] == measure], x="drive", y="value", color="drive", facet_row="vehicle_type", title = 'Estimated {} by economy and drive type'.format(measure))
        #save as html and open in bvrwser
        fig.write_html("../plotting_output/estimations/eff_mileage_occ_load/{}_by_economy_and_drive_type.html".format(measure), auto_open=True)

# %%
