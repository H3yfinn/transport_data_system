#%%
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import os
import re
import datetime
#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
FILE_DATE_ID = 'DATE20221214'
#%%
#load 8th data
eigth_edition_transport_data = pd.read_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_{}.csv'.format(FILE_DATE_ID))
#%%

#there are too many missing values for 2017 in new_vehicle_efficiency, we will jsut fill them in with the values for 2018
new_vehicle_efficiency = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'new_vehicle_efficiency') & (eigth_edition_transport_data['Dataset'] == '8th edition transport model'),:]
#filter out anny values for 2017
new_vehicle_efficiency_not_2017 = new_vehicle_efficiency[new_vehicle_efficiency['Year'] != 2017]
new_vehicle_efficiency_2017 = new_vehicle_efficiency.loc[new_vehicle_efficiency['Year'] == 2018]
#set year to 2017
new_vehicle_efficiency_2017['Year'] = 2017
#concat
new_vehicle_efficiency = pd.concat([new_vehicle_efficiency_not_2017, new_vehicle_efficiency_2017])

#add data back to eigth_edition_transport_data
eigth_edition_transport_data = eigth_edition_transport_data.loc[~((eigth_edition_transport_data['Measure'] == 'new_vehicle_efficiency') & (eigth_edition_transport_data['Dataset'] == '8th edition transport model')),:]
eigth_edition_transport_data = pd.concat([eigth_edition_transport_data, new_vehicle_efficiency])

###################################################################

#%%

#WE ONLY HAVE PRE-2017 DATA FOR OCC_LOAD.
occupance_load = eigth_edition_transport_data.loc[(eigth_edition_transport_data['Measure'] == 'occupancy_or_load') & (eigth_edition_transport_data['Dataset'] == '8th edition transport model'),:]
#we will set all values for BASE_YEAR to the values for 2016 in occupance_load.
occupance_load = occupance_load.loc[occupance_load.Year == 2016,:]
occupance_load['Year'] = 2017

#add data back to eigth_edition_transport_data
eigth_edition_transport_data = eigth_edition_transport_data.loc[~((eigth_edition_transport_data['Measure'] == 'occupancy_or_load') & (eigth_edition_transport_data['Dataset'] == '8th edition transport model')),:]
eigth_edition_transport_data = pd.concat([eigth_edition_transport_data, occupance_load])
####################################################################################################################################
#%%
#
#save data to same file
eigth_edition_transport_data.to_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_{}.csv'.format(FILE_DATE_ID), index = False)

#%%