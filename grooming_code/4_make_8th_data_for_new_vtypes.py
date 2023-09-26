#take in data for 8th edition and asdjust its vehicler types to suit the ones that are defined in the ccg data. that is:
# vehicle_data = [
#     {
#         "Mode": "Motorcycle",
#         "Definition": "Motor vehicles with less than four wheels and some lightweight four-wheelers",
#         "EU Vehicle Category Code": ["L1", "L2", "L3", "L4", "L5", "L6", "L7"],
#         "Includes": ["Moped", "motorcycle", "motor tricycle", "quadricycle"]
#     },
#     {
#         "Mode": "Car",
#         "Definition": "Power-driven vehicles having four wheels and used for the carriage of passengers",
#         "EU Vehicle Category Code": ["M1"],
#         "Includes": ["Standard car with 2, 3, 4 doors", "SUV"]
#     },
#     {
#         "Mode": "Light-duty vehicle (passenger)",
#         "Definition": "Power-driven vehicles used for carriage of passengers, larger than a car, but comprising not more than eight seats in addition to the driver's seat",
#         "EU Vehicle Category Code": ["M1"],
#         "Includes": ["Pick-up truck", "minivan"]
#     },
#     {
#         "Mode": "Minibus",
#         "Definition": "Power-driven vehicles used for the carriage of passengers, comprising more than eight seats in addition to the driver's seat, and having a maximum mass not exceeding 5 tonnes",
#         "EU Vehicle Category Code": ["M2"],
#         "Includes": ["Microbus", "minicoach", "light bus"]
#     },
#     {
#         "Mode": "Bus",
#         "Definition": "Power-driven vehicles used for the carriage of passengers, comprising more than eight seats in addition to the driver's seat, and having a maximum mass exceeding 5 tonnes",
#         "EU Vehicle Category Code": ["M3"],
#         "Includes": ["Coach"]
#     },
#     {
#         "Mode": "Light-duty vehicle (freight)",
#         "Definition": "Power-driven vehicles used for the carriage of goods and having a maximum mass not exceeding 3.5 tonnes (pick-up truck, van)",
#         "EU Vehicle Category Code": ["N1"],
#         "Includes": ["Pick-up truck", "van"]
#     },
#     {
#         "Mode": "Medium-duty vehicle",
#         "Definition": "Power driven-vehicles used for the carriage of goods and having a maximum mass exceeding 3.5 tonnes but not exceeding 12 tonnes (commercial truck)",
#         "EU Vehicle Category Code": ["N2"],
#         "Includes": ["Truck", "van"]
#     },
#     {
#         "Mode": "Heavy-duty vehicle",
#         "Definition": "Power-driven vehicles used for the carriage of goods and having a maximum mass exceeding 12 tonnes (commercial truck)",
#         "EU Vehicle Category Code": ["N3"],
#         "Includes": ["Truck", "lorry", "fork lift", "bulldozer"]
#     }
# ]

#BUT NOTE THAT WE DROPPED MINIBUS AND ALSO ADDED suv as a caterogry between car and ldv and freight ldv to lcv

#ahh forgwet it. jsut changed it to lcv and lpv, which are aggregations of lt and lv in previous edition. then we convert that to lt/car/suv in passenger data later in sleection process using outside dataset. Everything else is fine enough.
#%%
import pandas as pd
import numpy as np
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import os
import re
import datetime
import re
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
import sys
folder_path = './aggregation_code'  # Replace with the actual path of the folder you want to add
sys.path.append(folder_path)
import utility_functions 
#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
# file_date = datetime.datetime.now().strftime("%Y%m%d")

file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/8th_edition_transport_model/', 'eigth_edition_transport_data_final_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
new_eigth_edition_transport_data=pd.read_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_final_{}.csv'.format(FILE_DATE_ID))
# file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/8th_edition_transport_model/', 'non_filtered_eigth_edition_transport_data_final_')
# FILE_DATE_ID = 'DATE{}'.format(file_date)
# eigth_edition_transport_data=pd.read_csv('intermediate_data/8th_edition_transport_model/non_filtered_eigth_edition_transport_data_final_{}.csv'.format(FILE_DATE_ID))
#%%




#%%
#take a look at the data:
# new_eigth_edition_transport_data.Drive.unique()
# array(['cng', 'd', 'g', 'lpg', 'bev', 'phevd', 'fcev', nan, 'phevg',
#        'all'], dtype=object)

# new_eigth_edition_transport_data['Vehicle Type'].unique()
#array(['ht', '2w', 'bus', nan, 'ldv'], dtype=object)

#so in that, we will  ldv in freight transport type to light-commercial vehicle (lcv)
#then since we dont knwo how to split ht we will leave it as is
new_eigth_edition_transport_data.loc[(new_eigth_edition_transport_data['Vehicle Type']=='ldv') & (new_eigth_edition_transport_data['Transport Type']=='passenger'), 'Vehicle Type']='lpv'
new_eigth_edition_transport_data.loc[(new_eigth_edition_transport_data['Vehicle Type']=='ldv') & (new_eigth_edition_transport_data['Transport Type']=='freight'), 'Vehicle Type']='lcv'

#and also might as well convert the drive types to the ones we are now using, that is, ice_g, ice_d, phev_g, phev_d, bev, fcev, cng, lpg
new_eigth_edition_transport_data.loc[new_eigth_edition_transport_data['Drive']=='d', 'Drive']='ice_d'
new_eigth_edition_transport_data.loc[new_eigth_edition_transport_data['Drive']=='g', 'Drive']='ice_g'
new_eigth_edition_transport_data.loc[new_eigth_edition_transport_data['Drive']=='phevd', 'Drive']='phev_d'
new_eigth_edition_transport_data.loc[new_eigth_edition_transport_data['Drive']=='phevg', 'Drive']='phev_g'

#and lastly, so it doesnt get mixed up with the otehr data, change dataset to 
new_eigth_edition_transport_data['Dataset']='8th - new vtypes and drives'
#%%
# #chekc for duplicates when ignore vlasue col
# cols = new_eigth_edition_transport_data.columns.tolist()
# cols.remove('Value')
# cols.remove('fuel')
# new_eigth_edition_transport_data_d = new_eigth_edition_transport_data[new_eigth_edition_transport_data.duplicated(keep=False, subset=cols )]
#%%
#then save
new_eigth_edition_transport_data.to_csv('intermediate_data/8th_edition_transport_model/eigth_edition_transport_data_final_new_vtypes_drives_{}.csv'.format(FILE_DATE_ID), index=False)




#%%