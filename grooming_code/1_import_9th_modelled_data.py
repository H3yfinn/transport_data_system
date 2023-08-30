#%%
import pandas as pd
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import re
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')

import datetime
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
#%%
###########Thailand:####################### #based on https://chat.openai.com/share/bb174391-f64f-47b4-b502-84de77a829b2 and work that htanan and praw did in finding the values. they are heavily simplified from what they gave me
# Load the data
df = pd.read_excel("./input_data/Thailand/Vehicle stock analysis DEC 2022.xlsx", sheet_name="data_system_input 2020")
# Melt the data
df_melted = df.melt(id_vars=df.columns[0], var_name='Vehicle Type', value_name='Value')

# Rename the columns as per user's request
df_melted.rename(columns={"Unnamed: 0": "Vehicle Type", "Vehicle Type": "Drive"}, inplace=True)
#map transport type: based on vehicle type:
transport_type_map = {'lpv':'passenger', 'ht':'freight', 'lcv':'freight', 'bus':'passenger', '2w':'passenger', 'Van':'freight', 'Others':'passenger'}
df_melted['transport_type'] = df_melted['Vehicle Type'].map(transport_type_map)

# Set all columns to the specified values
df_melted["economy"] = "19_THA"
df_melted["date"] = 2020
df_melted["medium"] = "road"
df_melted["measure"] = "stocks"
df_melted["dataset"] = "9th_model_first_iteration"
df_melted["source"] = ""
df_melted["unit"] = "stocks"
df_melted["fuel"] = "all"
df_melted["comment"] = "no_comment"
df_melted["scope"] = "national"
df_melted["frequency"] = "yearly"

# Save the transformed data to an Excel file
df_melted.to_csv('intermediate_data/THA/thailand_new_stocks_9th_model_first_iteration_{}.csv'.format(FILE_DATE_ID),index=False)
#%%

#######################:#######################
#now do it for usa. currently jsut adjusting stocks with drive set to all. Hopefully the way it gets spat out by the model will reflect an accurate representation of drive distributions

usa_stocks = pd.read_excel("./input_data/USA/9th_first_iteration_checking.xlsx", sheet_name="data_system_input_2020")
#set source to blank
usa_stocks["Source"] = ""
# Save the transformed data to an Excel file
usa_stocks.to_csv('intermediate_data/USA/usa_new_stocks_9th_model_first_iteration_{}.csv'.format(FILE_DATE_ID),index=False)
#%%
#######################:#######################
#and for cda:
canada_stocks2017 = pd.read_excel("./input_data/Canada/9th_model_first_iteration.xlsx", sheet_name="2017")

canada_stocks2018 = pd.read_excel("./input_data/Canada/9th_model_first_iteration.xlsx", sheet_name="2018")

canada_stocks2019 = pd.read_excel("./input_data/Canada/9th_model_first_iteration.xlsx", sheet_name="2019")

canada_stocks2020 = pd.read_excel("./input_data/Canada/9th_model_first_iteration.xlsx", sheet_name="2020")

canada_stocks2021 = pd.read_excel("./input_data/Canada/9th_model_first_iteration.xlsx", sheet_name="2021")

#from all, remove the cols after I and below row 8
canada_stocks2017 = canada_stocks2017.iloc[:7,:8]
canada_stocks2018 = canada_stocks2018.iloc[:7,:8]
canada_stocks2019 = canada_stocks2019.iloc[:7,:8]
canada_stocks2020 = canada_stocks2020.iloc[:7,:8]
canada_stocks2021 = canada_stocks2021.iloc[:7,:8]
#now melt all
canada_stocks2017_melted = canada_stocks2017.melt(id_vars=canada_stocks2017.columns[0], var_name='Vehicle Type', value_name='Value')
canada_stocks2018_melted = canada_stocks2018.melt(id_vars=canada_stocks2018.columns[0], var_name='Vehicle Type', value_name='Value')
canada_stocks2019_melted = canada_stocks2019.melt(id_vars=canada_stocks2019.columns[0], var_name='Vehicle Type', value_name='Value')
canada_stocks2020_melted = canada_stocks2020.melt(id_vars=canada_stocks2020.columns[0], var_name='Vehicle Type', value_name='Value')
canada_stocks2021_melted = canada_stocks2021.melt(id_vars=canada_stocks2021.columns[0], var_name='Vehicle Type', value_name='Value')

#now add the date column to each
canada_stocks2017_melted["date"] = 2017
canada_stocks2018_melted["date"] = 2018
canada_stocks2019_melted["date"] = 2019
canada_stocks2020_melted["date"] = 2020
canada_stocks2021_melted['date'] = 2021

#concat them all
canada_stocks = pd.concat([canada_stocks2017_melted, canada_stocks2018_melted, canada_stocks2019_melted, canada_stocks2020_melted, canada_stocks2021_melted])
# Rename the columns as per user's request
canada_stocks.rename(columns={"Unnamed: 0": "Vehicle Type", "Vehicle Type": "Drive"}, inplace=True)
#map transport type: based on vehicle type:
transport_type_map = {'lpv':'passenger', 'ht':'freight', 'lcv':'freight', 'bus':'passenger', '2w':'passenger','car':'passenger', 'suv':'passenger', 'lt':'passenger'}
canada_stocks['transport_type'] = canada_stocks['Vehicle Type'].map(transport_type_map)

# Set all columns to the specified values
canada_stocks["economy"] = "03_CDA"
canada_stocks["medium"] = "road"
canada_stocks["measure"] = "stocks"
canada_stocks["dataset"] = "9th_model_first_iteration"
canada_stocks["source"] = ""
canada_stocks["unit"] = "stocks"
canada_stocks["fuel"] = "all"
canada_stocks["comment"] = "no_comment"
canada_stocks["scope"] = "national"
canada_stocks["frequency"] = "yearly"

# Save the transformed data to an Excel file
canada_stocks.to_csv('intermediate_data/CDA/canada_new_stocks_9th_model_first_iteration_{}.csv'.format(FILE_DATE_ID),index=False)

#%%
#######################:#######################
#and for jap:
Japan_stocks2017 = pd.read_excel("./input_data/Japan/9th_model_first_iteration.xlsx", sheet_name="2017")

Japan_stocks2018 = pd.read_excel("./input_data/Japan/9th_model_first_iteration.xlsx", sheet_name="2018")

Japan_stocks2019 = pd.read_excel("./input_data/Japan/9th_model_first_iteration.xlsx", sheet_name="2019")

Japan_stocks2020 = pd.read_excel("./input_data/Japan/9th_model_first_iteration.xlsx", sheet_name="2020")

Japan_stocks2021 = pd.read_excel("./input_data/Japan/9th_model_first_iteration.xlsx", sheet_name="2021")

#from all, remove the cols after I and below row 8
Japan_stocks2017 = Japan_stocks2017.iloc[:4,:4]
Japan_stocks2018 = Japan_stocks2018.iloc[:4,:4]
Japan_stocks2019 = Japan_stocks2019.iloc[:4,:4]
Japan_stocks2020 = Japan_stocks2020.iloc[:4,:4]
Japan_stocks2021 = Japan_stocks2021.iloc[:4,:4]
#now melt all
Japan_stocks2017_melted = Japan_stocks2017.melt(id_vars=Japan_stocks2017.columns[0], var_name='Vehicle Type', value_name='Value')
Japan_stocks2018_melted = Japan_stocks2018.melt(id_vars=Japan_stocks2018.columns[0], var_name='Vehicle Type', value_name='Value')
Japan_stocks2019_melted = Japan_stocks2019.melt(id_vars=Japan_stocks2019.columns[0], var_name='Vehicle Type', value_name='Value')
Japan_stocks2020_melted = Japan_stocks2020.melt(id_vars=Japan_stocks2020.columns[0], var_name='Vehicle Type', value_name='Value')
Japan_stocks2021_melted = Japan_stocks2021.melt(id_vars=Japan_stocks2021.columns[0], var_name='Vehicle Type', value_name='Value')

#now add the date column to each
Japan_stocks2017_melted["date"] = 2017
Japan_stocks2018_melted["date"] = 2018
Japan_stocks2019_melted["date"] = 2019
Japan_stocks2020_melted["date"] = 2020
Japan_stocks2021_melted['date'] = 2021

#concat them all
Japan_stocks = pd.concat([Japan_stocks2017_melted, Japan_stocks2018_melted, Japan_stocks2019_melted, Japan_stocks2020_melted, Japan_stocks2021_melted])
# Rename the columns as per user's request
Japan_stocks.rename(columns={"Unnamed: 0": "Vehicle Type", "Vehicle Type": "Drive"}, inplace=True)
#map transport type: based on vehicle type:
transport_type_map = {'lpv':'passenger', 'ht':'freight', 'lcv':'freight', 'bus':'passenger', '2w':'passenger','car':'passenger', 'suv':'passenger', 'lt':'passenger'}
Japan_stocks['transport_type'] = Japan_stocks['Vehicle Type'].map(transport_type_map)

# Set all columns to the specified values
Japan_stocks["economy"] = "08_JPN"
Japan_stocks["medium"] = "road"
Japan_stocks["measure"] = "stocks"
Japan_stocks["dataset"] = "9th_model_first_iteration"
Japan_stocks["source"] = ""
Japan_stocks["unit"] = "stocks"
Japan_stocks["fuel"] = "all"
Japan_stocks["comment"] = "no_comment"
Japan_stocks["scope"] = "national"
Japan_stocks["frequency"] = "yearly"

# Save the transformed data to an Excel file
Japan_stocks.to_csv('intermediate_data/JPN/japan_new_stocks_9th_model_first_iteration_{}.csv'.format(FILE_DATE_ID),index=False)
#%%