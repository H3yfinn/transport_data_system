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
df_melted["source"] = "thanan"
df_melted["unit"] = "stocks"
df_melted["fuel"] = "all"
df_melted["comment"] = "no_comment"
df_melted["scope"] = "national"
df_melted["frequency"] = "yearly"

# Save the transformed data to an Excel file
df_melted.to_csv('intermediate_data/THA/thailand_new_stocks_9th_model_first_iteration_{}.csv'.format(FILE_DATE_ID),index=False)
#%%

#######################:#######################