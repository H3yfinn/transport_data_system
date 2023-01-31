
#set working directory as one folder back
import os
import re
import pandas as pd
import numpy as np
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')
#%%
#read in the data we are missing
missing = pd.read_csv('./intermediate_data/9th_dataset/missing_rows.csv')
#%%
missing.head()
#%%
missing.Measure.unique()
# %%
import pandas as pd
import plotly.express as px

columns_to_plot =['Measure','Transport Type', 'Medium', 'Vehicle Type','Drive']#, 'Economy']
#dsrop turnover_rate form measure
missing = missing[missing.Measure != 'Turnover_rate']
#%%
#create a count of the number of isntances for every group, going forward in columns to plot. eg if we are plotting transport type, medium, vehicle type, drive, we will add a count of the number of instances of each transport type then each transport type and medium, then each transport type, medium and vehicle type, then each transport type, medium, vehicle type and drive
#first make any NAs into 'nan' so that we can count them
missing = missing.fillna('nan')
for i in range(3,len(columns_to_plot)+1):
    #make a new column with the name of the column we are counting
    new_col_name = 'Count of ' + columns_to_plot[i-1]
    #make a new column with the count of the number of instances of each group
    #missing.groupby(columns_to_plot[:i])[columns_to_plot[i-1]].count()
    missing[new_col_name] = missing.groupby(columns_to_plot[:i])[columns_to_plot[i-1]].transform('count')
    #attach this to drive
    missing[columns_to_plot[i-1]] = missing[columns_to_plot[i-1]] + ' ' + missing[new_col_name].astype(str)
#to make the treemap easier to read we will try inserting the name of the column at the beginning of the name of the value (unless its 'nan')
for col in columns_to_plot:
    missing[col] = missing[col].apply(lambda x: col + ': ' + str(x) if str(x) != 'nan' else str(x))
#%%

#and make one that can fit on my home screen which will be 1.3 times taller and 3 times wider
fig = px.treemap(missing, path=columns_to_plot)
#make it bigger
fig.update_layout(width=2500, height=1300)
#make title
# fig.update_layout(title_text=measure)
#show it in browser rather than in the notebook
fig.write_html("plotting_output/estimations/data_coverage_trees/missing_data_tree.html", auto_open=True)
#%%


###################
#%%
import os
import re
import pandas as pd
import numpy as np
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

import data_estimation_functions as data_estimation_functions
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data = pd.read_csv('./intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))
#%%
#we know we can ignore any stocks of rail ship and air so set them all to 0
