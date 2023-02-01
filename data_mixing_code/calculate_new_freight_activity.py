

#%%


#BASICALLY THE INTENTION IS TO TAKE IN DATA FROM THE 8TH MODEL AND THEN ANY DATA THAT COULD REPLACE THAT NUT WE DONT WANT TO RISK INSERTING A VALUE THAT ISNT FROM THE SAME SOURCE AND THEREFORE MAY BE STEMMING FROM A DIFFERENT SCALE. IN THIS CASE WE WILL KEEP THE 8TH DATA BUT CHANGE THE PROPORTIONS WITHIN THE 8TH DATA TO REFLECT THE NEW DATA. FOR EXAMPLE IF WE HAVE NEW DATA FOR THE NUMBER AND PROPORTION OF EVS IN PASSENGER LV'S (LIKE WE HAVE FROM IEA GLOBAL DATA EXPLORER) THEN WE CAN USE THAT TO UPDATE THE PROPORTIONS OF EVS IN THE 8TH DATA. THIS WILL MEAN THAT THE TOTAL STOCKS FOR PASSENGER LV'S WILL BE THE SAME AS THE 8TH DATA BUT THE PROPORTIONS OF EVS WILL BE UPDATED TO MORE ACCURATELY REFLECT THE REAL WORLD. UNTIL PUBLICLY AVAILABLE TRANSPORT DATA COVERS ENOUGH DETAIL THEN WE WILL ALWAYS HAVE TO DO THIS KIND OF FIXING.

#todo:
#incorporate new road freight tonne km total for ato economys


#incorporate new bus proportion of raod passenger km total for ato economys where the stats are from the same dataset
##otherwise incorporate total road passenger km toal for ato economys where we cannot be sure about the bus proportion because its from a ddifferent dataset within ATO dataset




#%%

#set working directory as one folder back
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

# #stack ldv da
#####################
#%%
#incorporate new road freight tonne km total for ato economys
#this will invovle adjusting all the data for levels below road freight tonne km so that it adds up to the new total. We will use the output from the 8th edition transport model as the basis for this. 

#get ato data
ato_data = combined_data[combined_data['Measure'] == 'freight_tonne_km']
#check if Dataset contains 'ATO' using regex
ato_data = ato_data[ato_data['Dataset'].str.contains('ATO')]
#grab road medium only where vehicle type and drive are np.nan
ato_data = ato_data[ato_data['Transport Type'] == 'freight']
ato_data = ato_data[ato_data['Vehicle Type'].isnull()]
ato_data = ato_data[ato_data['Drive'].isnull()]
#medium is road
ato_data = ato_data[ato_data['Medium'] == 'road']
#drop vehicle type and drive
ato_data = ato_data.drop(columns=['Vehicle Type','Drive'])

eight_edition_transport_model_data = combined_data[combined_data['Dataset']=='8th edition transport model $ Reference'] #from only the reference dataset
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Measure'] == 'freight_tonne_km']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Medium'] == 'road']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Transport Type'] == 'freight']
#remove where vehicle type or drive are nan as they are totals themselves
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Vehicle Type'].notnull()]
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Drive'].notnull()]

#%%
#so we want to replace the actual values with this but then recaclaute the values below that. To do this we should calcualte proportions for all the levels below road freight tonne km and then multiply those by the new total.
#so input the data from the 8th edition transport model to convert_to_proportions(levels,non_level_index_cols, df)
df = eight_edition_transport_model_data.copy()
levels = ['Vehicle Type','Drive']#this is like the hierarchy of the data.
non_level_index_cols = ['Measure', 'Economy','Date', 'Scope','Frequency', 'Fuel_Type', 'Unit','Comments']

proportions_df = data_estimation_functions.convert_to_proportions(levels,non_level_index_cols, df)
#%%
#now we insert the new total from the ato data and progressively times the proportions by the new total to get the new values:
#join ato data to the proportions
proportions_df = proportions_df.merge(ato_data, on=non_level_index_cols, how='inner')

#now times the proportions by the new total successively to get our final value for the lowest level in the hierarchy (drive)
#so times Value by vehicle type proportion to get the total for each vehicle type
proportions_df['Value'] = proportions_df['Value']*proportions_df['Vehicle Type_proportion']
#now times Value by drive proportion to get the total for each drive
proportions_df['Value'] = proportions_df['Value']*proportions_df['Drive_proportion']
#now drop the proportions
proportions_df = proportions_df.drop(columns=['Vehicle Type_proportion','Drive_proportion'])

#%%
#check thatr the columns are same in proportions_df and combined_data
diff_cols = [x for x in combined_data.columns if x not in proportions_df.columns]
diff_cols2 = [x for x in proportions_df.columns if x not in combined_data.columns]
if diff_cols != [] or diff_cols2 != []:
       print('the following columns are in combined_data but not in proportions_df: {}'.format(diff_cols))
       print('the following columns are in proportions_df but not in combined_data: {}'.format(diff_cols2))

#%%
#DONE! now rename the dataset to 'estimate 8th/ATO' and save the data
proportions_df['Dataset'] = 'FreightKm_est'
proportions_df['Source'] = '8th/ATOCountryOfficialStatistics'


#%%

#save to intermediate data/estimated/{}_8th_ATO_road_freight_tonne_km.csv'.format(FILE_DATE_ID)
proportions_df.to_csv('./intermediate_data/estimated/{}_8th_ATO_road_freight_tonne_km.csv'.format(FILE_DATE_ID), index=False)

#%%

#########################################################################
#########################################################################
#########################################################################
#%%
analysis = False
if analysis:
       #we want to compare to the previous data to see hjow things have changed
       #we will create a plotting df that has the old and new data in a values cola and a col to indcate if new or old
       #first merge the old data with the new data so we keep only the oldl data where the new data is different
       plotting_df = proportions_df.merge(eight_edition_transport_model_data, on=non_level_index_cols+['Transport Type', 'Medium','Vehicle Type','Drive'], how='left', suffixes=('_new','_old'))
       #wed end up with a lot of lines if we plotted change for every level. instead lets just show the change for every vehicle type. So group by and sum to get the total for each vehicle type
       plotting_df = plotting_df.groupby(['Measure',
       'Economy',
       'Date',
       'Scope',
       'Frequency']+['Transport Type', 'Medium','Vehicle Type']).sum().reset_index()
       #We want to plot how the values have changed as a result of this. We will plot the new values against the old values for each economy where the values changed
       plotting_df['Diff'] = plotting_df['Value_new'] - plotting_df['Value_old']
       # #fitler to only where the diff is not 0
       # new_values_df_copy = new_values_df_copy[new_values_df_copy['Diff'] != 0]
       # #and filter out any really small differences
       # new_values_df_copy = new_values_df_copy[abs(new_values_df_copy['Diff']) > 0.01]

       #We want to plot how the values have changed as a result of this. We will plot the new values against the old values for each economy where the values changed
       #so we will melt so we ahve all vlaues in one column and a new_old column to indicate which is which
       plotting_df_wide = plotting_df.melt(id_vars=['Vehicle Type', 'Economy', 'Date'],value_vars=['Value_old','Value_new'], var_name='New_old', value_name='Value')
       #drop dupes
       plotting_df_wide = plotting_df_wide.drop_duplicates()

       #########################################################################
       import plotly
       import plotly.express as px
       pd.options.plotting.backend = "plotly"#set pandas backend to plotly plotting instead of matplotlib
       import plotly.io as pio
       #we will do a plotly line plot with a facet for each economy, different color for each vehicle type and diferent line dash for new vs old
       #first concat the data together, and create a column to indicate which is the old and which is the new

       title = 'Proportion of bus passenger km to total road passenger km for each economy in the ATO Country official statistics dataset'

       #plot

       fig = px.line(plotting_df_wide, x="Date", y="Value", color="Vehicle Type", line_dash='New_old', facet_col="Economy", facet_col_wrap=3, title=title)#, #facet_col="Economy",

       plotly.offline.plot(fig, filename='./plotting_output/estimations/new_freightkm_data/' + title + '.html', auto_open=True)

       #%%