#incorporate new ev and phev stock shares for ldv (passenger), bus (passenger), hdv and ht (freight) from iea #BUT canot incorporate hdv because we dont know if it is for passenger or freight

#SORRY THIS IS A BIT LONG BUT ITS QUITE EASY TO READ. EVERY SECTIONN IS BROKEN APOART BY A LINE OF HASHES AND THE SECTIONS ARE BASICALLY THE SAME WITH VLAUES SUBSTITUTED IN FOR DIFFERENT VEHICLE TYPES, AND SOME SMALL CHANGES TO THE ORIGIN OF THE DATA


#%%

#set working directory as one folder back
import os
import re
import pandas as pd
import numpy as np
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

#%%


#####################
#%%

import data_estimation_functions as data_estimation_functions
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data = pd.read_csv('./intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))


levels = ['Drive']#this is like the hierarchy of the data.
non_level_index_cols = ['Measure', 'Economy','Date', 'Scope','Frequency', 'Unit', 'Dataset', 'Transport Type', 'Medium', 'Vehicle Type']

#early on we will drop the Fuel type and comments col
combined_data = combined_data.drop(columns=['Fuel_Type','Comments'])

#1 #incorporate new ev and phev stock shares for ldv (passenger), bus (passenger), hdv and ht (freight) from iea #BUT canot incorporate hdv because we dont know if it is for passenger or freight

#to do this we will grab the Stock share of 'bev and phev', and grab the indivdual stock amounts for phev and bev. Then find the ratio of phev to bev. Times that by the stock share to calculate stock share for phev and bev individually. Then we can swap this in using the insert_new_proportions() function.
#########################################################################
#########################################################################
#########################################################################
#LDVs
#########################################################################
#########################################################################
#########################################################################
#get ato data
# iea_passenger_ldv = combined_data[combined_data['Measure'] == 'Stocks']
#grab ATO $ Country Official Statistics data
iea_passenger_ldv = combined_data[combined_data['Dataset'].str.contains('IEA EVs')]
iea_passenger_ldv = iea_passenger_ldv[iea_passenger_ldv['Transport Type'] == 'passenger']
#grab bus data
iea_passenger_ldv = iea_passenger_ldv[iea_passenger_ldv['Vehicle Type'] == 'ldv']
#grab stock share data
iea_passenger_ldv_stock_share = iea_passenger_ldv[iea_passenger_ldv['Measure'] == 'Stock share']
#convert stock share to a proportion by dividing by 100
iea_passenger_ldv_stock_share['Value'] = iea_passenger_ldv_stock_share['Value']/100
#grab stock data
iea_passenger_ldv_stock = iea_passenger_ldv[iea_passenger_ldv['Measure'] == 'Stocks']
#pivot so we have bev and phev in a column
iea_passenger_ldv_stock_wide = iea_passenger_ldv_stock.pivot(index=non_level_index_cols, columns='Drive', values='Value').reset_index()
#set nas to 0 in bev and phev cols
iea_passenger_ldv_stock_wide['bev'] = iea_passenger_ldv_stock_wide['bev'].fillna(0)
iea_passenger_ldv_stock_wide['phev'] = iea_passenger_ldv_stock_wide['phev'].fillna(0)
#claculate shares of total stocks of phev to bev
iea_passenger_ldv_stock_wide['phev_share'] = iea_passenger_ldv_stock_wide['phev']/(iea_passenger_ldv_stock_wide['bev']+iea_passenger_ldv_stock_wide['phev'])
iea_passenger_ldv_stock_wide['bev_share'] = iea_passenger_ldv_stock_wide['bev']/(iea_passenger_ldv_stock_wide['bev']+iea_passenger_ldv_stock_wide['phev'])
#drop bev and phev cols
iea_passenger_ldv_stock_wide = iea_passenger_ldv_stock_wide.drop(columns=['bev', 'phev'])
#merge with stock share data
merge_cols = ['Economy','Date', 'Scope','Frequency','Dataset', 'Transport Type', 'Medium', 'Vehicle Type']
iea_passenger_ldv_stock_share = iea_passenger_ldv_stock_share.merge(iea_passenger_ldv_stock_wide, on = merge_cols, how='left')

#times Value by shares to get new stock share for phev and bev
iea_passenger_ldv_stock_share['phev'] = iea_passenger_ldv_stock_share['Value']*iea_passenger_ldv_stock_share['phev_share']
iea_passenger_ldv_stock_share['bev'] = iea_passenger_ldv_stock_share['Value']*iea_passenger_ldv_stock_share['bev_share']
#%%
#drop cols
iea_passenger_ldv_stock_share_new = iea_passenger_ldv_stock_share.drop(columns=['phev_share','bev_share', 'Value','Unit_x',  'Unit_y', 'Measure_x', 'Measure_y'])
#melt so we have bev and phev in a column called drive 
iea_passenger_ldv_stock_share_new = iea_passenger_ldv_stock_share_new.melt(id_vars=merge_cols, value_vars=['bev', 'phev'], var_name='Drive', value_name='Drive_proportion')

#%%
#now we have stock shares of ldvs for bev and phev, we can insert these into the ldv data we creatred using the 8th data set
#getthe data from only the reference dataset
eight_edition_transport_model_data = combined_data[combined_data['Dataset']=='8th edition transport model $ Reference']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Measure'] == 'Stocks']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Vehicle Type'] == 'ldv']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Transport Type'] == 'passenger']

#%%
#convert to proportions
levels = ['Drive']#this is like the hierarchy of the data.
non_level_index_cols = ['Transport Type','Medium', 'Vehicle Type', 'Economy','Date', 'Scope','Frequency']
df = eight_edition_transport_model_data.copy()
original_proportions_df = data_estimation_functions.convert_to_proportions(levels,non_level_index_cols, df)

#%%
#now we insert the new proportion from using the functiuon insert_new_proportions(proportions_df, new_proportions_df, levels, non_level_index_cols)
levels= ['Transport Type','Medium', 'Vehicle Type', 'Drive']
non_level_index_cols = ['Economy','Date', 'Scope','Frequency']
#keep only levels and non_level_index_cols and Vehicle Type_proportion in both
iea_passenger_ldv_stock_share_new = iea_passenger_ldv_stock_share_new[non_level_index_cols+levels+['Drive_proportion']]
original_proportions_df2 = original_proportions_df[non_level_index_cols+levels+['Drive_proportion']]
#drop duplicates
original_proportions_df2 = original_proportions_df2.drop_duplicates()
iea_passenger_ldv_stock_share_new = iea_passenger_ldv_stock_share_new.drop_duplicates()

updated_proportions_df = data_estimation_functions.insert_new_proportions(original_proportions_df2, iea_passenger_ldv_stock_share_new, levels, non_level_index_cols)

#%%
#now calcualte the totals for the original ldv vlaues by vehicle type and times those by the new proportions to get the new ldv values by drive type
non_level_index_cols = ['Transport Type','Medium', 'Vehicle Type', 'Economy','Date', 'Scope','Frequency']
ldv_totals = eight_edition_transport_model_data.groupby(non_level_index_cols)['Value'].sum().reset_index()

#join the totals to the updated proportions
updated_proportions_df = updated_proportions_df.merge(ldv_totals, on=non_level_index_cols, how='inner')

#now times the proportions by the new total to get our final value for the lowest level in the hierarchy (drive)
updated_proportions_df['Value'] = updated_proportions_df['Value']*updated_proportions_df['Drive_proportion']
#%%
#DONE! now rename the dataset to 'estimate 8th/ATO' and save the data
updated_proportions_df['Dataset'] = 'LdvEv_est'
updated_proportions_df['Source'] = '8th/IeaEv'

#drop the drive proportion col
updated_proportions_df = updated_proportions_df.drop(columns=['Drive_proportion'])
#%%

#ACTUALLY INSTEAD OF SAVING WE WILL CONCAT ALL DATA AT THE END OF THIS FILE
ldv_updated_proportions_df = updated_proportions_df.copy()
# #save to intermediate data/estimated/{}_8th_ATO_road_freight_tonne_km.csv'.format(FILE_DATE_ID)
# updated_proportions_df.to_csv('./intermediate_data/estimated/{}_ldv_ev_8th_iea_ev.csv'.format(FILE_DATE_ID), index=False)



#########################################################################
#########################################################################
#########################################################################
#%%
analysis = False
if analysis:
       #we want to compare to the previous data to see hjow things have changed
       #we will create a plotting df that has the old and new data in a values cola and a col to indcate if new or old
       #first merge the old data with the new data so we keep only the oldl data where the new data is different
       plotting_df = updated_proportions_df.merge(eight_edition_transport_model_data, on=non_level_index_cols+['Drive'], how='left', suffixes=('_new','_old'))
       #We want to plot how the values have changed as a result of this. We will plot the new values against the old values for each economy where the values changed
       plotting_df['Diff'] = plotting_df['Value_new'] - plotting_df['Value_old']
       # #fitler to only where the diff is not 0
       # new_values_df_copy = new_values_df_copy[new_values_df_copy['Diff'] != 0]
       # #and filter out any really small differences
       # new_values_df_copy = new_values_df_copy[abs(new_values_df_copy['Diff']) > 0.01]

       #We want to plot how the values have changed as a result of this. We will plot the new values against the old values for each economy where the values changed
       #so we will melt so we ahve all vlaues in one column and a new_old column to indicate which is which
       plotting_df_wide = plotting_df.melt(id_vars=['Drive', 'Economy', 'Date'],value_vars=['Value_old','Value_new'], var_name='New_old', value_name='Value')
       #drop dupes
       plotting_df_wide = plotting_df_wide.drop_duplicates()

       #########################################################################
       import plotly
       import plotly.express as px
       pd.options.plotting.backend = "plotly"#set pandas backend to plotly plotting instead of matplotlib
       import plotly.io as pio
       #we will do a plotly line plot with a facet for each economy, different color for each vehicle type and diferent line dash for new vs old
       #first concat the data together, and create a column to indicate which is the old and which is the new

       title = 'New vs Old LDV EV data'#

       #plot

       fig = px.line(plotting_df_wide, x="Date", y="Value", color="Drive", line_dash='New_old', facet_col="Economy", facet_col_wrap=3, title=title)#, #facet_col="Economy",

       plotly.offline.plot(fig, filename='./plotting_output/estimations/new_ev_data/' + title + '.html', auto_open=True)

       #%%





#########################################################################
#########################################################################
#########################################################################
#BUS
#########################################################################
#########################################################################
#########################################################################

levels = ['Drive']#this is like the hierarchy of the data.
non_level_index_cols = ['Measure', 'Economy','Date', 'Scope','Frequency', 'Unit', 'Dataset', 'Transport Type', 'Medium', 'Vehicle Type']
# ATO $ Country Official Statistics data
iea_passenger_bus = combined_data[combined_data['Dataset'].str.contains('IEA EVs')]
iea_passenger_bus = iea_passenger_bus[iea_passenger_bus['Transport Type'] == 'passenger']
#grab bus data
iea_passenger_bus = iea_passenger_bus[iea_passenger_bus['Vehicle Type'] == 'bus']
#grab stock share data
iea_passenger_bus_stock_share = iea_passenger_bus[iea_passenger_bus['Measure'] == 'Stock share']
#convert stock share to a proportion by dividing by 100
iea_passenger_bus_stock_share['Value'] = iea_passenger_bus_stock_share['Value']/100
#grab stock data
iea_passenger_bus_stock = iea_passenger_bus[iea_passenger_bus['Measure'] == 'Stocks']
#pivot so we have bev and phev in a column
iea_passenger_bus_stock_wide = iea_passenger_bus_stock.pivot(index=non_level_index_cols, columns='Drive', values='Value').reset_index()
#set nas to 0 in bev and phev cols
iea_passenger_bus_stock_wide['bev'] = iea_passenger_bus_stock_wide['bev'].fillna(0)
iea_passenger_bus_stock_wide['phev'] = iea_passenger_bus_stock_wide['phev'].fillna(0)
#claculate shares of total stocks of phev to bev
iea_passenger_bus_stock_wide['phev_share'] = iea_passenger_bus_stock_wide['phev']/(iea_passenger_bus_stock_wide['bev']+iea_passenger_bus_stock_wide['phev'])
iea_passenger_bus_stock_wide['bev_share'] = iea_passenger_bus_stock_wide['bev']/(iea_passenger_bus_stock_wide['bev']+iea_passenger_bus_stock_wide['phev'])
#drop bev and phev cols
iea_passenger_bus_stock_wide = iea_passenger_bus_stock_wide.drop(columns=['bev', 'phev'])
#merge with stock share data
merge_cols = ['Economy','Date', 'Scope','Frequency','Dataset', 'Transport Type', 'Medium', 'Vehicle Type']
iea_passenger_bus_stock_share = iea_passenger_bus_stock_share.merge(iea_passenger_bus_stock_wide, on = merge_cols, how='left')

#times Value by shares to get new stock share for phev and bev
iea_passenger_bus_stock_share['phev'] = iea_passenger_bus_stock_share['Value']*iea_passenger_bus_stock_share['phev_share']
iea_passenger_bus_stock_share['bev'] = iea_passenger_bus_stock_share['Value']*iea_passenger_bus_stock_share['bev_share']
#%%
#drop cols
iea_passenger_bus_stock_share_new = iea_passenger_bus_stock_share.drop(columns=['phev_share','bev_share', 'Value','Unit_x',  'Unit_y', 'Measure_x', 'Measure_y'])
#melt so we have bev and phev in a column called drive 
iea_passenger_bus_stock_share_new = iea_passenger_bus_stock_share_new.melt(id_vars=merge_cols, value_vars=['bev', 'phev'], var_name='Drive', value_name='Drive_proportion')

#%%
#now we have stock shares of buss for bev and phev, we can insert these into the bus data in the 8th data set
#getthe data from only the reference dataset
eight_edition_transport_model_data = combined_data[combined_data['Dataset']=='8th edition transport model $ Reference']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Measure'] == 'Stocks']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Vehicle Type'] == 'bus']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Transport Type'] == 'passenger']

#%%
#convert to proportions
levels = ['Drive']#this is like the hierarchy of the data.
non_level_index_cols = ['Transport Type','Medium', 'Vehicle Type', 'Economy','Date', 'Scope','Frequency']
df = eight_edition_transport_model_data.copy()
original_proportions_df = data_estimation_functions.convert_to_proportions(levels,non_level_index_cols, df)

#%%
#now we insert the new proportion from using the functiuon insert_new_proportions(proportions_df, new_proportions_df, levels, non_level_index_cols)
levels= ['Transport Type','Medium', 'Vehicle Type', 'Drive']
non_level_index_cols = ['Economy','Date', 'Scope','Frequency']
#keep only levels and non_level_index_cols and Vehicle Type_proportion in both
iea_passenger_bus_stock_share_new = iea_passenger_bus_stock_share_new[non_level_index_cols+levels+['Drive_proportion']]
original_proportions_df2 = original_proportions_df[non_level_index_cols+levels+['Drive_proportion']]
#drop duplicates
original_proportions_df2 = original_proportions_df2.drop_duplicates()
iea_passenger_bus_stock_share_new = iea_passenger_bus_stock_share_new.drop_duplicates()

updated_proportions_df = data_estimation_functions.insert_new_proportions(original_proportions_df2, iea_passenger_bus_stock_share_new, levels, non_level_index_cols)

#%%
#now calcualte the totals for the original bus vlaues by vehicle type and times those by the new proportions to get the new bus values by drive type
non_level_index_cols = ['Transport Type','Medium', 'Vehicle Type', 'Economy','Date', 'Scope','Frequency']
bus_totals = eight_edition_transport_model_data.groupby(non_level_index_cols)['Value'].sum().reset_index()

#join the totals to the updated proportions
updated_proportions_df = updated_proportions_df.merge(bus_totals, on=non_level_index_cols, how='inner')

#now times the proportions by the new total to get our final value for the lowest level in the hierarchy (drive)
updated_proportions_df['Value'] = updated_proportions_df['Value']*updated_proportions_df['Drive_proportion']
#%%
#DONE! now rename the dataset to 'estimate 8th/ATO' and save the data
updated_proportions_df['Dataset'] = 'BusEv_est'
updated_proportions_df['Source'] = '8th/IeaEv'

#drop the drive proportion
updated_proportions_df = updated_proportions_df.drop(columns=['Drive_proportion'])
#%%

# #save to intermediate data/estimated/{}_8th_ATO_road_freight_tonne_km.csv'.format(FILE_DATE_ID)
# updated_proportions_df.to_csv('./intermediate_data/estimated/{}_bus_ev_8th_iea_ev.csv'.format(FILE_DATE_ID), index=False)

#ACTUALLY INSTEAD OF SAVING WE WILL CONCAT ALL DATA AT THE END OF THIS FILE
bus_updated_proportions_df = updated_proportions_df.copy()

#########################################################################
#########################################################################
#########################################################################
#%%
analysis = False
if analysis:
       #we want to compare to the previous data to see hjow things have changed
       #we will create a plotting df that has the old and new data in a values cola and a col to indcate if new or old
       #first merge the old data with the new data so we keep only the oldl data where the new data is different
       plotting_df = updated_proportions_df.merge(eight_edition_transport_model_data, on=non_level_index_cols+['Drive'], how='left', suffixes=('_new','_old'))
       #We want to plot how the values have changed as a result of this. We will plot the new values against the old values for each economy where the values changed
       plotting_df['Diff'] = plotting_df['Value_new'] - plotting_df['Value_old']
       # #fitler to only where the diff is not 0
       # new_values_df_copy = new_values_df_copy[new_values_df_copy['Diff'] != 0]
       # #and filter out any really small differences
       # new_values_df_copy = new_values_df_copy[abs(new_values_df_copy['Diff']) > 0.01]

       #We want to plot how the values have changed as a result of this. We will plot the new values against the old values for each economy where the values changed
       #so we will melt so we ahve all vlaues in one column and a new_old column to indicate which is which
       plotting_df_wide = plotting_df.melt(id_vars=['Drive', 'Economy', 'Date'],value_vars=['Value_old','Value_new'], var_name='New_old', value_name='Value')
       #drop dupes
       plotting_df_wide = plotting_df_wide.drop_duplicates()

       #########################################################################
       import plotly
       import plotly.express as px
       pd.options.plotting.backend = "plotly"#set pandas backend to plotly plotting instead of matplotlib
       import plotly.io as pio
       #we will do a plotly line plot with a facet for each economy, different color for each vehicle type and diferent line dash for new vs old
       #first concat the data together, and create a column to indicate which is the old and which is the new

       title = 'New vs Old Bus EV data'#

       #plot

       fig = px.line(plotting_df_wide, x="Date", y="Value", color="Drive", line_dash='New_old', facet_col="Economy", facet_col_wrap=3, title=title)#, #facet_col="Economy",

       plotly.offline.plot(fig, filename='./plotting_output/estimations/new_ev_data/' + title + '.html', auto_open=True)

       #%%




#########################################################################
#########################################################################
#########################################################################
#ht (freight)
#########################################################################
#########################################################################
#########################################################################

levels = ['Drive']#this is like the hierarchy of the data.
non_level_index_cols = ['Measure', 'Economy','Date', 'Scope','Frequency', 'Unit', 'Dataset', 'Transport Type', 'Medium', 'Vehicle Type']
# ATO $ Country Official Statistics data
iea_passenger_ht = combined_data[combined_data['Dataset'].str.contains('IEA EVs')]
iea_passenger_ht = iea_passenger_ht[iea_passenger_ht['Transport Type'] == 'freight']
#grab ht data
iea_passenger_ht = iea_passenger_ht[iea_passenger_ht['Vehicle Type'] == 'ht']
#grab stock share data
iea_passenger_ht_stock_share = iea_passenger_ht[iea_passenger_ht['Measure'] == 'Stock share']
#convert stock share to a proportion by dividing by 100
iea_passenger_ht_stock_share['Value'] = iea_passenger_ht_stock_share['Value']/100
#grab stock data
iea_passenger_ht_stock = iea_passenger_ht[iea_passenger_ht['Measure'] == 'Stocks']
#pivot so we have bev and phev in a column
iea_passenger_ht_stock_wide = iea_passenger_ht_stock.pivot(index=non_level_index_cols, columns='Drive', values='Value').reset_index()
#set nas to 0 in bev and phev cols
iea_passenger_ht_stock_wide['bev'] = iea_passenger_ht_stock_wide['bev'].fillna(0)
iea_passenger_ht_stock_wide['phev'] = iea_passenger_ht_stock_wide['phev'].fillna(0)
#claculate shares of total stocks of phev to bev
iea_passenger_ht_stock_wide['phev_share'] = iea_passenger_ht_stock_wide['phev']/(iea_passenger_ht_stock_wide['bev']+iea_passenger_ht_stock_wide['phev'])
iea_passenger_ht_stock_wide['bev_share'] = iea_passenger_ht_stock_wide['bev']/(iea_passenger_ht_stock_wide['bev']+iea_passenger_ht_stock_wide['phev'])
#drop bev and phev cols
iea_passenger_ht_stock_wide = iea_passenger_ht_stock_wide.drop(columns=['bev', 'phev'])
#merge with stock share data
merge_cols = ['Economy','Date', 'Scope','Frequency','Dataset', 'Transport Type', 'Medium', 'Vehicle Type']
iea_passenger_ht_stock_share = iea_passenger_ht_stock_share.merge(iea_passenger_ht_stock_wide, on = merge_cols, how='left')

#times Value by shares to get new stock share for phev and bev
iea_passenger_ht_stock_share['phev'] = iea_passenger_ht_stock_share['Value']*iea_passenger_ht_stock_share['phev_share']
iea_passenger_ht_stock_share['bev'] = iea_passenger_ht_stock_share['Value']*iea_passenger_ht_stock_share['bev_share']
#%%
#drop cols
iea_passenger_ht_stock_share_new = iea_passenger_ht_stock_share.drop(columns=['phev_share','bev_share', 'Value','Unit_x',  'Unit_y', 'Measure_x', 'Measure_y'])
#melt so we have bev and phev in a column called drive 
iea_passenger_ht_stock_share_new = iea_passenger_ht_stock_share_new.melt(id_vars=merge_cols, value_vars=['bev', 'phev'], var_name='Drive', value_name='Drive_proportion')

#%%
#now we have stock shares of hts for bev and phev, we can insert these into the ht data in the 8th data set
#getthe data from only the reference dataset
eight_edition_transport_model_data = combined_data[combined_data['Dataset']=='8th edition transport model $ Reference']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Measure'] == 'Stocks']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Vehicle Type'] == 'ht']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Transport Type'] == 'freight']

#%%
#convert to proportions
levels = ['Drive']#this is like the hierarchy of the data.
non_level_index_cols = ['Transport Type','Medium', 'Vehicle Type', 'Economy','Date', 'Scope','Frequency']
df = eight_edition_transport_model_data.copy()
original_proportions_df = data_estimation_functions.convert_to_proportions(levels,non_level_index_cols, df)

#%%
#now we insert the new proportion from using the functiuon insert_new_proportions(proportions_df, new_proportions_df, levels, non_level_index_cols)
levels= ['Transport Type','Medium', 'Vehicle Type', 'Drive']
non_level_index_cols = ['Economy','Date', 'Scope','Frequency']
#keep only levels and non_level_index_cols and Vehicle Type_proportion in both
iea_passenger_ht_stock_share_new = iea_passenger_ht_stock_share_new[non_level_index_cols+levels+['Drive_proportion']]
original_proportions_df2 = original_proportions_df[non_level_index_cols+levels+['Drive_proportion']]
#drop duplicates
original_proportions_df2 = original_proportions_df2.drop_duplicates()
iea_passenger_ht_stock_share_new = iea_passenger_ht_stock_share_new.drop_duplicates()

updated_proportions_df = data_estimation_functions.insert_new_proportions(original_proportions_df2, iea_passenger_ht_stock_share_new, levels, non_level_index_cols)

#%%
#now calcualte the totals for the original ht vlaues by vehicle type and times those by the new proportions to get the new ht values by drive type
non_level_index_cols = ['Transport Type','Medium', 'Vehicle Type', 'Economy','Date', 'Scope','Frequency']
ht_totals = eight_edition_transport_model_data.groupby(non_level_index_cols)['Value'].sum().reset_index()

#join the totals to the updated proportions
updated_proportions_df = updated_proportions_df.merge(ht_totals, on=non_level_index_cols, how='inner')

#now times the proportions by the new total to get our final value for the lowest level in the hierarchy (drive)
updated_proportions_df['Value'] = updated_proportions_df['Value']*updated_proportions_df['Drive_proportion']
#%%
#DONE! now rename the dataset to 'estimate 8th/ATO' and save the data
updated_proportions_df['Dataset'] = 'HtEv_est'
updated_proportions_df['Source'] = '8th/IeaEv'
#drop the drive proportion
updated_proportions_df = updated_proportions_df.drop(columns=['Drive_proportion'])

#%%

# #save to intermediate data/estimated/{}_8th_ATO_road_freight_tonne_km.csv'.format(FILE_DATE_ID)
# updated_proportions_df.to_csv('./intermediate_data/estimated/{}_ht_ev_8th_iea_ev.csv'.format(FILE_DATE_ID), index=False)


#ACTUALLY INSTEAD OF SAVING WE WILL CONCAT ALL DATA AT THE END OF THIS FILE
ht_updated_proportions_df = updated_proportions_df.copy()

#########################################################################
#########################################################################
#########################################################################
#%%
analysis = False
if analysis:
       #we want to compare to the previous data to see hjow things have changed
       #we will create a plotting df that has the old and new data in a values cola and a col to indcate if new or old
       #first merge the old data with the new data so we keep only the oldl data where the new data is different
       plotting_df = updated_proportions_df.merge(eight_edition_transport_model_data, on=non_level_index_cols+['Drive'], how='left', suffixes=('_new','_old'))
       #We want to plot how the values have changed as a result of this. We will plot the new values against the old values for each economy where the values changed
       plotting_df['Diff'] = plotting_df['Value_new'] - plotting_df['Value_old']
       # #fitler to only where the diff is not 0
       # new_values_df_copy = new_values_df_copy[new_values_df_copy['Diff'] != 0]
       # #and filter out any really small differences
       # new_values_df_copy = new_values_df_copy[abs(new_values_df_copy['Diff']) > 0.01]

       #We want to plot how the values have changed as a result of this. We will plot the new values against the old values for each economy where the values changed
       #so we will melt so we ahve all vlaues in one column and a new_old column to indicate which is which
       plotting_df_wide = plotting_df.melt(id_vars=['Drive', 'Economy', 'Date'],value_vars=['Value_old','Value_new'], var_name='New_old', value_name='Value')
       #drop dupes
       plotting_df_wide = plotting_df_wide.drop_duplicates()

       #########################################################################
       import plotly
       import plotly.express as px
       pd.options.plotting.backend = "plotly"#set pandas backend to plotly plotting instead of matplotlib
       import plotly.io as pio
       #we will do a plotly line plot with a facet for each economy, different color for each vehicle type and diferent line dash for new vs old
       #first concat the data together, and create a column to indicate which is the old and which is the new

       title = 'New vs Old ht EV data'#

       #plot

       fig = px.line(plotting_df_wide, x="Date", y="Value", color="Drive", line_dash='New_old', facet_col="Economy", facet_col_wrap=3, title=title)#, #facet_col="Economy",

       plotly.offline.plot(fig, filename='./plotting_output/estimations/new_ev_data/' + title + '.html', auto_open=True)

       #%%


##########################################################################
#########################################################################
#########################################################################
#Save all:
#%%
#concat all data
all_data = pd.concat([ldv_updated_proportions_df,bus_updated_proportions_df, ht_updated_proportions_df], axis=0)
#%%
#set measure and unit to Stocks
all_data['Measure'] = 'Stocks'
all_data['Unit'] = 'Stocks'

all_data.to_csv('./intermediate_data/estimated/{}_8th_iea_ev_all_stock_updates.csv'.format(FILE_DATE_ID), index=False)

#%%