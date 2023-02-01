
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


#####################
#%%
levels = ['Vehicle Type','Drive']#this is like the hierarchy of the data.
non_level_index_cols = ['Measure', 'Economy','Date', 'Scope','Frequency', 'Unit', 'Dataset', 'Transport Type', 'Medium']

#early on we will drop the Fuel type and comments col
combined_data = combined_data.drop(columns=['Fuel_Type','Comments'])

#1 incorporate new bus proportion of raod passenger km total for ato economys where the stats are from the same dataset

##2 otherwise incorporate total road passenger km toal for ato economys where we cannot be sure about the bus proportion because its from a ddifferent dataset within ATO dataset

#1
#need to calculate the proportion of bus passenger km to total road passenger km for each economy in the ATO 'Country official statistics' dataset
#then calcualte the proportions for all passenger road data in our 8th edition dataset
#then swap in the new proportion of bus passenger km to total road passenger km using the function insert_new_proportions(proportions_df, new_proportions_df, levels, non_level_index_cols)

##2.
#this will invovle adjusting all the data for levels below total road passenger km so that it adds up to the new total. We will use the output from the 8th edition transport model as the basis for this. 

#get ato data
ato_data = combined_data[combined_data['Measure'] == 'passenger_km']
#grab ATO $ Country Official Statistics data
ato_data = ato_data[ato_data['Dataset'] == 'ATO $ Country Official Statistics']
ato_data = ato_data[ato_data['Transport Type'] == 'passenger']
#grab bus data
ato_bus_data = ato_data[ato_data['Vehicle Type'] == 'bus']
#grab total road passenger km data
ato_road_passenger_km_data = ato_data[ato_data['Vehicle Type'].isnull()]
#medium is road
ato_road_passenger_km_data = ato_road_passenger_km_data[ato_road_passenger_km_data['Medium'] == 'road']
#drop vehicle type and drive
ato_road_passenger_km_data = ato_road_passenger_km_data.drop(columns=['Vehicle Type','Drive'])

#merge on the bus data to the total road passenger km data using suffixes
bus_road = ato_bus_data.merge(ato_road_passenger_km_data, on=non_level_index_cols, how='inner', suffixes=('_bus','_road'))

#calcualte the proportion of bus passenger km to total road passenger km for each economy in the ATO 'Country official statistics' dataset
bus_road['Vehicle Type_proportion'] = bus_road['Value_bus']/bus_road['Value_road']
bus_road = bus_road.drop(columns=['Value_bus','Value_road', 'Drive'])

#%%
#get 8th edition transport model data from only the reference dataset
eight_edition_transport_model_data = combined_data[combined_data['Dataset']=='8th edition transport model $ Reference']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Measure'] == 'passenger_km']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Medium'] == 'road']
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Transport Type'] == 'passenger']
#remove where vehicle type or drive are nan as they are totals themselves
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Vehicle Type'].notnull()]
eight_edition_transport_model_data = eight_edition_transport_model_data[eight_edition_transport_model_data['Drive'].notnull()]

#%%
#so we want to replace the actual values with this but then recaclaute the values below that. To do this we should calcualte proportions for all the levels below road km and then multiply those by the new total.
#so input the data from the 8th edition transport model to convert_to_proportions(levels,non_level_index_cols, df)
df = eight_edition_transport_model_data.copy()
original_proportions_df = data_estimation_functions.convert_to_proportions(levels,non_level_index_cols, df)

#%%
#now we insert the new proportion from the ato data using the functiuon insert_new_proportions(proportions_df, new_proportions_df, levels, non_level_index_cols)
levels= ['Transport Type','Medium', 'Vehicle Type']
non_level_index_cols = ['Measure', 'Economy','Date', 'Scope','Frequency', 'Unit']
#keep only levels and non_level_index_cols and Vehicle Type_proportion in both
bus_road = bus_road[non_level_index_cols+levels+['Vehicle Type_proportion']]
original_proportions_df2 = original_proportions_df[non_level_index_cols+levels+['Vehicle Type_proportion']]
#drop duplicates
original_proportions_df2 = original_proportions_df2.drop_duplicates()
bus_road = bus_road.drop_duplicates()

updated_proportions_df = data_estimation_functions.insert_new_proportions(original_proportions_df2, bus_road, levels, non_level_index_cols)

#%%
#replace the values in the 8th edition transport model data with the new proportions
new_proportions_df = original_proportions_df.merge(updated_proportions_df, on=non_level_index_cols+levels, how='left', suffixes=('','_new'))
#%%
#replace any values where the new proportion is not null
new_proportions_df['Vehicle Type_proportion'] = new_proportions_df['Vehicle Type_proportion_new'].where(new_proportions_df['Vehicle Type_proportion_new'].notnull(), new_proportions_df['Vehicle Type_proportion'])
#now we can drop the new proportion column
new_proportions_df = new_proportions_df.drop(columns=['Vehicle Type_proportion_new'])

#%%
#NOW WE CAN RECALCULATE THE VALUES BELOW ROAD KM
#we need to do this for the 8th edition transport model data 
#first sum up the values to the road level
eight_edition_transport_model_data_total = eight_edition_transport_model_data.groupby(non_level_index_cols+['Transport Type','Medium']).sum().reset_index()
#%%
#join onto the new proportions df
new_values_df = new_proportions_df.merge(eight_edition_transport_model_data_total, on=non_level_index_cols+['Transport Type','Medium'], how='left')
#%%
#time the Value by the new proportion
new_values_df['New_value'] = new_values_df['Value']*new_values_df['Vehicle Type_proportion']
#and then by the next level downs proportion (drive)
new_values_df['New_value'] = new_values_df['New_value']*new_values_df['Drive_proportion']

#drop value
new_values_df = new_values_df.drop(columns=['Value'])

#rename New_value to Value 
new_values_df = new_values_df.rename(columns={'New_value':'Value'})

#drop the Vehicle Type_proportion and Drive_proportion columns
new_values_df = new_values_df.drop(columns=['Vehicle Type_proportion','Drive_proportion'])
#%%
#rename Datset to 'estimated 8th/ATOCountryOfficialStatistics'
new_values_df['Dataset'] = 'BusPassengerKm_est'
new_values_df['Source'] = '8th/ATOCountryOfficialStatistics'

#%%
#We really only want to keep where the changes were not negligible so we will drop any where the change is less than 0.01
new_values_df_copy = new_values_df.copy()
#merge with the original 8th edition transport model data so we can see the old values
new_values_df_copy = new_values_df_copy.merge(eight_edition_transport_model_data, on=non_level_index_cols+['Transport Type','Medium', 'Vehicle Type', 'Drive'], how='left', suffixes=('_new','_old'))
#%%
# #we want to see the changes to the vehicle type sums so we will group by and sum for all but Drive
# new_values_df_copy_plot = new_values_df_copy.groupby(non_level_index_cols+['Transport Type','Medium', 'Vehicle Type']).sum().reset_index()
#Calculate the difference froim which we can filter out minor differences
new_values_df_copy['Diff'] = new_values_df_copy['Value_new'] - new_values_df_copy['Value_old']
#%%
#fitler to only where the abs diff is not near 0
new_values_df_copy = new_values_df_copy[abs(new_values_df_copy['Diff']) > 1]
#%%
#now remove cols and rename before save
new_values_df_copy = new_values_df_copy.rename(columns={'Value_new':'Value', 'Dataset_new':'Dataset'})
new_values_df_copy = new_values_df_copy.drop(columns=['Value_old','Diff', 'Dataset_old'])

#%%
#save to csv
new_values_df_copy.to_csv('./intermediate_data/estimated/{}_8th_ATO_bus_update.csv'.format(FILE_DATE_ID), index=False)




#%%
################################################################################################################################################################

#%%
analysis = False
if analysis:
       #copy dataset so we can visualise the changes 
       new_values_df_copy = new_values_df.copy()
       #merge with the original 8th edition transport model data so we can see the old values
       new_values_df_copy = new_values_df_copy.merge(eight_edition_transport_model_data, on=non_level_index_cols+['Transport Type','Medium', 'Vehicle Type', 'Drive'], how='left', suffixes=('_new','_old'))
       #we want to see the changes to the vehicle type sums so we will group by and sum for all but Drive
       new_values_df_copy = new_values_df_copy.groupby(non_level_index_cols+['Transport Type','Medium', 'Vehicle Type']).sum().reset_index()
       #We want to plot how the values have changed as a result of this. We will plot the new values against the old values for each economy where the values changed
       new_values_df_copy['Diff'] = new_values_df_copy['Value_new'] - new_values_df_copy['Value_old']
       #fitler to only where the diff is not 0
       new_values_df_copy = new_values_df_copy[new_values_df_copy['Diff'] != 0]
       #and filter out any really small differences
       new_values_df_copy = new_values_df_copy[abs(new_values_df_copy['Diff']) > 0.01]

       #We want to plot how the values have changed as a result of this. We will plot the new values against the old values for each economy where the values changed
       #so we will melt so we ahve all vlaues in one column and a new_old column to indicate which is which
       new_values_df_copy_wide = new_values_df_copy.melt(id_vars=['Vehicle Type', 'Economy', 'Date'],value_vars=['Value_old','Value_new'], var_name='New_old', value_name='Value')
       #drop dupes
       new_values_df_copy_wide = new_values_df_copy_wide.drop_duplicates()

       import plotly
       import plotly.express as px
       pd.options.plotting.backend = "plotly"#set pandas backend to plotly plotting instead of matplotlib
       import plotly.io as pio
       #we will do a plotly line plot with a facet for each economy, different color for each vehicle type and diferent line dash for new vs old
       #first concat the data together, and create a column to indicate which is the old and which is the new

       title = 'Proportion of bus passenger km to total road passenger km for each economy in the ATO Country official statistics dataset'

       #plot
       # fig = px.line(combined_df, x="Year", y="Energy", color="TransportType_VehicleType_Drive_Scenario", line_dash='Dataset', facet_col="Economy", facet_col_wrap=7, title=title)#, #facet_col="Economy",
       #              #category_orders={"Scenario": ["Reference", "Carbon Neutral"]})
       # fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))#remove 'Economy=X' from titles
       fig = px.line(new_values_df_copy_wide, x="Date", y="Value", color="Vehicle Type", line_dash='New_old', facet_col="Economy", facet_col_wrap=3, title=title)#, #facet_col="Economy",

       plotly.offline.plot(fig, filename='./plotting_output/estimations/new_bus_data/' + title + '.html', auto_open=True)



#%%
################################################################################
################################################################################


#we also want to do 2.
##2 otherwise incorporate total road passenger km toal for ato economys where we cannot be sure about the bus proportion because its from a ddifferent dataset within ATO dataset
##2.
#this will invovle adjusting all the data for levels below total road passenger km so that it adds up to the new total. We will use the output from the 8th edition transport model as the basis for this. 

#the problem is that we have a few different Datasets of new values. I guess it would be best to first identify any that have the same values, remove them. Then we can loop through the remaining ones and calculate new values for each one in turn. We can save their estimates to one csv but with different dataset names.

#get ato data
ato_data = combined_data[combined_data['Measure'] == 'passenger_km']
ato_data = ato_data[ato_data['Medium'] == 'road']
ato_data = ato_data[ato_data['Dataset'].str.contains('ATO')]
ato_data = ato_data[ato_data['Vehicle Type'].isnull()]
ato_data = ato_data[ato_data['Drive'].isnull()]
#drop vehicle type and drive cols
ato_data = ato_data.drop(columns=['Vehicle Type','Drive'])
#drop dupes
ato_data = ato_data.drop_duplicates()
ato_data['id']= 'ATO'
#we want to plot the different datasets using plotly to see how they compare but we should include the 8th edition transport model data as well. So we will concat the 8th edition transport model data to the ato data
eighth_data = combined_data[combined_data['Measure'] == 'passenger_km']
eighth_data = eighth_data[eighth_data['Medium'] == 'road']
eighth_data = eighth_data[eighth_data['Dataset'].str.contains('8th')]
#create col which states it is 8th_model
eighth_data['id'] = '8th_model'
#now sum up so we remove the Vehicle Type and Drive cols
cols = ato_data.columns.to_list()
cols.remove('Value')
eighth_data = eighth_data.groupby(cols).sum().reset_index()
# #drop vehicle type and drive cols
# eighth_data = eighth_data.drop(columns=['Vehicle Type','Drive'])
#drop dupes
eighth_data = eighth_data.drop_duplicates()

#concat
plotting_data = pd.concat([ato_data, eighth_data], ignore_index=True)
#%%
################################################################################################################################################################
analysis = False
if analysis:

       import plotly
       import plotly.express as px
       pd.options.plotting.backend = "plotly"#set pandas backend to plotly plotting instead of matplotlib
       import plotly.io as pio
       #we will do a plotly line plot with a facet for each economy, different color for each vehicle type and diferent line dash for new vs old
       #first concat the data together, and create a column to indicate which is the old and which is the new

       title = 'New ATO road passenger km data'

       #plot
       # fig = px.line(combined_df, x="Year", y="Energy", color="TransportType_VehicleType_Drive_Scenario", line_dash='Dataset', facet_col="Economy", facet_col_wrap=7, title=title)#, #facet_col="Economy",
       #              #category_orders={"Scenario": ["Reference", "Carbon Neutral"]})
       # fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))#remove 'Economy=X' from titles
       fig = px.line(plotting_data, x="Date", y="Value", color="Dataset",line_dash='id',facet_col="Economy", facet_col_wrap=7, title=title)#, #facet_col="Economy",

       plotly.offline.plot(fig, filename='./plotting_output/estimations/new_passenger_data/' + title + '.html', auto_open=False)



#%%
################################################################################
################################################################################
non_level_index_cols = ['Measure', 'Medium', 'Unit', 'Economy', 'Transport Type','Scope', 'Frequency', 'Date']

#split into unique datasets in ato_data
ato_data_unique = ato_data['Dataset'].unique()
data_dict = {}
for ato_data_unique_i in ato_data_unique:
       data_dict[ato_data_unique_i] = ato_data[ato_data['Dataset'] == ato_data_unique_i]

#merge them all together on the cols:'Measure', 'Medium', 'Unit', 'Economy', 'Transport Type','Scope', 'Frequency', 'Date'
dict_keys = list(data_dict.keys())
df = data_dict[dict_keys[0]]
for i in range(1,len(dict_keys)):
       df = df.merge(data_dict[dict_keys[i]], on=non_level_index_cols, how='outer', suffixes=('_'+str(i), '_'+str(i+1)))

#for every permutation of value col names, calculate the difference between the two cols and if there are any differences < 1%, make one of the values NA
#we will do this by looping through the cols and then looping through the rows
import itertools
value_col_permutations = list(itertools.combinations(df.filter(regex='Value').columns, 2))
data_sets_not_to_remove = ['ATO $ Country Official Statistics']#if the difference between two datasets is < 1% and one of the datasets is in this list, we will not remove the value for that dataset
index_rows_that_changed = []
for value_col_permutations_i in value_col_permutations:
       for index, row in df.iterrows():
              if (abs(row[value_col_permutations_i[0]] - row[value_col_permutations_i[1]]))/row[value_col_permutations_i[0]] < 0.01:
                     index_rows_that_changed.append(row)
                     df.loc[index,value_col_permutations_i[1]] = np.nan
                     print('changed, {} to nan. index row added to list'.format(value_col_permutations_i[1]))


#%%
# Now we have removed the same values from the different datasets, we will now incorporate the new data into the 8th edition transport model data proportions
eighth_data_not_summed = combined_data[combined_data['Measure'] == 'passenger_km']
eighth_data_not_summed = eighth_data_not_summed[eighth_data_not_summed['Medium'] == 'road']
eighth_data_not_summed = eighth_data_not_summed[eighth_data_not_summed['Dataset']=='8th edition transport model $ Reference']
#remove where vehicle type and drive are nan
eighth_data_not_summed = eighth_data_not_summed.dropna(subset=['Vehicle Type','Drive'])

levels = ['Vehicle Type','Drive']#this is like the hierarchy of the data.

proportions_df = data_estimation_functions.convert_to_proportions(levels,non_level_index_cols, eighth_data_not_summed)

#%%
if 'concatenated_changes' in locals():
       del concatenated_changes
#go through each dataset and add the new data to the proportions
value_cols = df.filter(regex='Value').columns
dataset_cols = df.filter(regex='Dataset').columns
#set index to all but the value and dataset cols
df2 = df.set_index(non_level_index_cols)
for i in range(0,len(value_cols)):
       dataset_col = dataset_cols[i]
       value_col = value_cols[i]
       df3 = df2[[dataset_col,value_col]].reset_index()
       df3 = df3.rename(columns={dataset_col:'Dataset',value_col:'Value'})
       df3 = df3.dropna(subset=['Value'])

       #now we insert the new total from the ato data and progressively times the proportions by the new total to get the new values:
       #join ato data to the proportions
       proportions_df_new = proportions_df.merge(df3, on=non_level_index_cols, how='inner')
       if len(proportions_df_new) == 0:
              continue
       #now times the proportions by the new total successively to get our final value for the lowest level in the hierarchy (drive)
       #so times Value by vehicle type proportion to get the total for each vehicle type
       proportions_df_new['Value'] = proportions_df_new['Value']*proportions_df_new['Vehicle Type_proportion']
       #now times Value by drive proportion to get the total for each drive
       proportions_df_new['Value'] = proportions_df_new['Value']*proportions_df_new['Drive_proportion']
       #now drop the proportions
       proportions_df_new = proportions_df_new.drop(columns=['Vehicle Type_proportion','Drive_proportion'])

       proportions_df_new['Dataset'] = 'PassengerKm_est'
       proportions_df_new['Source'] = '8th/{}'.format(proportions_df_new['Dataset'].iloc[0])

       #if concatenated_changes is not defined, define it
       if 'concatenated_changes' not in locals():
              concatenated_changes = proportions_df_new.copy()
       else:
              concatenated_changes = pd.concat([concatenated_changes,proportions_df_new], ignore_index=True)

concatenated_changes.to_csv('./intermediate_data/estimated/{}_8th_ATO_passenger_road_updates.csv'.format(FILE_DATE_ID), index=False)


#%%
################################################################################################################################################################
#TAKE A LOOK
#%%
analysis = False
if analysis:
       #copy dataset so we can visualise the changes 
       new_values_df_copy = concatenated_changes.copy()
       #merge with the original 8th edition transport model data so we can see the old values
       new_values_df_copy = new_values_df_copy.merge(eighth_data_not_summed, on=non_level_index_cols+['Vehicle Type', 'Drive'], how='left', suffixes=('_new','_old'))
       #We want to plot how the values have changed as a result of this. We will plot the new values against the old values for each economy where the values changed
       new_values_df_copy['Diff'] = new_values_df_copy['Value_new'] - new_values_df_copy['Value_old']
       #fitler to only where the diff is not 0, anmd print the number of rows that are not 0
       print(len(new_values_df_copy[abs(new_values_df_copy['Diff']) > 1]), 'rows have changed, {} rows have not changed'.format(len(new_values_df_copy[abs(new_values_df_copy['Diff']) <1])))
       new_values_df_copy = new_values_df_copy[abs(new_values_df_copy['Diff']) >1]
              
       #now separate the old and new values into two dataframes and keep the cols we need
       new_values_df_copy_old = new_values_df_copy.copy()
       new_values_df_copy_old = new_values_df_copy_old.drop(columns=['Value_new','Dataset_new','Diff'])
       new_values_df_copy_old = new_values_df_copy_old.rename(columns={'Value_old':'Value','Dataset_old':'Dataset'})
       new_values_df_copy_new = new_values_df_copy.copy()
       new_values_df_copy_new = new_values_df_copy_new.drop(columns=['Value_old','Dataset_old','Diff'])
       new_values_df_copy_new = new_values_df_copy_new.rename(columns={'Value_new':'Value','Dataset_new':'Dataset'})

       #concatenate the old and new values
       new_values_df_concat = pd.concat([new_values_df_copy_old, new_values_df_copy_new], ignore_index=True)
       
       #its too hard to see when we have drives. So we will sum the values by vehicle type and economy
       new_values_df_concat = new_values_df_concat.groupby(['Economy','Vehicle Type','Dataset','Date']).sum().reset_index()

       ###############################################################
       ###############################################################

       #import matplotlib
       import matplotlib.pyplot as plt
       import matplotlib
       #plot
       #assign a specific cololr to each vehicle type
       unique_colors = new_values_df_concat['Vehicle Type'].unique()
       #set the colors to use using a color map
       colors = plt.get_cmap('tab10')
       #set the number of colors to use
       num_colors = len(unique_colors)
       #set the colors to use, making sure that the colors are as different as possible
       colors_to_use = [colors(i) for i in np.linspace(0, 1, num_colors)]
       #assign each color to a unique id
       color_dict = dict(zip(unique_colors, colors_to_use))

       #also create a dictionary to assign a different linestyle to each dataset
       dataset_dict = new_values_df_concat['Dataset'].unique()
       linestyles = ['-', '--', '-.', ':']
       dataset_dict = dict(zip(dataset_dict, linestyles))

       marker_dict = new_values_df_concat['Dataset'].unique()
       markers = ['o', '1', 'x', 'p']
       marker_dict = dict(zip(marker_dict, markers))
       
       # new_values_df_concat['Vehicle Type'] = new_values_df_concat['Vehicle Type'] + ' ' + new_values_df_concat['Dataset']
       for economy in new_values_df_concat['Economy'].unique():
              economy_data = new_values_df_concat.loc[new_values_df_concat['Economy'] == economy]
              
              #use plt to plot the data
              fig, ax = plt.subplots()
              legend_dict = {}
              for vehicle_type in economy_data['Vehicle Type'].unique():
                     vehicle_type_data = economy_data.loc[economy_data['Vehicle Type'] == vehicle_type]
                     for dataset in vehicle_type_data['Dataset'].unique():
                            dataset_data = vehicle_type_data.loc[vehicle_type_data['Dataset'] == dataset]
                            #set the color
                            color = color_dict[vehicle_type]
                            #set the linestyle
                            linestyle = dataset_dict[dataset] 
                            marker = marker_dict[dataset] 
                            #plot the data
                            ax.plot(dataset_data['Date'], dataset_data['Value'], color=color, linestyle=linestyle, label=vehicle_type, marker=marker, markersize=15)
                            #record color and linestyle for the legend
                            legend_dict[vehicle_type+' '+dataset] = [color, linestyle, marker]
              #create legend for the color and linestyleand marker
              legend_elements = [matplotlib.lines.Line2D([0], [0], color=legend_dict[key][0], linestyle=legend_dict[key][1], marker=legend_dict[key][2], label=key) for key in legend_dict.keys()]
              #add the legend
              ax.legend(handles=legend_elements, loc='upper left')
              #set the title
              ax.set_title('New passenger data for {}'.format(economy))
              #make it quite big
              fig.set_size_inches(18.5, 10.5)
              plt.show()
              #save
              fig.savefig('./plotting_output/estimations/new_passenger_data/8th_ATO_passenger_road_updates_{}.png'.format(economy), dpi=300)


#%%
################################################################################
################################################################################


#%%
