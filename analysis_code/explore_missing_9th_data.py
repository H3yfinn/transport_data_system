
#set working directory as one folder back
import os
import re
import pandas as pd
import numpy as np
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

# %%
import pandas as pd
import plotly.express as px

columns_to_plot =['Measure','Transport Type', 'Medium', 'Vehicle Type','Drive', 'Economy']#, 'Economy']
#dsrop turnover_rate form measure



#%%
############################################################################
############################################################################


#%%
#also want to find out what rows we are missing data in all years for. To do this, drop the Date col, then group by the index and then count the number of rows in each group. If the count is less than 23 then we are missing data for that row

#read in the data we are missing
missing = pd.read_csv('./intermediate_data/9th_dataset/missing_rows_no_zeros.csv')

#drop Date col and then
missing = missing.drop(columns=['Date'])

#group by the index and then count the number of rows in each group. If the count is less than 23 then we are missing data for that row
#set index to ['Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Date', 'Economy','Frequency', 'Measure', 'Unit']

# missing = missing.set_index(['Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Economy','Frequency', 'Measure', 'Unit'])
#%%
#create count col
missing['Count'] = 1
#group by index and count the number of rows in each group
missing = missing.groupby(['Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Economy','Frequency', 'Measure', 'Unit']).count()
#drop all rows where count is not 13 (or thereabouts)
missing = missing[missing['Count'] == 13]
#reset index
missing = missing.reset_index()

missing = missing[missing.Measure != 'Turnover_rate']
missing = missing[missing.Measure != 'Occupancy']
missing = missing[missing.Measure != 'Load']
#drop new_vehicle_efficiency measure for now
# missing = missing[missing.Measure != 'New_vehicle_efficiency']

#and ignore any drive = fcev, lpg, cng or phevg or phevd
# missing = missing[~missing.Drive.isin(['fcev', 'lpg', 'cng', 'phevg', 'phevd'])]

#removce any rows where a value in any col is 'nonspecified'
missing = missing[~missing.isin(['nonspecified']).any(axis=1)]

#drop Date col and then 
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
fig = px.treemap(, path=columns_to_plot)
#make it bigger
fig.update_layout(width=2500, height=1300)
#make title
# fig.update_layout(title_text=measure)
#show it in browser rather than in the notebook
fig.write_html("plotting_output/estimations/data_coverage_trees/missing_data_tree_multidate_no_zeros_economys.html", auto_open=True)
#NOTE THAT THIS SEEMS LIKE THE BEST GRAPH TO LOOK AT TO SEE WHAT WE ARE MISSING DATA FOR SINCE 


############################################################################
############################################################################



#IF WE ONLY WANT TO TAKE A LOOK AT ONE YEAR:

############################################################################






#%%
import os
import re
import pandas as pd
import numpy as np
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

# import data_estimation_functions as data_estimation_functions
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data = pd.read_csv('./intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))
#%%
#anaylse the data we ahve and see if we can find the data we are msising:
#%%
#read in the data we are missing
missing = pd.read_csv('./intermediate_data/9th_dataset/missing_rows_no_zeros.csv')
#%%
missing.head()
#%%
# filter for just one year:
missing = missing[missing.Date == '2017-12-31']
#%%
missing.Measure.unique()
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











#####################################################
############################################################################

#%%
#also want to find out what rows we are missing data in all years for. To do this, drop the Date col, then group by the index and then count the number of rows in each group. If the count is less than 23 then we are missing data for that row



############################################################################


#read in the data we are missing
missing = pd.read_csv('./intermediate_data/9th_dataset/missing_rows.csv')
#drop Date col and then
missing = missing.drop(columns=['Date'])
#group by the index and then count the number of rows in each group. If the count is less than 23 then we are missing data for that row
#set index to ['Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Date', 'Economy','Frequency', 'Measure', 'Unit']

# missing = missing.set_index(['Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Economy','Frequency', 'Measure', 'Unit'])

#create count col
missing['Count'] = 1
#group by index and count the number of rows in each group
missing = missing.groupby(['Medium', 'Transport Type', 'Vehicle Type', 'Drive', 'Economy','Frequency', 'Measure', 'Unit']).count()
#drop all rows where count is not 13 (or thereabouts)
missing = missing[missing['Count'] >= 12]
#reset index
missing = missing.reset_index()

missing = missing[missing.Measure != 'Turnover_rate']
missing = missing[missing.Measure != 'Occupancy']
missing = missing[missing.Measure != 'Load']
#drop new_vehicle_efficiency measure for now
missing = missing[missing.Measure != 'New_vehicle_efficiency']

#and ignore any drive = fcev, lpg, cng or phevg or phevd
missing = missing[~missing.Drive.isin(['fcev', 'lpg', 'cng', 'phevg', 'phevd'])]

#removce any rows where a value in any col is 'nonspecified'
missing = missing[~missing.isin(['nonspecified']).any(axis=1)] 
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
fig.write_html("plotting_output/estimations/data_coverage_trees/missing_data_tree_multidate.html", auto_open=True)


























#Use this to plot scatterplots with a discrete categorical axis on y axis and the date on x axis, showing what datappoints are available from what datasets forevery datapoint in the transport datasystem. The charts are split into many based on the datas transport type and measure. 
#the chart is basically the same as the one in plot_final_dataset_coverage.py but this one plots all the data points, not just the final ones. 
#To combat having two datapoints for a single date and index row, the chart will plot each datapoint consecutively with a decreasing size so that you can see the different data poitns colors as rings if they arent the top one.
#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None

import numpy as np
import os
import re
import pickle
import sys
import matplotlib
import matplotlib.pyplot as plt
%matplotlib notebook
# #somehow setting it to notebook will make it so nothing is plotted when using vscode (:
# %matplotlib qt
# #if using jupyter notebook then set the backend to inline so that the graphs are displayed in the notebook instead of in a new window

# %matplotlib inline

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False
use_9th_dataset = False
#%%
#this is for determining what are the best datpoints to use when there are two or more datapoints for a given time period
if not use_9th_dataset:
    #load data
    file_date = datetime.datetime.now().strftime("%Y%m%d")
    import utility_functions as utility_functions
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    combined_dataset = pd.read_csv('intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))
    combined_data_concordance= pd.read_csv('intermediate_data/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))
    subfolder='all_data'
else:
    subfolder = '9th_dataset'
    #load data
    file_date = datetime.datetime.now().strftime("%Y%m%d")
    import utility_functions as utility_functions
    file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/9th_dataset', 'combined_dataset_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    combined_dataset = pd.read_csv('intermediate_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID))
    combined_data_concordance= pd.read_csv('intermediate_data/9th_dataset/combined_dataset_concordance_{}.csv'.format(FILE_DATE_ID))

#%%
#merge on the dataset from combined_dataset to combined_data_concordance so where we have datasets for a given datapoint we can see which dataset it came from
#first get the columns that are in both datasets
columns_to_merge_on = [col for col in combined_data_concordance.columns if col in combined_dataset.columns]
#keep those and the Dataset column from combined_dataset and merge on the columns
data_coverage = combined_data_concordance.merge(combined_dataset[columns_to_merge_on + ['Dataset', 'Value']], on=columns_to_merge_on, how='outer')


#%%
#for now just drop where dataset is nan
data_coverage = data_coverage.dropna(subset=['Dataset'])

#preapre data
#replace all dataset values with the string version of the dataset to make plotting easier and so nan values can be plotted
data_coverage['Dataset'] = data_coverage['Dataset'].apply(lambda x: str(x))

#convert dates to years. remove anything thats frequncy is not 'yearly'
data_coverage['Date'] = data_coverage['Date'].apply(lambda x: int(x.split('-')[0]))
data_coverage = data_coverage[data_coverage['Frequency'] == 'Yearly']
#%%
#create a column which states whther the dataset value is na or not
# data_coverage[''] = data_coverage['Value'].notnull()

#SET COLORS AND MARKERS
#COLORS
#we are going to want to keep the label colors constant across all subplots. So we will set them now based on the unique index rows in the data
#get unique index rows
unique_index_rows = data_coverage.Dataset.unique()
#set the colors to use using a color map
colors = plt.get_cmap('tab10')
#set the number of colors to use
num_colors = len(unique_index_rows)
#set the colors to use, making sure that the colors are as different as possible
colors_to_use = [colors(i) for i in np.linspace(0, 1, num_colors)]

#plot the colors in case we want to see them
plot_colors_to_use = True
if plot_colors_to_use:
    plt.figure()
    plt.title('Colors to use')
    for i, color in enumerate(colors_to_use):
        plt.plot([i], [i], 'o', color=color)
    plt.savefig('plotting_output/plot_data_coverage/{}_colors_to_use.png'.format(FILE_DATE_ID))
    plt.close()

#assign each color to a unique index row
color_dict = dict(zip(unique_index_rows, colors_to_use))

#%%
#todo, why does this create the figures after all the rest of the code. weird.
               

def plot_data_coverage_for_measure_and_transport_type(measure, transport_type, plotting_df, FILE_DATE_ID,color_dict,subfolder,option=None):
    # Create a figure with 7 rows and 3 columns
    fig, ax = plt.subplots(7, 3,figsize=(40, 40))
    plt.subplots_adjust(wspace=0.2, hspace=0.2)

    #make backgrounbd white
    fig.patch.set_facecolor('white')

    handles_list = []
    labels_list = []

    row = 0
    col = 0

    for i,economy in enumerate(sorted(data_coverage['Economy'].unique())):
        #Calculate the row and column index for the subplot
        row = i // 3
        col = i % 3
        #subset the data for the current economy and measure
        current_economy_measure_data = plotting_df.loc[data_coverage['Economy'] == economy]

        #we will be plotting the y axis as the combination of index cols 'Medium','Vehicle Type', 'Drive'. So comboine them with _'s
        current_economy_measure_data['index_col'] = current_economy_measure_data['Medium'].fillna('') + '_' + current_economy_measure_data['Vehicle Type'].fillna('') + '_' + current_economy_measure_data['Drive'].fillna('')
        #sort
        current_economy_measure_data = current_economy_measure_data.sort_values(by=['index_col'])
        # #make the index_col the index
        # current_economy_measure_data = current_economy_measure_data.reset_index().set_index('index_col')
        #check if there are any duplicates
        current_economy_measure_data = current_economy_measure_data.drop_duplicates()

        # #make x axis ticks every year
        # ax[row, col].set_xticks(range(data_coverage.Date.min()-1, data_coverage.Date.max()+1, 1))

        num_economy_datapoints = len(current_economy_measure_data.Value.dropna())
        ax[row,col].set_title('{} - {} data points'.format(economy, num_economy_datapoints))
        # if row != 6:
        #     #make x labels invisible if not the bottom row
        #     ax[row,col].set_xticklabels([])
        if col == 0:
            #create unit label if on the left column
            ax[row,col].set_ylabel('(Medium, Vehicle Type, Drive)')

        #loop through the different Dates and index cols in the data and plot the datasets for each datapoint in slowly decreasing marker sizes, one on top of the other

        for Date in current_economy_measure_data.Date.unique():#rows are based on vehicle type medium and drive

            #filter for that date
            current_economy_measure_data_date = current_economy_measure_data.loc[current_economy_measure_data.Date == Date]

            for index_col in current_economy_measure_data_date.index_col.unique():

                current_economy_measure_data_date_index_col = current_economy_measure_data_date.loc[current_economy_measure_data_date.index_col == index_col]
                
                #get the different datasets for the current row and sort them
                datasets= current_economy_measure_data_date_index_col.Dataset.unique().tolist()
                datasets.sort()
                if datasets == None:
                    continue
                #define the marker based on if there is a value or not
                if len(datasets) == 1 and datasets[0] == 'nan':#TO DO do the datasets end up being formatted as nan?
                    marker = 'x'
                else:
                    marker = 'o'
                
                #now assign the first marker size based on the number of datasets (more datasets = bigger marker)
                marker_size = 10 + 10*len(datasets)
                #and then loop through the datasets and plot them in decreasing marker sizes
                
                for dataset in datasets:
                    
                    #filter for the current dataset
                    current_datapoint = current_economy_measure_data_date_index_col.loc[current_economy_measure_data_date_index_col.Dataset == dataset]

                    ax[row, col].scatter(current_datapoint.Date, current_datapoint.index_col,label=current_datapoint.Dataset,color=color_dict[dataset], marker=marker, s=marker_size)

                    #decrease the marker size
                    marker_size -= 10

                #create a grid in the chart with lines between each tick on each axis
                ax[row, col].grid(True, which='both', axis='both', color='grey', linestyle='-', linewidth=0.5, alpha=0.5)
                #make the x axis ticks 45deg
                ax[row, col].tick_params(axis='x', rotation=45)

                #get legend handles and labels
                handles, labels = ax[row, col].get_legend_handles_labels()
                #add the handles and labels to the list
                handles_list.extend(handles)
                labels_list.extend(labels)

    #get number of actual values in the data
    number_of_values = len(plotting_df.Value.dropna())   

    #Set the title of the figure
    fig.suptitle('{}_{} \nNumber of datapoints: {}'.format(measure,transport_type,number_of_values), fontsize=matplotlib.rcParams['font.size']*2)

    #Create custom legend based on the markers and color_dict and not handles or labels
    #create a list of markers
    markers = ['o','x']
    #create a list of labels
    labels = ['Actual data','No data']
    #create a list of colors
    colors = [color for color in color_dict.values()]
    colors_datasets = [dataset for dataset in color_dict.keys()]
    #create a patch which matches up labels[0] with markers[0] and all the colors
    patches = [matplotlib.lines.Line2D([0], [0], color=colors[i], label=colors_datasets[i], marker='o') for i in range(len(colors))]
    #add the patches to the legend
    plt.legend(handles=patches, loc='upper right')#, fancybox=True, shadow=True, ncol=5)

    #make sure there is minmmal white space on either side of the plot
    plt.tight_layout()
    if option != None:
        #save the plot with id for the date and the measure. Make the plot really high resolution so that it can be zoomed in on
        if number_of_values == 0:
            plt.savefig('plotting_output/plot_data_coverage/all_data/{}/NOVALUES{}_{}_{}_{}_plot.png'.format(subfolder,FILE_DATE_ID, measure, transport_type,option))
        else:
            plt.savefig('plotting_output/plot_data_coverage/all_data/{}/{}_{}_{}_{}_plot.png'.format(subfolder,FILE_DATE_ID, measure, transport_type, option))
    else:
        #save the plot with id for the date and the measure. Make the plot really high resolution so that it can be zoomed in on
        if number_of_values == 0:
            plt.savefig('plotting_output/plot_data_coverage/all_data/{}/NOVALUES_{}_{}_{}_plot.png'.format(subfolder,FILE_DATE_ID, measure, transport_type))
        else:
            plt.savefig('plotting_output/plot_data_coverage/all_data/{}/{}_{}_{}_plot.png'.format(subfolder,FILE_DATE_ID, measure, transport_type))


#%%

for measure in data_coverage['Measure'].unique():
    for transport_type in data_coverage['Transport Type'].unique():
        print('Plotting {} for {}'.format(measure, transport_type))
        # if measure == 'energy_use_tonnes' and transport_type == 'combined':
        #     break
        #######################
        plotting_df = data_coverage.loc[(data_coverage['Measure'] == measure) & (data_coverage['Transport Type'] == transport_type)]

        if len(plotting_df) > 1000:
            #we have too many data points to plot so we will split it into road and non road
            old_df = plotting_df.copy()
            for option in ['road', 'non-road']:
                if option == 'non-road':
                    plotting_df = old_df.loc[old_df['Transport Type'] != 'road']
                    if len(plotting_df) == 0:
                        continue
                    plot_data_coverage_for_measure_and_transport_type(measure, transport_type, plotting_df, FILE_DATE_ID,color_dict,subfolder,option)
                else:
                    plotting_df = old_df.loc[old_df['Transport Type'] == 'road']
                    if len(plotting_df) == 0:
                        continue
                    plot_data_coverage_for_measure_and_transport_type(measure, transport_type, plotting_df, FILE_DATE_ID,color_dict,subfolder,option)
        else:
            plot_data_coverage_for_measure_and_transport_type(measure, transport_type, plotting_df, FILE_DATE_ID,color_dict,subfolder)
