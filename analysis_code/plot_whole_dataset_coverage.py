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

    # if measure == 'energy_use_tonnes' and transport_type == 'combined':
    #     break
#ABOVE THE ISSUE IS:
#if 0 data points then x axis is creating all years at end of axis.
#if there are some datapoints then our legend becomes too big and wwe sometimes have too many unique index rows for a single y axis to hold
#nan and non spec values being weird
#legend getting stuffed up by duplicates?
#perhaps name file so itis autommatically recogniosable that the file doesnt have any data in it eg. bny including data points in the name
#^seems like legend can be made to just show the dataset associated with the color, not all the unqiue rows too.
        
#%%
#it looks like weve got some weird things happening in passenger_km for 10_MAS, 21_VN, 08_JPN and 19_THA
#also check that the values in PRC are mostly rail like the legend implies

# mas = data_coverage.loc[(data_coverage['Economy'] == '10_MAS') & (data_coverage['Measure'] == 'passenger_km') & (data_coverage['Transport Type'] == 'passenger')]

#             #it looks like weve got some weird things happening in passenger_km for 10_MAS, 21_VN, 08_JPN and 19_THA
# #             #also check that the values in PRC are mostly rail like the legend implies
# if (economy == '10_MAS') & (measure == 'passenger_km'):
#     saved_economy_measure_data1 = current_economy_measure_data
# elif (economy == '21_VN') & (measure == 'passenger_km'):
#     saved_economy_measure_data2 = current_economy_measure_data
# elif (economy == '08_JPN') & (measure == 'passenger_km'):
#     saved_economy_measure_data3 = current_economy_measure_data
# elif (economy == '19_THA') & (measure == 'passenger_km'):
#     saved_economy_measure_data4 = current_economy_measure_data
# elif (economy == '13_PRC') & (measure == 'passenger_km'):
#     saved_economy_measure_data5 = current_economy_measure_data
# else:
#     pass

#double check what happens to the labels and if they are not being changed between subplots

#%%
#although it seems like the above needs the use of plotly to provide better detail i think it would be better to focus on getting this right because of the issues with losing data in plotly. so we will focus on filling in details, creating more graphs so there are only a few lines per graph and so on:

#fix y axis values so they dont overflow into the next subplot (can always use k's and m's to indicate millions and thousands)
#or make graphs u
#add a legend to the whole plot so that similar categories in different subplots can be compared (and they will have the same colour)

#add a title to each subplot

#have the y axis go negative so that the data can be compared to the zero line
#provide supplementary graphs that show what data on what categories anbd years is missing so that plots that look empty can be explained

#rem,ove warning messages

#see if chat gpt has advicve for improving the colors and vibe. also once done ask if there are better libraries itn recommends or even better strategies for '?plotting a summary of what data is and is not available and the summary of that data we do have available

#OTHER CONCERNS
#the markers for different datasets should be different but the legend should just be for the different colors (not sep by marker)
#there should be multiple eyars of data for 01_aus freight_tonne_km, is currently only showing data for 2017
#if we get more data then the graphs will work well still. 
#%%









#%%
# #create a multi facet grid plot, with a facet for each measure, with y axis = economy, x = year which will show a green square if data_available is false for that year and economy, and a red square if data_available is true for that year and economy
# #bvut we will do this in a for loop on each index row
# for alt_measure in ATO_data_transport_checking_concordance_check['alt_measure'].unique():
#     #create a dataframe that only has the alt_measure we are looking at
#     alt_measure_df = ATO_data_transport_checking_concordance_check[ATO_data_transport_checking_concordance_check['alt_measure'] == alt_measure]
#     #because of the addition of the sheet column we will have to create a col for measure + sheet
#     alt_measure_df['measure_sheet'] = alt_measure_df['measure'] + ' ' + alt_measure_df['sheet']
#     title = 'ATO Economy Year NA Count'
#     #make sure that if data_available is true, the color is green, and red if false
#     fig = px.scatter(alt_measure_df, x='year', y='economy', color='data_available', facet_col='measure_sheet', facet_col_wrap=7, title=title, color_discrete_map={True: 'green', False: 'red'})
#     fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))#remove 'Economy=X' from titles
#     #fig.show()
#     #save graph as html in plotting_output/exploration_archive
#     fig.write_html('plotting_output/exploration_archive/' + title + ' ' + alt_measure + '.html')


#%%
# #%%
# INDEX_COLS = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Medium',
#        'Transport Type','Drive']
# #Remove year from the current cols without removing it from original list, and set it as a new list
# INDEX_COLS_no_year = INDEX_COLS.copy()
# INDEX_COLS_no_year.remove('Date')

# INDEX_COLS_no_year_no_measure_no_economy_no_type = INDEX_COLS_no_year.copy()
# INDEX_COLS_no_year_no_measure_no_economy_no_type.remove('Measure')
# INDEX_COLS_no_year_no_measure_no_economy_no_type.remove('Economy')
# INDEX_COLS_no_year_no_measure_no_economy_no_type.remove('Transport Type')


# #%%
# #since we have twenty one economys then fvor each measure we will plot a single line graph for each economy, for all the variations of data wihtin that measure. We may find that we need to create more suplots still, such as a graph fro each transport type and so on. We will see how it goes
# #since there are 21 economies then we will need 21 subplots
# number_of_economies = len(data_coverage['Economy'].unique())

# #%%
# #create indexes in the data so we can loop through it
# data_coverage.set_index(INDEX_COLS_no_year_no_measure_no_economy_no_type, inplace=True)

# #%%


# #%%
# # Loop through the data and plot each time series on its own subplot
# #loop through the unique measures, plot a figure for each measure, then loop through the economies and plot a subplot for each economy, then for each unique row in the data, plot a line for that row, for the years in the data
# if len(data_coverage['Economy'].unique()) != 21:
#     raise ValueError(' There are not 21 economies in the data')

# # Set the font size of text in the figure to 8 points
# matplotlib.rcParams['font.size'] = 18

# handles_labels_dict = {}

# #SET COLORS AND MARKERS
# #COLORS
# #we are going to want to keep the label colors constant across all subplots. So we will set them now based on the unique index rows in the data
# #get unique index rows
# unique_index_rows = data_coverage.index.unique()
# #set the colors to use using a color map
# colors = plt.get_cmap('hsv')#.colors
# #set the number of colors to use
# num_colors = len(unique_index_rows)
# #set the colors to use, making sure that the colors are as different as possible
# colors_to_use = [colors(i) for i in np.linspace(0, 1, num_colors)]

# #plot the colors in case we want to see them
# plot_colors_to_use = True
# if plot_colors_to_use:
#     plt.figure()
#     plt.title('Colors to use')
#     for i, color in enumerate(colors_to_use):
#         plt.plot([i], [i], 'o', color=color)
#     plt.savefig('plotting_output/plot_finalised_data/{}_colors_to_use.png'.format(FILE_DATE_ID))
#     plt.close()

# #assign each color to a unique index row
# color_dict = dict(zip(unique_index_rows, colors_to_use))
# #MARKERS
# # Get a list of all available markers
# marker_names = list(matplotlib.markers.MarkerStyle.markers.keys())
# #keep the first 25 because the others are less useful and sometimes dont work
# marker_names = marker_names[:25]

# # Generate a list of equally space markers and create one for each unique dataset
# num_categories = len(data_coverage.Dataset.unique())
# markers = [marker_names[i] for i in np.linspace(0, len(marker_names)-1, num_categories).astype(int)]
# #now assign each marker to a unique dataset
# marker_dict = dict(zip(data_coverage.Dataset.unique(), markers))

# #plot the markers in case we want to see them
# plot_markers_to_use = True
# if plot_markers_to_use: 
#     plt.figure()
#     plt.title('Markers to use')
#     for i, marker in enumerate(markers):
#         plt.plot([i], [i], marker, color='black')
#     plt.savefig('plotting_output/plot_finalised_data/{}_markers_to_use.png'.format(FILE_DATE_ID))
#     plt.close()

# #%%
# #take a look at where dataset is 'ATO'
# data_coverage.loc[data_coverage['Dataset'] == 'ATO']