#plot_finalised_data.py
#Use this to plot line graphs with value on the y axis and date on the x axis. These will show the time series for all the different possible time series in the final data that was selected using the automatic and/or manual methods and, if the user chooses, interpolated. On each graph could be a number of time series lines (or singular values if there is only one for that time series) for that graphs economy, measure and transport type. The time series line will have markers at each different datapoint to indicate what dataset teh value is from

# PLEASE NOTE THAT THIS WILL TAKE A LONG TIME TO RUN. IT PLOTS A LOT OF DATA AND GRAPHS SINCE IT WILL LOOP THROUGH ALL OF THE DATA AND POSSIBLE DATA POINNTS THAT ARE MISSING.

#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None

import numpy as np
import os
import re
import matplotlib
import matplotlib.pyplot as plt
%matplotlib notebook
#somehow setting it to notebook will make it so nothing is plotted when using vscode (:
# %matplotlib qt
#if using jupyter notebook then set the backend to inline so that the graphs are displayed in the notebook instead of in a new window

# %matplotlib inline

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False
use_9th_dataset = False
#%%
#laod in data that has been interpolated and data that hasnt been (since we may at times want to plot the data before it has been interpolated). They are both the same structure:
if not use_9th_dataset:
    file_date = datetime.datetime.now().strftime("%Y%m%d")
    import utility_functions as utility_functions
    file_date = utility_functions.get_latest_date_for_data_file('./output_data/', 'combined_dataset_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    interpolated_data = pd.read_csv('output_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))

    subfolder = 'all_data'
else:
    file_date = datetime.datetime.now().strftime("%Y%m%d")
    import utility_functions as utility_functions
    file_date = utility_functions.get_latest_date_for_data_file('./output_data/9th_dataset/', 'combined_dataset_')
    FILE_DATE_ID = 'DATE{}'.format(file_date)
    interpolated_data = pd.read_csv('output_data/9th_dataset/combined_dataset_{}.csv'.format(FILE_DATE_ID))

    subfolder = '9th_dataset'

#%%
#add together the Dataset and source cols
interpolated_data['Dataset'] = interpolated_data['Dataset'] + ' $ ' + interpolated_data['Source']
INDEX_COLS = ['Economy', 'Measure', 'Vehicle Type', 'Unit', 'Medium',
       'Transport Type', 'Drive','Scope', 'Fuel_Type', 'Frequency', 'Date',
       'Dataset']
#Remove year from the current cols without removing it from original list, and set it as a new list
INDEX_COLS_no_year = INDEX_COLS.copy()
INDEX_COLS_no_year.remove('Date')

INDEX_COLS_no_year_no_measure_no_economy_no_type = INDEX_COLS_no_year.copy()
INDEX_COLS_no_year_no_measure_no_economy_no_type.remove('Measure')
INDEX_COLS_no_year_no_measure_no_economy_no_type.remove('Economy')
INDEX_COLS_no_year_no_measure_no_economy_no_type.remove('Transport Type')
INDEX_COLS_no_year_no_measure_no_economy_no_type.remove('Dataset')
INDEX_COLS_no_year_no_measure_no_economy_no_type.remove('Unit')



#%%
#since we have twenty one economys then fvor each measure we will plot a single line graph for each economy, for all the variations of data wihtin that measure. We may find that we need to create more suplots still, such as a graph fro each transport type and so on. We will see how it goes
#since there are 21 economies then we will need 21 subplots
number_of_economies = len(interpolated_data['Economy'].unique())


#convert dates to years. remove anything thats frequncy is not 'yearly'
interpolated_data['Date'] = interpolated_data['Date'].apply(lambda x: int(x.split('-')[0]))
interpolated_data = interpolated_data[interpolated_data['Frequency'] == 'Yearly']

#make all nans as strings
interpolated_data = interpolated_data.fillna('nan')

#create indexes in the data so we can loop through it
interpolated_data.set_index(INDEX_COLS_no_year_no_measure_no_economy_no_type, inplace=True)


#%%
# Loop through the data and plot each time series on its own subplot
#loop through the unique measures, plot a figure for each measure, then loop through the economies and plot a subplot for each economy, then for each unique row in the data, plot a line for that row, for the years in the data
if len(interpolated_data['Economy'].unique()) != 21:
    raise ValueError(' There are not 21 economies in the data')

# Set the font size of text in the figure to 8 points
matplotlib.rcParams['font.size'] = 18

handles_labels_dict = {}

#SET COLORS AND MARKERS

#SETTING COLORS AND MARKERS WITHIN PLOTTING LOOP SO THERE ARE FEWER TOTAL COLORS PER ITERATION AND THEREFORE THEY ARE MORE DDIFFERENT. FOR NOW WE WILL JSUT LOAD IN a color map
colors = plt.get_cmap('hsv')#.colors
#MARKERS
# Get a list of all available markers
marker_names = list(matplotlib.markers.MarkerStyle.markers.keys())
#keep the first 25 because the others are less useful and sometimes dont work
marker_names = marker_names[:25]


#%%
#take a look at where dataset is 'ATO'
# interpolated_data.loc[interpolated_data['Dataset'] == 'ATO']



def plot_data(plotting_df, measure, transport_type,subfolder, option=None):
    #GET COLORS

    #we are going to want to keep the label colors constant across all subplots. So we will set them now based on the unique index rows in the data
    #get unique index rows
    unique_index_rows = plotting_df.index.unique()
    #set the number of colors to use
    num_colors = len(unique_index_rows)
    #set the colors to use, making sure that the colors are as different as possible
    colors_to_use = [colors(i) for i in np.linspace(0, 1, num_colors)]
    #assign each color to a unique index row
    color_dict = dict(zip(unique_index_rows, colors_to_use))

    #GET MARKERS
            
    # Generate a list of equally space markers and create one for each unique dataset
    num_categories = len(plotting_df.Dataset.unique())
    markers = [marker_names[i] for i in np.linspace(0, len(marker_names)-1, num_categories).astype(int)]
    #now assign each marker to a unique dataset
    marker_dict = dict(zip(plotting_df.Dataset.unique(), markers))

    #######################

    # Create a figure with 7 rows and 3 columns
    fig, ax = plt.subplots(7, 3,figsize=(50, 40))
    plt.subplots_adjust(wspace=0.2, hspace=0.2)
    
    #make backgrounbd white
    fig.patch.set_facecolor('white')

    # Set the y-axis formatter to use scientific notation and powerlimits to -3 and 4 which are quite low, making it so we remove a lot of extra zeros
    formatter = plt.ScalarFormatter(useMathText=True, useOffset=True)
    formatter.set_powerlimits((-3,4))
    handles_list = []
    labels_list = []

    # #set min and max so we have a bit of space around the data. first will have to get the min and max of the data, then grab the year, increase it by 1 and decrease it by 1
    # min_x = interpolated_data.Date.min() -1
    # #set the max x
    # max_x = interpolated_data.Date.max() +1

    #get unit for the current measure and transport type
    unit = plotting_df.Unit.unique()[0]
    #if there are more than one unit then just let the user know (they prob already know)
    if len(plotting_df.Unit.unique()) > 1:
        print('There are more than one unit for the measure {} and transport type {}'.format(measure, transport_type))

    for i,economy in enumerate(sorted(interpolated_data['Economy'].unique())):
        # Calculate the row and column index for the subplot
        row = i // 3
        col = i % 3
        #subset the data for the current economy and measure
        current_economy_measure_data = plotting_df.loc[plotting_df['Economy'] == economy]

        try:
            ax[row,col].set_ylim(-current_economy_measure_data.Value.max()*0.1, current_economy_measure_data.Value.max()*1.5)
        except ValueError:
            pass

        num_economy_datapoints = len(current_economy_measure_data.Value.dropna())
        ax[row,col].set_title('{} - {} data points'.format(economy, num_economy_datapoints))
        if row != 6:
            #make x labels invisible if not the bottom row
            ax[row,col].set_xticklabels([])
        if col == 0:
            #create unit label if on the left column
            ax[row,col].set_ylabel(unit)

        #loop through the rows in the data and plot a line for each row
        for unique_row in current_economy_measure_data.index.unique():#rows are based on vehicle type medium and drive

            #get the data for the current row
            current_row_data = current_economy_measure_data.loc[unique_row,:]
            
            #plot the data for the current row in the current economys subplot 
            ax[row, col].plot(current_row_data.Date, current_row_data.Value, label=unique_row, linewidth=0.5, color=color_dict[unique_row], marker = 'None', alpha=0.5)#

        #Now this subplot is done, get handles and labels and add them to the legend
        handles, labels = ax[row, col].get_legend_handles_labels()
        handles_list.extend(handles)
        labels_list.extend(labels)

        #now for each economy plot the datasets
        for unique_row in current_economy_measure_data.index.unique():#rows are based the index rows
            for unique_dataset in current_economy_measure_data.Dataset.unique():

                #if dataset is nan then skip it
                if (pd.isnull(unique_dataset)) or (unique_dataset == 'nan'):
                    continue
                else:
                    #get the data for the current row and dataset
                    current_row_data = current_economy_measure_data.loc[unique_row,:].drop_duplicates()
                    if len(current_row_data.shape) > 1:#spot where there are multiple rows
                        current_row_data = current_row_data.loc[current_row_data['Dataset'] == unique_dataset,:]
                    else:
                        if current_row_data['Dataset'][0] != unique_dataset:
                            continue
                #plot the data for the current row in the current economys subplot 
                ax[row, col].scatter(current_row_data.Date, current_row_data.Value, label=unique_row, color=color_dict[unique_row], marker=marker_dict[unique_dataset], s=200)
        
    #get number of actual values in the data
    number_of_values = len(plotting_df.Value.dropna())   

    # Set the title of the figure and move title up a bit so it doesnt intercept with a suplot title
    fig.suptitle('{}_{} \n Units: {}, Number of datapoints: {}'.format(measure,transport_type, unit,number_of_values), fontsize=matplotlib.rcParams['font.size']*2, y=1.001)
    
    # Create the legend based on the labels from all subplots:
    #filter out duplicates
    handles, labels = [], []
    for handle, label in zip(handles_list, labels_list):
        if label not in labels:
            if handle not in handles:
                handles.append(handle)
                labels.append(label)
    #make the lines in legend a bit thicker
    handles = [matplotlib.lines.Line2D([0], [0], color=h.get_color(), linewidth=2) for h in handles]
    fig.legend(handles, labels, loc='upper right', ncol=1, bbox_to_anchor=(1.05, 1))

    #then create another legend which is just for the markers for the datasets
    handles2 = []
    labels2 = []
    for dataset in plotting_df.Dataset.unique():
        if pd.isnull(dataset):
            continue
        handles2.append(matplotlib.lines.Line2D([0], [0], marker=marker_dict[dataset], color='w', label=dataset, markerfacecolor='black', markersize=10))
        labels2.append(dataset)
    
    
    fig.legend(handles2, labels2, loc='center right', ncol=1, bbox_to_anchor=(1.05, 0.1))

    #make sure there is minmmal white space on either side of the plot
    plt.tight_layout()

    if option != None:
        #save the plot with id for the date and the measure. Make the plot really high resolution so that it can be zoomed in on
        plt.savefig('plotting_output/plot_finalised_data_timeseries/{}/{}_{}_{}_{}_plot.png'.format(subfolder,FILE_DATE_ID, measure, transport_type, option), bbox_inches='tight')
    else:
        plt.savefig('plotting_output/plot_finalised_data_timeseries/{}/{}_{}_{}_plot.png'.format(subfolder,FILE_DATE_ID, measure, transport_type), bbox_inches='tight')
    
    #close the plot so that it doesnt take up memory    
        

#%%



#%%
for measure in interpolated_data['Measure'].unique():
    for transport_type in interpolated_data['Transport Type'].unique():
        plotting_df = interpolated_data.loc[(interpolated_data['Measure'] == measure) & (interpolated_data['Transport Type'] == transport_type)]
        #if there is no values in Value COL except na's for the current measure and transport type then skip it and let user know
        if interpolated_data.loc[(interpolated_data['Measure'] == measure) & (interpolated_data['Transport Type'] == transport_type)].Value.isnull().all():
            print('There are no values for the measure {} and transport type {} so skipping the plotting of it'.format(measure, transport_type))
            continue
        
        if len(plotting_df) > 1000:
            lots_of_data = True
            #we have too many data points to plot so we will split it into road and non road
            old_df = plotting_df.copy()
            for option in ['road', 'non-road']:
                if option == 'non-road':
                    plotting_df = old_df.loc[old_df['Transport Type'] != 'road']
                    if plotting_df.empty:
                        continue
                    plot_data(plotting_df, measure, transport_type,subfolder, option)
                else:
                    plotting_df = old_df.loc[old_df['Transport Type'] == 'road']
                    if plotting_df.empty:
                        continue
                    plot_data(plotting_df, measure, transport_type,subfolder, option)
        else:
            plot_data(plotting_df, measure, transport_type,subfolder)
        
#%%


#%%
