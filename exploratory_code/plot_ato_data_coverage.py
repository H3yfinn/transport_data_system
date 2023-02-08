#we want to plot the ato data coverage so we can understand whats missing/not missing

#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None

import numpy as np
import os
import re
import math
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
USE_INTERPOLATED_DATA = True
#%%
#laod in data
file_date = datetime.datetime.now().strftime("%Y%m%d")
import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('intermediate_data/ATO_data/', 'ATO_data_cleaned_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
ATO_data = pd.read_csv('intermediate_data/ATO_data/ATO_data_cleaned_{}.csv'.format(FILE_DATE_ID))

#%%
#fill missing years and months between min and max year/months for all data
#we will do this using cross joins
#first, based on if Frequency is Yearly or Monthly we will create a concordance table with all the years or months in the range MIN to MAX for each row in the concordance table
ATO_data_years = ATO_data[ATO_data['Frequency'] == 'Yearly'].drop(columns=['Value'])
ATO_data_months = ATO_data[ATO_data['Frequency'] == 'Monthly'].drop(columns=['Value'])
#YEARS:
MAX = ATO_data_years['Date'].max()
MIN = ATO_data_years['Date'].min()
#using datetime creates a range of dates, separated by year with the first year being the MIN and the last year being the MAX
years = pd.date_range(start=MIN, end=MAX, freq='Y')
#drop date from ATO_data_years
ATO_data_years = ATO_data_years.drop(columns=['Date'])
#now do a cross join between the concordance and the years array
ATO_data_concordance_new = ATO_data_years.merge(pd.DataFrame(years, columns=['Date']), how='cross')
#MONTHS:
MAX = ATO_data_months['Date'].max()
MIN = ATO_data_months['Date'].min()
#using datetime creates a range of dates, separated by month with the first month being the MIN and the last month being the MAX
months = pd.date_range(start=MIN, end=MAX, freq='M')
#drop date from ATO_data_months
ATO_data_months = ATO_data_months.drop(columns=['Date'])
ATO_data_months = ATO_data_months.merge(pd.DataFrame(months, columns=['Date']), how='cross')
#concat the months and years concordances together
ATO_data_concordance_new = pd.concat([ATO_data_concordance_new, ATO_data_months], axis=0)
#converty date to object
ATO_data_concordance_new['Date'] = ATO_data_concordance_new['Date'].astype(str)

#join back on values
ATO_data_concordance_new = ATO_data_concordance_new.merge(ATO_data, how='left', on=ATO_data.drop(columns=['Value']).columns.to_list())

#%%
#Now we have a concordance with all the unique rows in the original data, with all the years in the range MIN to MAX for each row
#we will start by plotting exactly what data we have. To do this we will use tree maps
#we will plot the data for each measure, medium, extra_detail, units, year, economy, scope, subcategory, sheet, transport_type

#%%
visualise = False
if visualise:
    #now check data by creating a visualisation of it
    #for now we'll use a treemap in plotly to visualise the data
    import plotly.express as px
    columns_to_plot =['Measure', 'Transport Type', 'Medium']
    #set any na's to 'NA'
    ATO_data_concordance_new = ATO_data_concordance_new.fillna('NA')
    fig = px.treemap(ATO_data_concordance_new, path=columns_to_plot)#, values='Value')
    #make it bigger
    fig.update_layout(width=1000, height=1000)
    #show it in browser rather than in the notebook
    fig.show()
    fig.write_html("plotting_output/ATO analysis/all_data_tree.html")

    #and make one that can fit on my home screen which will be 1.3 times taller and 3 times wider
    fig = px.treemap(ATO_data_concordance_new, path=columns_to_plot)
    #make it bigger
    fig.update_layout(width=2500, height=1300)
    #show it in browser rather than in the notebook
    fig.write_html("plotting_output/ATO analysis/all_data_tree_big.html")

#%%
############################################################################################################
#filter for data only after 2010
ATO_data_concordance_new = ATO_data_concordance_new[ATO_data_concordance_new['Date'] >= '2010-01-01']

#%%
#great now we can get to plotting data coverage
#create a column which states whther the dataset value is na or not
# interpolated_data[''] = interpolated_data['Value'].notnull()

#if fuel_type is na, set it to ''
ATO_data_concordance_new['Fuel_Type'] = ATO_data_concordance_new['Fuel_Type'].fillna('')
#replace 'NA' with ''
ATO_data_concordance_new['Fuel_Type'] = ATO_data_concordance_new['Fuel_Type'].replace('NA', '')


#if the value is na then set the sheet to na
ATO_data_concordance_new['Sheet'] = ATO_data_concordance_new['Sheet'].where(ATO_data_concordance_new['Value'].notnull(), 'NA')

#remove duplicates
ATO_data_concordance_new = ATO_data_concordance_new.drop_duplicates()
#%%
#SET COLORS AND MARKERS
#COLORS
#we are going to want to keep the label colors constant across all subplots. So we will set them now based on the unique index rows in the data
#get unique index rows
unique_index_rows = ATO_data_concordance_new.Sheet.unique()
#scramble the order of the unique index rows
np.random.shuffle(unique_index_rows)
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
#PLOT
#import mdates from matplotlib
import matplotlib.dates as mdates

for measure in ATO_data_concordance_new['Measure'].unique():
    #filter for measure
    ATO_data_concordance_new_measure = ATO_data_concordance_new[ATO_data_concordance_new['Measure'] == measure]
    for transport_type in ATO_data_concordance_new_measure['Transport Type'].unique():
        ATO_data_concordance_new_measure_t_type = ATO_data_concordance_new_measure[ATO_data_concordance_new_measure['Transport Type'] == transport_type]
        #######################
        # Create a figure with 3 columns and then enough rows to fit all the economies
        rows = math.ceil(len(ATO_data_concordance_new_measure_t_type['Economy'].unique()) / 3)
        fig, ax = plt.subplots(rows, 3,figsize=(40, 40))
        plt.subplots_adjust(wspace=0.2, hspace=0.2)

        #make backgrounbd white
        fig.patch.set_facecolor('white')

        handles_list = []
        labels_list = []

        row = 0
        col = 0

        for i,economy in enumerate(sorted(ATO_data_concordance_new_measure_t_type['Economy'].unique())):
            #Calculate the row and column index for the subplot
            row = i // 3
            col = i % 3
            #subset the data for the current economy and t type
            current_economy_measure_data = ATO_data_concordance_new_measure_t_type.loc[(ATO_data_concordance_new_measure_t_type['Economy'] == economy)]

            #we will be plotting the y axis as the combination of other index cols
            current_economy_measure_data['index_col'] = current_economy_measure_data['Medium'] + '_' + current_economy_measure_data['Fuel_Type'] 
            #+ '_' + current_economy_measure_data['Scope']
            #if scope is not 'National' then add it to the index col
            current_economy_measure_data['index_col'] = current_economy_measure_data['index_col'].where(current_economy_measure_data['Scope'] != 'National', current_economy_measure_data['index_col'] + '_' + current_economy_measure_data['Scope'])
            #sort
            current_economy_measure_data = current_economy_measure_data.sort_values(by=['index_col'])
            #make the index_col the index
            current_economy_measure_data = current_economy_measure_data.reset_index().set_index('index_col')
            #check if there are any duplicates
            current_economy_measure_data = current_economy_measure_data.drop_duplicates()

            num_economy_datapoints = len(current_economy_measure_data.Value.dropna())
            ax[row,col].set_title('{} - {} data points'.format(economy, num_economy_datapoints))

            #order data by date so that the data is plotted in the correct order
            current_economy_measure_data = current_economy_measure_data.sort_values(by=['Date'])

            #loop through the different datasets in the data and plot the scatter marks for that sheet
            for sheet in current_economy_measure_data.Sheet.unique():#rows are based on vehicle type medium and drive

                #define the marker based on if there is a value or not
                if sheet == 'NA':
                    marker = ''#this will show no marker
                else:
                    marker = 'o'

                #get the data for the current row
                current_row_data = current_economy_measure_data.loc[current_economy_measure_data.Sheet == sheet,:]

                ax[row, col].scatter(current_row_data.Date, current_row_data.index,label=current_row_data.Sheet,color=color_dict[sheet], marker=marker, s=20)

                #make the x axis ticks 45deg
                ax[row, col].tick_params(axis='x', rotation=45)

                #create a grid in the chart with lines between each tick on each axis
                ax[row, col].grid(True, which='both', axis='both', color='grey', linestyle='-', linewidth=0.5, alpha=0.5)

                #get legend handles and labels
                handles, labels = ax[row, col].get_legend_handles_labels()
                #add the handles and labels to the list
                handles_list.extend(handles)
                labels_list.extend(labels)

        #get number of actual values in the data
        number_of_values = len(ATO_data_concordance_new_measure_t_type.Value.dropna())   

        #Set the title of the figure
        fig.suptitle('{}_{} \nNumber of datapoints: {}'.format(measure,transport_type,number_of_values), fontsize=matplotlib.rcParams['font.size']*2)

        #Create custom legend
        sheets = ATO_data_concordance_new_measure_t_type.Sheet.unique().tolist()
        #remove 'NA' from the list if it is there
        if 'NA' in sheets:
            sheets.remove('NA')
        #create a list of colors based on the sheets in the data
        colors = [color_dict[sheet] for sheet in sheets]

        #create a patch which matches up labels[0] with markers[0] and all the colors
        patches = [matplotlib.lines.Line2D([0], [0], color=colors[i], label=sheets[i]) for i in range(len(colors))]#,marker='o'
        #add the patches to the legend
        plt.legend(handles=patches, loc='upper right')#, fancybox=True, shadow=True, ncol=5)

        #save the plot with id for the date and the measure. Make the plot really high resolution so that it can be zoomed in on
        if number_of_values == 0:
            plt.savefig('plotting_output/plot_data_coverage/ATO/NOVALUES_{}_{}_{}_plot.png'.format(FILE_DATE_ID, measure, transport_type))
        else:
            plt.savefig('plotting_output/plot_data_coverage/ATO/{}_{}_{}_plot.png'.format(FILE_DATE_ID, measure, transport_type))


# %%





