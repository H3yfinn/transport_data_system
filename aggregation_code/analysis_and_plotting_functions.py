#create functions to estiamte data using other data, in a way that can be repeated
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
from PIL import Image
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import itertools
import data_formatting_functions
import logging
import analysis_and_plotting_functions
logger = logging.getLogger(__name__)


def plot_plotly_non_road_energy_estimations_by_economy(egeda_energy_combined_data_merged_tall):
    #plot a plotly graph for each economy, with the absolute and proportion as facets. so first we will melt the data. But also plot it as a scatter plot so the data is not lost
    egeda_energy_combined_data_merged_tall_new= egeda_energy_combined_data_merged_tall.copy()
    egeda_energy_combined_data_merged_tall_new= egeda_energy_combined_data_merged_tall_new.melt(id_vars=['economy', 'date','option','medium'], var_name='measure', value_name='value')
    for economy in egeda_energy_combined_data_merged_tall_new['economy'].unique():
        fig = px.line(egeda_energy_combined_data_merged_tall_new[egeda_energy_combined_data_merged_tall_new['economy']==economy], x="date", y="value", color='medium',title=economy, facet_col='measure', facet_col_wrap=2, line_dash='option', markers=True)
        #make the facets y axis independent
        fig.update_yaxes(matches=None, showticklabels=True)
        #save as html 
        fig.write_html('plotting_output/data_selection/analysis/by_economy_plotly/egeda_energy_combined_data_merged_tall_new_{}.html'.format(economy))

        
def plot_matplotlib_non_road_energy_estimations_by_economy(egeda_energy_combined_data_merged_tall):    
    #plot everything using matplotlib
    #we will plot a file for each economy.
    #each graph will have a subplot for y=absolute and a subplot for y=proportion
    #medium will define the color of the line
    #option will define the line style
    #limit x axis to be only 2010-2023
    #just in case there is only one value we will also plot points
    for economy in egeda_energy_combined_data_merged_tall['economy'].unique():
        fig, ax = plt.subplots(2,1,figsize=(10,10))
        for medium in egeda_energy_combined_data_merged_tall['medium'].unique():
            for option in egeda_energy_combined_data_merged_tall['option'].unique():
                plot_data = egeda_energy_combined_data_merged_tall[(egeda_energy_combined_data_merged_tall['economy']==economy) & (egeda_energy_combined_data_merged_tall['medium']==medium) & (egeda_energy_combined_data_merged_tall['option']==option)]
                #plot absolute
                ax[0].plot(plot_data['date'],plot_data['absolute'],label='{} {} absolute'.format(medium,option))
                ax[0].scatter(plot_data['date'],plot_data['absolute'], label='{} {} absolute'.format(medium,option))
                ax[0].set_xlim([2010,2023])
                #plot proportion on
                ax[1].plot(plot_data['date'],plot_data['proportion'],label='{} {} proportion'.format(medium,option))
                ax[1].scatter(plot_data['date'],plot_data['proportion'], label='{} {} proportion'.format(medium,option))
                ax[1].set_xlim([2010,2023])      
        ax[0].legend()
        ax[0].set_title('Absolute')
        ax[1].set_title('Proportion')
        plt.savefig('plotting_output/data_selection/analysis/by_economy/egeda_energy_{}.png'.format(economy))

def plot_plotly_non_road_energy_estimations(egeda_energy_combined_data_merged_tall):
    #create the plotly time series
    #create the plotly time series for absolute values
    fig = px.line(egeda_energy_combined_data_merged_tall, x="date", y="absolute", color='medium', facet_col='economy', facet_col_wrap=7, title='Energy use by medium', line_dash='option', markers=True)
    #save as html
    fig.write_html("plotting_output/data_selection/analysis/egeda_energy_combined_data_merged_tall_absolute.html", auto_open=False)
    #create the plotly time series for proportions
    fig = px.line(egeda_energy_combined_data_merged_tall, x="date", y="proportion", color='medium', facet_col='economy', facet_col_wrap=7, title='Energy use by medium', line_dash='option', markers=True)
    #save as html
    fig.write_html("plotting_output/data_selection/analysis/egeda_energy_combined_data_merged_tall_proportion.html", auto_open=False)

def plot_scaled_and_remainder(egeda_energy_combined_data_merged_tall):
    #Ask user if they want to plot the results, if yes, plot
    plotting = False
    if plotting:
        plot = input('Do you want to plot the results? (y/n)')
        if plot == 'y':
            plot_plotly_non_road_energy_estimations(egeda_energy_combined_data_merged_tall)
            plot_plotly_non_road_energy_estimations_by_economy(egeda_energy_combined_data_merged_tall)
            plot_matplotlib_non_road_energy_estimations_by_economy(egeda_energy_combined_data_merged_tall)

    return egeda_energy_combined_data_merged_tall

def graph_egeda_road_energy_use_vs_calculated(egeda_energy_road_combined_data):
    import plotly.express as px
    #graph the egeda energy use for freight vs the calculated energy use for freight. Use plotly and plot each economy as a facet
    fig = px.scatter(egeda_energy_road_combined_data, x = 'date', y = 'value', color = 'type', facet_col = 'economy', facet_col_wrap = 7)
    #save as html
    fig.write_html('plotting_output/data_selection/analysis/egeda_road_energy_use_vs_calculated.html', auto_open = True)
    

def analyse_missing_energy_road_data(egeda_energy_road_combined_data):
    #analyse missing data:
    missing_total_road_energy_use = egeda_energy_road_combined_data[egeda_energy_road_combined_data['egeda_total_road_energy_use'].isna()]
    missing_passenger_road_energy_use = egeda_energy_road_combined_data[egeda_energy_road_combined_data['calculated_passenger_road_energy_use'].isna()]
    #show user the missing data
    logger.info('\nmissing_total_road_energy_use:\n')
    logger.info(missing_total_road_energy_use)
    logger.info('\nmissing_passenger_road_energy_use:\n')
    logger.info(missing_passenger_road_energy_use)


def plot_non_road_energy_use_by_transport_type(non_road_energy):
    # use plotly to plot the energy use for each transport type, with a facet for each medium. 
    for economy in non_road_energy['economy'].unique():
        fig = px.line(non_road_energy[non_road_energy['economy']==economy], x="date", y="value", color='transport_type', facet_col='medium', title=economy)
        # save the figure
        fig.write_html('plotting_output/data_selection/analysis/by_economy/'+'/non_road_energy_use_by_transport_type_'+economy+'.html')

def plot_activity(activity,calculated_road_passenger_km,previous_energy_activity_wide):
    #plot the new activity against the activity in previous_energy_activity_wide to see how it looks
    #merge the new activity with the previous activity

    #join road_passenger_km to previous_energy_activity_wide. But firstrename medium to 'road_calcualted'
    calculated_road_passenger_km['medium'] = 'road_calculated'
    calculated_road_passenger_km.rename(columns={'value':'activity'}, inplace=True)
    #drop energy and intensity
    previous_energy_activity_wide = previous_energy_activity_wide.drop(columns=['energy','intensity'])
    #concat
    previous_energy_activity_wide = pd.concat([previous_energy_activity_wide,calculated_road_passenger_km])

    previous_energy_activity_wide = previous_energy_activity_wide.merge(activity,how='outer',on=['date',	'economy',	'medium', 'transport_type'])
    #rename the columns
    previous_energy_activity_wide = previous_energy_activity_wide.rename(columns={'activity':'previous_activity','value':'new_activity'})

    #make them into long format
    previous_energy_activity_long = pd.melt(previous_energy_activity_wide,id_vars=['date',	'economy',	'medium', 'transport_type'],value_vars=['previous_activity','new_activity'],var_name='activity_type',value_name='activity')
    #join transport type and medium
    previous_energy_activity_long['transport_type'] = previous_energy_activity_long['transport_type']+'_'+previous_energy_activity_long['medium']

    #where activity type is previous_activity and transport type is passenger_road_calculated, set activity type to calculated_activity
    previous_energy_activity_long.loc[(previous_energy_activity_long['activity_type']=='previous_activity') & (previous_energy_activity_long['transport_type']=='passenger_road_calculated'),'activity_type'] = 'calculated_activity'
    #then change passenger_road_calculated to passenger_road
    previous_energy_activity_long.loc[previous_energy_activity_long['transport_type']=='passenger_road_calculated','transport_type'] = 'passenger_road'

    #now plot a line graph of the activity over time for each economy using plotly. make the marker for new activity twice as big as the previous activity
    import plotly.express as px
    for economy in previous_energy_activity_long['economy'].unique():
        fig = px.line(previous_energy_activity_long[previous_energy_activity_long['economy']==economy], x="date", y="activity",facet_col='transport_type',facet_col_wrap=2, color='activity_type',title='activity for {}'.format(economy), markers=True)
        #make the marker for new activity twice as big as the previous activity
        fig.for_each_trace(lambda t: t.update(marker=dict(size=10 if t.name=='previous_activity' else 20)))
        #save as html]
        fig.write_html('plotting_output/data_selection/analysis/by_economy_plotly/activity_comparison_{}.html'.format(economy))
