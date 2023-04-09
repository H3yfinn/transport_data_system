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
logger = logging.getLogger(__name__)

def plot_plotly_non_road_energy_estimations_by_economy(egeda_energy_combined_data_merged_tall, paths_dict):
    #plot a plotly graph for each economy, with the absolute and proportion as facets. so first we will melt the data. But also plot it as a scatter plot so the data is not lost
    egeda_energy_combined_data_merged_tall_new= egeda_energy_combined_data_merged_tall.copy()
    egeda_energy_combined_data_merged_tall_new= egeda_energy_combined_data_merged_tall_new.melt(id_vars=['economy', 'date','option','medium'], var_name='measure', value_name='value')
    for economy in egeda_energy_combined_data_merged_tall_new['economy'].unique():
        fig = px.line(egeda_energy_combined_data_merged_tall_new[egeda_energy_combined_data_merged_tall_new['economy']==economy], x="date", y="value", color='medium',title=economy, facet_col='measure', facet_col_wrap=2, line_dash='option', markers=True)
        #make the facets y axis independent
        fig.update_yaxes(matches=None, showticklabels=True)
        #save as html 
        fig.write_html(paths_dict['plotting_paths']['plot_plotly_non_road_energy_estimations_by_economy']+'/egeda_energy_combined_data_merged_tall_new_{}.html'.format(economy))

        
def plot_matplotlib_non_road_energy_estimations_by_economy(egeda_energy_combined_data_merged_tall,paths_dict):    
    #plot everything using matplotlib
    #we will plot a file for each economy.
    #each graph will have a subplot for y=absolute and a subplot for y=proportion
    #medium will define the color of the line
    #option will define the line style
    #limit x axis to be only 2010-2023
    #just in case there is only one value we will also plot points
    plot_this =False
    if plot_this:
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
            plt.savefig(paths_dict['plotting_paths']['plot_plotly_non_road_energy_estimations_by_economy']+'/egeda_energy_{}.png'.format(economy))

def plot_plotly_non_road_energy_estimations(egeda_energy_combined_data_merged_tall,paths_dict):
    #create the plotly time series
    #create the plotly time series for absolute values
    fig = px.line(egeda_energy_combined_data_merged_tall, x="date", y="absolute", color='medium', facet_col='economy', facet_col_wrap=7, title='Energy use by medium', line_dash='option', markers=True)
    #save as html
    fig.write_html(paths_dict['plotting_paths']['plot_plotly_non_road_energy_estimations']+'/egeda_energy_combined_data_merged_tall_absolute.html', auto_open=False)
    #create the plotly time series for proportions
    fig = px.line(egeda_energy_combined_data_merged_tall, x="date", y="proportion", color='medium', facet_col='economy', facet_col_wrap=7, title='Energy use by medium', line_dash='option', markers=True)
    #save as html
    fig.write_html(paths_dict['plotting_paths']['plot_plotly_non_road_energy_estimations']+'/egeda_energy_combined_data_merged_tall_proportion.html', auto_open=False)

def plot_scaled_and_remainder(egeda_energy_combined_data_merged_tall,paths_dict):
    plot_plotly_non_road_energy_estimations(egeda_energy_combined_data_merged_tall,paths_dict)
    plot_plotly_non_road_energy_estimations_by_economy(egeda_energy_combined_data_merged_tall,paths_dict)
    plot_matplotlib_non_road_energy_estimations_by_economy(egeda_energy_combined_data_merged_tall,paths_dict)

def graph_egeda_road_energy_use_vs_calculated(egeda_energy_road_combined_data,paths_dict):
    import plotly.express as px
    #graph the egeda energy use for freight vs the calculated energy use for freight. Use plotly and plot each economy as a facet
    fig = px.line(egeda_energy_road_combined_data, x = 'date', y = 'value', color = 'type', facet_col = 'economy', facet_col_wrap = 7, markers=True)
    #save as html
    # fig.write_html('plotting_output/data_selection/analysis/egeda_road_energy_use_vs_calculated.html', auto_open = False)
    fig.write_html(paths_dict['plotting_paths']['graph_egeda_road_energy_use_vs_calculated']+'/egeda_road_energy_use_vs_calculated.html', auto_open = False)
    

def analyse_missing_energy_road_data(egeda_energy_road_combined_data):
    #analyse missing data:
    missing_total_road_energy_use = egeda_energy_road_combined_data[egeda_energy_road_combined_data['egeda_total_road_energy_use'].isna()]
    missing_road_energy_use = egeda_energy_road_combined_data[egeda_energy_road_combined_data['calculated_road_energy_use'].isna()]
    #show user the missing data
    logger.info('\nmissing_total_road_energy_use:\n')
    logger.info(missing_total_road_energy_use)
    logger.info('\nmissing_road_energy_use:\n')
    logger.info(missing_road_energy_use)


def plot_non_road_energy_use_by_transport_type(non_road_energy,paths_dict):
    # use plotly to plot the energy use for each transport type, with a facet for each medium. 
    for economy in non_road_energy['economy'].unique():
        fig = px.line(non_road_energy[non_road_energy['economy']==economy], x="date", y="value", color='transport_type', facet_col='medium', title=economy, markers=True)
        # save the figure
        fig.write_html(paths_dict['plotting_paths']['plot_non_road_energy_use_by_transport_type']+'/non_road_energy_use_by_transport_type_'+economy+'.html')

# def plot_activity(activity,calculated_road_activity,previous_energy_activity_wide,paths_dict):
#     #calculated_road_activity is the activity calcualted using mileage, efficiency and so on.
#     #activity is the activity calciualted using intensity. We wont use this for road but will for non road

#     #merge the activities together

#     #join road_km to previous_energy_activity_wide. But firstrename medium to 'road_calcualted'
#     calculated_road_activity['medium'] = 'road_calculated'
#     calculated_road_activity.rename(columns={'value':'previous_activity'}, inplace=True)
#     previous_energy_activity_wide.rename(columns={'activity':'previous_activity'}, inplace=True)
#     new_activity = activity.copy()
#     new_activity.rename(columns={'value':'new_activity'}, inplace=True)
#     #concat
#     previous_energy_activity_wide = pd.concat([previous_energy_activity_wide,calculated_road_activity])
#     previous_energy_activity_wide = previous_energy_activity_wide.merge(new_activity,how='outer',on=['date',	'economy',	'medium', 'transport_type'])
#     #make them into long format
#     previous_energy_activity_long = pd.melt(previous_energy_activity_wide,id_vars=['date',	'economy',	'medium', 'transport_type'],value_vars=['previous_activity','new_activity'],var_name='activity_type',value_name='activity')
#     #join transport type and medium
#     previous_energy_activity_long['transport_type'] = previous_energy_activity_long['transport_type']+'_'+previous_energy_activity_long['medium']

#     #where activity type is previous_activity , set activity type to calculated_activity 
#     previous_energy_activity_long.loc[(previous_energy_activity_long['activity_type']=='previous_activity'),'activity_type'] = 'calculated_activity'
#     #then change road_calculated to road
#     previous_energy_activity_long.loc[previous_energy_activity_long['transport_type']=='passenger_road_calculated','transport_type'] = 'passenger_road'
#     #then change road_calculated to road
#     previous_energy_activity_long.loc[previous_energy_activity_long['transport_type']=='freight_road_calculated','transport_type'] = 'freight_road'


#     #now plot a line graph of the activity over time for each economy using plotly. make the marker for new activity twice as big as the previous activity
#     import plotly.express as px
#     for economy in previous_energy_activity_long['economy'].unique():
#         fig = px.line(previous_energy_activity_long[previous_energy_activity_long['economy']==economy], x="date", y="activity",facet_col='transport_type',facet_col_wrap=2, color='activity_type',title='activity for {}'.format(economy), markers=True)
#         #make the marker for new activity twice as big as the previous activity
#         fig.for_each_trace(lambda t: t.update(marker=dict(size=10 if t.name=='previous_activity' else 20)))
#         #save as html]
#         fig.write_html(paths_dict['plotting_paths']['plot_activity']+'/activity_comparison_{}.html'.format(economy), auto_open = False)

def plot_intensity(intensity,paths_dict):
    #plot a box and whisker of intensity so we can see the spread of intensity and therefore understand how it might affect activity calculations. this will be using the medium$transport_type as the x axis and the intensity as the y axis
    plot = px.box(intensity,x='intensity',y='medium$transport_type',color='medium$transport_type', hover_data=['economy'], title='intensity by medium and transport type (PJ / km))')
    plot.write_html(paths_dict['plotting_paths']['plot_intensity']+'/intensity_boxes.html', auto_open=False)

def plot_final_data_energy_activity(all_new_combined_data,paths_dict):
    #PREP
    #drop any 0s or any nas in the value col
    all_new_combined_data = all_new_combined_data[all_new_combined_data['value']!=0]
    all_new_combined_data = all_new_combined_data.dropna(subset=['value'])

    #plot the data's activity and energy use using plotly
    all_new_combined_data_summary = all_new_combined_data.copy()
    # #rename passengerkm and freightkm to activity
    # all_new_combined_data_summary.loc[all_new_combined_data_summary['measure']=='passenger_km','measure'] = 'activity'
    # all_new_combined_data_summary.loc[all_new_combined_data_summary['measure']=='freight_tonne_km','measure'] = 'activity'

    #drop cols taht arent: date, economy, medium, transport_type, measure, value
    all_new_combined_data_summary = all_new_combined_data_summary[['date', 'economy', 'medium', 'transport_type', 'measure', 'value']]

    #sum up duplicate rows
    all_new_combined_data_summary = all_new_combined_data_summary.groupby(['date', 'economy', 'medium', 'transport_type', 'measure']).sum().reset_index()

    #pivot dat so we have a col for each masure
    cols = all_new_combined_data_summary.columns.tolist()
    cols.remove('value')
    cols.remove('measure')

    all_new_combined_data_summary = all_new_combined_data_summary.pivot_table(index=cols, columns='measure', values='value').reset_index()
    
    #PLOT
    for economy in all_new_combined_data_summary['economy'].unique():
        fig = px.line(all_new_combined_data_summary[all_new_combined_data_summary['economy']==economy], x="date", y="energy",facet_col='transport_type',facet_col_wrap=2, color='medium',title='energy for {}'.format(economy), markers=True)
        #save as html]
        fig.write_html(paths_dict['plotting_paths']['plot_final_data_energy_activity']+'/enegy_comparison_{}.html'.format(economy), auto_open=False)

        #times activity by 1billion 
        all_new_combined_data_summary['activity_billion'] = all_new_combined_data_summary['activity']*1000000000
        fig = px.line(all_new_combined_data_summary[all_new_combined_data_summary['economy']==economy], x="date", y="activity_billion",facet_col='transport_type',facet_col_wrap=2, color='medium',title='activity for {}'.format(economy), markers=True)
        #save as html]
        # fig.write_html('plotting_output/data_selection/analysis/finalised_by_economy_plotly/activity_comparison_{}.html'.format(economy), auto_open=False)
        fig.write_html(paths_dict['plotting_paths']['plot_final_data_energy_activity']+'/activity_comparison_{}.html'.format(economy), auto_open=False)

        #calculate intensity suig activity buillion
        all_new_combined_data_summary['intensity'] = all_new_combined_data_summary['energy']/all_new_combined_data_summary['activity_billion']
        fig = px.line(all_new_combined_data_summary[all_new_combined_data_summary['economy']==economy], x="date", y="intensity",facet_col='transport_type',facet_col_wrap=2, color='medium',title='intensity for {} (pj/km)'.format(economy), markers=True)
        #save as html
        fig.write_html(paths_dict['plotting_paths']['plot_final_data_energy_activity']+'/intensity_comparison_{}.html'.format(economy), auto_open=False)

    #now plot every unique measure for road. We will plot a plot for each economy, and then facet cols by vehicle type. then color will be the drive.
    #first get the data
    road = all_new_combined_data[(all_new_combined_data['medium']=='road')].copy()

    for measure in road.measure.unique():
        for transport_type in road.transport_type.unique():
            for economy in road.economy.unique():
                fig = px.line(road[(road['economy']==economy) & (road['measure']==measure) &(road.transport_type == transport_type)], x="date", y="value",facet_col='vehicle_type',facet_col_wrap=2, color='drive',title='{} for {}'.format(measure,economy), markers=True, hover_data=['dataset'])
                #save as html
                fig.write_html(paths_dict['plotting_paths']['plot_final_data_energy_activity']+'/{}_{}_{}_road.html'.format(measure,transport_type,economy), auto_open=False)

def compare_egeda_and_new_energy_totals(egeda_energy_combined_data,all_new_combined_data,plotting_folder,paths_dict):
    #double check we ahve the required directories:
    if not os.path.exists(paths_dict['plotting_paths']['plot_egeda_comparison']+'{}'.format(plotting_folder)):
        os.makedirs(paths_dict['plotting_paths']['plot_egeda_comparison']+'{}'.format(plotting_folder))
    #CHECK THAT THE PROPORTIONS ARE THE SAME
    #join transport type and medium into one col then pivot
    egeda_plot = egeda_energy_combined_data.copy()
    all_new_combined_data_plot = all_new_combined_data.copy()
    egeda_plot['medium'] = egeda_plot['medium'] + '$' + egeda_plot['transport_type']
    #calcualte total energy use for each year for each year and then merge that back in
    egeda_plot['total_energy_use'] = egeda_plot.groupby(['economy','date'])['value'].transform('sum')
    egeda_plot['proportion'] = egeda_plot['value'] / egeda_plot['total_energy_use']

    #do the same for the new data
    all_new_combined_data_plot['medium'] = all_new_combined_data_plot['medium'] + '$' + all_new_combined_data_plot['transport_type']
    #calcualte total energy use for each year for each year and then merge that back in
    all_new_combined_data_plot['total_energy_use'] = all_new_combined_data_plot.groupby(['economy','date'])['value'].transform('sum')
    all_new_combined_data_plot['proportion'] = all_new_combined_data_plot['value'] / all_new_combined_data_plot['total_energy_use']

    #join the two together
    egeda_plot = egeda_plot.merge(all_new_combined_data_plot, on=['economy','date','medium'], how='outer', suffixes=('_egeda','_new'))

    #calcualte the diff between the two and plot
    egeda_plot['diff'] = egeda_plot['proportion_egeda'] - egeda_plot['proportion_new']
    #if any diffs are less than 1% then set to 0
    egeda_plot.loc[abs(egeda_plot['diff'])<0.01,'diff'] = 0

    #if everything is 0 then tell the user and dont plot
    if egeda_plot['diff'].sum() == 0:
        print('no differences between egeda and new data so not plotting comparison between them')
    else:
        #create plotly with each economy as a facet
        fig = px.line(egeda_plot, x="date", y="diff", color='medium', facet_col='economy', facet_col_wrap=7, markers=True)
        #save as html
        fig.write_html(paths_dict['plotting_paths']['plot_egeda_comparison']+'{}/medium_proportions_diff.html'.format(plotting_folder), auto_open=False)

        #filter for 2017 only and plot as bars now
        egeda_plot_2017 = egeda_plot[egeda_plot['date']==2017].copy()
        fig = px.bar(egeda_plot_2017, x="medium", y="diff", facet_col='economy', facet_col_wrap=7, color='medium')
        #save as html
        fig.write_html(paths_dict['plotting_paths']['plot_egeda_comparison']+'{}/medium_proportions_diff_2017.html'.format(plotting_folder), auto_open=False)

    #now compare the total energy use for each economy to the egeda data. We will calcualte the % difference and the we will plot it on a single bar with all the eocnomys on it'
    #first get the total energy use for each economy and year
    egeda_plot_total = egeda_plot.groupby(['economy','date']).sum().reset_index()
    egeda_plot_total['diff'] = egeda_plot_total['value_egeda'] - egeda_plot_total['value_new']

    #if the value_egeda or value_new is 0 then set the diff to 0
    egeda_plot_total.loc[(egeda_plot_total['value_egeda']==0) | (egeda_plot_total['value_new']==0),'diff'] = 0

    egeda_plot_total['diff_percent'] = egeda_plot_total['diff'] / egeda_plot_total['value_egeda']
    
    #if any diffs are less than 1% then set to 0
    egeda_plot_total.loc[abs(egeda_plot_total['diff_percent'])<0.01,'diff_percent'] = 0
    
    #if everything is 0 then tell the user and dont plot
    if egeda_plot_total['diff_percent'].sum() == 0:
        print('no differences between egeda and new data so not plotting comparison between them')
    else:
        #put diff and diff percent on one column
        egeda_plot_total = pd.melt(egeda_plot_total, id_vars=['economy','date'], value_vars=['diff','diff_percent'], var_name='diff_type', value_name='value')

        #create plotly which will plot a bar for each economy and the diff type will be the facet.
        fig = px.bar(egeda_plot_total, x="date", y="value", color='economy', barmode='group', facet_row='diff_type', color_discrete_sequence=px.colors.qualitative.Dark24)
        #make the y axis independent
        fig.update_yaxes(matches=None, showticklabels=True)
        #save as html
        fig.write_html(paths_dict['plotting_paths']['plot_egeda_comparison']+'{}/total_energy_use_diff.html'.format(plotting_folder), auto_open=False)

def plot_new_and_old_road_measures(road_combined,measures,new_measures,paths_dict):
    #plot the old and new data for each measure using plotly. We will do this on a different plot for each measure, faceting by economy:
    #drop non useufl cols
    road_combined_plot = road_combined.copy()
    road_combined_plot = road_combined_plot[['economy','date','drive','vehicle_type', 'transport_type'] + measures + new_measures]
    #now melt measures
    road_combined_plot = road_combined_plot.melt(id_vars=['economy','date','drive','vehicle_type','transport_type'], value_vars=measures + new_measures, var_name='measure', value_name='value')
    #now plot
    for measure in measures:
        for transport_type in road_combined_plot['transport_type'].unique():
            for economy in road_combined_plot['economy'].unique():
                plot_data = road_combined_plot[road_combined_plot['measure'].isin([measure,'NEW_' + measure]) & (road_combined_plot['economy'] == economy) & (road_combined_plot['transport_type'] == transport_type)].copy()

                fig = px.line(plot_data, x="date", y='value', facet_col="vehicle_type", color='drive', line_dash='measure', facet_col_wrap=7, markers=True)
                #make the new values have different shaped markers so we can see them even if there is no line
                fig.for_each_trace(lambda t: t.update(marker_symbol=t.marker_symbol + 4) if t.name == 'NEW_' + measure else ())
                #save
                fig.write_html(paths_dict['plotting_paths']['plot_new_and_old_road_measures'] + '/'+ transport_type+'_'+measure + '_' +economy+'.html', auto_open=False)

