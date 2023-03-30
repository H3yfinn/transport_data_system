#load transport_data_system/intermediate_data/selection_process/DATE20230328/interpolated_stocks_mileage_occupancy_efficiency_combined_data_concordance.pkl then see where we dont have data
#%%
import pandas as pd
import numpy as np
import os
import re
import data_formatting_functions
import data_estimation_functions
import utility_functions
import datetime
import analysis_and_plotting_functions
#set dir one back
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

INDEX_COLS = ['date',
 'economy',
 'measure',
 'vehicle_type',
 'unit',
 'medium',
 'transport_type',
 'drive',
 'fuel',
 'frequency',
 'scope']

EARLIEST_DATE="2010-01-01"
LATEST_DATE='2023-01-01'


previous_FILE_DATE_ID ='DATE20230329'
FILE_DATE_ID = 'DATE20230330'

paths_dict = utility_functions.setup_paths_dict(FILE_DATE_ID,INDEX_COLS, EARLIEST_DATE, LATEST_DATE,previous_FILE_DATE_ID)

unfiltered_combined_data = pd.read_pickle(paths_dict['unfiltered_combined_data'])

road_combined_data = pd.read_pickle(paths_dict['intermediate_folder']+'/road_combined_data_TEST.pkl')

non_road_energy = pd.read_pickle(paths_dict['intermediate_folder']+'/non_road_energy.pkl')
#%%
import logging
logger = logging.getLogger(__name__)

#%%

#%% 
#get energy from egeda and either:
#	1- use the values from egeda and scale them according to how large the total road energy is compared to what it is for egeda (so the same proportion of enegry is used for non road as it is in egeda)
#    2- use the values from egeda but make it the remainder of the total road energy use (so scale between non-road energy uses are the same but the proportion comapred to total road energy use is different)

#%%
#%%
# def estimate_freight_activity(unfiltered_combined_data,road_combined_data,previous_draft_transport_data_system_df):
#this will be very similar to the process for eztiamting non road activity.


def prepare_previous_energy_activity_data(previous_draft_transport_data_system_df):
    #grab the activity an energy data for non road
    energy_activity = previous_draft_transport_data_system_df.loc[previous_draft_transport_data_system_df['measure'].isin(['energy','freight_tonne_km', 'passenger_km'])]
    #drop nonspecified transport type an medium
    energy_activity = energy_activity.loc[energy_activity['transport_type']!='nonspecified']
    energy_activity = energy_activity.loc[energy_activity['medium']!='nonspecified']
    #rename freight_tonne and passenger_km to activity
    energy_activity = energy_activity.replace({'freight_tonne_km':'activity','passenger_km':'activity'})
    #drop wehre scope is not national
    energy_activity = energy_activity.loc[energy_activity['scope']=='national']
    #keep only: date	economy	medium	transport_type energy	freight_tonne_km	passenger_km
    energy_activity = energy_activity[['date','economy','medium','transport_type','measure','value']]
    #sum energy and activity by date, economy, medium and transport type
    energy_activity = energy_activity.groupby(['date','economy','medium','transport_type','measure']).sum().reset_index()

    #pivot so energy and activity are in the same row. 
    cols = energy_activity.columns.tolist()
    cols.remove('value')
    cols.remove('measure')
    energy_activity_wide = energy_activity.pivot(index=cols, columns='measure', values='value').reset_index()

    return energy_activity_wide

def calcualte_intensity_from_previous_data(energy_activity_wide):
    #calvc intensity
    energy_activity_wide['intensity'] = energy_activity_wide['energy']/energy_activity_wide['activity']

    #we will really only have intensity for 2017 because we lack energy data for the other years. But we can just use the 2017 intensity for all years if needed. Also some economys will have really weird intensity values. So we will calcualte the avergae intensity for each transport type/medium combination and use that for the other economys.
    #so lets group by transport type and medium, filter out outliers and then calcualte the average intensity for each group
    intensity_average = energy_activity_wide.dropna(subset=['intensity'])
    #join medium and transport type
    intensity_average['medium$transport_type'] = intensity_average['medium'] + '$' + intensity_average['transport_type']
    unique_medium_transport_type = intensity_average['medium$transport_type'].unique()
    #drop activity and energy
    intensity_average = intensity_average.drop(columns=['activity','energy'])
    #pivot so we ahve a col for each unique medium$transport_type
    intensity_average_wide = intensity_average.pivot(index=['date','economy'], columns='medium$transport_type', values='intensity').reset_index()

    #do some stats magic to remove outliers and then fill then wth the mean of the rest of the values
    import scipy.stats as stats
    for col in unique_medium_transport_type:
        #get z score for this col and set any values to na where z score is > 2
        #make col int
        intensity_average_wide[col] = intensity_average_wide[col].astype(float)
        #replace 0's with nan
        intensity_average_wide.loc[intensity_average_wide[col]==0, col] = np.nan
        intensity_average_wide[col+'_z'] = np.abs(stats.zscore(intensity_average_wide[col], nan_policy='omit'))
        intensity_average_wide.loc[intensity_average_wide[col+'_z']>2, col] = np.nan
        #calcualte mean for this col excluding na
        mean = intensity_average_wide[col].mean()
        #replace na with mean
        intensity_average_wide[col] = intensity_average_wide[col].fillna(mean)
        #drop z score col
        intensity_average_wide = intensity_average_wide.drop(columns=[col+'_z'])
    del stats
    #melt back to long format
    intensity_average = intensity_average_wide.melt(id_vars=['date','economy'], var_name='medium$transport_type', value_name='intensity')

    #separate medium and transport type
    intensity_average['medium'] = intensity_average['medium$transport_type'].str.split('$').str[0]
    intensity_average['transport_type'] = intensity_average['medium$transport_type'].str.split('$').str[1]
    #drop medium$transport_type
    intensity_average = intensity_average.drop(columns=['medium$transport_type'])
    intensity = intensity_average

    return intensity

def clean_activity(energy_activity):
    activity = energy_activity.drop(columns=['value','intensity'])
    #for a few economys we have no data because we have no energy data. So we will drop activty = na
    logging.info('dropping na values from activity, for the following rows: {}'.format(activity[activity['activity'].isna()]))
    activity = activity.dropna(subset=['activity'])
    #rename measure to either freight_tonne_km or passenger_km depending on the transport type
    activity.loc[activity['transport_type']=='freight','measure'] = 'freight_tonne_km'
    activity.loc[activity['transport_type']=='passenger','measure'] = 'passenger_km'
    #for unit make it equal to the measure
    activity['unit'] = activity['measure']
    #rename actitvity to vlaue
    activity = activity.rename(columns={'activity':'value'})
    #make source to previous_data_system
    activity['source'] = 'previous_data_system'
    #dataset to activity_est
    activity['dataset'] = 'activity_est $ previous_data_system'
    #and set these:	fuel	comment	scope	frequency	drive	vehicle_type
    activity['fuel'] = 'all'
    activity['comment'] = 'no_comment'
    activity['scope'] = 'national'
    activity['frequency'] = 'yearly'
    activity['drive'] = 'all'
    activity['vehicle_type'] = 'all'
    return activity

def extract_calculated_road_energy_passenger_km(road_combined_data):
    #extract raod energy and passenger_km
    road_energy_passenger_km = road_combined_data.loc[road_combined_data['measure'].isin(['energy', 'passenger_km'])]
    #sum up road activiy adn energy so we only ahve one row per date, economy, medium, transport type, measure
    road_energy_passenger_km = road_energy_passenger_km.groupby(['date','economy','medium','transport_type','measure'])['value'].sum().reset_index()
    #sepreate road energy and passenger_km
    road_energy = road_energy_passenger_km.loc[road_energy_passenger_km['measure']=='energy']
    road_passenger_km = road_energy_passenger_km.loc[road_energy_passenger_km['measure']=='passenger_km']
    #drop measure
    road_energy = road_energy.drop(columns=['measure'])
    road_passenger_km = road_passenger_km.drop(columns=['measure'])
    return road_energy, road_passenger_km


def plot_activity(activity,previous_energy_activity_wide,calculated_road_passenger_km):
    #plot the new activity against the activity in previous_energy_activity_wide to see how it looks
    #merge the new activity with the previous activity

    #join road_passenger_km to previous_energy_activity_wide. But firstrename medium to 'road_calcualted'
    calculated_road_passenger_km['medium'] = 'road_calculated'
    previous_energy_activity_wide = previous_energy_activity_wide.merge(calculated_road_passenger_km.rename(columns={'value':'activity'}),how='outer',on=['date',	'economy',	'medium', 'transport_type'])

    previous_energy_activity_wide = previous_energy_activity_wide.merge(activity,how='outer',on=['date',	'economy',	'medium', 'transport_type'])
    #rename the columns
    previous_energy_activity_wide = previous_energy_activity_wide.rename(columns={'activity':'previous_activity','value':'new_activity'})
    #drop energy and intensity
    previous_energy_activity_wide = previous_energy_activity_wide.drop(columns=['energy','intensity'])
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

# def estimate_activity(non_road_energy,road_combined_data):
#NOTE THAT THE PASSENGER ACTICVITY ESTIAMTE HERE IS JUST FOR ANALYSIS AND NOT THE FINAL ESTIAMTE AS WE HAVE A MUCH MORE DETAILED WAY OF ESTIMATING PASSENGER ACTIVITY
calculated_road_energy, calculated_road_passenger_km = extract_calculated_road_energy_passenger_km(road_combined_data)
#combined non road and road energy. 
energy = pd.concat([non_road_energy,calculated_road_energy])

previous_draft_transport_data_system_df = data_estimation_functions.import_previous_draft_selections()
previous_energy_activity_wide = prepare_previous_energy_activity_data(previous_draft_transport_data_system_df)
previous_intensity = calcualte_intensity_from_previous_data(previous_energy_activity_wide)

#now times the intenstiy by the energy values weve calcualted to get the activity
energy_activity = energy.merge(previous_intensity,how='outer',on=['date',	'economy',	'medium', 'transport_type'])
energy_activity['activity'] = energy_activity['value']/energy_activity['intensity']
#%%
activity = clean_activity(energy_activity)
#%%
plot=False
if plot:

    plot_activity(activity,previous_energy_activity_wide, calculated_road_passenger_km)#analysis_and_plotting_functions.

#drop rows where measure = passenger_km and medium = road
activity_non_passenger_road = activity.copy()
activity_non_passenger_road = activity_non_passenger_road.loc[~((activity_non_passenger_road['measure']=='passenger_km')&(activity_non_passenger_road['medium']=='road'))]
# return activity


#%%


#%%
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

# %%
