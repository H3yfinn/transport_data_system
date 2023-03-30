#load transport_data_system/intermediate_data/selection_process/DATE20230328/interpolated_stocks_mileage_occupancy_efficiency_combined_data_concordance.pkl then see where we dont have data
#%%
import pandas as pd
import numpy as np
import os
import re
import data_formatting_functions
x = pd.read_pickle('../intermediate_data/selection_process/DATE20230328/interpolated_stocks_mileage_occupancy_efficiency_combined_data_concordance.pkl')
y = pd.read_pickle('../intermediate_data/selection_process/DATE20230328/combined_data_concordance.pkl')
z = pd.read_pickle('../intermediate_data/selection_process/DATE20230328/combined_data.pkl')
calculated_passenger_energy_combined_data = pd.read_pickle('../intermediate_data/selection_process/DATE20230329/calculated_passenger_energy_combined_data.pkl')
#   passenger_road_combined_data.to_pickle(paths_dict['intermediate_folder']+'\\passenger_road_combined_data_TEST.pkl')
#     passenger_road_combined_data_concordance.to_pickle(paths_dict['intermediate_folder']+'\\passenger_road_combined_data_concordance_TEST.pkl')
passenger_road_combined_data = pd.read_pickle('../intermediate_data/selection_process/DATE20230329/passenger_road_combined_data.pkl')
passenger_road_combined_data_concordance = pd.read_pickle('../intermediate_data/selection_process/DATE20230329/passenger_road_combined_data_concordance.pkl')

# %%

#get unique measures in x and filter for them in y:
unique_measures = x.measure.unique()
y = y.loc[y.measure.isin(unique_measures)]
z = z.loc[z.measure.isin(unique_measures)]
calculated_passenger_energy_combined_data2 = calculated_passenger_energy_combined_data.copy()
calculated_passenger_energy_combined_data2 = calculated_passenger_energy_combined_data2.loc[calculated_passenger_energy_combined_data2.measure.isin(unique_measures)]
# %%
#to identify where we are missing data we will use data_formatting_functions.create_concordance_from_combined_data
calculated_passenger_energy_combined_data_concordance = data_formatting_functions.create_concordance_from_combined_data(calculated_passenger_energy_combined_data2)

#now join combined data to the concordance so we can see where we are missing data
cols = calculated_passenger_energy_combined_data_concordance.columns.tolist()

calculated_passenger_energy_merge = calculated_passenger_energy_combined_data_concordance.merge(calculated_passenger_energy_combined_data2, on = cols, how = 'left')

#%%
#now we want to see where we have missing data. we will print the number of nas for each unique measure. We will also print the number of nas for every group of vehicle types, measures and drives
measure_nas = calculated_passenger_energy_merge.copy()
measure_nas = measure_nas.loc[measure_nas.value.isna()]
#swap nas for 1's
measure_nas.value = 1
measure_nas = measure_nas.groupby('measure').value.sum()

v_m_d_nas = calculated_passenger_energy_merge.copy()
v_m_d_nas = v_m_d_nas.loc[v_m_d_nas.value.isna()]
#swap nas for 1's
v_m_d_nas.value = 1
v_m_d_nas = v_m_d_nas.groupby(['vehicle_type','measure','drive']).value.sum()

#plot them as a wide bar chart
# measure_nas.plot.bar()
v_m_d_nas.plot.bar( figsize = (30,10))

# %%
#we are missing about the same am ount of data for most groups. So we should just start by filling in defaults and then we can see if we need to do anything else
# %%


#%%
# INDEX_COLS_NO_YEAR = [
#  'economy',
#  'measure',
#  'vehicle_type',
#  'unit',
#  'medium',
#  'transport_type',
#  'drive',
#  'fuel',
#  'frequency',
#  'scope']
# # Check if we are missing eff occ and mileage data for this index row ('05_PRC', 'passenger_km', 'bus', 'passenger_km', 'road', 'passenger', 'bev', 'all', 'yearly', 'national') for 2017. as we are only getting "8th_edition_transport_model $ reference" and  "passenger_km_est $ 8th/_passenger_km_est"  for this row
# a = calculated_passenger_energy_combined_data.set_index(INDEX_COLS_NO_YEAR)
# index_row = ('05_PRC', 'passenger_km', 'bus', 'passenger_km', 'road', 'passenger', 'bev', 'all', 'yearly', 'national')

# year = 2017
# a.loc[index_row]

#%%


road_freight_energy_combined_data = estimate_road_freight_energy_use(combined_data,passenger_road_combined_data)












def estimate_road_freight_energy_use(combined_data,passenger_road_combined_data):
    #load in the combined_data from paths_dict['combined_data']
    egeda_energy_road_selection_dict = {'measure': 
        ['energy'],
    'medium': ['road'],
    'transport_type': ['freight'],
    'dataset': ['EGEDA_merged']}
    egeda_energy_road_combined_data = data_formatting_functions.filter_for_specifc_data(egeda_energy_road_selection_dict, combined_data)

    #and also grab the energy data for passenger road
    passenger_road_combined_data = passenger_road_combined_data[passenger_road_combined_data['measure'] == 'energy']

    #sum the energy use for passenger road by economy and year
    passenger_road_combined_data = passenger_road_combined_data.groupby(['economy','year']).sum().reset_index()

    #sum the energy use for egeda energy data by transport type, economy and year
    egeda_energy_road_combined_data = egeda_energy_road_combined_data.groupby(['transport_type','economy','year']).sum().reset_index()

    #pivot transport type to a column
    egeda_energy_road_combined_data = egeda_energy_road_combined_data.pivot(index = ['economy','year'], columns = 'transport_type', values = 'value').reset_index()

    #calcualte total road energy use by economy and year
    egeda_energy_road_combined_data['egeda_total_road_energy_use'] = egeda_energy_road_combined_data['freight'] + egeda_energy_road_combined_data['passenger']

    #merge the passenger road energy use with the egeda energy data
    #first rename value to calculated_passenger_road_energy_use
    passenger_road_combined_data = passenger_road_combined_data.rename(columns = {'value': 'calculated_passenger_road_energy_use'})
    egeda_energy_road_combined_data = egeda_energy_road_combined_data.merge(passenger_road_combined_data, on = ['economy','year'], how = 'outer')

    analyse_missing_data(egeda_energy_road_combined_data)   

    #calcualte the remaining freight energy use as total_road_energy_use - calculated_passenger_road_energy_use
    egeda_energy_road_combined_data['calculated_remaining_freight_energy_use'] = egeda_energy_road_combined_data['egeda_total_road_energy_use'] - egeda_energy_road_combined_data['calculated_passenger_road_energy_use']
    #compare that vlaue to the egeda energy use for freight by graphing it:

    #frist rename columns then put all values data in one column with the names of the columns in a new column called 'type'
    egeda_energy_road_combined_data = egeda_energy_road_combined_data.rename(columns = {'freight': 'egeda_freight', 'passenger': 'egeda_passenger'})
    egeda_energy_road_combined_data = pd.melt(egeda_energy_road_combined_data, id_vars = ['economy','year'], var_name = 'type', value_name = 'value')

    graph_egeda_road_energy_use_vs_calculated(egeda_energy_road_combined_data)
    prompt = 'Does the graph show that the calculated remaining road energy use matches the egeda energy use for freight? y/n'

    input1 = input(prompt)
    if input1 == 'y':
        pass
    elif input1 == 'n':
        #save results to csv and exit
        egeda_energy_road_combined_data.to_csv('egeda_energy_road_combined_data.csv')
        logger.info('egeda_energy_road_combined_data saved to csv')
        sys.exit()
    
    egeda_energy_road_combined_data = clean_energy_and_passenger_km(combined_data,egeda_energy_road_combined_data)
    
    return egeda_energy_road_combined_data
    #take in aperc data for energy use in road and calculate the remaining energy use after subtracting energy use for passenger road.
    # - if it doesnt match expectations we can either: 
    # 	- scale passenger down/up across all metricsm to make room for freight
    # 	- estimate new total raod enegry use (not popualr but for some economys we could expect they have different transport use than they have shown) and scale frieght up/down
    # 	- pick a vlaue from another dataset and rescale the other transport energy uses
def graph_egeda_road_energy_use_vs_calculated(egeda_energy_road_combined_data):
    import plotly.express as px
    #graph the egeda energy use for freight vs the calculated energy use for freight. Use plotly and plot each economy as a facet
    fig = px.scatter(egeda_energy_road_combined_data, x = 'year', y = 'value', color = 'type', facet_col = 'economy', facet_col_wrap = 7)
    #save as html
    fig.write_html('plotting_output/data_selection/analysis/egeda_road_energy_use_vs_calculated.html', auto_open = True)

def analyse_missing_data(egeda_energy_road_combined_data):
    #analyse missing data:
    missing_total_road_energy_use = egeda_energy_road_combined_data[egeda_energy_road_combined_data['egeda_total_road_energy_use'].isna()]
    missing_passenger_road_energy_use = egeda_energy_road_combined_data[egeda_energy_road_combined_data['calculated_passenger_road_energy_use'].isna()]
    #show user the missing data
    logger.info('\nmissing_total_road_energy_use:\n')
    logger.info(missing_total_road_energy_use)
    logger.info('\nmissing_passenger_road_energy_use:\n')
    logger.info(missing_passenger_road_energy_use)

def clean_energy_and_passenger_km(combined_data,egeda_energy_road_combined_data):
    #if the graph shows that the calculated remaining road energy use matches the egeda energy use for freight then we can use the calculated remaining road energy use as the energy use for freight
    #so clean up the data and return it as the freight energy use for each eocnomy for each year
    egeda_energy_road_combined_data = egeda_energy_road_combined_data[egeda_energy_road_combined_data['type'] == 'calculated_remaining_freight_energy_use']
    egeda_energy_road_combined_data = egeda_energy_road_combined_data[['economy','year','value']]

    #create columns for everything in the combined data columns:
    egeda_energy_road_combined_data['measure'] = 'energy'
    egeda_energy_road_combined_data['unit'] = 'pj'
    egeda_energy_road_combined_data['medium'] = 'road'
    egeda_energy_road_combined_data['transport_type'] = 'freight'
    egeda_energy_road_combined_data['dataset'] = 'calculation'
    egeda_energy_road_combined_data['source'] = 'egeda'
    egeda_energy_road_combined_data['unit'] = 'pj'
    egeda_energy_road_combined_data['fuel'] = 'all'
    egeda_energy_road_combined_data['comment'] = 'no_comment'
    egeda_energy_road_combined_data['scope'] = 'national'
    egeda_energy_road_combined_data['frequency'] = 'yearly'
    egeda_energy_road_combined_data['drive'] = 'all'
    egeda_energy_road_combined_data['vehicle_type'] = 'all'

    #doublec check we have all the cols
    assert set(egeda_energy_road_combined_data.columns) == set(combined_data.columns)

    return egeda_energy_road_combined_data

def estimate_freight_activity():
    
    
    # Estimate_freight_activity()
    # Its so hard to know what freight activity is. But for some economys we do have estimates. So for now, using the data we have we could esimate intensity and then estiamte activity for everyone using a weighted average from that
    return
