#create functions to estiamte data using other data, in a way that can be repeated
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
from PIL import Image
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import itertools
import data_formatting_functions

#eg from
#  Estimate Energy and passenger km using stocks:
# vkm = mileage * stocks
# energy = vkm * efficiency
# pkm = vkm * occupancy

#put it all thorugh calculate_new_data() which will:
# Using mileage, eff, occ and stocks we can estimate passenger km and energy. Where we are missing data eg. stocks, we will leave an na. 
# enable adjusting the mileage, occ and eff data by specified ranges to get a range of options

def calculate_energy_and_passenger_km(stocks_mileage_occupancy_efficiency_combined_data, paths_dict):

    #make combined data wide so we have a column for each measure
    INDEX_COLS_no_measure = paths_dict['INDEX_COLS'].copy()
    INDEX_COLS_no_measure.remove('measure')
    data = stocks_mileage_occupancy_efficiency_combined_data.pivot(index = INDEX_COLS_no_measure, columns = 'measure', values = 'value').reset_index()
    #to prevent any issues with div by 0 we will replace all 0s with nans. then any nans timesed by anything will be nan
    data = data.replace(0,np.nan)

    data['vehicle_km'] = data['mileage'] * data['stocks']
    data['passenger_km'] = data['vehicle_km'] * data['occupancy']
    data['energy'] = data['vehicle_km'] * data['new_vehicle_efficiency']

    #make long again
    data = data.melt(id_vars = INDEX_COLS_no_measure, var_name = 'measure', value_name = 'value')#todo i dont know how this will interact with the other possible columns we'll have. maybrwe should have id vars = data.columns
    
    #set dataset to 'estimated'
    data['dataset'] = 'estimated $ calculate_energy_and_passenger_km()'
    #todo: add in the range options
    return data

def estimate_road_freight_energy_use():
    #take in aperc data for energy use in road and calculate the remaining energy use after subtracting energy use for passenger road.
    # - if it doesnt match expectations we can either: 
    # 	- scale passenger down/up across all metricsm to make room for freight
    # 	- estimate new total raod enegry use (not popualr but for some economys we could expect they have different transport use than they have shown) and scale frieght up/down
    # 	- pick a vlaue from another dataset and rescale the other transport energy uses
    


# Road freight system:
# Will be the remaineder of enegry once weve calcualted passenger road. then activity is based on that.

# Estimate_freight_energy_use()
# Using the road energy use data we have from esto we can estimate freigth energy use from the remainder. We can then comapre that against any data we do have and check wether it matches what we expect.
# - if it doesnt match expectations we can either: 
# 	- scale passenger down/up across all metricsm to make room for freight
# 	- estimate new total raod enegry use (not popualr but for some economys we could expect they have different transport use than they have shown) and scale frieght up/down
# 	- pick a vlaue from another dataset and rescale the other transport energy uses

# Estimate_freight_activity()
# Its so hard to know what freight activity is. But for some economys we do have estimates. So for now, using the data we have we could esimate intensity and then estiamte activity for everyone using a weighted average from that


# Non road:
# This will be just like road frieght. For now we will jsutn split non road into freight/passenger after everything else.

# estimate_air_ship_rail_energy_use()
# Use esto values. Where we aren't confident we could:
# 	- pick a vlaue from another dataset and rescale the other transport energy uses
# 	-  change by a proportiona and rescale other transport enegry uses

# estiamte_passenger_freight_splits()
# Where we do have data from other datasets we can calcualte a split between passs and freight, if not, we can use a weighted average from what we do have

# Estimate_activity()
# Its so hard to know what activity is. But for some economys we do have estimates. So for now, using the data we have we could esimate intensity and then estiamte activity for everyone using a weighted average from that