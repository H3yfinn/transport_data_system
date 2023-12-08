#%%
#note that this will take a long time to run!
#%%
#set working directory as one folder back
import os
import re
import pandas as pd
import numpy as np
import datetime
import sys
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')
sys.path.append("./aggregation_code")
import utility_functions
#%%
FILE_DATE_ID = str(datetime.datetime.now().strftime('%Y%m%d'))
#THESE ARE PULLED FROM THE CONFIG FILE IN TRANSPORT MODEL
INDEX_COLS = ['Date', 'Economy', 'Measure', 'Vehicle Type', 'Medium',
       'Transport Type','Drive', 'Scenario', 'Unit']
#ESTO/9th_EBT to transport model mappings:
medium_mapping = {'air': '15_01_domestic_air_transport', 'road': '15_02_road', 'rail': '15_03_rail', 'ship': '15_04_domestic_navigation', 'pipeline':'15_05_pipeline_transport', 'nonspecified': '15_06_nonspecified_transport'}

transport_type_mapping = {'passenger': '01_passenger', 'freight': '02_freight', 'combined':'x'}#egeda data doesnt break dwown non road into transport type
inverse_transport_type_mapping = {'15_01_01_passenger': 'passenger', '15_01_02_freight': 'freight', '15_02_01_passenger': 'passenger', '15_02_02_freight': 'freight', '15_03_01_passenger': 'passenger', '15_03_02_freight': 'freight', '15_04_01_passenger': 'passenger', '15_04_02_freight': 'freight', 'x':'combined'}

vehicle_type_mapping_passenger = {'suv': '15_02_01_03_sports_utility_vehicle', 'lt': '15_02_01_04_light_truck', 'car': '15_02_01_02_car', 'bus': '15_02_01_05_bus', '2w': '15_02_01_01_two_wheeler','all':'x'}

vehicle_type_mapping_freight = {'mt': '15_02_02_03_medium_truck', 'lcv': '15_02_02_02_light_commercial_vehicle', 'ht': '15_02_02_04_heavy_truck', '2w': '15_02_02_01_two_wheeler_freight', 'all':'x'}

drive_mapping_inversed = {'x':'all',
    '15_02_01_01_01_diesel_engine': 'ice_d',
    '15_02_01_01_02_gasoline_engine': 'ice_g',
    '15_02_01_01_03_battery_ev': 'bev',
    '15_02_01_01_04_compressed_natual_gas': 'cng',
    '15_02_01_01_05_plugin_hybrid_ev_gasoline': 'phev_g',
    '15_02_01_01_06_plugin_hybrid_ev_diesel': 'phev_d',
    '15_02_01_01_07_liquified_petroleum_gas': 'lpg',
    '15_02_01_01_08_fuel_cell_ev': 'fcev',

    '15_02_01_02_01_diesel_engine': 'ice_d',
    '15_02_01_02_02_gasoline_engine': 'ice_g',
    '15_02_01_02_03_battery_ev': 'bev',
    '15_02_01_02_04_compressed_natual_gas': 'cng',
    '15_02_01_02_05_plugin_hybrid_ev_gasoline': 'phev_g',
    '15_02_01_02_06_plugin_hybrid_ev_diesel': 'phev_d',
    '15_02_01_02_07_liquified_petroleum_gas': 'lpg',
    '15_02_01_02_08_fuel_cell_ev': 'fcev',

    '15_02_01_03_01_diesel_engine': 'ice_d',
    '15_02_01_03_02_gasoline_engine': 'ice_g',
    '15_02_01_03_03_battery_ev': 'bev',
    '15_02_01_03_04_compressed_natual_gas': 'cng',
    '15_02_01_03_05_plugin_hybrid_ev_gasoline': 'phev_g',
    '15_02_01_03_06_plugin_hybrid_ev_diesel': 'phev_d',
    '15_02_01_03_07_liquified_petroleum_gas': 'lpg',
    '15_02_01_03_08_fuel_cell_ev': 'fcev',

    '15_02_01_04_01_diesel_engine': 'ice_d',
    '15_02_01_04_02_gasoline_engine': 'ice_g',
    '15_02_01_04_03_battery_ev': 'bev',
    '15_02_01_04_04_compressed_natual_gas': 'cng',
    '15_02_01_04_05_plugin_hybrid_ev_gasoline': 'phev_g',
    '15_02_01_04_06_plugin_hybrid_ev_diesel': 'phev_d',
    '15_02_01_04_07_liquified_petroleum_gas': 'lpg',
    '15_02_01_04_08_fuel_cell_ev': 'fcev',

    '15_02_01_05_01_diesel_engine': 'ice_d',
    '15_02_01_05_02_gasoline_engine': 'ice_g',
    '15_02_01_05_03_battery_ev': 'bev',
    '15_02_01_05_04_compressed_natual_gas': 'cng',
    '15_02_01_05_05_plugin_hybrid_ev_gasoline': 'phev_g',
    '15_02_01_05_06_plugin_hybrid_ev_diesel': 'phev_d',
    '15_02_01_05_07_liquified_petroleum_gas': 'lpg',
    '15_02_01_05_08_fuel_cell_ev': 'fcev',

    '15_02_02_01_01_diesel_engine': 'ice_d',
    '15_02_02_01_02_gasoline_engine': 'ice_g',
    '15_02_02_01_03_battery_ev': 'bev',
    '15_02_02_01_04_compressed_natual_gas': 'cng',
    '15_02_02_01_05_plugin_hybrid_ev_gasoline': 'phev_g',
    '15_02_02_01_06_plugin_hybrid_ev_diesel': 'phev_d',
    '15_02_02_01_07_liquified_petroleum_gas': 'lpg',
    '15_02_02_01_08_fuel_cell_ev': 'fcev',

    '15_02_02_02_01_diesel_engine': 'ice_d',
    '15_02_02_02_02_gasoline_engine': 'ice_g',
    '15_02_02_02_03_battery_ev': 'bev',
    '15_02_02_02_04_compressed_natual_gas': 'cng',
    '15_02_02_02_05_plugin_hybrid_ev_gasoline': 'phev_g',
    '15_02_02_02_06_plugin_hybrid_ev_diesel': 'phev_d',
    '15_02_02_02_07_liquified_petroleum_gas': 'lpg',
    '15_02_02_02_08_fuel_cell_ev': 'fcev',

    '15_02_02_03_01_diesel_engine': 'ice_d',
    '15_02_02_03_02_gasoline_engine': 'ice_g',
    '15_02_02_03_03_battery_ev': 'bev',
    '15_02_02_03_04_compressed_natual_gas': 'cng',
    '15_02_02_03_05_plugin_hybrid_ev_gasoline': 'phev_g',
    '15_02_02_03_06_plugin_hybrid_ev_diesel': 'phev_d',
    '15_02_02_03_07_liquified_petroleum_gas': 'lpg',
    '15_02_02_03_08_fuel_cell_ev': 'fcev',

    '15_02_02_04_01_diesel_engine': 'ice_d',
    '15_02_02_04_02_gasoline_engine': 'ice_g',
    '15_02_02_04_03_battery_ev': 'bev',
    '15_02_02_04_04_compressed_natual_gas': 'cng',
    '15_02_02_04_05_plugin_hybrid_ev_gasoline': 'phev_g',
    '15_02_02_04_06_plugin_hybrid_ev_diesel': 'phev_d',
    '15_02_02_04_07_liquified_petroleum_gas': 'lpg',
    '15_02_02_04_08_fuel_cell_ev': 'fcev'}

def convert_outlook_data_system_output_to_transport_data_system_input(FILE_DATE_ID):
    #note that this can be done on anymeasure or dataframe as long as it contains either fuels or technologies.
    date_id = utility_functions.get_latest_date_for_data_file('input_data/EGEDA', 'model_df_wide_')
    energy_use_esto = pd.read_csv(f'input_data/EGEDA/model_df_wide_{date_id}.csv')

    #drop subtotals from the data suing 'is_subtotal' column. this orevents us from double counting
    energy_use_esto = energy_use_esto.loc[energy_use_esto['is_subtotal']==False].copy()
    energy_use_esto.drop(columns=['is_subtotal'], inplace=True)

    df = energy_use_esto.melt(id_vars=['scenarios', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'fuels', 'subfuels'], var_name='Date', value_name='Value')

    value_col = 'Energy'
    #rename value col to Value
    df.rename(columns={value_col:'Value'}, inplace=True)

    #match up transport type/vehicle type/drive combinations with sectors in model variables:
    df = df.loc[df['sectors']=='15_transport_sector'].copy()

    #printhem all:
    unique_sectors = df[['scenarios', 'Date', 'economy', 'sectors', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'Value']].drop_duplicates()

    new_df = pd.DataFrame(columns=INDEX_COLS + ['Value'])
    #we can identify transport_type from sub2sectors, vehicle type from sub3sectors (if its road), drive from sub4sectors, and medium from sub1sectors, drive from sub4sectors (if its road) and medium from sub1sectors
    for row in unique_sectors.iterrows():
        transport_type = row[1]['sub2sectors']
        vehicle_type = row[1]['sub3sectors']
        drive = row[1]['sub4sectors']
        medium = row[1]['sub1sectors']
        # if all values above are x then skip
        if transport_type == 'x' and vehicle_type == 'x' and drive == 'x' and medium == 'x':
            continue
        inverse_medium_mapping = {v: k for k, v in medium_mapping.items()}
        new_medium = inverse_medium_mapping[medium]
        new_transport_type = inverse_transport_type_mapping[transport_type]

        inverse_vehicle_type_mapping_passenger = {v: k for k, v in vehicle_type_mapping_passenger.items()}
        inverse_vehicle_type_mapping_freight = {v: k for k, v in vehicle_type_mapping_freight.items()}

        if new_transport_type == 'passenger':
            new_vehicle_type = inverse_vehicle_type_mapping_passenger[vehicle_type]
        elif new_transport_type == 'freight':
            new_vehicle_type = inverse_vehicle_type_mapping_freight[vehicle_type]
        elif new_transport_type == 'combined':
            new_vehicle_type = 'all'
        else:
            raise ValueError(f'new_transport_type {new_transport_type} not recognised')

        new_drive_type = drive_mapping_inversed[drive]

        new_df_row = {'Date':row[1]['Date'], 'Economy':row[1]['economy'], 'Vehicle Type':new_vehicle_type, 'Medium':new_medium, 'Transport Type':new_transport_type, 'Drive':new_drive_type, 'Scenario':row[1]['scenarios'], 'Value':row[1]['Value'], 'Measure':'Energy', 'Unit':'PJ'}

        new_df = pd.concat([new_df, pd.DataFrame(new_df_row, index=[0])])
        
        # if len(new_df) > 1000:
        #     break
        
    #set nas to 0s
    new_df['Value'] = new_df['Value'].fillna(0)

    #sum up any duplicates
    new_final_df = new_df.groupby(INDEX_COLS).sum(numeric_only=True).reset_index().copy()

    #find any nans in other cols
    if new_final_df.isna().sum().sum() > 0:
        nans = new_final_df[new_final_df.isna().any(axis=1)]
        raise ValueError(f'There are nans in the esto ouput. Please check {nans}')
    #pivot the date column
    INDEX_COLS_no_date = INDEX_COLS.copy()
    INDEX_COLS_no_date.remove('Date')
    new_final_df = new_final_df.pivot(index=INDEX_COLS_no_date, columns='Date', values='Value').reset_index()

    #save this file to output_data\for_other_modellers
    new_final_df.to_csv(f'intermediate_data/EGEDA/{FILE_DATE_ID}_9th_outlook_esto.csv', index=False)

#%%
convert_outlook_data_system_output_to_transport_data_system_input(FILE_DATE_ID)

#%%