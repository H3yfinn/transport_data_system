#%%

import pandas as pd
import os
import re
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
#HERE WE WILL START TO CREATE A SET OF CONVERSIONS FROM COMMON VALUES TO WHAT WE NEED WITHIN APERC.

def create_factors_to_convert_mpg_to_useful_outputs_for_fuel(fuel_energy_content_mj_kg, density_kg_l, fuel):
    """
    Create values to convert miles per gallon to kilometers per petajoule for a given fuel.

    :param fuel_energy_content_mj_kg: Energy content of the fuel in megajoules per kilogram.
    :param density_kg_l: Density of the fuel in kilograms per liter.
    :return: a dict of conversion factors
    """

    # Conversion factors
    miles_to_km = 1.60934  # 1 mile = 1.60934 km
    gallons_to_liters = 3.78541  # 1 gallon = 3.78541 liters
    kj_to_pj = 1e-12  # 1 petajoule = 10^12 kilojoules
    mj_to_kj = 1e3  # 1 megajoule = 10^3 kilojoules

    # Convert mpg to km/L
    km_per_liter = miles_to_km / gallons_to_liters

    # Convert km/L to km/kg
    km_per_kg = km_per_liter / density_kg_l

    # Convert km/kg to km/PJ
    km_per_pj = km_per_kg / ((fuel_energy_content_mj_kg * mj_to_kj) * kj_to_pj)


    # new_df = {
    #     'mpg_to_km_per_liter': km_per_liter,
    #     'mpg_to_km_per_pj': km_per_pj,
    #     'mpg_to_billion_km_per_pj': km_per_pj / 1e9,
    #     'fuel_energy_content_mj_kg': fuel_energy_content_mj_kg,
    #     'density_kg_l': density_kg_l
    # }
    #create df with cols fuel	conversion_factor	value	original_unit	final_unit
    new_df = pd.DataFrame(columns=['fuel', 'conversion_factor', 'value', 'original_unit', 'final_unit'])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'mpg_to_km_per_liter', 'value': km_per_liter, 'original_unit': 'mpg', 'final_unit': 'km/L'}, index=[0])])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'mpg_to_km_per_pj', 'value': km_per_pj, 'original_unit': 'mpg', 'final_unit': 'km/PJ'}, index=[0])])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'mpg_to_billion_km_per_pj', 'value': km_per_pj / 1e9, 'original_unit': 'mpg', 'final_unit': 'billion km/PJ'}, index=[0])])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'fuel_energy_content_mj_kg', 'value': fuel_energy_content_mj_kg, 'original_unit': 'MJ/kg', 'final_unit': 'MJ/kg'}, index=[0])])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'density_kg_l', 'value': density_kg_l, 'original_unit': 'kg/L', 'final_unit': 'kg/L'}, index=[0])])
    return new_df

#%%
# Hydrogen (H2)	120-142 MJ/kg
# Methane (CH4)	50-55 MJ/kg
# Methanol (CH3OH)	22.7 MJ/kg
# Petrol/gasoline	44-46 MJ/kg
# Diesel fuel	42-46 MJ/kg
# Crude oil	42-47 MJ/kg
# Liquefied petroleum gas (LPG)	46-51 MJ/kg
# Natural gas	42-55 MJ/kg
# 26.8 ehtanol#https://energyeducation.ca/encyclopedia/Energy_density
# 38 biodiesel #https://energyeducation.ca/encyclopedia/Energy_density
#jet fuel 42.8 MJ/kg #chatgpt
#biojet = jet fuel #chatgpt

# Energy content per kg and densities for various fuels
energy_per_kg = {
    'hydrogen': 142, 'methanol': 22.7, 'petrol': 46, 'diesel': 46, 'crude_oil': 47, 
    'lpg': 51, 'natural_gas': 55, 'ethanol': 26.8, 'biodiesel': 38, 'jet_fuel': 42.8, 'biojet': 42.8
}
density_kg_l = {
    'hydrogen': 0.071, 'methanol': 0.791, 'petrol': 0.755, 'diesel': 0.832, 'crude_oil': 0.82, 
    'lpg': 0.493, 'natural_gas': 0.75, 'ethanol': 0.789, 'biodiesel': 0.88, 'jet_fuel': 0.804, 'biojet': 0.804
}

# Generate DataFrame for each fuel and combine them
all_fuel_dfs = pd.DataFrame()
for fuel, energy_content in energy_per_kg.items():
    fuel_df = create_factors_to_convert_mpg_to_useful_outputs_for_fuel(energy_content, density_kg_l[fuel], fuel)
    all_fuel_dfs = pd.concat([all_fuel_dfs, fuel_df], ignore_index=True)

#%%

conversion_factors_nested = {
    'oil': {
        'million_tonnes_to_pj': 41.666666666666664,
        'million_barrels_to_pj': 6.097560975609756
    },
    'coal': {
        'million_tonnes_to_pj': 15.037593984962406
    },
    'lng': {
        'million_tonnes_per_annum_to_pj': 50.0
    },
    'natural_gas': {
        'billion_cubic_meters_to_pj': 35.714285714285715,
        'billion_cubic_feet_to_pj': 1.019367991845056
    },
    'hydrogen': {
        'million_tonnes_to_pj': 125.0
    },
    'all': {
        'trillion_british_thermal_units_to_pj': 1.0548523206751055,
        'terawatt_hour_to_pj': 3.597122302158273,
        'gigacalorie_to_pj': 4.184100418410042
    }
}
#%%
#%%

# Convert the nested dictionary to a DataFrame format
data_for_df = []
for fuel_type, conversions in conversion_factors_nested.items():
    for conversion, value in conversions.items():
        original_unit, final_unit = conversion.split('_to_')
        data_for_df.append({
            'fuel': fuel_type,
            'conversion_factor': conversion,
            'value': value,
            'original_unit': original_unit,
            'final_unit': final_unit
        })
# Create DataFrame
conversion_factors_df = pd.DataFrame(data_for_df)


#%%
concat_conversion_factors_df = pd.concat([conversion_factors_df, all_fuel_dfs], ignore_index=True)
concat_conversion_factors_df.to_csv('config/conversion_factors.csv', index=False)
#%%
