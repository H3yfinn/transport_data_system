#%%

import pandas as pd
import os
import re
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')
#HERE WE WILL START TO CREATE A SET OF CONVERSIONS FROM COMMON VALUES TO WHAT WE NEED WITHIN APERC.

def create_factors_to_convert_mpg_to_useful_outputs_for_fuel(fuel_energy_content_mj_kg, density_kg_l, emissions_factor, fuel):
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
    mj_to_pj = 1e-9  # 1 petajoule = 10^9 megajoules
    mj_to_kj = 1e3  # 1 megajoule = 10^3 kilojoules
    MT_to_kg = 1e9 #1 megatonne = 10^9 kg
    kg_to_kt = 1e-6 #1 kilogram = 10^-6 megatonne

    # Convert mpg to km/L
    mgp_to_km_per_liter = miles_to_km / gallons_to_liters

    # Convert km/L to km/kg
    km_per_l_to_km_per_kg = mgp_to_km_per_liter / density_kg_l

    # Convert km/kg to km/PJ
    km_per_kg_to_km_per_pj = km_per_l_to_km_per_kg / ((fuel_energy_content_mj_kg * mj_to_kj) * kj_to_pj)
    km_per_kg_to_km_per_mj = km_per_kg_to_km_per_pj * mj_to_pj
    
    kt_to_pj = (fuel_energy_content_mj_kg * mj_to_pj) / kg_to_kt
    #now convert emissions factors*
    #first convert to MT to kg and PJ to mj, so we can get to kgC02/kg fuel, and kgc02/L fuel, then convert to kgCO2/km
    kgc02_per_mj = ((emissions_factor*MT_to_kg)*mj_to_pj)
    kgc02_per_kg_fuel = kgc02_per_mj * fuel_energy_content_mj_kg
    kgc02_per_l = kgc02_per_kg_fuel * density_kg_l
    
    # #then do some cross cutting ones (note that this could result in div/0 errors, but we will deal with that later)
    # try:
    #     km_per_mj_to_km_per_kgc02 = km_per_kg_to_km_per_mj / kgc02_per_mj
    # except ZeroDivisionError:
    #     km_per_mj_to_km_per_kgc02 = float('inf')#i dont know where i was going with this one. 
    # new_df = {
    #     'mpg_to_km_per_liter': km_per_liter,
    #     'mpg_to_km_per_pj': km_per_pj,
    #     'mpg_to_billion_km_per_pj': km_per_pj / 1e9,
    #     'fuel_energy_content_mj_kg': fuel_energy_content_mj_kg,
    #     'density_kg_l': density_kg_l
    # }
    #create df with cols fuel	conversion_factor	value	original_unit	final_unit
    new_df = pd.DataFrame(columns=['fuel', 'conversion_factor', 'value', 'original_unit', 'final_unit'])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'mpg_to_km_per_liter', 'value': mgp_to_km_per_liter, 'original_unit': 'mpg', 'final_unit': 'km/L'}, index=[0])])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'mpg_to_km_per_pj', 'value': km_per_kg_to_km_per_pj, 'original_unit': 'mpg', 'final_unit': 'km/PJ'}, index=[0])])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'mpg_to_billion_km_per_pj', 'value': km_per_kg_to_km_per_pj / 1e9, 'original_unit': 'mpg', 'final_unit': 'billion km/PJ'}, index=[0])])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'fuel_energy_content_mj_kg', 'value': fuel_energy_content_mj_kg, 'original_unit': 'kg', 'final_unit': 'MJ'}, index=[0])])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'density_kg_per_l', 'value': density_kg_l, 'original_unit': 'L', 'final_unit': 'kg'}, index=[0])])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'km_per_kg_to_km_per_mj', 'value': km_per_kg_to_km_per_mj, 'original_unit': 'MJ', 'final_unit': 'km'}, index=[0])])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'kt_to_pj', 'value': kt_to_pj, 'original_unit': 'kt', 'final_unit': 'PJ'}, index=[0])])
    #from this we can also add in conversions from km/L to km/PJ 
    # km/L:
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'km_per_liter_to_km_per_pj', 'value': mgp_to_km_per_liter / ((fuel_energy_content_mj_kg * mj_to_pj)), 'original_unit': 'km/L', 'final_unit': 'km/PJ'}, index=[0])])
    #km / L to km / mj
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'km_per_liter_to_km_per_mj', 'value': mgp_to_km_per_liter / (fuel_energy_content_mj_kg), 'original_unit': 'km/L', 'final_unit': 'km/MJ'}, index=[0])])
    #and now add in the emissions factors
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'kgc02_per_l', 'value': kgc02_per_l, 'original_unit': 'L', 'final_unit': 'kgC02'}, index=[0])])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'kgc02_per_kg_fuel', 'value': kgc02_per_kg_fuel, 'original_unit': 'kg fuel', 'final_unit': 'kgC02'}, index=[0])])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'kgc02_per_mj', 'value': kgc02_per_mj, 'original_unit': 'MJ', 'final_unit': 'kgC02'}, index=[0])])
    new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'Megatonne c02 per PJ of fuel', 'value': emissions_factor, 'original_unit': 'PJ', 'final_unit': 'MTC02'}, index=[0])])
    # new_df = pd.concat([new_df, pd.DataFrame({'fuel': fuel, 'conversion_factor': 'km_per_mj_to_km_per_kgc02', 'value': km_per_mj_to_km_per_kgc02, 'original_unit': 'km_per_mj', 'final_unit': 'km_per_kgc02'}, index=[0])])
    
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
#coal https://world-nuclear.org/information-library/facts-and-figures/heat-values-of-various-fuels.aspx - includeng some quick assumptions about the coal types eg. met coal is bituminous, thermal coal is sub-bituminous
# Energy content per kg and densities for various fuels
energy_per_kg = {
    'hydrogen': 131, 'methanol': 22.7, 'petrol': 46, 'diesel': 46, 'crude_oil': 47, 
    'lpg': 51, 'natural_gas': 55, 'ethanol': 26.8, 'biodiesel': 38, 'jet_fuel': 42.8, 'biojet': 42.8, 'sub-bituminous coal':20, 'bituminous coal': 24,'lignite': 15.6, 'metallurgical coal': 24, 'thermal coal': 20
}
density_kg_l = {
    'hydrogen': 0.071, 'methanol': 0.791, 'petrol': 0.755, 'diesel': 0.832, 'crude_oil': 0.82, 
    'lpg': 0.493, 'natural_gas': 0.75, 'ethanol': 0.789, 'biodiesel': 0.88, 'jet_fuel': 0.804, 'biojet': 0.804
}
#set up emissions factors too, where we have them
emissions_factors = pd.read_csv('config/9th_edition_emissions_factors.csv')
emissions_factors = emissions_factors[emissions_factors['simple_name'].isin(energy_per_kg.keys())]
#identify any missing fuels
missing_fuels = set(energy_per_kg.keys()) - set(emissions_factors['simple_name'].unique())
print('The following fuels are missing from simple name column in emissions factors: {}'.format(missing_fuels))
emissions_factors = emissions_factors.set_index('simple_name')['Emissions factor (MT/PJ)'].to_dict()
#%%

# Generate DataFrame for each fuel and combine them
all_fuel_dfs = pd.DataFrame()
for fuel, energy_content in energy_per_kg.items():
    emissions_factor = emissions_factors[fuel]
    fuel_df = create_factors_to_convert_mpg_to_useful_outputs_for_fuel(energy_content, density_kg_l[fuel], emissions_factor, fuel)
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
        'million_tonnes_to_pj': 131
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
#Mt (megatonne) to kg = 1e9
#PJ to MJ = 1e9

# PLEASE NOTE THAT IM NOT 100% ABOUT THESEE. THEY SHOULD BE USED WITH CAUTION