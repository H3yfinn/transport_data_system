#%%
#testing. i think this is a good library:
# https://github.com/haimkastner/unitsnet-py/tree/main
from unitsnet_py import Energy, EnergyUnits

#convert 12million tonnes of hydrogen to pj:
# Given constants
mass_tonnes = 12e6  # 12 million tonnes
energy_per_kg_hydrogen = 142  # MJ/kg

# Conversion factors
kg_per_tonne = 1000
MJ_per_PJ = Energy(1, EnergyUnits.Petajoule).megajoules

# Calculate energy in petajoules
energy_PJ = (mass_tonnes * energy_per_kg_hydrogen * kg_per_tonne) / MJ_per_PJ
print(f"The energy content of {mass_tonnes} tonnes of hydrogen is approximately {energy_PJ:.2f} PJ.")

#%%

#HERE WE WILL START TO CREATE A SET OF CONVERSIONS FROM COMMON VALUES TO WHAT WE NEED WITHIN APERC.

def create_factors_to_convert_mpg_to_useful_outputs_for_fuel(fuel_energy_content_mj_kg, density_kg_l):
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

    # Convert mpg to km/L
    km_per_liter = miles_to_km / gallons_to_liters

    # Convert km/L to km/kg
    km_per_kg = km_per_liter / density_kg_l

    # Convert km/kg to km/PJ
    km_per_pj = km_per_kg * (fuel_energy_content_mj_kg * 1000) / kj_to_pj

    new_dict = {
        'mpg_to_km_per_liter': km_per_liter,
        'mpg_to_km_per_pj': km_per_pj,
        'mpg_to_billion_km_per_pj': km_per_pj / 1e9,
        'fuel_energy_content_mj_kg': fuel_energy_content_mj_kg,
        'density_kg_l': density_kg_l
    }
    return new_dict

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
energy_per_kg_hydrogen = 142  # MJ/kg
energy_per_kg_methanol = 22.7  # MJ/kg
energy_per_kg_petrol = 46  # MJ/kg
energy_per_kg_diesel = 46  # MJ/kg
energy_per_kg_crude_oil = 47  # MJ/kg
energy_per_kg_lpg = 51  # MJ/kg
energy_per_kg_natural_gas = 55  # MJ/kg
energy_per_kg_ethanol = 26.8  # MJ/kg
energy_per_kg_biodiesel = 38  # MJ/kg
energy_per_kg_jet_fuel = 42.8  # MJ/kg
energy_per_kg_biojet = 42.8  # MJ/kg

#and their densities in kg/L:
density_kg_l_hydrogen = 0.071  # kg/L
density_kg_l_methanol = 0.791  # kg/L
density_kg_l_petrol = 0.755  # kg/L
density_kg_l_diesel = 0.832  # kg/L
density_kg_l_crude_oil = 0.82  # kg/L
density_kg_l_lpg = 0.493  # kg/L
density_kg_l_natural_gas = 0.75  # kg/L
density_kg_l_ethanol = 0.789  # kg/L
density_kg_l_biodiesel = 0.88  # kg/L
density_kg_l_jet_fuel = 0.804  # kg/L
density_kg_l_biojet = 0.804  # kg/L

mpg_to_useful_outputs_dict = {}
mpg_to_useful_outputs_dict['hydrogen'] = create_factors_to_convert_mpg_to_useful_outputs_for_fuel(energy_per_kg_hydrogen, density_kg_l_hydrogen)
mpg_to_useful_outputs_dict['methanol'] = create_factors_to_convert_mpg_to_useful_outputs_for_fuel(energy_per_kg_methanol, density_kg_l_methanol)
mpg_to_useful_outputs_dict['petrol'] = create_factors_to_convert_mpg_to_useful_outputs_for_fuel(energy_per_kg_petrol, density_kg_l_petrol)
mpg_to_useful_outputs_dict['diesel'] = create_factors_to_convert_mpg_to_useful_outputs_for_fuel(energy_per_kg_diesel, density_kg_l_diesel)
mpg_to_useful_outputs_dict['crude_oil'] = create_factors_to_convert_mpg_to_useful_outputs_for_fuel(energy_per_kg_crude_oil, density_kg_l_crude_oil)
mpg_to_useful_outputs_dict['lpg'] = create_factors_to_convert_mpg_to_useful_outputs_for_fuel(energy_per_kg_lpg, density_kg_l_lpg)
mpg_to_useful_outputs_dict['natural_gas'] = create_factors_to_convert_mpg_to_useful_outputs_for_fuel(energy_per_kg_natural_gas, density_kg_l_natural_gas)
mpg_to_useful_outputs_dict['ethanol'] = create_factors_to_convert_mpg_to_useful_outputs_for_fuel(energy_per_kg_ethanol, density_kg_l_ethanol)
mpg_to_useful_outputs_dict['biodiesel'] = create_factors_to_convert_mpg_to_useful_outputs_for_fuel(energy_per_kg_biodiesel, density_kg_l_biodiesel)
mpg_to_useful_outputs_dict['jet_fuel'] = create_factors_to_convert_mpg_to_useful_outputs_for_fuel(energy_per_kg_jet_fuel, density_kg_l_jet_fuel)
mpg_to_useful_outputs_dict['biojet'] = create_factors_to_convert_mpg_to_useful_outputs_for_fuel(energy_per_kg_biojet, density_kg_l_biojet)
#%%
# #%%
# #testing. i think this is a good library:
# #
mpg_to_useful_outputs_dict


#TODO THIS ISNT RIGHT. AND IM ALSO WORRIED ABOUT THE vALUE mpg_to_km_per_PJ =1.3497462477054316*10**7#this conversion factor came from ??? IN 1_CLEAN_USA_AFDC_FACTORS.PY

# angle = Angle.from_degrees(180)
# # equals to
# angle = Angle(180, AngleUnits.Degree)

# print(angle.radians)  # 3.141592653589793
# print(angle.microradians)  # 3141592.65358979
# print(angle.gradians)  # 200
# print(angle.microdegrees)  # 180000000


# # As an alternative, a convert style method are also available
# print(angle.convert(AngleUnits.Degree))  # 3.141592653589793
# print(angle.convert(AngleUnits.Microradian))  # 3141592.65358979
# print(angle.convert(AngleUnits.Gradian))  # 200
# print(angle.convert(AngleUnits.Microdegree))  # 180000000


# # Print the default unit to_string (The default for angle is degrees)
# print(angle.to_string())  # 180 °

# print(angle.to_string(AngleUnits.Degree))  # 180 °
# print(angle.to_string(AngleUnits.Radian))  # 3.141592653589793 rad

# # Additional methods

# length1 = Length.from_meters(10)
# length2 = Length.from_decimeters(100)
# length3 = Length.from_meters(3)

# # 'equals' method
# print(length1 == length2)  # True
# print(length1 == length3)  # False

# # 'compareTo' method
# print(length3 > length1)  # False
# print(length3 < length1)  # True
# print(length2 >= length1)  # True

# # Arithmetics methods
# results1 = length1 + length3
# results2 = length1 - length3
# results3 = length1 * length3
# results4 = length1 / length3
# results5 = length1 % length3
# results6 = length1 ** length3
# print(results1.to_string(LengthUnits.Meter))  # 13 m
# print(results2.to_string(LengthUnits.Meter))  # 7 m
# print(results3.to_string(LengthUnits.Meter))  # 30 m
# print(results4.to_string(LengthUnits.Meter))  # 3.3333333333333335 m
# print(results5.to_string(LengthUnits.Meter))  # 1 m
# print(results6.to_string(LengthUnits.Meter))  # 1000 m

# # Complex objects

# # Any object supports arithmetic operations can be used as well as unit
# # see numpay array example:
# import numpy as np

# np_array = np.array([[2, 4, 6], [7, 8, 9]])

# np_array_length = Length.from_kilometers(np_array)
# print(np_array_length.meters) # [[2000. 4000. 6000.][7000. 8000. 9000.]]

# np_array_double_length = np_array_length + np_array_length
# print(np_array_double_length.kilometers) # [[ 4.  8. 12.][14. 16. 18.]]
# print(np_array_double_length.meters) # [[ 4000.  8000. 12000.][14000. 16000. 18000.]]
# #%%
# %%
