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
