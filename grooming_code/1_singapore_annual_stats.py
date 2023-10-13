#%%# Import libraries
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import re
import datetime
#%%
# Set working directory
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')

#please note that the code here was developed using this: https://chat.openai.com/share/b178eaee-8fa8-471b-a5b7-26c3a83386b2 > the data files that were used are in  transport_data_system\input_data\Singapore\chatgpt

#%%
# Load the updated Singapore annual statistics stocks data
singapore_data_updated = pd.read_excel('input_data/Singapore/chatgpt/singapore_annual_statistics_stocks.xlsx')

# Filter out rows where 'Total_bool' is True
filtered_singapore_data = singapore_data_updated[singapore_data_updated['Total_bool'] == False]

# Initialize a new transformed dataframe with the filtered data
filtered_transformed_df = pd.DataFrame({
    'economy': '17_SIN',
    'date': 2020,
    'medium': 'road',
    'measure': 'stocks',
    'unit': 'stocks',
    'fuel': 'all',
    'comment': 'no_comment',
    'scope': 'national',
    'frequency': 'yearly',
}, index=range(len(filtered_singapore_data)))

# Custom mapping for Singapore's vehicle types to more generic terms
custom_vehicle_mapping = {
    'Private cars': 'car',
    'Company cars': 'car',
    'Tuition cars': 'car',
    'Private Hire (Self-Drive) cars': 'car',
    'Private Hire (Chauffeur) cars': 'car',
    'Off peak cars': 'car',
    'Taxis': 'car',
    'Motorcycles & Scooters': '2w',
    'Goods-cum-passenger vehicles (GPVs)': 'lcv',
    'Light Goods Vehicles (LGVs)': 'lcv',
    'Heavy Goods Vehicles (HGVs)': 'mt',
    'Very Heavy Goods Vehicles (VHGVs)': 'ht',
    'Omnibuses': 'bus',
    'School buses (CB)': 'bus',
    'Private buses': 'bus',
    'Private hire buses': 'bus',
    'Excursion buses': 'bus',
    'Cars & Station-wagons (Tax Exempted)': 'car',
    'Motorcycles & Scooters (Tax Exempted)': '2w',
    'Buses (Tax Exempted)': 'bus',
    'Goods & Other Vehicles (Tax Exempted)' : 'Goods & Other Vehicles (Tax Exempted)'
}

# Transport type categories based on vehicle types
passenger_types = ['car', 'bus', '2w', 'suv', 'lt']
freight_types = ['lcv', 'ht', 'mt']

# Reset the index for both DataFrames to align them correctly
filtered_singapore_data.reset_index(drop=True, inplace=True)
filtered_transformed_df.reset_index(drop=True, inplace=True)

# Apply the custom mapping for vehicle_type and update the 'value' column
filtered_transformed_df['vehicle_type'] = filtered_singapore_data['Date'].map(custom_vehicle_mapping)
filtered_transformed_df['value'] = filtered_singapore_data.iloc[:, 2].values

#%%
# Function to handle the special case where vehicle type is 'Goods & Other Vehicles (Tax Exempted)'
def handle_special_case(df):
    special_case_df = df[df['vehicle_type'] == 'Goods & Other Vehicles (Tax Exempted)'].copy()
    if not special_case_df.empty:
        # Split the values evenly among 'ht', 'mt', and 'lcv'
        special_case_df['value'] = special_case_df['value'] / 3
        #round to 0 decimals
        special_case_df['value'] = special_case_df['value'].round(0)
        special_case_df['transport_type'] = 'freight'
        
        # Create new DataFrames for 'ht' and 'mt' with the same properties
        ht_df = special_case_df.copy()
        ht_df['vehicle_type'] = 'ht'
        mt_df = special_case_df.copy()
        mt_df['vehicle_type'] = 'mt'
        lcv_df = special_case_df.copy()
        lcv_df['vehicle_type'] = 'lcv'
        
        # Append all three DataFrames to the original DataFrame
        df = pd.concat([df, ht_df, mt_df, lcv_df], ignore_index=True)
        
        # Remove the original 'Goods & Other Vehicles (Tax Exempted)' rows
        df = df[df['vehicle_type'] != 'Goods & Other Vehicles (Tax Exempted)']
        
    return df

# Handle the special case
filtered_transformed_df = handle_special_case(filtered_transformed_df)

# Update 'transport_type' based on the new 'vehicle_type'
filtered_transformed_df['transport_type'] = filtered_transformed_df['vehicle_type'].apply(
    lambda x: 'passenger' if x in passenger_types else ('freight' if x in freight_types else 'unknown')
)

# Identify unmapped vehicle types in the filtered data
unmapped_vehicle_types = filtered_singapore_data[~filtered_singapore_data['Date'].isin(custom_vehicle_mapping.keys())]['Date'].unique()

# Alert the user if there are any unmapped vehicle types
unmapped_alert = "All vehicle types have been successfully mapped."
if len(unmapped_vehicle_types) > 0:
    unmapped_alert = f"Warning: The following vehicle types are not mapped: {', '.join(unmapped_vehicle_types)}"
    
print(unmapped_alert)

# Additional steps
filtered_transformed_df['drive'] = 'all'
filtered_transformed_df['dataset'] = 'singapore_annual_stocks'

singapore_df = filtered_transformed_df.groupby(['economy', 'date', 'medium', 'measure', 'unit', 'fuel', 'comment', 'scope', 'frequency', 'vehicle_type', 'transport_type', 'drive', 'dataset'], as_index=False).agg({'value': 'sum'})

# Create FILE_DATE_ID
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

# Save to CSV
singapore_df.to_csv(f'intermediate_data/SIN/{FILE_DATE_ID}_singapore_annual_stocks.csv', index=False)

#%%
print(singapore_df.head())
# %%
