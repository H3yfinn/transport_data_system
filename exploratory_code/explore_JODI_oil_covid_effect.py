#load in input_data\EGEDA\JODI_OIL_NewProcedure_Secondary_CSV.csv
#then analyse.
#this data is from 
#%%
# Import libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
#change directory to root absed on the location of this file
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('../')
#%%

file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

# Load in the data
file_path = 'input_data/EGEDA/JODI_OIL_NewProcedure_Secondary_CSV.csv'
data = pd.read_csv(file_path)#Index(['REF_AREA', 'TIME_PERIOD', 'ENERGY_PRODUCT', 'FLOW_BREAKDOWN',
#    'UNIT_MEASURE', 'OBS_VALUE', 'ASSESSMENT_CODE', 'ShortName', 'Name',
#    'NewName', 'fuel', 'conversion_factor', 'value', 'original_unit',
#    'final_unit'],
#%%
data.REF_AREA.unique()
# %%
# array(['AE', 'AL', 'AM', 'AO', 'AR', 'AT', 'AU', 'AZ', 'BB', 'BD', 'BE',
#        'BG', 'BH', 'BM', 'BN', 'BO', 'BR', 'BY', 'BZ', 'CA', 'CH', 'CL',
#        'CN', 'CO', 'CR', 'CU', 'CY', 'CZ', 'DE', 'DK', 'DO', 'DZ', 'EC',
#        'EE', 'EG', 'ES', 'FI', 'FR', 'GA', 'GB', 'GD', 'GE', 'GM', 'GQ',
#        'GR', 'GT', 'GY', 'HK', 'HN', 'HR', 'HT', 'HU', 'ID', 'IE', 'IN',
#        'IQ', 'IR', 'IS', 'IT', 'JM', 'JP', 'KR', 'KW', 'KZ', 'LT', 'LU',
#        'LV', 'LY', 'MA', 'MD', 'MK', 'MM', 'MT', 'MU', 'MX', 'MY', 'NE',
#        'NG', 'NI', 'NL', 'NO', 'NP', 'NZ', 'OM', 'PA', 'PE', 'PG', 'PH',
#        'PL', 'PT', 'PY', 'QA', 'RO', 'RU', 'SA', 'SD', 'SE', 'SG', 'SI',
#        'SK', 'SR', 'SV', 'SY', 'SZ', 'TH', 'TJ', 'TN', 'TR', 'TT', 'TW',
#        'UA', 'US', 'UY', 'VE', 'VN', 'YE', 'ZA'], dtype=object)

#these can be mapped to their economy names using Country names sheet in jodi-oil-country-note.xlsx
mappings = pd.read_excel('input_data/EGEDA/JODI-oil-country-note.xlsx', sheet_name='Country names')#ShortName	Name	NewName

#drop dupes in NewName
mappings = mappings.drop_duplicates(subset='NewName')

data = pd.merge(data, mappings, how='left', left_on='REF_AREA', right_on='NewName')
apec_countries = ['Australia', 'Brunei Darussalam', 'Canada', 'Chile', 'China', 'Hong Kong China', 'Indonesia', 'Japan', 'Korea', 'Malaysia', 'Mexico', 'New Zealand', 'Papua New Guinea', 'Peru', 'Philippines', 'Russian Federation', 'Singapore', 'Chinese Taipei', 'Thailand', 'United States of America', 'Vietnam']

data = data[data['Name'].isin(apec_countries)]

#keep only time period from 2019 to latest
#note time period iscureetnly in 2002-01 YYYY-MM format
data['TIME_PERIOD'] = pd.to_datetime(data['TIME_PERIOD'], format='%Y-%m')
data = data[data['TIME_PERIOD'] >= '2019-01-01']
#extract daemand
data = data[data.FLOW_BREAKDOWN=='TOTDEMO']
#grab KBBL in UNIT_MEASURE
data = data[data.UNIT_MEASURE=='KTONS']#Thousand Metric Tons (kmt) 
#FUELS:
# (ENERGY_PRODUCT)
# World primary table World secondary table
# Crude oil CRUDEOIL Liquefied petroleum gases LPG
# NGL NGL Naphtha NAPHTHA
# Other OTHERCRUDE Motor and aviation gasoline GASOLINE
# Total TOTCRUDE Kerosenes KEROSENE
#  of which: kerosene type jet fuel JETKERO
# Gas/diesel oil GASDIES
# Fuel oil RESFUEL
# Other oil products ONONSPEC
# Total oil products TOTPRODS
data_copy = data.copy()
#%%
#load in conversion factors from config/conversion_factors.csv
conversion_factors = pd.read_csv('config/conversion_factors.csv')
# array(['oil', 'coal', 'lng', 'natural_gas', 'hydrogen', 'all', 'methanol',
#        'petrol', 'diesel', 'crude_oil', 'lpg', 'ethanol', 'biodiesel',
#        'jet_fuel', 'biojet'], dtype=object)
#create a mapping from fuel to FUEL
fuel_mapping = {
    'crude_oil':'CRUDEOIL',
    'lpg':'LPG',
    'petrol':'GASOLINE',
    'diesel':'GASDIES',
    'jet_fuel':'JETKERO',
    'fuel_oil':'RESFUEL'}
fuel_mapping_inv = {v: k for k, v in fuel_mapping.items()}
#extract those fuels from conversion_factors
conversion_factors = conversion_factors[conversion_factors.fuel.isin(fuel_mapping.keys())]

#use kt_to_pj to convert to pj
conversion_factors = conversion_factors[conversion_factors['conversion_factor']=='kt_to_pj']

#%%
data['fuel'] = data['ENERGY_PRODUCT'].map(fuel_mapping_inv)
#drop rows with no fuel mapping
data = data.dropna(subset=['fuel'])
data = pd.merge(data, conversion_factors, how='left', left_on='fuel', right_on='fuel')

data['OBS_VALUE'] = pd.to_numeric(data['OBS_VALUE'], errors='coerce')
data = data.dropna(subset=['OBS_VALUE'])
data['pj'] = data['OBS_VALUE'] * data['value']

#%%
#now using plotly create a plot of the energy use of these fuels, with Name as the facet, color as the fuel, and time as the x axis
import plotly.express as px
fig = px.line(data, x='TIME_PERIOD', y='pj', color='fuel', facet_col='Name', facet_col_wrap=4, labels={'pj':'PJ', 'TIME_PERIOD':'Time', 'fuel':'Fuel', 'Name':'Country'})
#make the axis independent
fig.update_yaxes(matches=None, showticklabels=True)
#write to htmkl
fig.write_html('plotting_output/JODI_covid_analysis/energy_use_by_fuel.html')


#label teh period of time where covid was in the dataframe based on the date. try and make this specific to the country:
# ['Australia', 'Brunei Darussalam', 'Canada', 'Chile', 'China', 'Hong Kong China', 'Indonesia', 'Japan', 'Korea', 'Malaysia', 'Mexico', 'New Zealand', 'Papua New Guinea', 'Peru', 'Philippines', 'Russian Federation', 'Singapore', 'Chinese Taipei', 'Thailand', 'United States of America', 'Vietnam']

# Step 1: Create a dictionary of COVID-19 restriction periods for each country
apec_countries_and_their_covid_periods = {
    'Australia': ('2020-03-01', '2021-12-31'),
    'Brunei Darussalam': ('2020-03-01', '2021-11-30'),
    'Canada': ('2020-03-01', '2021-09-30'),
    'Chile': ('2020-03-01', '2021-10-31'),
    'China': ('2020-01-01', '2020-12-31'),
    'Hong Kong China': ('2020-01-01', '2022-04-30'),
    'Indonesia': ('2020-03-01', '2022-05-31'),
    'Japan': ('2020-04-01', '2021-09-30'),
    'Korea': ('2020-02-01', '2022-03-31'),
    'Malaysia': ('2020-03-01', '2021-10-31'),
    'Mexico': ('2020-03-01', '2021-09-30'),
    'New Zealand': ('2020-03-01', '2021-12-31'),
    'Papua New Guinea': ('2020-03-01', '2022-04-30'),
    'Peru': ('2020-03-01', '2021-10-31'),
    'Philippines': ('2020-03-01', '2021-11-30'),
    'Russian Federation': ('2020-03-01', '2020-07-31'),
    'Singapore': ('2020-04-01', '2020-06-30'),
    'Chinese Taipei': ('2020-01-01', '2020-06-30'),
    'Thailand': ('2020-03-01', '2021-10-31'),
    'United States of America': ('2020-03-01', '2021-06-30'),
    'Vietnam': ('2020-04-01', '2021-09-30')
}

# Step 2: Convert the dictionary to a DataFrame and merge it with your main DataFrame
covid_periods_df = pd.DataFrame.from_dict(apec_countries_and_their_covid_periods, orient='index', columns=['start_date', 'end_date'])
covid_periods_df.reset_index(inplace=True)
covid_periods_df.rename(columns={'index': 'Name'}, inplace=True)

# Convert 'start_date' and 'end_date' to datetime
covid_periods_df['start_date'] = pd.to_datetime(covid_periods_df['start_date'])
covid_periods_df['end_date'] = pd.to_datetime(covid_periods_df['end_date'])

# Merge with your main DataFrame
data = pd.merge(data, covid_periods_df, on='Name', how='left')

# Step 3: Add a 'COVID_restriction' column
data['COVID_restriction'] = (data['TIME_PERIOD'] >= data['start_date']) & (data['TIME_PERIOD'] <= data['end_date'])

#%%
# Ensure TIME_PERIOD is datetime
data['TIME_PERIOD'] = pd.to_datetime(data['TIME_PERIOD'])

# Create a DataFrame to hold the COVID start dates
covid_start_dates = []

# Loop over each country and fuel
for (country, fuel), group in data.groupby(['Name', 'fuel']):
    # Filter data to December 2019 to May 2020
    mask = (group['TIME_PERIOD'] >= '2019-12-01') & (group['TIME_PERIOD'] <= '2020-05-31')
    group_period = group.loc[mask].sort_values('TIME_PERIOD')
    # Check if we have enough data points
    if len(group_period) >= 2:
        # Calculate month-to-month differences in 'pj'
        group_period['pj_diff'] = group_period['pj'].diff()
        # Find the month with the biggest negative drop (most negative 'pj_diff')
        min_diff_row = group_period.loc[group_period['pj_diff'].idxmin()]
        covid_start_date = min_diff_row['TIME_PERIOD'] - pd.DateOffset(months=3)
        # Append the result
        covid_start_dates.append({
            'Name': country,
            'fuel': fuel,
            'covid_start_date': covid_start_date
        })
    else:
        # If not enough data, you may choose to set a default start date or skip
        pass

# Convert the list to a DataFrame
covid_start_dates_df = pd.DataFrame(covid_start_dates)

# Merge the COVID start dates back into the main DataFrame
data = pd.merge(data, covid_start_dates_df, on=['Name', 'fuel'], how='left')

# #drop uneccessary columns ['REF_AREA', 'TIME_PERIOD', 'ENERGY_PRODUCT', 'FLOW_BREAKDOWN',
#        'UNIT_MEASURE', 'OBS_VALUE', 'ASSESSMENT_CODE', 'ShortName', 'Name',
#        'NewName', 'fuel', 'conversion_factor', 'value', 'original_unit',
#        'final_unit', 'pj', 'start_date', 'end_date', 'COVID_restriction',
#        'covid_start_date'
data = data.drop(columns=['REF_AREA', 'ENERGY_PRODUCT', 'FLOW_BREAKDOWN', 'UNIT_MEASURE', 'OBS_VALUE', 'ASSESSMENT_CODE', 'ShortName', 'NewName', 'conversion_factor', 'value', 'original_unit', 'final_unit', 'COVID_restriction'])

# Now, we can proceed to plot the data, incorporating the COVID periods

#%%

#extract fuel 

# %%
# Step 1: For each country and fuel, create data points for the COVID period
covid_data_list = []

for (country, fuel), group in data.groupby(['Name', 'fuel']):
    start_date = group['covid_start_date'].iloc[0]
    end_date = group['end_date'].iloc[0]
    if pd.notna(start_date) and pd.notna(end_date):
        # Find the pj value at the start of the COVID period
        # Get the data points before or at the start_date
        data_before_start = group[group['TIME_PERIOD'] <= start_date]
        if not data_before_start.empty:
            # Get the latest pj value before or at the start_date
            pj_value = data_before_start.sort_values('TIME_PERIOD').iloc[-1]['pj']
        else:
            # If there's no data before start_date, use the earliest pj value available
            pj_value = group.sort_values('TIME_PERIOD').iloc[0]['pj']
        
        # Create a DataFrame for the COVID period line
        covid_df = pd.DataFrame({
            'Name': country,
            'TIME_PERIOD': [start_date, end_date],
            'pj': [pj_value, pj_value],
            'fuel': fuel,
            'line_dash': 'dash'  # Set line_dash to 'dash' for COVID period lines
        })
        covid_data_list.append(covid_df)

# Combine all COVID DataFrames
covid_data = pd.concat(covid_data_list, ignore_index=True)

# Step 2: Add 'line_dash' column to your main data
data['line_dash'] = 'solid'  # Set line_dash to 'solid' for normal data

# Step 3: Append the COVID data to your main DataFrame
data_with_covid = pd.concat([data, covid_data], ignore_index=True)

# Step 4: Plot the data including the COVID period lines
import plotly.express as px

fig = px.line(
    data_with_covid, 
    x='TIME_PERIOD', 
    y='pj', 
    color='fuel', 
    line_dash='line_dash',  # Use 'line_dash' to differentiate lines
    facet_col='Name', 
    facet_col_wrap=4, 
    labels={'pj':'PJ', 'TIME_PERIOD':'Time', 'fuel':'Fuel', 'Name':'Country'}
)

# Make the y-axes independent
fig.update_yaxes(matches=None, showticklabels=True)

# Optional: Customize the appearance of the COVID period lines
for trace in fig.data:
    if trace.line['dash'] == 'dash':
        trace.update(line=dict(width=2, dash='dash'))

# Save the plot to an HTML file
fig.write_html('plotting_output/JODI_covid_analysis/energy_use_by_fuel_with_covid_periods.html')

# %%

# Step 1: Compute the Smoothed Trend Lines

# Sort the data to ensure correct rolling calculations
data_with_covid = data_with_covid.sort_values(by=['Name', 'fuel', 'TIME_PERIOD'])

#extract only when covid restrictions are not, which is where line_dash is solid
data_smoothed = data_with_covid[data_with_covid['line_dash']=='solid'].copy()


# Compute the 3-month rolling average for 'pj' within each group
data_smoothed['pj'] = data_smoothed.groupby(['Name', 'fuel'])['pj'].transform(
    lambda x: x.rolling(window=6, min_periods=1).mean()
)
#now set line dash to smoothed
data_smoothed['line_dash'] = 'dot'

plot_data = pd.concat([data_with_covid, data_smoothed], ignore_index=True)

#%%

# Step 3: Plot the Data Including the Smoothed Trend Lines
#drop the full line
plot_data = plot_data[plot_data['line_dash'] != 'solid']

fig = px.line(
    plot_data,
    x='TIME_PERIOD',
    y='pj',
    color='fuel',
    line_dash='line_dash',  # Use 'line_type' to differentiate lines
    facet_col='Name',
    facet_col_wrap=4,
    labels={'pj': 'PJ', 'TIME_PERIOD': 'Time', 'fuel': 'Fuel', 'Name': 'Country'}
)

# Make the y-axes independent
fig.update_yaxes(matches=None, showticklabels=True)

# Save the plot to an HTML file
fig.write_html('plotting_output/JODI_covid_analysis/energy_use_with_smoothed_trends_and_covid_periods.html')

#%%













# Ensure TIME_PERIOD is datetime
data['TIME_PERIOD'] = pd.to_datetime(data['TIME_PERIOD'])

# Step 1: Calculate COVID Start Dates Based on the Biggest Drop
covid_start_dates = []

for (country, fuel), group in data.groupby(['Name', 'fuel']):
    # Filter data to December 2019 to May 2020
    mask = (group['TIME_PERIOD'] >= '2019-12-01') & (group['TIME_PERIOD'] <= '2020-05-31')
    group_period = group.loc[mask].sort_values('TIME_PERIOD')
    
    if len(group_period) >= 2:
        # Calculate month-to-month differences in 'pj'
        group_period['pj_diff'] = group_period['pj'].diff()
        # Find the month with the biggest negative drop
        min_diff_row = group_period.loc[group_period['pj_diff'].idxmin()]
        covid_start_date = min_diff_row['TIME_PERIOD']
        # Append the result
        covid_start_dates.append({
            'Name': country,
            'fuel': fuel,
            'covid_start_date': covid_start_date
        })
    else:
        # If not enough data, set a default start date (optional)
        covid_start_dates.append({
            'Name': country,
            'fuel': fuel,
            'covid_start_date': pd.NaT  # No date available
        })

# Convert the list to a DataFrame
covid_start_dates_df = pd.DataFrame(covid_start_dates)

# Set a fixed end date for the COVID period
covid_end_date = pd.to_datetime('2020-12-31')

# Merge the COVID start dates back into the main DataFrame
data = pd.merge(data, covid_start_dates_df, on=['Name', 'fuel'], how='left')

# Step 2: Compute the Smoothed Trend Lines

# Sort the data to ensure correct rolling calculations
data = data.sort_values(by=['Name', 'fuel', 'TIME_PERIOD'])

# Compute the 3-month rolling average for 'pj' within each group
data['pj_smoothed'] = data.groupby(['Name', 'fuel'])['pj'].transform(
    lambda x: x.rolling(window=3, min_periods=1).mean()
)

# Step 3: Prepare the Data for Plotting

# Create a column to distinguish between original, smoothed, and COVID period data
data['line_type'] = 'Original'

# Prepare smoothed data
smoothed_data = data.copy()
smoothed_data['pj'] = smoothed_data['pj_smoothed']
smoothed_data['line_type'] = 'Smoothed'

# Prepare COVID period data
covid_data_list = []

for (country, fuel), group in data.groupby(['Name', 'fuel']):
    covid_start_date = group['covid_start_date'].iloc[0]
    if pd.notna(covid_start_date):
        # Get the pj value at the start of the COVID period
        covid_pj = group.loc[group['TIME_PERIOD'] == covid_start_date, 'pj'].values
        if len(covid_pj) > 0:
            covid_pj = covid_pj[0]
        else:
            # If no exact match, use the closest previous value
            covid_pj = group[group['TIME_PERIOD'] < covid_start_date]['pj'].iloc[-1]
        # Create DataFrame for the COVID period line
        covid_df = pd.DataFrame({
            'Name': country,
            'TIME_PERIOD': [covid_start_date, covid_end_date],
            'pj': [covid_pj, covid_pj],
            'fuel': fuel,
            'line_type': 'COVID Period'
        })
        covid_data_list.append(covid_df)

# Combine all COVID period DataFrames
covid_period_data = pd.concat(covid_data_list, ignore_index=True)

# Combine all data
plot_data = pd.concat([data, smoothed_data, covid_period_data], ignore_index=True)

# Step 4: Plot the Data Including Smoothed Trend Lines and COVID Period Lines

fig = px.line(
    plot_data,
    x='TIME_PERIOD',
    y='pj',
    color='fuel',
    line_dash='line_type',
    facet_col='Name',
    facet_col_wrap=4,
    labels={'pj': 'PJ', 'TIME_PERIOD': 'Time', 'fuel': 'Fuel', 'Name': 'Country'}
)

# Make the y-axes independent
fig.update_yaxes(matches=None, showticklabels=True)

# Customize line styles
for trace in fig.data:
    if ',Smoothed' in trace.name:
        # Smoothed trend lines
        trace.update(line=dict(width=3, dash='dash'))
    elif ',COVID Period' in trace.name:
        # COVID period lines
        trace.update(line=dict(color='red', width=2, dash='dot'))
    else:
        # Original data lines
        trace.update(line=dict(width=1))

# Save the plot to an HTML file
fig.write_html('plotting_output/JODI_covid_analysis/energy_use_with_smoothed_trends_and_covid_periods.html')


##################################################################
##################################################################
##################################################################

# %%
#quick analysis of jodi gas data:

file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

# Load in the data
file_path = 'input_data/EGEDA/JODI_GAS_STAGING_world_NewFormat.csv'
data = pd.read_csv(file_path)#Index(['REF_AREA', 'TIME_PERIOD', 'ENERGY_PRODUCT', 'FLOW_BREAKDOWN',
#    'UNIT_MEASURE', 'OBS_VALUE', 'ASSESSMENT_CODE', 'ShortName', 'Name',
#    'NewName', 'fuel', 'conversion_factor', 'value', 'original_unit',
#    'final_unit'],
#%%
data.REF_AREA.unique()
# %%
# array(['AE', 'AL', 'AM', 'AO', 'AR', 'AT', 'AU', 'AZ', 'BB', 'BD', 'BE',
#        'BG', 'BH', 'BM', 'BN', 'BO', 'BR', 'BY', 'BZ', 'CA', 'CH', 'CL',
#        'CN', 'CO', 'CR', 'CU', 'CY', 'CZ', 'DE', 'DK', 'DO', 'DZ', 'EC',
#        'EE', 'EG', 'ES', 'FI', 'FR', 'GA', 'GB', 'GD', 'GE', 'GM', 'GQ',
#        'GR', 'GT', 'GY', 'HK', 'HN', 'HR', 'HT', 'HU', 'ID', 'IE', 'IN',
#        'IQ', 'IR', 'IS', 'IT', 'JM', 'JP', 'KR', 'KW', 'KZ', 'LT', 'LU',
#        'LV', 'LY', 'MA', 'MD', 'MK', 'MM', 'MT', 'MU', 'MX', 'MY', 'NE',
#        'NG', 'NI', 'NL', 'NO', 'NP', 'NZ', 'OM', 'PA', 'PE', 'PG', 'PH',
#        'PL', 'PT', 'PY', 'QA', 'RO', 'RU', 'SA', 'SD', 'SE', 'SG', 'SI',
#        'SK', 'SR', 'SV', 'SY', 'SZ', 'TH', 'TJ', 'TN', 'TR', 'TT', 'TW',
#        'UA', 'US', 'UY', 'VE', 'VN', 'YE', 'ZA'], dtype=object)

#these can be mapped to their economy names using Country names sheet in jodi-oil-country-note.xlsx
mappings = pd.read_excel('input_data/EGEDA/JODI-oil-country-note.xlsx', sheet_name='Country names')#ShortName	Name	NewName

#drop dupes in NewName
mappings = mappings.drop_duplicates(subset='NewName')

data = pd.merge(data, mappings, how='left', left_on='REF_AREA', right_on='NewName')
apec_countries = ['Australia', 'Brunei Darussalam', 'Canada', 'Chile', 'China', 'Hong Kong China', 'Indonesia', 'Japan', 'Korea', 'Malaysia', 'Mexico', 'New Zealand', 'Papua New Guinea', 'Peru', 'Philippines', 'Russian Federation', 'Singapore', 'Chinese Taipei', 'Thailand', 'United States of America', 'Vietnam']

data = data[data['Name'].isin(apec_countries)]

# %%
