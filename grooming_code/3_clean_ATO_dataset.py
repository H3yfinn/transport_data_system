#abnout exploring the data that we extracted in extract_ATO_dataset.py
#we will have to convert the measures into the same names and categories as we use in the transport model. then we c na chekc the data against the concordances we have made to see what data we are and are not missing. This will end up as a df the same size as the model concordances that will ahve true or false vlaues in each cell to inidcate if we have that spec ific data point or not. because the concrdances may change it simportant that this is done in a way that can be replicated for different concordances.

#this first part of the script will just be about epxlporation of the data
#load datra IN 
#%%


from matplotlib.pyplot import title
import pandas as pd
import numpy as np
import os
import re
import plotly.express as px
pd.options.plotting.backend = "plotly"#set pandas backend to plotly plotting instead of matplotlib
import plotly.io as pio
pio.renderers.default = "browser"#allow plotting of graphs in the interactive notebook in vscode #or set to notebook

import itertools

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')
PRINT_GRAPHS_AND_STATS = False

#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = '_DATE{}'.format(file_date)
# FILE_DATE_ID = ''

#%%
#load data

ATO_data_transport_checking_concordance_check = pd.read_csv('intermediate_data/ATO_data/ATO_data_transport_checking_concordance_check.csv')

#%%
#next steps:
#convert data to the measures we need in the transport model. This will involve:filtering out measures we dont need and changing the names of measures we do need (we should do this by creating an extra column called original_measure and then changing the measure column to the measure we need). 
# We should also check what dates are the earliest for which we have a reasonable amount of data
#we should also convert medium to the values we need in the transport model

#once this is done we will have a much smaller dataframe, and we can start to look at the data in more detail, perhaps using teh same processes as above.
#how about wee start those processes in a new file called 'explore_ATO_dataset_2nd_step.py'

#%%
#convert column names in ATO_data_transport_checking_concordance_check to the names we use in the transport model (eg. capital letters at the start of column names (even though this seems silly)) 
#'year', 'economy', 'medium', 'measure', 'alt_measure', 'unit', 'transport_type'
ATO_data_transport_checking_concordance_check = ATO_data_transport_checking_concordance_check.rename(columns={'measure': 'Measure','transport_type': 'Transport Type','year': 'Year','economy': 'Economy','medium': 'Medium'})

#create vehicle type col
ATO_data_transport_checking_concordance_check['Vehicle Type'] = ATO_data_transport_checking_concordance_check['Medium']#this is done for all rows regardless of Measure
#create drive type col
ATO_data_transport_checking_concordance_check['Drive'] = ATO_data_transport_checking_concordance_check['Medium']#this is done for all rows regardless of Measure

#%%

#now print measures in ATO_data_transport_checking_concordance_check
ATO_data_transport_checking_concordance_check['Measure'].unique()

#%%
#create dict which matches up a measure from ATO data and transport model. We will have to do this manually since we have to manually decide teh ATO data matches to our model

#join up the same measures for different mediums. Eg. since 'passenger_km' is the concat of 'Passengers Kilometer Travel - Roads', 'Passengers Kilometer Travel -Aviation (Domestic)', 'Railways.... we will remove these medium values and instead set their names to 'passenger_km'
pass_km_medium_column_names = ['Passengers Kilometer Travel - Aviation (Domestic)','Passengers Kilometer Travel - Railways','Passengers Kilometer Travel -Bus','Passengers Kilometer Travel -HSR', 'Passengers Kilometer Travel - Waterways/shipping', 'Passengers Kilometer Travel - Roads']
#ignored: 'PKM By LDV', 'PKM by Motorised 2W', 
#first set a vehicle type column in ATO data which, for now, is jsut the same value as Medium, but if the Measure is 'Passengers Kilometer Travel - Bus' then set vehicle type to 'Bus'
ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Passengers Kilometer Travel -Bus'), 'Vehicle Type'] = 'bus'

#since the below m,easures dont have any data we ingnore them:
# #do the same for 'PKM By LDV', 'PKM by Motorised 2W'
# ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='PKM By LDV'), 'Vehicle Type'] = 'lv'#LDV also includes lt's but since we dont know how to split it we will jsut use lv and think about saimulating the uptake of more lt's using decreasing fuel efficiency of lv's.
# ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='PKM by Motorised 2W'), 'Vehicle Type'] = '2w'

ATO_data_transport_checking_concordance_check['Measure'] = ATO_data_transport_checking_concordance_check['Measure'].replace(to_replace=pass_km_medium_column_names,value='passenger_km')

#and now for freight tm km we want to do the same thing
# 'Freight Transport - Tonne-km for Aviation (Domestic)',
#        'Freight Transport - Tonne-km for Railways',
#        'Freight Transport - Tonne-km for Roads',
#        'Freight Transport - Tonne-km for Waterways/shipping (Domestic)',
#        'Freight Transport - Tonne-km for Waterways/shipping (Domestic+International)',
#        'Freight Transport - Tonne-km for Aviation (Domestic+International)',
#note that we are gnoring the international values for now.
freight_km_medium_column_names = ['Freight Transport - Tonne-km for Aviation (Domestic)','Freight Transport - Tonne-km for Railways', 'Freight Transport - Tonne-km for Roads', 'Freight Transport - Tonne-km for Waterways/shipping (Domestic)']
ATO_data_transport_checking_concordance_check['Measure'] = ATO_data_transport_checking_concordance_check['Measure'].replace(to_replace=freight_km_medium_column_names,value='freight_tonne_km')

#%%
#same as above for vehicle registration:
    #    ' Electric Vehicle registration  (Bus)',
    #    'Data on non-motorized 2 wheelers -  e.g. pedicabs, bike rickshaws',
    #    'Electric Vehicle registration  (2W)',
    #    'Electric Vehicle registration  (3w)',
    #    'Electric Vehicle registration  (LDV)',
    #    'Freight Vehicle registration', 'Total Vehicle Registration',
    #    'Vehicle registration  (Bus)', 'Vehicle registration  (LDV)',
    #    'Vehicle registration  (Motorised 2W)',
    #    'Vehicle registration  (Motorised 3W)',
    #    'Vehicle registration  (Utility Vehicle/Mini Bus)',
    #    'Vehicle registration (Others)',
#ignroed: 'Electric Vehicle registration  (2W)','Electric Vehicle registration  (3w)','Electric Vehicle registration  (LDV)',
vehicle_registration_measure_names = [
       'Freight Vehicle registration', 
       'Vehicle registration  (Bus)', 'Vehicle registration  (LDV)',
       'Vehicle registration  (Motorised 2W)',
       'Vehicle registration  (Motorised 3W)',
       'Vehicle registration  (Utility Vehicle/Mini Bus)']
#NOTE for now we will treat 3w as its own vehiclke type. we can add it to 2w it later if we think thats needed

# #set vehicle type and drive based on what is in the measure name then save the  measure name to be Vehicle registration
# ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Electric Vehicle registration  (2W)'), 'Vehicle Type'] = '2w'
# ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Electric Vehicle registration  (2W)'), 'Drive'] = 'bev'
# ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Electric Vehicle registration  (3w)'), 'Vehicle Type'] = '3w'
# ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Electric Vehicle registration  (3w)'), 'Drive'] = 'bev'
# ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Electric Vehicle registration  (LDV)'), 'Vehicle Type'] = 'lv'
# ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Electric Vehicle registration  (LDV)'), 'Drive'] = 'bev'
# ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Vehicle registration  (LDV)'), 'Vehicle Type'] = 'lv'

#NOTE for all vehicle registration measures that dont state tehy are for electric vehicles, we dont know if tehy include bev rego's. So for now we will assume they are and leave drive as the same value as Medium.
ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Vehicle registration  (Bus)'), 'Vehicle Type'] = 'bus'
ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Vehicle registration  (LDV)'), 'Vehicle Type'] = 'lv'
ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Vehicle registration  (Motorised 2W)'), 'Vehicle Type'] = '2w'
ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Vehicle registration  (Motorised 3W)'), 'Vehicle Type'] = '3w'
ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Vehicle registration  (Utility Vehicle/Mini Bus)'), 'Vehicle Type'] = 'lt'
#NOTE for 'Freight Vehicle registration',  we will assume this is for heavy trucks, as this seems very likely
ATO_data_transport_checking_concordance_check.loc[(ATO_data_transport_checking_concordance_check['Measure']=='Freight Vehicle registration'), 'Vehicle Type'] = 'ht'
#now set everything to have measure: vehicle registration
ATO_data_transport_checking_concordance_check['Measure'] = ATO_data_transport_checking_concordance_check['Measure'].replace(to_replace=vehicle_registration_measure_names,value='Vehicle registration')
#%%
ATO_data_to_outlook_measures_dict = {
    'passenger_km': 'passenger_km',
    'freight_tonne_km': 'freight_tonne_km',
    'Vehicle registration' : 'Stocks'
}

#%%
#create a clean version of teh ATO_data_transport_checking_concordance_check df
ATO_clean = ATO_data_transport_checking_concordance_check.copy()
#remove 'alt_measure' and 'data_available' columns
ATO_clean.drop(columns=['alt_measure', 'data_available'],inplace=True)
#change measure values using teh ATO_data_to_outlook_measures_dict
ATO_clean['Measure'] = ATO_clean['Measure'].replace(to_replace=ATO_data_to_outlook_measures_dict)
#filter for only measures in the dict
ATO_clean = ATO_clean[ATO_clean['Measure'].isin(ATO_data_to_outlook_measures_dict.values()) ]

#IF 'SCOPE' ONLY CONTAINS 'National' and nan values, drop column, else let user know
unique_scopes_for_adb = ATO_clean['scope'].unique()
for scope in unique_scopes_for_adb:
    if scope not in ['National',np.nan]:
        print('WARNING: ATO data has a scope that is not National: ', scope)
        raise ValueError #you may want to get rid of this line
    
#drop scopes column
ATO_clean.drop(columns=['scope'],inplace=True)

#for now we will keep the sheet and unit cols as is bcause they arent so important to fix yet.
#but we will make sure the first letter of all column names is in capitals
ATO_clean.columns = [col.capitalize() for col in ATO_clean.columns]

#%%
#theres a vew sheet's that onmly ahve NA values. we will drop the rows for these sheets. To do this we will first find the sheets that only have NA values in the 'Value' column
sheets_with_only_na_values = []
for sheet in ATO_clean['Sheet'].unique():
    if ATO_clean[ATO_clean['Sheet']==sheet]['Value'].isna().all():
        sheets_with_only_na_values.append(sheet)
#now drop the rows for these sheets
ATO_clean = ATO_clean[~ATO_clean['Sheet'].isin(sheets_with_only_na_values)]

#%%

#now we have a dataframe which resembles much more closely what we want to send to the model. We will do some analysis using graphs to see what data is missing/what we actually have!!!

#create a col that says whetehr a value is na or not:
ATO_clean['data_available'] = ~ATO_clean['Value'].isna()

if PRINT_GRAPHS_AND_STATS:
    for measure in ATO_clean['Measure'].unique():
        #create a dataframe that only has the ATO_clean we are looking at
        measure_df = ATO_clean[ATO_clean['Measure'] == measure]

        title = 'ATO CLEAN Economy Year NA Count for {}'.format(measure)
        #make sure that if data_available is true, the color is green, and red if false

        measure_df['Vehicle_type_sheet'] =  measure_df['Vehicle type'] + ' - ' + measure_df['Sheet']

        fig = px.scatter(measure_df, x='Year', y='Economy', color='data_available', facet_col='Vehicle_type_sheet',facet_col_wrap=7, title=title, color_discrete_map={True: 'green', False: 'red'})
        # #fig.show()
        #save graph as html in plotting_output/exploration_archive
        fig.write_html('plotting_output/exploration_archive/' + title + '.html')
#%%

#since we've got way less values we cna also feel better about graphing teh values of the data we have. SO lets do that now usinng a for loop for each measure, for each vehicle type, plot a line on the graph for each sheet, and a facet for each economy, using the year as the x axis and the value as the y axis.

#BELOW IS NOT RENDERING PROPERLY  UNLES YOU USE POITNS IN THE LINES
# for measure in ATO_clean['Measure'].unique():
#     #filter for the measure we are looking at
#     measure_df = ATO_clean[ATO_clean['Measure'] == measure]
#     for vehicle in measure_df['Vehicle type'].unique():
#         #filter for the vehicle type we are looking at
#         vehicle_df = measure_df[measure_df['Vehicle type'] == vehicle]
#         #create a figure
#         title = 'ATO CLEAN Economy Year Value for {} - {}'.format(measure, vehicle)
#         fig = px.line(vehicle_df, x='Year', y='Value', color='Sheet', facet_col='Economy', facet_col_wrap=7, title=title)
#         # #fig.show()
#         #save graph as html in plotting_output/exploration_archive
#         fig.write_html('plotting_output/exploration_archive/' + title + '.html')

#BELOW IS NOT RENDERING PROPERLY UNLES YOU USE POITNS IN THE LINES
#reduce data points by filtering for data after 2015 only
ATO_clean_2015 = ATO_clean[ATO_clean['Year']>=2015]
#and to see if we can make it even better, try plotting all vehicle types for each emasure on the same graph
#sort values by year
ATO_clean_2015.sort_values(by='Year',inplace=True)
if PRINT_GRAPHS_AND_STATS:
    for measure in ATO_clean_2015['Measure'].unique():
        #filter for the measure we are looking at``
        measure_df = ATO_clean_2015[ATO_clean_2015['Measure'] == measure]
        #create a figure
        title = 'ATO CLEAN Economy Year Value for {}'.format(measure)
        fig = px.line(measure_df, x='Year', y='Value', color='Vehicle type', facet_col='Economy', facet_col_wrap=7, title=title, markers=True)
        #fig.show()
        #save graph as html in plotting_output/exploration_archive
        fig.write_html('plotting_output/exploration_archive/' + title + '.html')
#wonder if connectgaps=True could have helped
#%%
#LET'S JOIN SHEETS DATA TOGHETHER. iT WILL BE IMPORTANT THAT IN THIS PROCESS WE HAVE VERY GOOD VISUALISATION SO WE KNOW WHEN JOINING SOME DATA TOGEHTER IS NOT A GOOD IDEA. 
#So we will create a copy of the df, for each Vehicle type, measure, economy group, choose the sheet with the most data points, and then fill any holes in it, that we can, with data from the other sheets. Remove all other data points.
ATO_clean_2015_new = pd.DataFrame()
#creat col to indicate if the value is from a different sheet than the default, and if there is more than 1 potential value, indicate that too
ATO_clean_2015['default_sheet'] = True
ATO_clean_2015['multiple_values'] = False

for vehicle in ATO_clean_2015['Vehicle type'].unique():
    vehicle_df = ATO_clean_2015[ATO_clean_2015['Vehicle type'] == vehicle]
    for measure in vehicle_df['Measure'].unique():
        measure_df = vehicle_df[vehicle_df['Measure'] == measure]
        for economy in measure_df['Economy'].unique():
            economy_df = measure_df[measure_df['Economy'] == economy]
            #now we have a df that has only 1 economy, 1 measure, and 1 vehicle type. We will now choose the sheet with the most data points, and then fill any holes in it, that we can, with data from the other sheets. Remove all other data points.
            #first drop na values
            economy_df.dropna(subset=['Value'], inplace=True)
            #if the df is empty, skip it
            if economy_df.empty:
                continue
            #first find the sheet with the most data points
            sheet_with_most_data = economy_df['Sheet'].value_counts().index[0]
            #now filter for only the data points from this sheet
            new_df = economy_df[economy_df['Sheet'] == sheet_with_most_data]
            #now we need to fill in any missing data points
            #first find the years we are missing
            years_with_data = new_df['Year'].unique()
            years_missing_data = np.setdiff1d(np.arange(ATO_clean_2015.Year.min(), ATO_clean_2015.Year.max()+1),years_with_data)
            #now we will loop through the years we are missing and see if we can fill them in with data from the other sheets
            for year in years_missing_data:
                #create a df that only has the year we are looking at
                year_df = economy_df[economy_df['Year'] == year]
                #now we need to find the other sheets that have data for this year
                other_sheets_with_data = year_df['Sheet'].unique()
                #now we need to loop through these sheets and see if we can fill in the data for this year
                for sheet in other_sheets_with_data:
                    #create a df that only has the sheet we are looking at
                    sheet_df = year_df[year_df['Sheet'] == sheet]
                    #set the default sheet to false
                    sheet_df['default_sheet'] = False
                    if len(other_sheets_with_data) > 1:
                        sheet_df['multiple_values'] = True
                    if len(sheet_df) > 1:
                        raise Exception('There is more than 1 value for this year and sheet')
                    #now we need to add this value to the economy_df by concatenating sheet_df to it.
                    new_df = pd.concat([new_df, sheet_df])
            #now just concat teh value to ATO_clean_2015_new
            ATO_clean_2015_new = pd.concat([ATO_clean_2015_new, new_df])

#%%
# # #Then when we plot the data we will create one trace which is a line for each Vehicle type, measure, economy group. Then create another trace which will plot the points on the line, but different colors depending on the sheet.
# # #however its also important that the plot contains a facet for every vehicle type, so all the lines for each vehicle type are on the same graph.

#the above wont work but we can make soemthign simnilar work:
#first we will set Sheet_id to '' if the value is from the default sheet, or the second to last character in the sheet name if it is not
ATO_clean_2015_new['Sheet_id'] = ATO_clean_2015_new['Sheet'].str[-2]
ATO_clean_2015_new.loc[ATO_clean_2015_new['default_sheet'] == True, 'Sheet_id'] = ''
ATO_clean_2015_new.sort_values(by='Year',inplace=True)
if PRINT_GRAPHS_AND_STATS:
    for measure in ATO_clean_2015_new['Measure'].unique():
        #filter for the measure we are looking at
        measure_df = ATO_clean_2015_new[ATO_clean_2015_new['Measure'] == measure]
        #create a figure
        title = 'ATO CLEAN Economy Year Value for {}'.format(measure)
        fig = px.line(measure_df, x='Year', y='Value', color='Vehicle type', text='Sheet_id', facet_col='Economy', facet_col_wrap=7, title=title, markers=True)

        #fig.show()
        #save graph as html in plotting_output/exploration_archive
        fig.write_html('plotting_output/exploration_archive/' + title + '.html')


#%%
#THOUGHTS
#passenger km data seems only really available for 2018. plus too bad we dont know the vehicle type breakdowns. 

#vehicle stocks are rleatively populated for all economies but dont have idea if they are jsut for ices or all drives. 

#frieght seems to be as good as we can make it for now. Issues with waterways data for a few economys where they probably ahve waterways use (e.g. indonesia, phillipines) but this is no surprise, personally.. And then for road freight we are missing data for about half of economys. This will need something done about it i guess. 

#%%
ATO_clean_2015_concordance = ATO_clean_2015_new.copy()
#drop some cols
ATO_clean_2015_concordance.drop(columns=['data_available','default_sheet','multiple_values','Sheet_id'], inplace=True)
#probably best to do another concordance merge to point exactly where our holes are now, so we can easily see where we need to go to get the data.
#so we will create the concordance using the data in ATO_clean_2015_new.
#first we will create a df that has all the unique combinations of economy, measure and year. Then join that do a measure, vehicle type sheet df so as to keep vehicle type connected to emasure
year = ATO_clean_2015_concordance['Year'].unique()
economy = ATO_clean_2015_concordance['Economy'].unique()
measure = ATO_clean_2015_concordance['Measure'].unique()

df1 = pd.DataFrame(list(itertools.product(year, economy, measure)), columns=['Year', 'Economy', 'Measure']).drop_duplicates()
df2 = ATO_clean_2015_concordance[['Measure', 'Vehicle type']].drop_duplicates() 
#we can join unit and sheet on later based on a merge of measure year economy and vehicle type
df3 = pd.merge(df1, df2, on='Measure', how='left')

#now we can join the original data to the df3 to work out what data we ahve an are missing
ATO_clean_2015_concordance = pd.merge(df3, ATO_clean_2015_concordance, on=['Year', 'Economy', 'Measure', 'Vehicle type'], how='left')

#%%
#plot the concordance so we can easily see where we have NA's
#createw data_available col
ATO_clean_2015_concordance['data_available'] = ATO_clean_2015_concordance['Value'].notna()
ATO_clean_2015_concordance.sort_values(by='Year',inplace=True)
if PRINT_GRAPHS_AND_STATS:
    for measure in ATO_clean_2015_concordance['Measure'].unique():
        #filter for the measure we are looking at
        measure_df = ATO_clean_2015_concordance[ATO_clean_2015_concordance['Measure'] == measure]
        #create a figure
        title = 'ATO NAs Economy Year for {}'.format(measure)
        
        fig = px.scatter(measure_df, x='Year', y='Economy', color='data_available', facet_col='Vehicle type', title=title, color_discrete_map={True: 'green', False: 'red'})
        fig.update_traces(marker=dict(size=20))
        #fig.show()
        #save graph as html in plotting_output/exploration_archive
        fig.write_html('plotting_output/exploration_archive/' + title + '.html')
#%%

#OKAY WRAPPING UP FOR NOW
#remove data_available col
ATO_clean_2015_concordance.drop(columns=['data_available'], inplace=True)
#remove sheet col
ATO_clean_2015_concordance.drop(columns=['Sheet'], inplace=True)
#save data
ATO_clean_2015_concordance.to_csv('output_data/ATO_data/ATO_dataset_clean_{}.csv'.format(FILE_DATE_ID), index=False)
#i guerss plan from here is to look at how to fill in the hoels and also find more measures.

#%%
#extra measures which may be very useful can be saved separately: eg.
    #    'Vehicle registration (Others)',
    #     'Total Vehicle Registration',
    #   'Commercial Vehicle Sales (motorised)',
    #    'Motorized Two Wheeler Sales',
    #    'Passenger Vehicle Sales (motorised)',
    #    'Total Vehicle sales (motorised)',
    #    'Electric vehicle share in Total vehicle registrations',
    #    'Efficiency of Train Services',
    #    'Efficiency of air transport services',
    #    'Efficiency of seaport services',