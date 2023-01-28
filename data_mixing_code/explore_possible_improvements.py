

#%%


#BASICALLY THE INTENTION IS TO TAKE IN DATA FROM THE 8TH MODEL AND THEN ANY DATA THAT COULD REPLACE THAT NUT WE DONT WANT TO RISK INSERTING A VALUE THAT ISNT FROM THE SAME SOURCE AND THEREFORE MAY BE STEMMING FROM A DIFFERENT SCALE. IN THIS CASE WE WILL KEEP THE 8TH DATA BUT CHANGE THE PROPORTIONS WITHIN THE 8TH DATA TO REFLECT THE NEW DATA. FOR EXAMPLE IF WE HAVE NEW DATA FOR THE NUMBER AND PROPORTION OF EVS IN PASSENGER LV'S (LIKE WE HAVE FROM IEA GLOBAL DATA EXPLORER) THEN WE CAN USE THAT TO UPDATE THE PROPORTIONS OF EVS IN THE 8TH DATA. THIS WILL MEAN THAT THE TOTAL STOCKS FOR PASSENGER LV'S WILL BE THE SAME AS THE 8TH DATA BUT THE PROPORTIONS OF EVS WILL BE UPDATED TO MORE ACCURATELY REFLECT THE REAL WORLD. UNTIL PUBLICLY AVAILABLE TRANSPORT DATA COVERS ENOUGH DETAIL THEN WE WILL ALWAYS HAVE TO DO THIS KIND OF FIXING.



#%%

#set working directory as one folder back
import os
import re
import pandas as pd
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

import utility_functions as utility_functions
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/', 'combined_dataset_')
FILE_DATE_ID = 'DATE{}'.format(file_date)
combined_data = pd.read_csv('./intermediate_data/combined_dataset_{}.csv'.format(FILE_DATE_ID))
#%%

#%%
#generate plotly tree diagrams of the data that is available in different datasetss to see if we can use it instead of the 8th edition data
%matplotlib inline
#we want to plot a tree diagram which helps to understand exactly wehat we have
#remove 8th datasets
tree_data = combined_data[~combined_data['Dataset'].isin(['8th edition transport model $ Reference','8th edition transport model $ Carbon neutrality', 'ldvs_8th $ 8th_cn', 'ldvs_8th $ 8th_ref'])]
#and remove any datasets with 'est' in their names
tree_data = tree_data[~tree_data['Dataset'].str.contains('est')]
#keep only road medium
tree_data = tree_data[tree_data['Medium'] == 'road']
#set all nan values to 'nan' so that they can be plotted
tree_data = tree_data.fillna('nan')
analyse = True
if analyse:
    tree_data.columns
    import plotly.express as px
    for measure in ['Stocks', 'passenger_km', 'Energy', 'freight_tonne_km','Stock share', 'revenue_passenger_km', 'Sales']:
        columns_to_plot =['Dataset','Transport Type', 'Vehicle Type', 'Economy','Drive']
        #filter for measure
        tree_data_measure = tree_data[tree_data['Measure'] == measure]
        #to make the treemap easier to read we will try inserting the name of the column at the beginning of the name of the value (unless its 'nan')
        for col in columns_to_plot:
            tree_data_measure[col] = tree_data_measure[col].apply(lambda x: col + ': ' + str(x) if str(x) != 'nan' else str(x))

        #and make one that can fit on my home screen which will be 1.3 times taller and 3 times wider
        fig = px.treemap(tree_data_measure, path=columns_to_plot)
        #make it bigger
        fig.update_layout(width=2500, height=1300)
        #make title
        fig.update_layout(title_text=measure)
        #show it in browser rather than in the notebook
        fig.write_html("plotting_output/estimations/data_coverage_trees/road_data_tree_big_{}.html".format(measure))

#%%
#generate plotly tree diagrams of the data that is available in different datasetss to see if we can use it instead of the 8th edition data
%matplotlib inline
#we want to plot a tree diagram which helps to understand exactly wehat we have
#remove 8th datasets
tree_data = combined_data[~combined_data['Dataset'].isin(['8th edition transport model $ Reference','8th edition transport model $ Carbon neutrality', 'ldvs_8th $ 8th_cn', 'ldvs_8th $ 8th_ref'])]
#and remove any datasets with 'est' in their names
tree_data = tree_data[~tree_data['Dataset'].str.contains('est')]
#keep only road medium
tree_data = tree_data[tree_data['Medium'] != 'road']
#set all nan values to 'nan' so that they can be plotted
tree_data = tree_data.fillna('nan')
analyse = True
if analyse:
    tree_data.columns
    import plotly.express as px
    for measure in ['Stocks', 'passenger_km', 'Energy', 'freight_tonne_km','Stock share', 'revenue_passenger_km', 'Sales']:
        columns_to_plot =['Dataset','Transport Type', 'Medium', 'Economy']
        #filter for measure
        tree_data_measure = tree_data[tree_data['Measure'] == measure]
        if tree_data_measure.empty:
            continue
        #to make the treemap easier to read we will try inserting the name of the column at the beginning of the name of the value (unless its 'nan')
        for col in columns_to_plot:
            tree_data_measure[col] = tree_data_measure[col].apply(lambda x: col + ': ' + str(x) if str(x) != 'nan' else str(x))

        #and make one that can fit on my home screen which will be 1.3 times taller and 3 times wider
        fig = px.treemap(tree_data_measure, path=columns_to_plot)
        #make it bigger
        fig.update_layout(width=2500, height=1300)
        #make title
        fig.update_layout(title_text=measure)
        #show it in browser rather than in the notebook
        fig.write_html("plotting_output/estimations/data_coverage_trees/non_road_data_data_tree_big_{}.html".format(measure))
#todo:
#incorporate new road freight tonne km total for ato economys
#incorporate new bus proportion of raod passenger km total for ato economys where the stats are from the same dataset
##otherwise incorporate total road passenger km toal for ato economys where we cannot be sure about the bus proportion because its from a ddifferent dataset within ATO dataset
#incropaorte new total raod energy for ato economys
#incorporate new ev and phev stock shares for ldv, bus, hdv and ht from iea #BUT we can only incorporate the ldv bus and ht data since they have transport types but the others dont







#SCRAMBLINGS BELOW:
#goal is to create stocks data for the model. The data we do have is:
# PASSENGER
#8th edition transport model $ Reference
#  - by drive type
#  - not very good quality
#ATO/iTem data:
#  - by drive type for asian economies
#  - will need to estaimte g/d splits for all economies, and for phevs
#IEA evs data
#  - ev stocks data for all iea economies
#  - ev sales data for all iea economies
#  - ev STOCK SHARE DATA FOR ALL IEA ECONOMIES

#FREIGHT
#8th edition transport model $ Reference
#  - by drive type
#  - not very good quality
#ATO/iTem data:
#  - only total road freight stocks
#  - some van stocks may also be part of passenger stocks
#IEA evs data
#  - ht ev stocks and sales data for all iea economies?
#%%

#given the above we want to estimate stocks for as many data points as possible. I think we should do this by:
#PASSSENGER
#use ATO data where avaiable 
#For bev and phev calcualte passenger drive proportions using the proportions in tranpsort type = combined. 
#for  non bev and phev then set drive proportions to what we have in the 8th edition transport model divided by 1 - bev and phev proporitons.

#also what if we were to assume there were no ldvs in freight. just a thought. 

#%%
#so to gbet this straight:
#1.
#PASSENGER
##for ato economys:
#we have data for vehicle types = bev and phev and a total of all drives. So we can calcualte the proportion of passenger ldvs and buses that are phev and bev.
##For iea economys
#For buses, vans, cars and trucks: we have data for (bev+phev) = bev stock share. But we also ahve bev stocks and phev stocks. But we dont have total stocks in each vehicle type so we cant calcualte the proportion of bev and phev in each vehicle type.
#EXCEPT we could clauclate bev stock share using: bev_stocks + phev_stocks = ev_stocks > bev_stocks / ev_stocks = bev stock share of evs. bev stock share of evs * ev_stock_share = bev stock share of all drive types.
#so have stock share for bev and phev from iea too.
#if we add vans and cars then we can have 'ldv's too, which we'll jsut assume to be passenger. 

#Given the above it is probably best to use 8th data like so:
#for buses and ldvs, sum up all stocks in all drive types. 
#calculate proportion for each drive type. 
#where we do have bev and/or phev proportions then swap those in and set column 'better_ev_data'=True, and calculate 1 minus those. Then where better_ev_data'=True divide the 8th proportions for remainign drive types by "1 minus those".
#now we should have a series of drive proportions for buses and ldvs that add up to 1.
#times the drive proportions by the total stocks for each vehicle type to get the stocks for each drive type.
#The effect of this will be updating the stocks data to better reflect the amount of bev and phev in each vehicle type, accoridng to the iea and/or ato data.

# 2.
#since we want to be able to do this again if we get better data we should design a system that allows us to do this. It will also be useful for applying to other problems.

#function design:
#original_drive_proportions
#original_total_stocks
#better_drive_proportions
#better_total_stocks
#new_original_drive_proportions = original_drive_proportions * (1 - sum(better_drive_proportions))

#as an extra example we can also apply this to passenger 2w where we only have new data for the better_total_stocks. We would just times origianl_drive_proportions by better_total_stocks to get better drive values.

# 3.
#The above can then be applied to freight like so:
#fROM iea we have better ht ev and phev stocks and the ev stock share. So we can do a similar thing to what was done for buses and ldvs above to calcualte bev and phev stock shares for freight.

#one issue for freight is that we arent very sure about our total stocks for the ht vehicle type, so we should keep a look out for better data on that. Also ldvs are currently all being counted as passenger, even though one could expect many ldv's are used for freight. 

# 4.
#PERHAPS we can apply the function to energy and activity too. As follows:
#activity:
#freight: We only have new data for the total freight-tone-km for each medium. However this is good enough for such a difficult type
#Passenger: Like freight, except we do have new bus passengerkm for some economies. We can create a similar function to the above for this but liek so:

#importing bus passengerkm and new total for road passenger km:
#original drive proportions of vehicle total
#original vehicle proportions of transport type total
#better drive proportions of vehicle total
#better vehicle proportions of transport type total
#new_original_drive_proportions = original_drive_proportions * (1 - sum(better_drive_proportions))
#new_original_vehicle_proportions = original_vehicle_proportions * (1 - sum(better_vehicle_proportions))

#this will allow us to incorporate a new total for the bus and road passengerkm for each economy. BUT it should probably only be done when the new total for bus and road are from the same dataset, otherwise we will be mixing data from different sources and the bus poroportion will probably be wrong. Instead, in that situation we would just incorpoarte the new total for road passenger km and leave the bus passenger km as it is.

#energy:
#Mostly the new energy data is just for each medium when you dont consider the transpoort type. So that can be used to udpate the energy data for each medium like so:

# function:
#original energy proportion of all energy total
#new energy proportion of all energy total
#THIS WILL THEN AFFECT THE ENERGY TOTALS FOR EACH SUBSEQUENT LEVEL DOWN (LIKE MEDIUMS, TRANSPORT TPYES, V TYPES, ETC)]

#NEW IDEA, WHY NOT CONVERT EVERYTHIG INTO SHARES? THAT WAY WE CAN JUST ADD THEM UP AND THEN MULTIPLY BY THE TOTALS. THIS WILL MAKE IT EASIER TO DO THE ABOVE.





#we have new air/rail/ship values for freight-tonne-km. Done.

