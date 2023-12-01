Please view the Wiki here for contextual information:

https://github.com/H3yfinn/transport_data_system/wiki

## SETUP

### Install Anaconda
run:
conda env create --prefix ./env_transport_data_system --file ./config/env_transport_data_system.yml

Then:
conda activate ./env_transport_data_system

Note that installing those libraries in the yml files will result in a few other dependencies also being installed.

## About
This system is intended for handling all data that is needed in the transport model and other APERC transport related systems/analysis. The main goal is to gather data and transform it into the same format so it can be used for analysis easily. In doing so there are a few major things that are done:

 - Data is extracted from various sources and put into the format that is needed for analysis (year | economy | transport_type | vehicle_type | drive | medium | measure | unit | fuel_type | dataset | value | source | comments). Where possible, it is also converted to have the same categories as other data in the system, which are based on the categories used in the transport model. 
    - This is done in /grooming_code/ 
    - scripts are numbered to indicate what step of the grooming process they are in. Quite often it is a single step process, but sometimes it is a multi step process, in which case filenames will be similar and numbered accordingly.
 - Some data is 'mixed' which is a general term to describe the process of creating new datapoints from existing ones.
    - This is done in /mixing_code/ 
    - scripts are numbered to indicate what step of the mixing process they are in.
 - Data is aggregated into one large dataset which can be used for analysis. This is called
    - This is done in /aggregation_code/ and handled by aggregation_code/main.py
    - the final dataset containing all the data is called 'unfiltered_combined_data' and is stored in intermediate_data\selection_process/{FILE_DATE_ID}
 - Data is filtered to match the required data for the transport model. then decisions are made as to what datapoints to keep when there are multiple datapoints for the same category. This is done in /aggregation_code/ and handled by aggregation_code/main.py
    - the final dataset containing all the data is called 'output_data/combined_data_DATE{FILE_DATE_ID}.csv'.
    - At least, for now, this is treated as the final, major, output from this transport data system. This is because the transport model is seen as the centre of all analysis.This is also why the data should be in the format that the transport model needs. 
    - Decisions to choose data can be automatic or manual, based on the users deicsion. But please note, the manual selection process is a bit fiddly, which is a result of the complexity of the task:
        - For all data that is not the only datapoint for a category, the user will be asked to choose which datapoint to keep through a python prompt popup. To help the user, this will be communicated series by series, and the data for each series will pop up in a time series so the user can see how each data point fits in. That way they can avoid obvious outliers and use their own judgement to choose the best data (which is often the best way to do this, but usually not enhanced by such a timely visualisation! -  note that i ventured into creating a dashboard of all 'related' timeseries as well but this took too long... some parts of that remain in the code so if you want, go create it!).
        - For all data that is the only datapoint for a category, the datapoint will be used anyway.
        - When the user is using the automatic selection process they can still specify what 'datasets' they want the automatic method to prioritise choosing, however, when there are two values for datasets that have been specified to be chosen, the method will just choose the 1st value.
        - The user can use a mix of the automatic and manual selection process so they must manually select data where no chosen dataset is available, and otherwise the automatic selection process will be used. This is a great way to speed up the process yet still have control over the data.
        - Note that the manual process can be pretty slow because there are so many data points and the user has to make a decision for each one. To speed this up, the user can also choose datasets to always ignore.
    - After the choosing of data via automatic/manual methods, the main.py file will also interpolate between datapoints of the smae series. This is done because sometimes there are gaps in the data, and the transport model needs data for every year. This is done in /aggregation_code/interpolation_functions.py. It is a bit complicated but the concept is simple. It will take a series of datapoints and fill in the gaps between them and after them. It does this by using the previous and next datapoints to calculate the gradient of the line between them. There are some options you can choose before running it, and there is also a popup visualisation to choose between different interpoaltions. This is not used by default but it is there if you want to use it.
    - The final dataset is stored in output_data/combined_data_DATE{FILE_DATE_ID}.csv but you can always use any other data in this system for analysis too.
    - There was some effort into visualising/analysing all of the data in this system Data is analysed and visualised. This is done in /analysis_code, /exploratory_code/ and /other_code/ but these didnt prove very helpful. the code is still there if you want to use it though.
    
# Extra details about aggregation_code/main.py:
- Later on in the process of using this system I realised that most activity values and a lot of others are jsut calcualted by the people who produce them using random methods and sometimes incorrect data. This was noticed because i could never make sense of how acitvity data comapred across sources. Anyway, i now calculate activity and energy within main.py using data from different sources on stocks, efficiency, mileage and so on. This both simplifies the data requirements and ensures more comparability. There is still a lot of activity and energy data in unfiltered_combined_data, but it is not used in the final dataset that is sent to the transport model.
    - For the reason above the main.py file also follows a strucutre of picking all data except energy and activity, then calculating energy and activity, then adding that to the dataset of all energy and activity. then from this, data is picked for the final versions of activity and energy. Most of the time you will want to default to the calculated versions of activity and energy, but sometimes you might want to use the original data. 
    - lastly, non road data is dealth with separately. This is because of a quirk of the ESTO data, which is beased on data sent from economies. Most economies dont estiamte their non road data very well, but in the transport mdoel we need to take the stance that the economies are alwaysright. So we basically ahve to back calcualte all values in the non road dataset from the energy data, even if it will be wrong! These calcaultions are informed by estimates of intensity in non road. 
        - TODO i cant quite remember how these intensit values come about, or at least how we choose which ones to use (because im pretty sure i estimated most of them using averages of other values). Anyway, since the ESTO data is so inaccurate most of the time, it doesnt really matter what we use here. But it would be good to have a better method/clarity for this.
        - note that its not ESTO's fault specifically, im throwing shade at the situation in general. ESTO is great and what they do for us is awesome!
- The last step of the aggregation process is optional and can recalcaulte stocks/eff/mileage/occ so that their product equates to the energy/activity to match the ESTO data. However, it is now irrelevant as we have a better method in the transport mdoel which uses optimsiation (machine learning) to choose the best values for stocks/mielage/occupancy/efficiency so their product is equal to the egeda totals, but they remain close to what you would expect. One day it might be a good idea to move that method to this system, but for now it is not needed.

## Reasoning and idealism:
I always struggle with the intput data requirements of a data intensive model. Very often you have to mix and match sources but then this requries choosing between them too. Also when you are dealing with so much data and so many sources you end up having to format/reformt/redo the data many times. So its good to have code to do the formatting data, but it shouldnt be written in a slow way because it might become obsolete when the source changes their strucutre or a better source becomes available. 

So a middle ground has to be achieved between speed, flexibitlity and clean code. So basically, i have the some pretty messy code in the grooming_code and mixing_code folders,but that gets cleaner the more likely i think the code will be used again. The aggregation_code folder has the cleanest code because it is the most likely to be used again. 

The main reason for the system is for the transport model, but it can be used for any analysis. So it is important that the data is in the format that the transport model needs. This is why the aggregation_code folder is the most important. It is also why the data is filtered to match the transport model requirements. But if you want you can always use the outputs from the grooming_code and mixing_code folders for analysis too, they can be found in the intermediate_data folder.

Anyway, its all pretty messy but i would be very very happy if someone else finds it useful!

## The code is organised into folders based on the purpose of the code. The main folders are:

# Data:

 - input_data: where all the input data is stored
 - output_data: where all the output data is stored

# Scripts:

 - grooming_code: extract data from csv's/xlsx's/databases, groom the data so that it is all in the correct format
 - mixing_code: use the data extracted in grooming_code to create new data which can also be selected for. Sometimes uses outputs from other folders too.
 - analysis_code: for analysing the data and creating visualisations. This needs a lot fo work and is not currently used.
 - exploratory_code: for testing out new ideas and seeing if they work. This is not currently used.
 
# Functions/modular code:

 - aggregation_code: There is a main.py file in here. it will only run files from this folder, but it pulls data created in grooming_code and mixing_code. This is the folder which will sort through all the data and create the best dataset from it. The code is organised into different files based on the major prupose, such as 'data_selection_fucntions.py' which contains all the functions for selecting data from the dataset. It will also do it's own form of data_mixing to create new datapoints the user can choose to use. There will be a section below which explains the code here. 
    - uses the file selecton_config.yml to understand what index columns to use and more importantly, what datasets created in the grooming/mixing code to select from when using the automatic selection method. THis allows you to remove datasets that arent likely to be selected from from the pool.

# Other folders:

 - config: where all the config files are stored
 - etc

So generally the system will extract data from csv's/xlsx's/databases in grooming_code, groom the data so that it is all in the correct format in grooming_code, craete some extra data in data_mixiong code and then concatenate everything into one large dataframe in aggregation_code. Then aggregation code will also remove duplicate datapoints (eg. if you have two different values for bus stocks in one timeframe, it will need to choose the best one of those two) by choosing the best data via a very streamlined manual selection process. It will also calcualte some data based on input data (eg. if you have the number of vehicles, average km travelled by car and the average fuel efficiency, it can calculate the total fuel consumption). This is done in aggregation_code and is it's own form of the data_mixing folder. The final dataframe can then be analysed in analysis_code.

The structure of the final dataframe will be for these columns (but this might change as we add new cols):

year | economy | transport_type | vehicle_type | drive | medium | measure | unit | fuel_type | dataset | value | source | comments
---|---|----|----|----|----|----|----|---- | ---- | ---- | ---- | ----
2017 | 01_AUS | freight | ht | bev | road | energy | pj | electricity | ato | 0.1 | ato_country_estimates | this_is_a_comment

## Using selecton_config.yml
This file contains a registry of most of the data that is formatted and output from the grooming_code and mixing_code fodlers. This is then used to inform the aggregation_code functions which use it to gather all of teh data into one dataframe, as well as provide the user a place to state what datasets to automatically choose, if that option is used in the aggregation_code. The file is in yaml format. it is pretty messy but for now it works.

## Intention to integrate with iTEM database
Eventually, if we can, it would be good to design the files so they can be used in iTEM's transport database for extracting data and putting it into the format they have in their database. They might also be looking to integrate with the UN so we could watch that too.

## Data sources
Please note that the following will be quite messy. It is a work in progress and will be updated as we go along.

# 9th_model_first_iteration 
As i do the modelling, these will contain changes i make to the data that i used from this system. As such it will generally be used by default. They essentially are the most accurate data i have in this system.

# EGEDA/ESTO data:
Based on energy data collected by our colleageus in the ESTO team. Since the model needs to match the base year for this data, all data outputs from this system can be put through a method to make that so. 

# usa_alternative_fuels_data_center
Currently being used for mielage, efficiency and other factors for all economies (but not every single datapoint). Will slowly be replaced by other data sources as they become available. 

# ATO data:
This data is from the ATO and can be found here: https://asiantransportoutlook.com/snd/
It is likely that this data will be updated so its important that the scripts allow for future updates
The data covers all asian regions and some others, but not the american continents. It is in the format of a xlsx file with many sheets.
The code to interact with this data goes through the excel file, extracts everything and then filters out most of that so we only have the data i deemed important. Due to this subjectivity it would be good to go back over the data and try include more of it/see how we can utilise more of it. This may involve cleaning up the code which would be good.
Its been noted that the data is not always consistent and accurate, although this is not ATO's fault. some economys have supplied them data for certain categories where there should obviously be no data in that category. This is something that we will have to deal with eventually perhaps with the help of ATO because it is quite difficult for one person to go through all the data and make sure it is correct.

# IEA data:
EV data explorer is a great source of info.

# APERC trasport model 8th edition:
This data is what was used in the 8th edition of the APERC transport model. The major issue with this data is that it has no sources or methods. However it is complete so it can make a good starting point for testing out any analysis. The dataset includes data that was projected as part of the 8th edition. this projected data is from around 2019 onwards, but that isn't specified in the dataset anywhere.
The dataset includes data for Energy, activity and road stocks. There is also data that i trust less and think was estimated based on the scenario the model was based on: turnover_rate, occupance_load and new_vehicle_efficiency

# iTEM data
From item's database which is hosted here https://zenodo.org/record/4121180

# EIA data
Data from the EIA's outlooks are very detailed and useful. They provide an API so the method to extract this data is relatively simple if you know code. Note that the data is for years in the future so it doesnt really fit in with the methods used in main.py. however, this system is not jsut for creating the input data for the transport mdoel, it's outputs can be used for any analysis. So it is still useful to have this data in the system, especailly in the same format as the other data so the model output can be compared to this data. This data may also be sueful for informing assumptions which can be data heavy at times (for example providing sales shares for evs for every year of the outlook projection).

## weird terminology in the work i do:
Sometimes i probably use words like 'concordance' in the wrong contexts. Here are some examples in case it helps:

Concordance table: used to describe a data table used to state certain rules within the data i'm using. for example it can be used to create a mapping between sets of categories (eg. drive types to fuel types) or to state what categories of data are allowed or are present in the data i'm using.

INDEX_COLS: these are used for interacting with pandas indexing functions, which suit having a constant set of columns which are treated as the index. This is used in the aggregation code to make sure that the data is always in the same format and can be easily compared to other data.

## Github LFS (Large File Storage)
So that someone who clones this repo can see the files I input into the system i have implemeented LFS. You may notice that some csv and xlsx files are only 1kb large. This is because they are pointers to the actual files which are stored in the LFS. This is because the files are too large to be stored in the repo. If you want to see the files you can download them from the LFS. To do this you need to install LFS and run the following command in the terminal:
> git lfs install
> git lfs pull

# My current LFS strategy:
I have found it a little tricky to work with LFS but here is what i believe is best:
Create lfs pointers for all files in input_data/** and output_data/** (except those that are ignored by the .gitignore)
This is done by using the commands:
> git lfs track "input_data/**"
> git lfs track "output_data/**"

(You can see the files that are being tracked by LFS in .gitattributes.)

This means that as you add files to input_data and output_data push them to github remote, they will be replaced with pointers on the remote server, which point to the files which are stored in the LFS. If you were then to clone or pull from the github remote you will be pulling the pointers. To download the actual files from the LFS, just run the command which will transform the 1kb pointer files into their actual sized files:
> git lfs pull