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
This system is intended for handling all data that is needed in the transport model and other APERC transport related systems/analysis.

The code is organised into folders based on the purpose of the code. The main folders are:

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
    - uses the file selecton_config.yml to understand what index columns to use and more importantly, what datasets created in the grooming/mixing code to select from. THis allows you to remove datasets that arent likely to be selected from from the pool.

# Other folders:

 - config: where all the config files are stored
 - etc

So generally the system will extract data from csv's/xlsx's/databases in grooming_code, groom the data so that it is all in the correct format in grooming_code, craete some extra data in data_mixiong code and then concatenate everything into one large dataframe in aggregation_code. Then aggregation code will also remove duplicate datapoints (eg. if you have two different values for bus stocks in one timeframe, it will need to choose the best one of those two) by choosing the best data via a very streamlined manual selection process. It will also calcualte some data based on input data (eg. if you have the number of vehicles, average km travelled by car and the average fuel efficiency, it can calculate the total fuel consumption). This is done in aggregation_code and is it's own form of the data_mixing folder. The final dataframe can then be analysed in analysis_code.

The structure of the final dataframe will be for these columns (but this might change as we add new cols):

year | economy | transport_type | vehicle_type | drive | medium | measure | unit | fuel_type | dataset | value | source | comments
---|---|----|----|----|----|----|----|---- | ---- | ---- | ---- | ----
2017 | 01_AUS | freight | ht | bev | road | energy | pj | electricity | ato | 0.1 | ato_country_estimates | this_is_a_comment

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

# weird terminology in the work i do:
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