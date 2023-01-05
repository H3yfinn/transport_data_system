Please view the Wiki here for contextual information:

https://github.com/H3yfinn/transport_data_system/wiki

## SETUP
There are two options for environments. They depend if you want to use jupyter or just the command line to run the model. I prefer to use jupyter but i know that it takes a lot of space/set-up-time.
 - config/env_jupyter.yml
 - config/env_no_jupyter.yml

run:
conda env create --prefix ./env_jupyter --file ./config/env_jupyter.yml

Then:
conda activate ./env_jupyter

Note that installing those libraries in the yml files will result in a few other dependencies also being installed.


## About
This system is intended for handling all data that is needed in the transport model and other APERC transport related systems/analysis.

So the system will extract data from csv's/xlsx's/databases, groomm the data so that it is all in the correct format and then concatenate everything into one large dataframe.

The structure of the dataframe will be for these columns:

year | economy | transport_type | vehicle_type | drive | medium | measure | unit
---|---|----|----|----|----|----|----
2017 | 01_AUS | freight | ht | bev | road | energy | PJ

There will be a lot of files intended for one set of input data and these may become irrelevant over time. So it's expected that an effort will be put towards keepinng things systematic and well documented. 

### Intention to integrate with iTEM database
Also, if we can, it would be good to design the files so they can be used in iTEM's transport database for extracting data and putting it into the format they have in their database.

## Data sources
Please note that the following will be quite messy. It is a work in progress and will be updated as we go along.

# ATO data:
This data is from the ATO and can be found here: https://asiantransportoutlook.com/snd/
It is likely that this data will be updated so its important that the scripts allow for future updates
The data covers all asian regions and some others, but not the american continents. It is in the format of a xlsx file with many sheets.
The code to interact with this data goes through the excel file, extracts everything and then filters out most of that so we only have the data i deemed important. Due to this subjectivity it would be good to go back over the data and try include more of it/see how we can utilise more of it. This may involve cleaning up the code which would be good.
Its been noted that the data is not always consistent and accurate, although this is not ATO's fault. some economys have supplied them data for certain categories where there should obviously be no data in that category. This is something that we will have to deal with eventually perhaps with the help of ATO because it is quite difficult for one person to go through all the data and make sure it is correct.

# IEA data:
None yet but intended we get something!

# APERC trasport model 8th edition:
This data is what was used in the 8th edition of the APERC transport model. The major issue with this data is that it has no sources or methods. However it is complete so it can make a good starting point for testing out any analysis. The dataset includes data that was projected as part of the 8th edition. this projected data is from around 2019 onwards, but that isn't specified in the dataset anywhere.
The dataset includes data for Energy, activity and road stocks. There is also data that i trust less and think was estimated based on the scenario the model was based on: turnover_rate, occupance_load and new_vehicle_efficiency


# weird terminology in the work i do:
Sometimes i probably use words like 'concordance' in the wrong contexts. Here are some examples in case it helps:
Concordance table: used to describe a data table used to state certain rules within the data i'm using. for example it can be used to create a mapping between sets of categories (eg. drive types to fuel types) or to state what categories of data are allowed or are present in the data i'm using.

# Github LFS (Large File Storage)
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