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

So the system will extract data from csv's/xlsx's/databases?, groomm the data so that it is all in the correct format and then concatenate everything into one large dataframe.

The structure of the dataframe will be for these columns:

year | economy | transport_type | vehicle_type | drive | medium | measure | unit
---|---|----|----|----|----|----|----
2017 | 01_AUS | freight | ht | bev | road | energy | PJ

There will be a lot of files intended for one set of input data and these may become irrelevant over time. So it's expected that an effort will be put towards keepinng things systematic and well documented. Also, if we can, it would be good to design the files so they can be used in iTEM's transport database for extracting data and putting it into the format they have in their database.
