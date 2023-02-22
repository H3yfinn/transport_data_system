import os
import re
import datetime
import pandas as pd
import numpy as np

# #set cwd to the root of the project
# os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

def make_economy_code_to_name_tall(economy_code_to_name):
    """Make the economy code to name data wide. 
    """
    #format economy code to name to name to be tall so we can access all possible econmoy names.
    #stack the Economy name, and any columns that begin with Alt_name into a single column
    alt_name_cols = [col for col in economy_code_to_name.columns if col.startswith('Alt_name')]

    economy_code_to_name = economy_code_to_name.melt(id_vars=['Economy'], value_vars=['Economy_name']+alt_name_cols, var_name='column_name', value_name='Economy name').drop(columns=['column_name'])

    #drop na values from economy name
    economy_code_to_name = economy_code_to_name.dropna(subset=['Economy name'])
    return economy_code_to_name
