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
    economy_code_to_name = economy_code_to_name.melt(id_vars=['Economy'], value_vars=['Economy_name']+['Alt_name_{}'.format(i) for i in range(1, 10)], var_name='column_name', value_name='Economy name').drop(columns=['column_name'])
    return economy_code_to_name
