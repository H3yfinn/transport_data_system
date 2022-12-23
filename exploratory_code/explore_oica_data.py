#we will explore the data and the best way to import it before doign so. 

# %%

import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import plotly.express as px
pd.options.plotting.backend = "plotly"#set pandas backend to plotly plotting instead of matplotlib
import plotly.io as pio
pio.renderers.default = "browser"#allow plotting of graphs in the interactive notebook in vscode #or set to notebook
import plotly.graph_objects as go
import plotly
import datetime

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

PRINT_GRAPHS_AND_STATS = False

#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)


#%%
#first load all the spreadsheets in input_data\oica_data and put them in a dictionary using the name of the file as the key
oica_data_dict = {}
for file in os.listdir('input_data/oica_data'):
    if file.endswith('.xlsx'):
        df = pd.read_excel('input_data/oica_data/{}'.format(file))
        oica_data_dict[file] = df
#%%
#now we need to clean the data.
#generally they all follow the same format. at line 2 is title , line 4 are col names, then starting line 6 is the data. however there are sums of the regions for the data interspersed in the data. so we need to remove those. They can be recognise because they are in red or bold text, compared to individual countries that are in regular text.

#so we will loop through each dataframe in the dictionary, record the title in line 2 in a new dictionary (with file naem as key), then set the col names as the data in row 4, then loop the rest of the ropws to remove the sums of regions/countries. Then add that data to a new dictionary with the file name as the key.

#first create the dictionaries
oica_data_dict_cleaned = {}
oica_data_dict_titles = {}

for file in oica_data_dict:
    df = oica_data_dict[file]
    #first get the title
    title = df.iloc[1,0]
    oica_data_dict_titles[file] = title
    #now set the col names
    df.columns = df.iloc[3]
    #now loop through the rows and remove the rows that are sums of regions/countries
    #UP TO HERE. WAITING FOR CHATGPT3 TO ANSWER THIS QUEASTION:
    # is there a way that python can recognise text formatting in an xlsx workbook? this would help for extracting data


#some tips below:
import openpyxl as xl

wb = xl.load_workbook('FormatTest.xlsx')   #, read_only=True)
ws = wb.active
matchingCells = []
cells = ws._cells
for row, col in cells.keys():
    cell = cells[row, col]
    if cell.font.italic or cell.font.bold:
        matchingCells.append(cell)

for cell in matchingCells:
    formatDesc = " and ".join(x[0] for x in [("italic", cell.font.italic), ("bold", cell.font.bold)] if x[1])
    print(f'cell {cell.coordinate} with value "{cell.value}" is {formatDesc}')