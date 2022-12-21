#%%
#take in data from sheets in the excel file TRANSPORT ACTIVITY & SERVICES (TAS).xlsx and put them into a dataframe.
#the process wil be mostly about exploring what the best way of extracting this data easily is and how to put it into a dataframe. because the data in the excel file is not in a format that is easily readable by pandas.
#basically the xlsx has a a contents sheet called TOC. From this we COULD choose the codes of data that we want to extract. Although it seems the best route is to just find a way to automatically extract all the data form the sheets and label it acoording to the details from A1 to B11 in the datasheets. 

#the actual data in the sheets always starts at row 15, and the first two columns (A and B) are always the counntry code and the country name (column names: Code | ADB members, plus Iran and Russia). Then the proceeding columns will be the year columns for as many years are available (note that sometimes the year columnns can skip years, for example, have data for 2013, 2018 and 2019). 
# Sometimes the final Year column can be followed by a column called 'Dataset', Update source or $YEAR$ Index. These will, for now, not be included in the extracted data as it is easier to not include them.
#Then after the last year column (or the extra columns metnioned above) there will be at least 1 blank colummns, followed by the columns: 
    #Region	Subregion	Income Group (World Bank)	Annex I or Non-Annex I	OECD Country	G20	ATO Member	Least Developed Countries	SIDS	LandLocked	Regional Environmentally Sustainable Transport (EST) Forum	Pacific Islands Forum (PIF)	Asia-Pacific Economic Cooperation (APEC)	South Asian Association for Regional Cooperation (SAARC)	Association of South East Asian Nations (ASEAN)	Greater Mekong Subregion (GMS)	BIMP-EAGA	Central Asia Regional Economic Cooperation (CAREC) 	Indonesia-Malaysia-Thailand Growth Triangle (IMT-GT)	South Asia Subregional Economic Cooperation (SASEC)	Shanghai Cooperation Organisation	Central and West Asia Department	South Asia Department	East Asia Department	Pacific Department	South East Asia Department
#These columns seem to be the same for all the sheets. All columns before 'OECD Country' have categories unique to their own columns. Then the next three (G20	ATO Member	Least Developed Countries) are eitehr yes/no. The columns after 'Least Developed Countries' are all either blank or Yes.
#there is never anything in the rows after the final row of data, so we can just extract all data from row 15 to the end of the sheet.
#all this data after the last year coluymn will be extracted b ut but into a new dataframe with the details from A1 to B11 in the datasheets as it's index (as well as the country code and country name), like we will also do with the actual data.
#the details from row 1 to row 14 are for the same fields, as follows: 
# Description
# Scope
# Mode
# Sector
# Unit
# Indicator Dataset Code
# Dataset
# Website

#after we have extracted all the data from all sheets, then we will need to put it into a dataframe. The dataframe will have the following columns:
#Code | ADB members, plus Iran and Russia | Year | Value | Description | Scope | Mode | Sector | Unit | Indicator Dataset Code | Dataset | Website
#then based on the data that we find the dataset has we will filter for what is useful to us in another script.

#also, ignore the sheet License & Citation. It is just a sheet that has the license and citation for the data. Incidentally we might as well ignore TOC sheet as well. It is just a table of contents for the data anbd more suited to manual data extraction.

#first, open the excel workbook and extract the names of all the sheets in the workbook. we will use those to extract data from the sheets iteratively.
#%%
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
# import openpyxl
import re
import datetime

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

#create FILE_DATE_ID to be used in the file name of the output file and for referencing input files that are saved in the output data folder
file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)
#%%
#open the workbook using pandas
wb = pd.ExcelFile('./input_data/TRANSPORT ACTIVITY & SERVICES (TAS).xlsx')
#extract the sheets in the workbook
sheets = wb.sheet_names
#remove the sheets that we don't want to extract data from
sheets.remove('TOC')
sheets.remove('License & Citation')


# #%%
# #find the last row of data and find the last column of data
# last_row = sheet.range('A14').end('down').row
# last_column = sheet.range('A14').end('right').column
# #extract the data from the sheet
# data = sheet.range((14,1),(last_row,last_column)).value

# #show data
# data
#%%

#now we need to extract the data from each sheet in the workbook. We will do this by iterating through the list of sheets and extracting the data from each sheet. We will then append the data from each sheet to a list of dataframes. Then we will concatenate all the dataframes in the list into one dataframe.

#first we will create an empty list to store the dataframes in.
dataframes = []

#now we will iterate through the list of sheets and extract the data from each sheet.
actual_df_agg = False
other_df_agg = False
for sheet_name in sheets:

    #extract the data from the sheet
    sheet_data = wb.parse(sheet_name)

    ############################
    #extract the details from the sheet

    #grab data from the A1 to B10
    details = sheet_data.iloc[0:10,0:2]
    #transpose the details so the name of the details is the column name and the values are the values in the column
    details = details.T
    #set first row as column names and drop the first row
    details.columns = details.iloc[0]
    details = details.drop(details.index[0])
    #remove Description and Dataset cols since they have a lot of text in them and arent veryu sueful for this kind of large scale dataframe
    details = details.drop(['Description','Source'], axis=1)

    #include the sheet name as a column
    details['Sheet'] = sheet_name
    
    ############################
    #extract the data from the sheet from row 12 onwards for non details data
    data = sheet_data.iloc[13:,:]
    col_names = data.iloc[0]
    #make column names to col_names
    data.columns = col_names
    #drop the first row
    data = data.drop(data.index[0])

    ################
    #on rare occasion there will be a sheet where the Years cols are replaced by categorical  columns, like 'Aviation', 'Road', 'Rail', to indicate mode shares. In this case, the script will ignore the sheet but make it clear to the suer in case the user wants to manually extract the data from the sheet.

    #we will identify these situations by searching for floats in the col names. if there are none then its very likely this is a sheet with categorical columns instead of year columns and we will deal with it accordingly.

    #this will also be the case if the sheet has no data in it, so we will also check if the sheet has any data in it. if it doesnt then we will also deal with it accordingly.
    if not any((isinstance(x, float) and not np.isnan(x)) for x in col_names):
        #now if the 3rd col is a nan then we know that the sheet has no data in it. if it is not a nan then we know that the sheet has data in it.
        if not pd.isnull(data.iloc[0,2]):
            #now we know that the sheet has data in it, but the columns are categorical. we will deal with this later.
            print('Sheet '+sheet_name+' has categorical columns instead of year columns. This sheet will be ignored. Please manually extract the data from this sheet.')
        else:
            #now we know that the sheet has no data in it.
            print('Sheet '+sheet_name+' has no data in it. This sheet will be ignored.')
        continue
    #double check we are ignoring TAS-PAG-014. if not code isnt working right
    if sheet_name == 'TAS-PAG-014':
        print('error' + col_names)
    ################
    #ELSE (if the sheet has year columns and not categorical columns)
    #grab data that comes before (and including) the last year column. This last year col can be indentified as it is the last float in the first row. This data will be extracted into a new dataframe and then attached to the details from A1 to B11 in the datasheets as it's index (as well as the country code and country name), like we will also do with the other data after this.

    #first we need to find the index of the last float in the first row that isnt a nan
    last_year_index = [i for i, x in enumerate(col_names) if isinstance(x, float) and not np.isnan(x)][-1]
    #now we separate the data into two dataframes. one with the actual data and one with the data that comes after the last year column.
    actual_data = data.iloc[:,0:last_year_index+1]

    #add on the details df as new columns with its vlaues replicated for each row
    for col in details.columns:
        actual_data[col] = details[col].values[0]
    
    #grab the country code and country name from the sheet to use in other data later
    actual_data_country_cols = actual_data[['Code', 'ADB members, plus Iran and Russia']]
    #melt the actual data so that the years are in one column and the values are in another column. 
    actual_data = actual_data.melt(id_vars=['Code', 'ADB members, plus Iran and Russia']+list(details.columns), var_name='Year', value_name='Value')
    #convert the year column to int
    actual_data['Year'] = actual_data['Year'].astype(int)

    #include the sheet name as a column
    actual_data['Sheet'] = sheet_name
    #if this is the first sheet then we will just set the actual_df_agg to the actual_data 
    if actual_df_agg is False:
        actual_df_agg = actual_data.copy()
        
    #else we will concatenate the actual_data to the actual_df_agg
    else:
        actual_df_agg = pd.concat([actual_df_agg, actual_data], axis=0)

    ################
    #and finally get the other data from the sheet and add it to the other_df_agg
    #this data has its column names in the 9th row but the data starts in the 16th row (because of cell merging in excel). So we will extract the column names from the 9th row and then extract the data from the 16th row onwards.
    #we can tell the column index where the data starts by looking in the 9th row for the first non nan value aftrer the 2nd column (which is where the details are). This will be the index of the first column that has data in it.
    col_names = sheet_data.iloc[7,2:]
    #return col number of the first non nan value in the 9th row 
    start_index = col_names.first_valid_index()#pandas fucntion to return the index of the first non nan value in a series
    #grab col number for this index
    start_col = col_names.index.get_loc(start_index)
    #now we can extract the data from the 16th row onwards
    other_data = sheet_data.iloc[14:,2+start_col:]
    #remove columns of nan values
    other_data = other_data.dropna(axis=1, how='all')
    #set the column names   to the col_names
    other_data.columns = col_names[start_col:]
    # #remove the first row
    # other_data = other_data.drop(other_data.index[0])

    #add on the details df as new columns with its vlaues replicated for each row
    for col in details.columns:
        other_data[col] = details[col].values[0]

    #add on the country code and country name cols which are the first 2 cols of actual_df to the other_df
    other_data = pd.concat([actual_data_country_cols, other_data], axis=1)

    #if this is the first sheet then we will just set the other_df_agg to the other_data 
    if other_df_agg is False:
        other_df_agg = other_data.copy()
        
    #else we will concatenate the other_data to the other_df_agg 
    else:
        other_df_agg = pd.concat([other_df_agg, other_data], axis=0)
#%%
																									
################################################################################################################################################################

#%%
# #so now we have the data. We will save this data to 2 csv files. One for the actual data and one for the other data. But first we will check if the data is the same as the data we have already saved. If it is then we will not save the data again. If it is not then we will save the data again, with a date stamp in the file name so that we can keep track of the changes.
#so first,, checking all files with 'other_data' in the name, against other_data_agg
same_other_data = False
same_actual_data = False
for file in os.listdir('intermediate_data/ATO_data/'):
    if 'other_data' in file:
        other_data_agg_old = pd.read_csv('intermediate_data/ATO_data/' + file)
        if other_df_agg.equals(other_data_agg_old):
            same_other_data = True
            print('other_data_agg is the same as the data in '+file)
            break
if same_other_data == False:
    print('other_data_agg is different from the data in all files with "other_data" in the name')
    other_df_agg.to_csv('intermediate_data/ATO_data/ATO_other_data_'+FILE_DATE_ID+'.csv', index=False)

for file in os.listdir('intermediate_data/ATO_data/'):
    if 'actual_data' in file:
        actual_data_agg_old = pd.read_csv('intermediate_data/ATO_data/' + file)
        if actual_df_agg.equals(actual_data_agg_old):
            same_actual_data
            print('actual_data_agg is the same as the data in '+file)
            break
if same_actual_data == False:
    print('actual_data_agg is different from the data in all files with "actual_data" in the name')
    actual_df_agg.to_csv('intermediate_data/ATO_data/ATO_actual_data_'+FILE_DATE_ID+'.csv', index=False)


#%%