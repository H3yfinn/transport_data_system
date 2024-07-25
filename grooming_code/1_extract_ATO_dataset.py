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
wb = pd.ExcelFile('./input_data/Asian_transport_outlook_ATO/ATO National Data - indicatorview june2024.xlsx')
#extract the sheets in the workbook
sheets = wb.sheet_names


# #remove the sheets that we don't want to extract data from
sheets.remove('TOC')
# sheets.remove('License & Citation')

#%%
# #%%
# #find the last row of data and find the last column of data
# last_row = sheet.range('A14').end('down').row
# last_column = sheet.range('A14').end('right').column
# #extract the data from the sheet
# data = sheet.range((14,1),(last_row,last_column)).value

# #show data
# data
#%%
#define assumnptions
YEAR_ROW_NO = 13
OTHER_TITLES_ROW_NO=YEAR_ROW_NO-2
#%%

#now we need to extract the data from each sheet in the workbook. We will do this by iterating through the list of sheets and extracting the data from each sheet. We will then append the data from each sheet to a list of dataframes. Then we will concatenate all the dataframes in the list into one dataframe.

#first we will create an empty list to store the dataframes in.
dataframes = []

#now we will iterate through the list of sheets and extract the data from each sheet.
for sheet_name in sheets:
    #extract the data from the sheet, making sure to deal with merged cells
    sheet_data = wb.parse(sheet_name)

    #convert col names to col numbers to more easily extract data
    sheet_data.columns = range(0,len(sheet_data.columns))
    
    ############################
    #extract the details from the sheet

    #grab data from the A1 to B10
    details = sheet_data.iloc[0:9,0:2]
    #transpose the details so the name of the details is the column name and the values are the values in the column
    details = details.T
    #set first row as column names and drop the first row
    details.columns = details.iloc[0]
    details = details.drop(details.index[0])
    #remove Description and Dataset cols since they have a lot of text in them and arent veryu sueful for this kind of large scale dataframe
    details = details.drop(['Description:','Website:'], axis=1)#could be better to remove Website, not source but for now keep as is

    #include the sheet name as a column
    details['Sheet'] = sheet_name
    
    ############################
    #extract the data from the sheet from row 12 onwards for non details data
    actual_data = sheet_data.iloc[YEAR_ROW_NO:,:]
    col_names = actual_data.iloc[0]
    #some years will register as floats, so we will remove the decimal point from any cols that have a 4 digit number followed by a decimal point
    col_names = [col_name if not re.match('^\d{4}\.0$', str(col_name)) else str(col_name).replace('.0','') for col_name in col_names]
    
    #make column names to col_names
    actual_data.columns = col_names
    #drop the first row
    actual_data = actual_data.drop(actual_data.index[0])

    #Identify the columns that are not year columns. 
    #We will do this by checking if the column name is a 4 digit number or a 4 digit number followed by a hyphen and 2 digit number.
    #first create a list of the columns that are year columns
    year_cols = actual_data.columns[actual_data.columns.str.contains('^\d{4}$') | actual_data.columns.str.contains('^\d{4}-\d{2}$')]
    #we also want to split the non year cols by whether they come after or before the year cols
    after_non_year_cols = False
    non_year_cols_after = []
    non_year_cols_before = []
    #and get the index numbers of the year cols during this process
    year_col_index = []
    i = 0
    for col in actual_data.columns:
        if col in year_cols:
            after_non_year_cols = True
            year_col_index.append(i)
        elif not after_non_year_cols:
            non_year_cols_before.append(col)
        elif after_non_year_cols:
            non_year_cols_after.append(col)
        i+=1

    #There will be two rows at the bottom of the sheet with ":Developed with the support of: Asian Developme..." and "See terms of use at https://asiantransportoutl...". We will remove these rows and the two rows above them (as they are just NaNs). We can identify this by searching for "Developed with the support of: Asian Development Bank" in the second column of the sheet.
    bottom_rows = actual_data[actual_data.iloc[:,1] == 'Developed with the support of: Asian Development Bank'].index
    #drop that row, the row below it and the two rows above it
    actual_data = actual_data.drop(bottom_rows)
    actual_data = actual_data.drop(bottom_rows+1)
    actual_data = actual_data.drop(bottom_rows-2)
    actual_data = actual_data.drop(bottom_rows-1)

    #CHECK if there are no year or year-month cols then throw an error with the sheet name and col names so we can create a separate script to deal with this
    if (not actual_data.columns[actual_data.columns.str.contains('^\d{4}$')].any()) & (not actual_data.columns[actual_data.columns.str.contains('^\d{4}-\d{2}$')].any()):
        raise ValueError('There are no year or year-month cols in sheet {}, with col names {}'.format(sheet_name, actual_data.columns))

    ############################


    ################
    #In some cases there is one or two extra rows of column names above the year cols. To get around this the most simply, we will asssume there is an extra row of column names above the year cols and extract them.
    extra_col_names = sheet_data.iloc[OTHER_TITLES_ROW_NO:YEAR_ROW_NO,year_col_index[0]:year_col_index[-1]+1]

    #fix issue of merged cells
    extra_col_names = extra_col_names.fillna(method='ffill', axis=1)

    #add together the two rows of col names, even if there is only one or no rows of extra col names
    #first replace any nan values with empty strings
    extra_col_names = extra_col_names.replace(np.nan, '', regex=True)
    extra_col_names_concat = extra_col_names.iloc[0] + '%%' + extra_col_names.iloc[1]

    #find the unique values in the extra col names 
    unique_extra_col_names = extra_col_names_concat.unique()

    #find the indexes of the unique values in the extra col names and put them into a list of lists, ordered in the same way as the unique values
    extra_col_names_index = []
    for col_name in unique_extra_col_names:
        extra_col_names_index.append(extra_col_names_concat[extra_col_names_concat == col_name].index.to_list())

    #Now iteratively transform the actual_data into a tall format, treating each extra col name as a separate dataframe but at the end of each iteration, concat the tall versions of the dataframes together
    large_df = pd.DataFrame()
    for i in range(len(unique_extra_col_names)):
        col_name = unique_extra_col_names[i]
        indexes = extra_col_names_index[i]
        #extract the data in the cols that corresponds to the indexes of the extra col names
        extra_col_names_data = actual_data.iloc[:,indexes]
        #add the non_col_names_before and non_col_names_after to the data thenm melt the data so we have a year col
        extra_col_names_data = pd.concat([extra_col_names_data, actual_data[non_year_cols_before], actual_data[non_year_cols_after]], axis=1)
        extra_col_names_data = pd.melt(extra_col_names_data, id_vars=non_year_cols_before+non_year_cols_after, var_name='year', value_name='value')
        #now create a col witht he col name as the vlaue
        extra_col_names_data['extra_col_name'] = col_name
        #now concat the tall version of the dataframe to the large_df
        large_df = pd.concat([large_df, extra_col_names_data], axis=0)

    ###################
    #CHECK
    #to do come up with a test to make sure the data is in the correct format
    

    ###################
    #Now adsd the details of the sheet to the large_df. We will just duplicate the details for each row in the large_df using the details df which only has one row
    for col in details.columns:
        large_df[col] = details[col].iloc[0]
		
    #Now add the large_df to the list of dfs
    dataframes.append(large_df)

#Now concat all the dfs in the list into one df
all_data_aggregated = pd.concat(dataframes, axis=0)

################################################################################################################################################################
#%%
#We'll now load in the TOC sheet we originally removed and join it to the all_data_aggregated df using the indicator code/name to extract the useful Subcategory column
toc = pd.read_excel('./input_data/Asian_transport_outlook_ATO/ATO National Data - indicatorview june2024.xlsx', sheet_name='TOC')
#Find row in col 2 that has name 'Subcategory'
toc_row = toc[toc.iloc[:,1] == 'Subcategory'].index[0]
# #find what cols contain 'Indicator Code', 'Indicator Name'
# toc_cols = toc.iloc[toc_row, :].str.contains('Indicator Code|Indicator Name|Subcategory')
#Find first row after toc_row that is a nan.
toc_bottom_row = toc.iloc[toc_row:,1].isna().idxmax()
#extract all data from toc_row to toc_bottom_row
toc = toc.iloc[toc_row:toc_bottom_row, :]
#make the first row the col names
toc.columns = toc.iloc[0]
#remove the first row
toc = toc.iloc[1:,:]
#keep cols  = 'Indicator Code', 'Indicator Name' and 'Subcategory'
toc = toc[['Indicator code', 'Indicator name', 'Subcategory']]
#join
all_data_aggregated = all_data_aggregated.merge(toc, how='left', left_on='Indicator ATO Code:', right_on='Indicator code')
#%%
# #so now we have the data. We will save this data to 2 csv files. One for the actual data and one for the other data. But first we will check if the data is the same as the data we have already saved. If it is then we will not save the data again. If it is not then we will save the data again, with a date stamp in the file name so that we can keep track of the changes.
#so first,, checking all files with 'other_data' in the name, against other_data_agg
same_data = False

for file in os.listdir('intermediate_data/ATO/'):
    if 'ATO_extracted_data' in file:
        all_data_aggregated_old = pd.read_csv('intermediate_data/ATO/' + file)
        if all_data_aggregated.equals(all_data_aggregated_old):
            same_data = True
            print('actual_data_agg is the same as the data in '+file)
            break
if same_data == False:
    print('Saving data to csv')
    all_data_aggregated.to_csv('intermediate_data/ATO/ATO_extracted_data_'+FILE_DATE_ID+'.csv', index=False)


#%%