#%%
import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import pickle
import matplotlib.pyplot as plt
import warnings
import sys

#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')
 
def convert_to_proportions(levels,non_level_index_cols, df):
       #intention is to create a proportion of total value for each level of the hierarchy given one column of values for all levels
       # Levels is a list that is in order of the hierarchy within the dataframe
       #it contains every level below the one we want a proportion in the hierarchy since we will most likely want thosae proportions as well
       #each proportion for each level is calculated by dividing the value of the level by the total value of the level above it
       current_levels_list =[]
       #create a proportions df which only contains the index cols, level cols
       proportions_df = df[levels+non_level_index_cols].copy()

       for level in levels:#[1:]:
              #Copy df
              df_copy = df.copy()
              #add level to current levels list
              current_levels_list.append(level)
              
              #CALCUALTE SUM FOR CURRENT LEVEL
              #Goupby current levels list and sum
              summed_df = df_copy.groupby(current_levels_list+non_level_index_cols,dropna=False).sum().reset_index()
              name_1 = level+'_total'
              #make the value col name the level + _total
              summed_df = summed_df.rename(columns={'Value':name_1})
              #drop unnecessary columns
              unncessary_cols = [col for col in summed_df.columns if col not in current_levels_list+non_level_index_cols+[name_1]]
              summed_df = summed_df.drop(columns=unncessary_cols)
              #drop duplicates
              summed_df = summed_df[~summed_df.duplicated(keep='first')]

              #CALCUALTE SUM FOR LEVEL ABOVE
              #if there is no level above then sum for all levels
              if len(current_levels_list) == 1:
                     #groupby non level index cols and sum
                     summed_df2 = df_copy.groupby(non_level_index_cols,dropna=False).sum().reset_index()
                     #make name_2 'total'
                     name_2 = 'total'
                     #make the value col name2
                     summed_df2 = summed_df2.rename(columns={'Value':name_2})
                     #drop unnecessary columns
                     unncessary_cols = [col for col in summed_df2.columns if col not in non_level_index_cols+[name_2]]
                     summed_df2 = summed_df2.drop(columns=unncessary_cols)
                     #drop duplicates
                     summed_df2 = summed_df2[~summed_df2.duplicated(keep='first')]

                     #join df to summed_df2
                     df_copy = df_copy.merge(summed_df2, on=non_level_index_cols, how='left')
              else:
                     #groupby current levels list minus the current level and sum
                     summed_df2 = df_copy.groupby(current_levels_list[:-1]+non_level_index_cols,dropna=False).sum().reset_index()       
                     #make the value col name the previous level + _total
                     name_2 = current_levels_list[-2]+'_total'
                     summed_df2 = summed_df2.rename(columns={'Value':name_2})
                     #drop unnecessary columns
                     unncessary_cols = [col for col in summed_df2.columns if col not in non_level_index_cols+current_levels_list[:-1]+[name_2]]
                     summed_df2 = summed_df2.drop(columns=unncessary_cols)
                     #drop duplicates
                     summed_df2 = summed_df2[~summed_df2.duplicated(keep='first')]

                     #join df to summed_df2
                     df_copy = df_copy.merge(summed_df2, on=non_level_index_cols+current_levels_list[:-1], how='left')

              #join summed_df1 to df
              df_copy = df_copy.merge(summed_df, on=current_levels_list+non_level_index_cols, how='left')

              #Divide current level total by upper level total to get current level proportion
              df_copy[level+'_proportion'] = df_copy[name_1]/df_copy[name_2]
              #if we did 0/0 then the value would be nan, so we replace nan with 0
              df_copy[level+'_proportion'] = df_copy[level+'_proportion'].fillna(0)
              #remove uncessary columns
              unncessary_cols = [col for col in df_copy.columns if col not in current_levels_list+non_level_index_cols+[level+'_proportion']]
              df_copy = df_copy.drop(columns=unncessary_cols)
              #drop duplicates
              df_copy = df_copy[~df_copy.duplicated(keep='first')]
              #join proportion onto proportions_df
              proportions_df = proportions_df.merge(df_copy, on=current_levels_list+non_level_index_cols, how='left')
       return proportions_df


#one could argue that economy is a level, but it is not a level that we want to calculate proportions for.
def insert_new_proportions(proportions_df, new_proportions_df, levels, non_level_index_cols):
       """
       This will take new_proportions_df which has new values for the proportions we want to replace. Its columns will be for all index cols and then the levels up to and including the level we want to replace. For example if we want to replace buses as a proportion of total road stocks within the passenger transport type then the columns will be:
       Index cols: ['Measure', 'Date', 'Scope','Frequency', 'Fuel_Type','Dataset', 'Unit', 'Economy'] 
       Levels: ['Transport Type','Medium', 'Vehicle Type']
       and then the values we want to insert will be in a column named 'Vehicle Type_proportion'
       
       The process will merge the new vlaues into the proportions_df in a new column with suffix '_new'. 
       We will also create another df where we group by the Levels except the level we want to replace and then sum the values. Then create a new column 'one_minus' which is 1 minus the sum of the new values. This will also be joined to the proportions_df, and then we will multiply the old values by the one_minus column.
       We will finally replace the old values with the new values where we can and then drop all the columns we created.

       Please note that this will remove any rows that are not in both proportions_df and new_proportions_df by using an inner join. So if we only have new bus data for aus, then it will return a df with only aus data.

       """
       #this will simply replace the old value with the new one, and then recalculate the proportions for the current level using (1-new * old)=old for each old proportion. 
       values_col = levels[-1]+'_proportion'
       
       #join new_proportions_df onto proportions_df
       proportions_df = proportions_df.merge(new_proportions_df, on=non_level_index_cols+levels, how='left', suffixes=('','_new'))

       #create one_minus column in new_proportions_df:
       #group and sum the new values for all levels except the level we want to replace
       new_proportions_df_grouped = new_proportions_df.groupby(non_level_index_cols+levels[:-1]).sum()
       #calcualte one_minus
       new_proportions_df_grouped['one_minus'] = 1-new_proportions_df_grouped[values_col]
       #join onto proportions_df using an inner join which will serve to filter out any rows that are not in both
       proportions_df = proportions_df.merge(new_proportions_df_grouped['one_minus'], on=non_level_index_cols+levels[:-1], how='inner')
       #multiply old values by one_minus
       proportions_df[values_col] = proportions_df[values_col]*proportions_df['one_minus']
       #replace old values with new values where we can
       proportions_df.loc[proportions_df[values_col+'_new'].notnull(),values_col] = proportions_df.loc[proportions_df[values_col+'_new'].notnull(),values_col+'_new']
       #drop all the columns we created
       proportions_df = proportions_df.drop(columns=[values_col+'_new','one_minus'])

       return proportions_df
