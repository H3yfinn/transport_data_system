
def set_up_sets_for_selection(selection_set, iterator,FILE_DATE_ID):
    #first create a folder with the selections from a previous manual data selection process
    selection_set_folder_name = '_'.join(selection_set)+FILE_DATE_ID
    #check we ahve a folde rfor this selection set
    if not os.path.exists('intermediate_data/data_selection/{}'.format(selection_set_folder_name)):
        os.makedirs('intermediate_data/data_selection/{}'.format(selection_set))

    #group data by the selection set
    iterator_grouped = iterator.groupby(selection_set)

    return iterator_grouped, selection_set_folder_name
    

def save_set_data_to_file(combined_set_data, group_name,selection_set, FILE_DATE_ID):
    #save the data to file
    selection_set_folder_name = '_'.join(selection_set)+FILE_DATE_ID
    combined_set_data.to_csv(f'intermediate_data/data_selection/{selection_set_folder_name}/{group_name}_piece.csv',index=False)

def mock_selection_handler(iterator_group,combined_data_concordance_manual,combined_data,duplicates_manual,INDEX_COLS,group_name,selection_set,FILE_DATE_ID=''):
    iterator_grouped, selection_set_folder_name = set_up_sets_for_selection(selection_set, iterator,FILE_DATE_ID)

    for group in iterator_grouped.groups:
        #send in the group to select_best_data_manual. 

        #create group id which is a string of the group values
        group_name = '_'.join(group)
        print('group id is {}'.format(group_name))
        #get the data for this group
        iterator_group = iterator_grouped.get_group(group)
        #send the data to the selection function
        # combined_data_concordance_manual, duplicates_manual, bad_index_rows, num_bad_index_rows = select_best_data_manual_by_group(combined_data_concordance_iterator,iterator_group,combined_data_concordance_manual,combined_data,duplicates_manual,INDEX_COLS,group_name, selection_set,FILE_DATE_ID=FILE_DATE_ID)

        save_set_data_to_file(combined_set_data, group_name,selection_set, FILE_DATE_ID)
    
    combine_data_from_selected_sets(selection_set, FILE_DATE_ID)

    #end

def combine_data_from_selected_sets(selection_set, FILE_DATE_ID):
    #now we can import the data from this folder, concatenate it all and then apply it to the combined data concordance and so on.
    selection_set_folder_name = '_'.join(selection_set)+FILE_DATE_ID
    #get the list of files in this folder
    files = os.listdir(f'intermediate_data/data_selection/manual_data_selection_sets/{selection_set_folder_name}')
    #remove non csv files
    files = [file for file in files if file[-4:] == '.csv']
    #if files is empty then let user know
    if files == []:
        print('WARNING: no files found in folder intermediate_data/data_selection/manual_data_selection_sets/{}'.format(selection_set_folder_name))
    #create a df to store the data
    combined_set_data = pd.DataFrame()
    #loop through the files and append the data to the combined df
    for file in files:
        #load the data
        data = pd.read_csv(f'intermediate_data/data_selection/manual_data_selection_sets/{selection_set_folder_name}/{file}')
        #append the data
        combined_set_data = combined_set_data.append(data)
    return combined_set_data



def import_previous_selections(selection_set, FILE_DATE_ID,import_previous_manual_concordance,pick_up_where_left_off):
    if import_previous_manual_concordance:
        if selection_set == []:

            file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/data_selection/', '_data_selection_manual')
            FILE_DATE_ID2 = 'DATE{}'.format(file_date)
            previous_selections = pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID2))

        else:

            selection_set_folder_name = '_'.join(selection_set)+FILE_DATE_ID
            file_date = utility_functions.get_latest_date_for_data_file(f'./intermediate_data/data_selection/{selection_set_folder_name}', '_piece.csv')
            FILE_DATE_ID2 = 'DATE{}'.format(file_date)
            previous_selections = combine_data_from_sets(selection_set, FILE_DATE_ID2)

        # previous_selections = pd.read_csv('intermediate_data/data_selection/DATE20230214_data_selection_manual - Copy (2).csv')
    elif pick_up_where_left_off:
        file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/data_selection/', '_duplicates_manual')
        FILE_DATE_ID2 = 'DATE{}'.format(file_date)

        previous_selections = pd.read_csv('intermediate_data/data_selection/{}_progress.csv'.format(FILE_DATE_ID2))



#TEMP
file_date = utility_functions.get_latest_date_for_data_file('./intermediate_data/data_selection/', '_data_selection_manual')
FILE_DATE_ID2 = 'DATE{}'.format(file_date)
previous_selections = pd.read_csv('intermediate_data/data_selection/{}_data_selection_manual.csv'.format(FILE_DATE_ID2))
#now split it into groups and save each group in a separate csv file in the folder manual_data_selection_sets
previous_selections_grouped = previous_selections.groupby(selection_set)
for group in previous_selections_grouped.groups:
    #create group id which is a string of the group values. since group is a tuple we should convert it to a list then join the list elements together
    group_name = '_'.join(group)
    print('group id is {}'.format(group_name))
    #get the data for this group
    previous_selections_group = previous_selections_grouped.get_group(group)
    #save the data for this group
    previous_selections_group.to_csv(f'intermediate_data/data_selection/manual_data_selection_sets/{selection_set_folder_name}/{group_name}_piece.csv',index=False)






set_up_sets_for_selection(selection_set, iterator)

#now we can import the data from this folder, concatenate it all and then apply it to the combined data concordance and so on.

for group in iterator_grouped.groups:
    #send in the group to select_best_data_manual. 

    #create group id which is a string of the group values
    group_name = '_'.join(group)
    #get the data for this group
    iterator_group = iterator_grouped.get_group(group)
    #send the data to the selection function
    # combined_data_concordance_manual, duplicates_manual, bad_index_rows, num_bad_index_rows = select_best_data_manual_by_group(combined_data_concordance_iterator,iterator_group,combined_data_concordance_manual,combined_data,duplicates_manual,INDEX_COLS,group_name, selection_set,FILE_DATE_ID=FILE_DATE_ID)



    # create a new folder named for the selection set in intermediate data/data selection/manual_data_selection_sets/
    #in this folder we will store the finished progress of the manual data selection for this set, named by the group id. Then if we ever want to import previous manual data selection we can just import the data from this folder, concatenate it all and then apply it to the combined data concordance and so on.
    #this way we can save effective checkpoints of the manual data selection process. 

    #first we will design the process that will allow us to import previous manual data selection, then we will design the process that will allow us to save checkpoints of the manual data selection process.
    

    

combined_data_concordance_manual, duplicates_manual, bad_index_rows, num_bad_index_rows = data_selection_functions.select_best_data_manual(combined_data_concordance_iterator,iterator,combined_data_concordance_manual,combined_data,duplicates_manual,INDEX_COLS,FILE_DATE_ID=FILE_DATE_ID)#

#TEMP over





def graph_dashboard_matplotlib(combined_data_group, ax_dash, fig_dash,original_fig_dash_size):
    #plot a chart on fig_dash_dash for the current group
    # def graph_dashboard(combined_data_group, fig_dash,original_fig_dash_size,color_cols=['Dataset','Drive']):
    #create set of subplots for each measure in the group data
    # fig_dash.subplots(len(combined_data_group.Measure.unique()),1)
    #for each measure in the group data, plot a subplot
    
    #make a shape for the set of subplots that will be as close to a square as possible
    ax_dash = []
    shape = (int(np.ceil(np.sqrt(len(combined_data_group.Measure.unique())))),int(np.floor(np.sqrt(len(combined_data_group.Measure.unique()))))) 
    i = 0
    #change fix size to be original fig size width and height * shape * 1.1
    fig_dash.set_size_inches(original_fig_dash_size*shape*1.1)

    #concat specified color cols
    color_cols_str = '_'.join(color_cols)
    combined_data_group[color_cols_str] = combined_data_group[color_cols].apply(lambda x: '_'.join(x), axis=1)
    for measure in combined_data_group.Measure.unique():
        i+=1
        #get data for this measure
        measure_data = combined_data_group.loc[combined_data_group.Measure == measure]
        #drop any 0's
        measure_data = measure_data.loc[measure_data.Value != 0]
        #plot data for this measure
        ax_dash.append(fig_dash.add_subplot(shape[0],shape[1],i))

        #for each dataset in the measure data, plot a line, making sure its sorted
        #but because we might end up with two lines on top of each other, make each successive line a little bit more transparent and thicker
        alpha_start = 1
        alpha_step = 1 / (len(measure_data[color_cols_str].unique())+5)
        thickness_end = 10
        thickness_step = 1
        thickness_start = thickness_end /(thickness_step * (len(measure_data[color_cols_str].unique())))

        for color_col in sorted(measure_data[color_cols_str].unique()):
            # pick color
            color = next(ax_dash[-1]._get_lines.prop_cycler)['color']
            color_cols_dataset = measure_data.loc[measure_data[color_cols_str] == color_col]

            #plot the data using line with markers
            ax_dash[-1].plot(color_cols_dataset.Date, color_cols_dataset.Value, label=color_col, color=color, marker='o', linestyle='-', alpha = alpha_start, markersize=thickness_start)
            #reduce alpha and thickness for next line
            alpha_start -= alpha_step
            thickness_start += thickness_step
        ax_dash[-1].set_title(measure)
        #set legend outside of plot and only plot scatter legend
        ax_dash[-1].legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    fig_dash.tight_layout()
    plt.show()
