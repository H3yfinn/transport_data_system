#%%
combined_data = data_formatting_functions.combine_datasets(datasets_transport, FILE_DATE_ID,paths_dict)

#%%
if create_9th_model_dataset:
    #import snapshot of 9th concordance
    model_concordances_base_year_measures_file_name = './intermediate_data/9th_dataset/{}'.format('model_concordances_measures.csv')
    combined_data = data_formatting_functions.filter_for_9th_edition_data(combined_data, model_concordances_base_year_measures_file_name, paths_dict)

#since we dont expect to run the data selection process that often we will just save the data in a dated folder in intermediate_data/data_selection_process/FILE_DATE_ID/

#%%
combined_data_concordance = data_formatting_functions.create_whole_dataset_concordance(combined_data, frequency = 'yearly')
# combined_data_concordance, combined_data = data_formatting_functions.change_column_names(combined_data_concordance, combined_data)

#%%
#save data to pickle so we dont have to do this again
# combined_data_concordance.to_pickle('combined_data_concordance.pkl')
# # combined_data.to_pickle('combined_data.pkl')
# #%%
# #laod data from pickle
# combined_data_concordance = pd.read_pickle('combined_data_concordance.pkl')
# combined_data = pd.read_pickle('combined_data.pkl')

#%%
sorting_cols = ['date','economy','measure','transport_type','medium', 'vehicle_type','drive','fuel','frequency','scope']
combined_data_concordance, combined_data = data_formatting_functions.prepare_data_for_selection(combined_data_concordance,combined_data,paths_dict, sorting_cols)#todo reaplce everything that is combined_dataset with combined_data
#%%
passsenger_road_measures_selection_dict = {'measure': 
    ['efficiency', 'occupancy', 'mileage', 'stocks'],
 'medium': ['road'],
 'transport_type': ['passenger']}

combined_data,combined_data_concordance = data_formatting_functions.filter_for_specifc_data(passsenger_road_measures_selection_dict,combined_data_concordance, combined_data)
#%%

grouping_cols = ['economy','vehicle_type','drive']
manual_data_selection_handler(grouping_cols, combined_data_concordance, combined_data, paths_dict)