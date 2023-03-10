#%%
import sys
%matplotlib notebook
sys.path.append('./aggregation_code')

import aggregation_code.data_selection_functions as data_selection_functions
#%%
#use this below to print names of all py files in this folder and folders inside. You can then easily copy them into the script to run them in the order you need.
import os
for root, dirs, files in os.walk(".", topdown=False):
    #remove dir
    for name in files:
        if name.endswith('.py'):
            filename = os.path.join(root, name).replace('\\', '/')
            #if filename has .git, env_jupyter, .vscode or __pycache__ in it, skip it
            list_of_strings_to_skip = ['.git', 'env_jupyter', '.vscode', '__pycache__', 'integrate.py', 'archive']
            #check if any of the strings in list_of_strings_to_skip are in filename
            if any(x in filename for x in list_of_strings_to_skip):
                pass
            else:
                print('exec(open("{}").read())'.format(filename))
            

#%%
#printed names of all py files in this folder and folders inside:
# .\aggregation_code\1_aggregate_cleaned_datasets.py
# .\aggregation_code\2_select_best_data.py
# .\analysis_code\track_data_requirements_template.py
# .\exploratory_code\explore_ATO_dataset.py
# .\exploratory_code\explore_ATO_dataset_2nd_step.py
# .\grooming_code\1_clean_8th_edition_data.py
# .\grooming_code\1_clean_other_8th_edition_input_data.py
# .\grooming_code\1_extract_ATO_dataset.py
# .\grooming_code\2_aggregate_8th_edition_data.py
# .\grooming_code\2_clean_ATO_dataset.py
# .\grooming_code\3_clean_ATO_dataset.py
#analysis_code\communicate_whole_dataset_details.py
#%%
this = False
#start integrated script:
if this:
    #run the files in this order:
    exec(open("./grooming_code/1_clean_iea_ev_data.py").read())
    exec(open("./grooming_code/1_clean_8th_edition_data.py").read())
    exec(open("./grooming_code/1_clean_item_data.py").read())
    exec(open("./grooming_code/1_clean_other_8th_edition_input_data.py").read())
    exec(open("./grooming_code/1_extract_ATO_dataset.py").read())
    exec(open("./grooming_code/1_clean_macro_data.py").read())
    exec(open("./grooming_code/2_aggregate_8th_edition_data.py").read())
    exec(open("./grooming_code/2_clean_ATO_data.3py").read())
    exec(open("./grooming_code/3_make_extra_changes_to_8th_data.py").read())
    exec(open("./grooming_code/1_clean_iea_ev_data.py").read())
#%%
exec(open("./aggregation_code/1_aggregate_cleaned_datasets.py").read())
exec(open("./aggregation_code/2_identify_duplicate_datapoints.py").read())
#%%
exec(open("./aggregation_code/3_select_best_data_automatic.py").read())
exec(open("./aggregation_code/4_select_best_data_manual.py").read())
exec(open("./aggregation_code/5_combine_manual_and_automatic_output_dfs.py").read())
#%%
%matplotlib notebook
exec(open("./aggregation_code/6_interpolate_missing_values.py").read())



#%%
exec(open("./analysis_code/communicate_whole_dataset_details.py").read())
exec(open("./analysis_code/plot_finalised_data.py").read())
exec(open("./analysis_code/plot_finalised_dataset_coverage.py").read())
exec(open("./analysis_code/plot_whole_dataset_coverage.py").read())
#%%

