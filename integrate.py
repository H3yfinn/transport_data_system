
#%%
#use this below to print names of all py files in this folder and folders inside. You can then easily copy them into the script to run them in the order you need.
import os

for root, dirs, files in os.walk(".", topdown=False):
    for name in files:
        if name.endswith('.py'):
            print(os.path.join(root, name))

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
#start integrated script:

#run the files in this order:
exec(open("./grooming_code/1_clean_8th_edition_data.py").read())
exec(open("./grooming_code/1_clean_other_8th_edition_input_data.py").read())
exec(open("./grooming_code/1_extract_ATO_dataset.py").read())
exec(open("./grooming_code/2_aggregate_8th_edition_data.py").read())
exec(open("./grooming_code/2_clean_ATO_dataset.py").read())
exec(open("./grooming_code/3_clean_ATO_dataset.py").read())
#%%
exec(open("./aggregation_code/1_aggregate_cleaned_datasets.py").read())
# exec(open("./aggregation_code/2_select_best_data.py").read())

# exec(open("./analysis_code/track_data_requirements_template.py").read())
exec(open("./analysis_code/communicate_whole_dataset_details.py").read())
#%%

