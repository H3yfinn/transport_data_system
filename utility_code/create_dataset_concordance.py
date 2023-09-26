#use this to create a concordance table which will establish what data we should theoretically have in all categories in the datasets, if the datasets didn't have holes in them. This will be used so that if any new datathat we werent expecting is found in the datasets, we can easily identify it and decide what to do with it. 

#This will also be useful for identifying when data might have been miscategorised, as we can expect that anything that is miscategorised will come up as a category we werent expecting to see in the concordance table.

#it will also complement the process of trying to document/understand what data we have / are meant to have in the datasets

#ISSUE WITH CODE BELOW IS that we are probably going to have very few datapoints that are duplicates because we will get lots of datapoints for lots of different categories that dont match exactly. eg. ato data is not split into different vehicle or engine types so there are no duplicates between that and 8th edition activity data. So the method of manually specifiying the categories for whch we expect data to be for wont work. we sahould instead let the data that we get determine what data we have concordances for, then when a system asks for data that we dont have, that system can specify what is missing and the process of estimating that data can be separate from teh transport data system, i guess. 
# #%%
# #we will create a list of measures, list of categories, and list of datasets

# manually_defined_transport_categories = pd.read_csv('config/concordances_and_config_data/manually_defined_transport_categories.csv')

# #drop duplicates
# manually_defined_transport_categories.drop_duplicates(inplace=True)

# #could create concordances for each year, economy and scenario and then cross that with the osemosys_concordances to get the final concordances
# model_concordances = pd.DataFrame(columns=manually_defined_transport_categories.columns)
# for year in range(BASE_YEAR, END_YEAR+1):
#     for economy in ECONOMY_LIST:#get economys from economy_code_to_name concordance in config.py
#         for scenario in SCENARIOS_LIST:
#             #create concordances for each year, economy and scenario
#             manually_defined_transport_categories_year = manually_defined_transport_categories.copy()
#             manually_defined_transport_categories_year['Year'] = str(year)
#             manually_defined_transport_categories_year['Economy'] = economy
#             manually_defined_transport_categories_year['Scenario'] = scenario
#             #merge with manually_defined_transport_categories_year
#             model_concordances = pd.concat([model_concordances, manually_defined_transport_categories_year])

# #save model_concordances with date
# model_concordances.to_csv('config/concordances_and_config_data/computer_generated_concordances/{}'.format(model_concordances_file_name), index=False)
#%%
#
#The final output will be a large, tall dataframe with one row for each unique combination of categories,  a years column, a vlaues column, a dataset column and a column to indicate if the value was manually selected or automatically selected and a column to indicate how many datapoints there were for that row

#So first of all we will create a dataframe which replicates the final dataframe but with no values in the dataset, value and duplicate columns. But fortunately that is created in ./utility_code/create_dataset_concordance.py so we can just import that
