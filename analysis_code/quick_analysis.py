
#set working directory as one folder back
import os
import re
import pandas as pd
import numpy as np
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

# %%
import pandas as pd
import plotly.express as px

# intermediate_data/selection_process/DATE20230519/combined_data.pkl
a =  pd.read_pickle('intermediate_data/selection_process/DATE20230519/combined_data.pkl')
b = pd.read_pickle('intermediate_data/selection_process/DATE20230519/interpolated_stocks_mileage_occupancy_load_efficiency_combined_data_concordance.pkl')
# %%

#take a look at unique datsets:
a['dataset'].unique()
#%%
b['dataset'].unique()

# %%
#see where dataset contains 8th_edition_transport_model
a[a['dataset'].str.contains('8th_edition_transport_model')]

# %%
b[b['dataset'].str.contains('8th_edition_transport_model')]
# %%

#take a look at where the cols economy	measure	vehicle_type are 21_VN	stocks	2w
b[(b['economy']=='21_VN')&(b['measure']=='stocks')&(b['vehicle_type']=='2w')]
# %%
