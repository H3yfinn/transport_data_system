#this will compare the data explorer versions of the IEA EV explorer so we can understand how their projections have changed over time.

#set working directory as one folder back
import os
import re
import pandas as pd
import numpy as np
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

# %%
import pandas as pd
import plotly.express as px

#%%
#read in the data
evs_2022 = pd.read_csv('intermediate_data/IEA/DATE20230531_evs_2022.csv')
evs_2023 = pd.read_csv('intermediate_data/IEA/DATE20230531_evs.csv')

#concat them
evs = pd.concat([evs_2022, evs_2023])


#filter for 20_USA and 05_PRC only where vehicle type = ldv and drive = bev and phev )
evs = evs[(evs['economy'].isin(['20_USA', '05_PRC'])) & (evs['vehicle type'].isin(['ldv', 'ht'])) & (evs['drive'].isin(['bev and phev']))]
#%%
#plot the Stock share measure, with color being the vehicle type + drive and line dash being the dataset, facet_col = economy and x = date, y =  value
evs['vehicle type + source'] = evs['vehicle type'] +  ' ' + evs['source'] 

fig = px.line(evs[evs['measure']=='Stock share'], x='date', y='value', color='vehicle type + source', line_dash='dataset', facet_col='economy', facet_col_wrap=4, title = 'Stock share for ldv/truck, bev and phev, by economy')

#save to html
fig.write_html('plotting_output/analysis/iea/DATE20230531_evs_stock_share_comparison.html', auto_open=False)

#%%
#do same for 'Sales share'. first sort by date and dataset
evs = evs.sort_values(by=['date','economy', 'dataset'])
fig = px.line(evs[evs['measure']=='Sales share'], x='date', y='value', color='vehicle type + source', line_dash='dataset', facet_col='economy', facet_col_wrap=4, title = 'Sales share for ldv/truck, bev and phev, by economy')

#save to html
fig.write_html('plotting_output/analysis/iea/DATE20230531_evs_sales_share_comparison.html', auto_open=False)
# %%

# #plot 'EV Charging points' by economy (facet)
# fig = px.line(evs[evs['measure']=='EV Charging points'], x='date', y='value', color='source', line_dash='dataset', facet_col='economy', facet_col_wrap=4)

# #save to html
# fig.write_html('plotting_output/analysis/iea/DATE20230531_evs_charging_points_comparison.html', auto_open=False)
# %%



#%%
#DO US VS CHINA PLOTS

#plot the Stock share measure, with color being the vehicle type + drive and line dash being the dataset, facet_col = economy and x = date, y =  value
evs_new = evs.copy()
#drop vehicle types other tahn ldv and ht
evs_new = evs_new[evs_new['vehicle type'].isin(['ldv', 'ht'])]
#where the dataset is IEA_ev_explorer then st it to 2023
evs_new.loc[evs_new['dataset']=='IEA_ev_explorer', 'dataset'] = '2023'
#where the dataset is IEA_ev_explorer_2022 then st it to 2022
evs_new.loc[evs_new['dataset']=='IEA_ev_explorer_2022', 'dataset'] = '2022'
#where source contains Projection- then drop it from the string
evs_new.loc[evs_new['source'].str.contains('Projection-'), 'source'] = evs_new.loc[evs_new['source'].str.contains('Projection-'), 'source'].str.replace('Projection-','')
#where dataset is 2023 and vehicle typ is ldv then drop 'APS' and set source to ''
evs_new = evs_new[~((evs_new['dataset']=='2023') & (evs_new['source']=='APS') & (evs_new['vehicle type']=='ldv'))]
#set soruce to ''
evs_new.loc[(evs_new['dataset']=='2023') & (evs_new['vehicle type']=='ldv') , 'source'] = 'Forecast'
#where source is STEPS or APS and dataset is 2023 and economy is 05_PRC then set source to 'Forecast'
evs_new.loc[(evs_new['dataset']=='2023') & (evs_new['economy']=='05_PRC') & (evs_new['source'].isin(['STEPS', 'APS'])), 'source'] = 'Forecast'
#drop Histrocial 2022
evs_new = evs_new[~((evs_new['dataset']=='2022') & (evs_new['source']=='Historical'))]

#drop Historical source where vehicle is ldv
evs_new = evs_new[~((evs_new['source']=='Historical') & (evs_new['vehicle type']=='ldv'))]
evs_new['vehicle type - year'] = evs_new['vehicle type'] +  ' ' + evs_new['dataset'] 
evs_new['vehicle type, source'] = evs_new['vehicle type'] +  ' ' + evs_new['source'] 
#concat economy and vehicle type
evs_new['economy - vehicle type'] = evs_new['economy'] +  ' ' + evs_new['vehicle type']
#ordr by v type
evs_new = evs_new.sort_values(by=['vehicle type', 'date', 'economy', 'dataset', 'source'])
#where source is Historical, set it to Forecast
evs_new.loc[evs_new['source']=='Historical', 'source'] = 'Forecast'

#create concat of vehicle type source, dataset
evs_new['vehicle type, source, dataset'] = evs_new['vehicle type'] +  ' ' + evs_new['source'] + ', ' + evs_new['dataset']
#evs_new['vehicle type, source, dataset'].unique() 'ht Historical, 2023', 'ht APS, 2022', 'ht STEPS, 2022',
#        'ht APS, 2023', 'ht STEPS, 2023', 'ldv , 2023', 'ldv APS, 2022',
#        'ldv STEPS, 2022'
#make colors so that any 2023s are red, any 2022s are blue. base the values on 'vehicle type, source, dataset'
# color_discrete_map = {'ldv , 2023':'red', 'ldv APS, 2022':'blue', 'ldv STEPS, 2022':'blue', 'ht Historical, 2023':'red', 'ht APS, 2022':'blue', 'ht STEPS, 2022':'blue', 'ht APS, 2023':'red', 'ht STEPS, 2023':'red'}

fig = px.line(evs_new[evs_new['measure']=='Stock share'], x='date', y='value', color='source', line_dash='dataset', facet_col='economy - vehicle type', facet_col_wrap=2, title = 'Stock share for ldv/truck, bev and phev, by economy',markers=True,line_dash_map={'2022':'dash', '2023':'solid'})#, color_discrete_map=color_discrete_map)
#make lines thicker and text larger
fig.update_traces(line=dict(width=4), selector=dict(type='scatter'))
fig.update_layout(font=dict(size=18))

#save to html
fig.write_html('plotting_output/analysis/iea/DATE20230531_evs_stock_share_comparison.html', auto_open=True)

#%%
fig = px.line(evs_new[evs_new['measure']=='Sales share'], x='date', y='value', color='source', line_dash='dataset', facet_col='economy - vehicle type', facet_col_wrap=2, title = 'IEA Sales share for ldv/truck, bev and phev, by economy',markers=True,line_dash_map={'2022':'dash', '2023':'solid'})#, color_discrete_map=color_discrete_map)
#make lines thicker and text larger
fig.update_traces(line=dict(width=4), selector=dict(type='scatter'))
fig.update_layout(font=dict(size=18))
#make y axis as percentage
fig.update_yaxes(title_text='Sales share (%)')

#save to html
fig.write_html('plotting_output/analysis/iea/DATE20230531_evs_sales_share_comparison.html', auto_open=True)

# %%
