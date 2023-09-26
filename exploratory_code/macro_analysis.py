
# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
import re
import os
import plotly.express as px
import plotly
#set cwd to the root of the project
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'\\transport_data_system')

file_date = datetime.datetime.now().strftime("%Y%m%d")
FILE_DATE_ID = 'DATE{}'.format(file_date)

#load macro data
FILE_DATE_ID = 'DATE20230301'
all_data = pd.read_csv('intermediate_data/Macro/all_macro_data_{}.csv'.format(FILE_DATE_ID))
#%%

#Exploration. Goals: 
# -understand ppp and how it relates to gdp
# -cehck if oecd and imf ppp data are the same
# -check the diff between oecd and imf gdp data

#Note that this is a useful guide https://www.imf.org/external/pubs/ft/weo/faq.htm#q4d
#Some useful notes:
# Q. Is there a real global GDP dollar series that can be used to compare economy size across countries?
# A. We do not report the value of constant price GDP in a common currency (say U.S. dollars) for the world.

# One way to convert an individual country's real GDP series to U.S. dollars is to do the following:

# 1) Take the current GDP level for that country in terms of U.S. dollars in the year specified as that country's base year (see Country Information).

# Series: Gross domestic product, current prices
# U.S. dollars
# 2) Extend the series from the base year value forwards and backwards by applying the growth rates of real GDP in local currency.
# Series: Gross domestic product, constant prices
# Annual percent change
# By following this procedure, a country's real GDP is in terms of U.S. dollars but maintains the growth rates of the real GDP series.

#AND

# Q. What is "Gross domestic product, current prices (purchasing power parity; international dollars)"?
# A. Also referred to as simply PPP GDP, it is calculated by dividing a country's nominal GDP in its own currency by the PPP exchange rate. It is used as main weights in WEO’s aggregate calculation.

#So basically a rule that holds it all together is that:
# (GDP_current_nominal / exchange_rate) / PPP = GDP_PPP
# where GDP_current_nominal = GDP_local_currency_nominal * exchange_rate

###########


#do some analysis about how PPP and GDP are related:
#For our data from IMF, GDP_current is in USD. So we need the exchange rate to compare to GDP_PPP which is in international dollars?
#So we know we could try: GDP_PPP * PPP = GDP_own_currency. 
#Therefore (GDP_PPP * PPP) / exchange_rate = GDP_current
#Therefore exchange_rate = (GDP_PPP * PPP) / GDP_current
#Since we dont have exchange rate data, we can estimate it, Let's try!
imf_ppp_gdp = all_data[(all_data['Dataset'] == 'IMF')]
# pivot the data so that the measure values are sep cols
imf_ppp_gdp = imf_ppp_gdp.pivot(index=['Economy','Date'], columns='Measure', values='Value').reset_index()
imf_ppp_gdp['estimated_exchange_rate'] = (imf_ppp_gdp['GDP_PPP'] * imf_ppp_gdp['PPP']) / imf_ppp_gdp['GDP_current']

#now compare that to the woirld bank exchange rate and DEC alternative conversion factor
wb_data = all_data[(all_data['Dataset'] == 'World Bank')]
# pivot the data so that the measure values are sep cols
wb_data = wb_data.pivot(index=['Economy','Date'], columns='Measure', values='Value').reset_index()

#join the two dataframes
exchange_rates = imf_ppp_gdp.merge(wb_data, on=['Economy','Date'], how='left')#this will ignore taiwan

#drop cols that are not needed
exchange_rates.drop(columns=['GDP_PPP',	'GDP_current',	'PPP'], inplace=True)
#melt so we have DEC alternative conversion factor	Official exchange rate and estimated_exchange_rate in one col
exchange_rates = pd.melt(exchange_rates, id_vars=['Economy','Date'], var_name='Measure', value_name='Value')
#%%
#plot using plotly with a facet col for each economy and a line for each measure
plot = False
if plot:
    title = 'exchange_rates'
    AUTO_OPEN_PLOTLY_GRAPHS = True
    fig = px.line(exchange_rates, x="Date", y='Value',color='Measure', facet_col="Economy", facet_col_wrap=7)
    fig.update_layout(height=2000, width=2000)
    #plot it in the browser
    plotly.offline.plot(fig, filename=f'./plotting_output/estimations' + title + '.html', auto_open=AUTO_OPEN_PLOTLY_GRAPHS)

#%%
#okay it worked!
#So basically a rule that holds it all together is that:
# (GDP_current / exchange_rate) / PPP = GDP_PPP
# where GDP_current = GDP_local_currency * exchange_rate
#%%

#NOw:
# -cehck if oecd and imf ppp data are the same
# -check the diff between oecd and imf gdp data

oecd_ppp_gdp = all_data[(all_data['Dataset'] == 'OECD')]
#keep only Measure = PPP
oecd_ppp_gdp = oecd_ppp_gdp[oecd_ppp_gdp['Measure'] == 'PPP']
#make measure = oecd_ppp
oecd_ppp_gdp['Measure'] = 'oecd_ppp'
#in imf data keep only ppp
imf_ppp = imf_ppp_gdp[['Economy','Date','PPP']]
#rename PPP to Value
imf_ppp.rename(columns={'PPP':'Value'}, inplace=True)
#set col 'Measure' to 'imf_ppp'
imf_ppp['Measure'] = 'imf_ppp'
#concat the two dfs
ppp_comparison = pd.concat([oecd_ppp_gdp, imf_ppp], axis=0)

#plot using plotly with a facet col for each economy and a line for each measure
plot = False
if plot:
    title = 'ppp_comparison'
    AUTO_OPEN_PLOTLY_GRAPHS = True
    fig = px.line(ppp_comparison, x="Date", y='Value',color='Measure', facet_col="Economy", facet_col_wrap=7)
    fig.update_layout(height=2000, width=2000)
    #plot it in the browser
    plotly.offline.plot(fig, filename=f'./plotting_output/estimations' + title + '.html', auto_open=AUTO_OPEN_PLOTLY_GRAPHS)

#they are basically the same. can just use imf in future, unless one is forecasted further than the other

#%%
#now check the difference between imf and oecd gdp
oecd_gdp = all_data[(all_data['Dataset'] == 'OECD')]
imf_gdp = all_data[(all_data['Dataset'] == 'IMF')]
#we need to convert all gdp to similar units.
#in oecd we have: 'GDPLTFORECAST' (MLN USD, Real to 2060), 'REALGDPFORECAST' (% grwoth, to 2024), 'NOMGDPFORECAST' (% growth to 2024)
#also some of oecd data is quarterly. its ok to ignore this for now
#in imf we have 'GDP_current', 'GDP_PPP'. This all goes to 2027.

#So I guess we calc growth rate for GDPLTFORECAST and GDP_current and GDP_PPP and then compare all the growth rates

#filter for the measures we need
oecd_gdp = oecd_gdp[(oecd_gdp['Measure'].isin(['GDPLTFORECAST','REALGDPFORECAST','NOMGDPFORECAST']))]
imf_gdp = imf_gdp[(imf_gdp['Measure'].isin(['GDP_current','GDP_PPP']))]

#concat the two dfs
gdp_comparison = pd.concat([oecd_gdp, imf_gdp], axis=0)
#keep only Yearly in Frequency col
gdp_comparison = gdp_comparison[gdp_comparison['Frequency'] == 'Yearly']
#sep the REALGDPFORECAST and NOMGDPFORECAST since they are growth rates
gdp_comparison_real = gdp_comparison[gdp_comparison['Measure'] == 'REALGDPFORECAST']
gdp_comparison_nom = gdp_comparison[gdp_comparison['Measure'] == 'NOMGDPFORECAST']
#filter out the REALGDPFORECAST and NOMGDPFORECAST
gdp_comparison = gdp_comparison[~gdp_comparison['Measure'].isin(['REALGDPFORECAST','NOMGDPFORECAST'])]
#now calc the growth rate for the GDP_current and GDP_PPP and GDPLTFORECAST
#sort by date
gdp_comparison.sort_values(by=['Date'], inplace=True)
#calc the growth rate
gdp_comparison['Value'] = gdp_comparison.groupby(['Economy', 'Measure'])['Value'].pct_change() *100
#concat the growth rates with the real and nom growth rates
gdp_comparison = pd.concat([gdp_comparison, gdp_comparison_real, gdp_comparison_nom], axis=0)

#join Dataset and Measure to make a new col
gdp_comparison['Measure'] = gdp_comparison['Dataset'] + '_' + gdp_comparison['Measure']
#%%
#plot using plotly with a facet col for each economy and a line for each measure
plot = True
if plot:
    title = 'gdp_comparison'
    AUTO_OPEN_PLOTLY_GRAPHS = True
    fig = px.line(gdp_comparison, x="Date", y='Value',color='Measure', facet_col="Economy", facet_col_wrap=7)
    fig.update_layout(height=2000, width=2000)
    #plot it in the browser
    plotly.offline.plot(fig, filename=f'./plotting_output/estimations' + title + '.html', auto_open=AUTO_OPEN_PLOTLY_GRAPHS)

#this is a very useful graph. Will have  a think about what else we can do without data but for now seems great
# %%
#plot un pop data:
pop = all_data[(all_data['Dataset'] == 'UN')]
#plot plotly with a line for each source and a facet col for each economy
plot = True
if plot:
    title = 'UN pop comparison'
    AUTO_OPEN_PLOTLY_GRAPHS = True
    fig = px.line(pop, x="Date", y='Value',color='Source', facet_col="Economy", facet_col_wrap=7)
    fig.update_layout(height=2000, width=2000)
    #plot it in the browser
    plotly.offline.plot(fig, filename=f'./plotting_output/estimations' + title + '.html', auto_open=AUTO_OPEN_PLOTLY_GRAPHS)

# #first grab only the IMF current and PPP data then calculate gdp_current * ppp and plot it against gdp_ppp
# imf_ppp_gdp = all_data[(all_data['Dataset'] == 'IMF')]
# # pivot the data so that the measure values are sep cols
# imf_ppp_gdp = imf_ppp_gdp.pivot(index=['Economy','Date'], columns='Measure', values='Value').reset_index()
# #convert to 
# #calc 'estimated gdp ppp' col
# imf_ppp_gdp['estimated_gdp_ppp'] = imf_ppp_gdp['GDP_current'] * imf_ppp_gdp['PPP']
# #plot using plotly with a facet col for each economy and a line for each measure
# #melt the data so we have one value col of GDP_PPP and estimated_gdp_ppp
# imf_ppp_gdp = pd.melt(imf_ppp_gdp[['Economy','Date','GDP_PPP','estimated_gdp_ppp']], id_vars=['Economy','Date'], var_name='Measure', value_name='Value')
# #%%
# import plotly.express as px
# fig = px.line(imf_ppp_gdp, x="Date", y='Value',color='Measure', facet_col="Economy", facet_col_wrap=7)
# fig.show()
# %%
