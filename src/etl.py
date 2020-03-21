import pandas as pd
import numpy as np
import requests
from pathlib import Path
import datetime
import matplotlib.pyplot as plt
import os


us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'Washington, D.C.': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands':'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Palau': 'PW',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY',
}

# thank you to @kinghelix and @trevormarburger for this idea
abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))



#########################################################
#
#
#
#
#
#
#########################################################
ts_raw = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv'
dth_raw = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv'
recov_raw = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv'

all_urls = [('conf', ts_raw),
            ('death', dth_raw),
            ('recovered', recov_raw),
            ]

dfs = []

for name, url in all_urls:
    df_orig = pd.read_csv(url)
    all_cols = df_orig.columns.to_list()
    temp_df= df_orig.copy()
    temp_df = temp_df.melt(id_vars = all_cols[:4], value_vars = all_cols[4:], var_name = 'date',value_name = 'confirmed')
    temp_df.columns = ['state','country','lat','long','date','var']
    temp_df['date'] = pd.to_datetime(temp_df['date'])
    temp_df['date'] = pd.to_datetime(temp_df['date'],format = '%Y-%m-%d')
    temp_df = temp_df.sort_values(by = ['state', 'country','date']).reset_index(drop = True)
    temp_df = temp_df.set_index(['state', 'country','date'])
    temp_df['new_cases_t0'] = temp_df['var'].diff()
    temp_df = temp_df.reset_index().reset_index(drop = True)
    temp_df.loc[temp_df['state'] != temp_df['state'].shift(1), 'new_cases_t0'] = np.nan
    temp_df['new_cases_t1'] = temp_df['new_cases_t0'].shift(1)
    temp_df.loc[temp_df['state'] != temp_df['state'].shift(1), 'new_cases_t1']= np.nan
    temp_df['var_name'] = name
   
    dfs.append(temp_df)

fin_df = pd.concat(dfs, ignore_index = True)
fin_df[['new_cases_t0','new_cases_t1']] = fin_df[['new_cases_t0','new_cases_t1']].fillna(0)

us_mask = fin_df['country'] == 'US'
fin_df['country'].unique()
fin_df['city'] = np.where(fin_df['state'].str.contains(',', na = False),1,0)
fin_df['derived_state'] = fin_df['state'].str.split(',',expand = True)[1].str.strip()
fin_df['derived_state'] = fin_df.derived_state.str.strip().map(abbrev_us_state)
fin_df.head()
fin_df['final_state'] = np.where((fin_df['city'] == 1) & (fin_df['country'] == 'US'), fin_df['derived_state'], fin_df['country'])
fin_df['final_state'] = np.where((fin_df['derived_state'].isna()) & (fin_df['country'] == 'US'), fin_df['state'], fin_df['final_state'])
fin_df['country_join'] = np.where(fin_df['country'] == 'US','US','other')

us_lat_long = (fin_df[(fin_df['city'] < 1) & (us_mask)]
                .groupby(['state','country_join'])
                .agg(state_lat = ('lat', np.mean),
                state_long = ('long', np.mean))
                .reset_index()
                )
nonus_lat_long = (fin_df[(fin_df['city'] < 1) & (~us_mask)]
                .groupby(['country','country_join'])
                .agg(state_lat = ('lat', np.mean),
                state_long = ('long', np.mean))
                .reset_index()
                )
nonus_lat_long.columns = us_lat_long.columns.to_list()
state_lat_long = pd.concat([us_lat_long,nonus_lat_long], axis = 0, ignore_index=True)
fin_df = (fin_df
            .merge(state_lat_long, how = 'left', left_on = ['final_state','country_join'], right_on = ['state','country_join'])
            )
fin_df['state_lat'] = np.where(fin_df['state_lat'].isna(),fin_df['lat'],fin_df['state_lat'])
fin_df['state_long'] = np.where(fin_df['state_long'].isna(),fin_df['long'],fin_df['state_long'])
fin_df[['state_lat', 'state_long']] = fin_df[['state_lat', 'state_long']].astype(np.float64)
fin_df.columns = ['region', 'country','date','lat', 'long', 'var', 'new_cases_t0','new_cases_t1','var_name','city','state_delete',
 'final_state','country_delete','state_delete_2','state_lat','state_long']
fin_df = fin_df[[col for col in fin_df.columns.to_list() if 'delete' not in col]]
fin_df.to_csv(Path(os.getenv('USERPROFILE')) / 'AnacondaProjects' /'corna' / 'data' /'processed' /'clean_data.csv', index = False)

##################################################
#
#
#
##################################################

df_comb = (fin_df
            .groupby(['country','date','var_name'])
            .agg(var = ('var',np.sum))
            .reset_index()
            )
countries = df_comb.country.unique()
var_names = df_comb.var_name.unique()
df_comb['days_since_100'] = np.nan

for c in countries:
   df_comb.loc[(df_comb['country'] == c) & (df_comb['var_name'] == 'conf'), 'days_since_100'] = \
       np.arange(
           -len(df_comb[(df_comb['country'] ==c) & (df_comb['var_name'] == 'conf') & (df_comb['var'] < 100)]),
           len(df_comb[(df_comb['country'] == c) & (df_comb['var_name'] == 'conf') & (df_comb['var'] >= 100)])
           )


df_comb.to_csv(Path(os.getenv('USERPROFILE')) / 'AnacondaProjects' / 'corna' / 'data' / 'processed' /'days_since.csv', index = False)

##################################################
#
#
#
##################################################

start_date = datetime.date(2020,3,3)
end_date = datetime.date.today()

case_url = "http://covidtracking.com/api/states/daily?date="

new_daily = []
for i in range((end_date - start_date).days):
    print(i)
    running_date = (start_date + datetime.timedelta(i)).strftime('%Y%m%d')
    extract_url = case_url + running_date
    print(extract_url)
    r = requests.get(extract_url)
    time.sleep(.75)
    if r.status_code == 200:
        new_daily.append(pd.DataFrame(json.loads(r.content)))

day_df = pd.concat(new_daily, ignore_index=True)
day_df = day_df.sort_values(by = ['state', 'date']).reset_index(drop = True)
day_df = day_df.set_index(['state', 'date'])
day_df['daily_new_tst_rcrd'] = day_df['total'].diff()
day_df['daily_new_tst_rcrd_t1'] = day_df['daily_new_tst_rcrd'].shift(1)
day_df = day_df.reset_index()
day_df.loc[day_df['state'] != day_df['state'].shift(1), 'daily_new_tst_rcrd']= np.nan
day_df.loc[day_df['state'] != day_df['state'].shift(2), 'daily_new_tst_rcrd_t1']= np.nan
day_df['full_state'] = day_df.state.str.strip().map(abbrev_us_state)


day_df.to_csv(Path(os.getenv('USERPROFILE')) / 'AnacondaProjects' /'corna' / 'data' /'processed' / 'states_daily_tests.csv', index = False)

