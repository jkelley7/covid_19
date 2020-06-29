import pandas as pd
import numpy as np
import requests
from pathlib import Path
import datetime
import matplotlib.pyplot as plt
import os
import time
import json



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
    'America Samoas': 'AS',
}

# thank you to @kinghelix and @trevormarburger for this idea
abbrev_us_state = dict(map(reversed, us_state_abbrev.items()))





pop_df = pd.read_csv(Path(os.getenv('USERPROFILE')) / 'AnacondaProjects' /'corna' / 'data' /'raw' / 'est_2019.csv')

case_url = "http://covidtracking.com/api/states/daily"
case_url2 = "https://covidtracking.com/api/v1/states/daily.json"


def get_data(url):
    """
    take URL and retrieve data. If not successful, pull again.
    """
    r = requests.get(url)
    if r.status_code == 200:
        return pd.DataFrame(json.loads(r.content))
    else:
        return get_data(url)

new_daily = get_data(case_url2)
day_df = new_daily.copy()
day_df = day_df.sort_values(by = ['state', 'date']).reset_index(drop = True)
day_df = day_df.set_index(['state', 'date'])
day_df['daily_new_tst_rcrd'] = day_df['total'].diff()
day_df['daily_new_tst_rcrd_t1'] = day_df['daily_new_tst_rcrd'].shift(1)
day_df['daily_new_positive'] = day_df['positive'].diff()
day_df['daily_new_positive_t1'] = day_df['daily_new_positive'].shift(1)
day_df['daily_new_death'] = day_df['death'].diff()
day_df['daily_new_death_t1'] = day_df['daily_new_death'].shift(1)
day_df = day_df.reset_index()
day_df.loc[day_df['state'] != day_df['state'].shift(1), ['daily_new_tst_rcrd','daily_new_positive','daily_new_death']]= np.nan
day_df.loc[day_df['state'] != day_df['state'].shift(2), ['daily_new_tst_rcrd_t1','daily_new_positive_t1','daily_new_death_t1']]= np.nan
day_df.loc[:, ['positive','daily_new_tst_rcrd','daily_new_positive', 'daily_new_tst_rcrd_t1','daily_new_positive_t1','daily_new_death','daily_new_death_t1']].fillna(0)
day_df['full_state'] = day_df.state.str.strip().map(abbrev_us_state)
day_df = day_df.sort_values(by = ['state','date'],ascending = True)
day_df['positive'] = day_df['positive'].fillna(0)
day_df['days_since_100'] = np.nan
day_df['total'] = day_df['total'].fillna(0)
states = day_df.state.unique()
for c in states:
    day_df.loc[(day_df['state'] == c) , 'days_since_100'] = \
        np.arange(
            -len(day_df[(day_df['state'] == c) & (day_df['positive'] < 100)]),
            len(day_df[(day_df['state'] == c ) & (day_df['positive'] >= 100)])
            )
day_df = day_df.merge(pop_df, how = 'left', left_on = 'full_state',right_on = 'NAME')
day_df.to_csv(Path(os.getenv('USERPROFILE')) / 'AnacondaProjects' /'corna' / 'data' /'processed' / 'states_daily_tests.csv', index = False)


#########################################################
#
#
#
#
# 
#
#########################################################
daily_raw = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'

start_date = datetime.date(2020,1,22)
phase_1_end = datetime.date(2020,2,29)


phase_2_start = datetime.date(2020,3,1)
phase_2_end = datetime.date(2020,3,20)

phase_3_start = datetime.date(2020,3,21)
end_date = datetime.date.today()

def return_data(start_date, end_date, base_url):
    """
    get data from jhu
    """
    dataf = []
    for idx in range((end_date - start_date).days+1):
        running_date = (start_date + datetime.timedelta(idx)).strftime('%m-%d-%Y')
        try:
            dataf.append(pd.read_csv(base_url + running_date + '.csv'))
        except:
            pass
    datafo = pd.concat(dataf, ignore_index=True)
    datafo.columns = [cols.lower() for cols in datafo.columns.to_list()]
    return datafo

df1 = return_data(start_date, phase_1_end, daily_raw)
df2 = return_data(phase_2_start, phase_2_end, daily_raw)
df3 = return_data(phase_3_start, end_date, daily_raw)

lat_cols = [('latitude','lat'),
            ('longitude','long_'),
            ('last update', 'last_update'),
            ('province/state', 'province_state'),
            ('country/region', 'country_region'),
            ]
for i in lat_cols:
    df3[i[0]] = np.where(df3[i[0]].isna(), df3[i[1]], df3[i[0]])
df3 = df3.drop(columns = ['lat','long_','last_update','province_state','country_region'])


fin_df = pd.concat([df1,df2,df3],ignore_index = True)

def fix_col_names(col_names):
    """
    Fix the column names
    """
    col_names = [i.lower() for i in col_names]
    col_names = [i.replace('/','_') for i in col_names]
    col_names = [i.replace(' ','_') for i in col_names]

    return col_names

fin_df.columns = fix_col_names(fin_df.columns.to_list())

# fin_df.columns =['active', 'us_county', 'combined_key', 'confirmed', 'country_region',
#        'deaths', 'fips', 'last_update', 'latitude', 'longitude',
#        'province_state', 'recovered']
#state_df = fin_df[fin_df['province_state'] == fin_df['us_county']].reset_index(drop = True)
#nons_df = fin_df[fin_df['province_state'] != fin_df['us_county']].reset_index(drop = True)
#nons_df['province_state'] = np.where(nons_df['province_state'].isna(),nons_df['country_region'],nons_df['province_state'])

#sorted(nons_df['province_state'].unique())

#df3.head()
fin_df.to_csv(Path(os.getenv('USERPROFILE')) / 'AnacondaProjects' /'corna' / 'data' /'processed' /'jhu_county.csv', index = False)

#########################################################
#
#
# JHU County
#
# 
#
#########################################################

def get_JHU_county() -> pd.DataFrame:
    """
    Get JHU County level data
    """
    jhu_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv'
    dataf = pd.read_csv(jhu_url)
    date_cols = dataf.columns.to_list()[11:]
    transformed_df = dataf.melt(
                    id_vars = dataf.columns.to_list()[:11],
                    value_vars = date_cols,
                    var_name = 'date'
                )
    return transformed_df
    
county_df = get_JHU_county()
county_df['date'] = pd.to_datetime(county_df['date'])
county_df = (county_df
            .sort_values(by = ['Lat','Long_', 'date'])
            .reset_index(drop = True)
            )

county_df['val_t1'] = county_df['value'].shift(1)
county_df['dod'] = county_df['value'] - county_df['val_t1']
county_df.loc[county_df['FIPS'] != county_df['FIPS'].shift(1), ['dod']]= np.nan
county_df['dod'] = county_df['dod'].fillna(0)
county_df = county_df.drop(['val_t1'], axis = 1)

county_df.to_csv(Path(os.getenv('USERPROFILE')) / 'AnacondaProjects' /'corna' / 'data' /'processed' / 'county_jhu.csv', index = False)

