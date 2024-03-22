from bs4 import BeautifulSoup
import requests
import pandas as pd
from pprint import pprint
import re
from db_configuration.db_config import USERNAME,SERVER_NAME
from sqlalchemy import create_engine

# Get my stats links
url = "https://scoutingtherefs.com/"
data = requests.get(url)

soup = BeautifulSoup(data.text, "lxml")
links = soup.find_all("a")
data_links  = [l.get('href') for l in links]
nhl_ref_stat_links = sorted(set([l for l in data_links if 'nhl-referee-stats' in l and 'playoffs' not in l]))

# Several of the headings of the tables have different names, but they refer to the same metric. I am cleaning this up
sub_headers = {
    'Num': 'Number',
    'Name' : 'Referee',
    'G/Gm' : 'Goals/Gm',
    'G/gm' : 'Goals/Gm',
    'Goalsper gm' : 'Goals/Gm',
    'Goals per gm' : 'Goals/Gm',
    'PP per gm': 'PP/Gm',
    'Min/Gm' : 'Minor/gm',
    'Minors per gm' : 'Minor/gm',
    'Penl per gm': 'Penl/gm',
    'Home\xa0Win %': 'Home Win %',
    '% Gm toOT/SO': 'Gm to OT/SO',
    '% Gm to OT/SO': 'Gm to OT/SO',
    '% Gm OT/SO': 'Gm to OT/SO',
    'Gm to OT': 'Gm to OT/SO',
    'PIM per gm': 'PIM/Gm',
    'PIM/gm': 'PIM/Gm',
    '#': 'Number',
    '% Penl on Hom': '%Penl on Home',
    '% Penl Home': '%Penl on Home',
    '% PenlHome': '%Penl on Home',
    'PP/ Gm': 'PP/Gm',
    'Min/ Gm': 'Minor/gm',
    'Penl/ Gm' : 'Penl/gm',
}

# Scrape all of my data into one dataframe
all_dfs = pd.DataFrame()
for url in nhl_ref_stat_links:
    df_l = pd.read_html(url)
    df = df_l[0] # Remember: read_html returns a list of data frames
    df['Season'] = url
    df.columns = list(map(sub_headers.get, df.columns, df.columns))
    all_dfs = pd.concat([all_dfs, df], ignore_index = True)

#Some data cleaning. This can be further improved
all_dfs['Season_Str'] = all_dfs['Season'].str.extract('(\d{4}-\d{2,4})')
all_dfs['Season_Str'].fillna('2015-16', inplace=True) # we know that the link without the season in it was for 2015-16
all_dfs = all_dfs.drop('Season', axis = 1)
all_dfs = all_dfs[all_dfs['Referee'] != '* NHL AVERAGE'] # filter out this row
all_dfs['Referee'] = all_dfs['Referee'].str.replace('*', '')
all_dfs['Referee'] = all_dfs['Referee'].str.replace('^', '')
all_dfs['%Penl on Home'] = all_dfs['%Penl on Home'].str.replace('%', '').astype('float')
all_dfs['%Penl on Home'] = round(all_dfs['%Penl on Home'] / 100,2)
all_dfs['Home Win %'] = all_dfs['Home Win %'].str.replace('%', '').astype('float')
all_dfs['Home Win %'] = round(all_dfs['Home Win %'] / 100,2)
all_dfs['Gm to OT/SO'] = all_dfs['Gm to OT/SO'].str.replace('%', '').astype('float')
all_dfs['Gm to OT/SO'] = round(all_dfs['Gm to OT/SO'] / 100,2)
all_dfs['% PP Opp for Hom'] = all_dfs['% PP Opp for Hom'].str.replace('%', '').astype('float')
all_dfs['% PP Opp for Hom'] = round(all_dfs['% PP Opp for Hom'] / 100,2)



# I set up a SQL Server Database to store the data simulating a tranasctional update. We can clean this further
# for data warehousing purposes
sql_engine = create_engine(f"mssql+pymssql://{SERVER_NAME}/NhlRefStats")
all_dfs.to_sql("NHLRefCallsStats", con=sql_engine, if_exists="replace", index = False)

