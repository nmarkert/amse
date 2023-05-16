import pandas as pd
import datetime as dt
import os

def get_abbreviations_dict() -> dict[str, str]:
    # https://www.destatis.de/DE/Methoden/abkuerzung-bundeslaender-DE-EN.html
    return {
        'Baden-Württemberg': 'BW',
        'Bayern': 'BY',
        'Berlin': 'BE',
        'Brandenburg': 'BB',
        'Bremen':'HB',
        'Hamburg':'HH',
        'Hessen':'HE',
        'Mecklenburg-Vorpommern':'MV',
        'Niedersachsen':'NI',
        'Nordrhein-Westfalen':'NW',
        'Rheinland-Pfalz':'RP',
        'Saarland':'SL',
        'Sachsen':'SN',
        'Sachsen-Anhalt':'ST',
        'Schleswig-Holstein':'SH',
        'Thüringen':'TH',
        'Germany':'DE'
    }


URL_DS_1 = 'https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeuleninfrastruktur.xlsx?__blob=publicationFile'
URL_DS_2_1 = 'https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ28/fz28_2023_03.xlsx?__blob=publicationFile'
URL_DS_2_2 = 'https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ28/fz28_2022_12.xlsx?__blob=publicationFile' # for year 2022
URL_DS_3 = 'https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/Gemeindeverzeichnis/Administrativ/02-bundeslaender.xlsx?__blob=publicationFile'
DATA_DIR = os.path.dirname(__file__)

# ------------------------------------------------------------------- #
#                        Relationship over time                       #
# ------------------------------------------------------------------- #

# -------------- Datasource 2.1 ---------------

ds2_1 = pd.read_excel(URL_DS_2_1,
                      sheet_name='FZ 28.2',
                      header=None,
                      index_col=1)

# Drop unimportant rows and columns
# Note: Just use the colums for electric cars and Plug-in-Hybrids because just that are 
# important for correlating with the charging infrastructure
ds2_1 = ds2_1.iloc[12:107, [1, 6, 8]]

# Rename the columns
# NR = New Registrations
ds2_1.columns = ['NR Overall', 'NR Electric', 'NR Hybrid (Plug-in)']

# Just take the rows for the annual amounts
ds2_1 = ds2_1.loc[list(filter(lambda i: 'Jahr' in i,  ds2_1.index))]

# Rename the index values 
ds2_1.index = pd.Index(map(lambda i: int(i.split(' ')[1]), ds2_1.index))
ds2_1.index.name = 'Year'

# Drop year 2023
ds2_1 = ds2_1.drop(2023)


# -------------- Datasource 1 ---------------

ds1 = pd.read_excel(URL_DS_1,
                    sheet_name='4.1 Ladepunkte je BL',
                    header=[6, 7],
                    index_col=4)

# Drop unimportant rows and columns
ds1 = ds1.iloc[:-1, 4:]

# Assign index and column names
ds1.columns.names = ['Date', 'Type']
ds1.index.name = 'State'

## ---- Prep for 'data over time' -----

ds1_years = ds1.loc['Summe'].unstack()
ds1_years = ds1_years[ds1_years.index.map(lambda d: d.day == 1 and d.month == 1)]
ds1_years.index = ds1_years.index.map(lambda d: d.year)
ds1_years.index.name = 'Year'
ds1_years.columns.name = None
ds1_years_increase = ds1_years.diff(periods=-1) * -1
# TODO not hardcoded on order -> use dict or something instead
# CP = Charging Points (overall), SCP = Standard Charging Points, FCP = Fast Charging Points
# Increase = Increase of chargning points over the year
ds1_years_increase.columns = ['SCP Increase', 'FCP Increase', 'CP Increase']
# Amount = Amount at the beginning of the year
ds1_years.columns = ['SCP Amount', 'FCP Amount', 'CP Amount']
ds1_years = pd.concat([ds1_years, ds1_years_increase], axis=1).sort_index(axis=1)

# Combine data
data_years = pd.concat([ds2_1, ds1_years], axis=1).dropna().astype(int)
# Save data to sqlite database
data_years.to_sql('over_time', f'sqlite:////{DATA_DIR}/data.sqlite', if_exists='replace')



# ------------------------------------------------------------------- #
#                        Relationship by state                        #
# ------------------------------------------------------------------- #
year = 2022

# ----- Prep Datasource 1 for 'data by state' ---------

# take the amount of Charging points at the start of the year
ds1_states = ds1[dt.datetime(year=year,month=1,day=1)]

# rename the columns
ds1_states.columns = ['Amount SCP', 'Amount FCP', 'Amount CP']

# rename the index for the sum over all states to 'Germany'
ds1_states.index.values[-1] = 'Germany'

# cast the values to integer
ds1_states = ds1_states.astype(int)


# -------------- Get Datasource 2.2 -------------------
ds2_2 = pd.read_excel(URL_DS_2_2,
                      sheet_name='FZ 28.9',
                      header=None,
                      index_col=1)

# Drop unimportant rows and columns
ds2_2 = ds2_2.iloc[30:47, [1, 6, 8]]

# Rename the columns
ds2_2.columns = ['NR Overall', 'NR Electric', 'NR Plug-in-Hybrid']

# Rename the first index value
ds2_2.index.values[0] = 'Germany'

# Assign index name
ds2_2.index.name = 'State'


# -------------- Get Datasource 3 -------------------
ds3 = pd.read_excel(URL_DS_3,
                    sheet_name='Bundesländer_mit_Hauptstädten',
                    usecols=[0,2],
                    header=None,
                    index_col=None)

# Drop unimportant rows
ds3 = ds3.iloc[7:-16]

# Rename the columns
ds3.columns = ['State', 'Area (km^2)']

# Remove the number from each state name
ds3['State'] = ds3['State'].apply(lambda x: x[4:] if type(x) is str else x)

# Set 'Germany' as the state for the last row
ds3['State'].iloc[-1] = 'Germany'

# Drop the rows which contain NaN (= rows for the states capitals)
ds3.dropna(inplace=True)

# Set the State column as the index
ds3.set_index('State', inplace=True)


# ----------- Get Abbreviations Dataframe -------------
abbreviations = get_abbreviations_dict()
df_abbreviations = pd.DataFrame(abbreviations.values(), abbreviations.keys(), columns=['Abbreviation'])
df_abbreviations.index.name = 'State'


# ---- Combine the data and save to database -------------
data_states = pd.concat([df_abbreviations, ds3, ds2_2, ds1_states], axis=1)
data_states.to_sql('by_states', f'sqlite:////{DATA_DIR}/data.sqlite', if_exists='replace')