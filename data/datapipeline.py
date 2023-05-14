import pandas as pd
import os
# File for the Datapipeline

PATH_DS_1 = 'https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeuleninfrastruktur.xlsx?__blob=publicationFile'
PATH_DS_2_1 = 'https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ28/fz28_2023_03.xlsx?__blob=publicationFile'
DATA_DIR = os.path.dirname(__file__)

# -------------- Datasource 2 ---------------

# Load the excel sheet
ds2_1 = pd.read_excel(PATH_DS_2_1,
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

# Load the excel sheet
ds1 = pd.read_excel(PATH_DS_1,
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

print(f'sqlite:////{DATA_DIR}/data.sqlite')
data_years.to_sql('over_time', f'sqlite:////{DATA_DIR}/data.sqlite', if_exists='replace')