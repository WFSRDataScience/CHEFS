"""
Created on Sat Oct 11 07:10:50 2025

@author: hoend008
"""
import os
import pathlib
import pandas as pd
from typing import List

import psycopg2
import numpy as np
import pandas as pd

# from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


"""
UTILS
"""
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = 'oefsa'
HOST = os.environ.get('DB_HOST_EFSA')

def df_trim(i_df: pd.DataFrame) -> pd.DataFrame:

    # select all columns that are object. Then run .strip() on all values
    for col in i_df.select_dtypes(include ='object'):
        i_df[col] = [str(x).strip() if pd.notnull(x) else x for x in i_df[col]]      

    return i_df
    
def df_tolower(i_df: pd.DataFrame) -> pd.DataFrame:

    # select all columns that are object. Then run .lower() on all values
    for col in i_df.select_dtypes(include ='object'):
        i_df[col] = [str(x).lower() if pd.notnull(x) else x for x in i_df[col]]
    
    return i_df    

def cleancolumns(cols: List) -> List:
    """
    all column names to lowercase. whitespaces are replaced with underscores. All non-word-number-underscore characters are removed
    use in this way:    df.columns = cleancolumns( df.columns )
    """
    pattern = r'[^\w\s]'
    
    #return cols.str.strip().str.replace('\s', '_').str.replace(r'\W' , '').str.lower()
    cols = cols.str.strip().str.lower()
    return cols.str.replace(pattern, '', regex=True).str.replace(' ', '_')

def basic_processing(df: pd.DataFrame) -> pd.DataFrame:
    # trim data and set to lowercase
    df = df_trim(df)
    df = df_tolower(df)
    
    # simplify column names
    df.columns = cleancolumns(df.columns)
    
    return df


class PostgresDatabase:
    def __init__(self, db_name: str, db_user: str, db_password: str, host: str):
        #print(f"connection to {host}")
        self.connection = psycopg2.connect(user = db_user,
                                           password = db_password,
                                           host = host,
                                           port = "5432",
                                           database = db_name)
        self.cursor = self.connection.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def commit(self):
        self.connection.commit()
    
    def rollback(self):
        self.connection.rollback()
            
    def close(self, commit=True):
        if commit:
            self.commit()
        self.cursor.close()
        self.connection.close()

    def execute(self, sql: str, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql: str, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()

    def querydf(self, sql: str) -> pd.DataFrame:
        return pd.read_sql_query(sql, self.connection)

    
"""
DIRECTORIES AND FILEPATHS
"""
DATATYPE = "Chronic"

WORKDIR = r"C:\Users\hoend008\CHEFS Datascience\CHEFS" # <-- FILL IN YOUR WORK DIRECTORY
DATADIR = pathlib.Path(WORKDIR, "10. Consumption Data")

RAWDATA_DIR = pathlib.Path(DATADIR, DATATYPE)

FINAL_DATADIR = pathlib.Path(RAWDATA_DIR, "final")
FINAL_SAVEPATH = pathlib.Path(FINAL_DATADIR, DATATYPE + ".csv")

CHRONIC_ALL_GDAY =          pathlib.Path(RAWDATA_DIR, "Chronic Food Consumption Grams per day gday - All Subjects.xlsx")
CHRONIC_CONSUMERS_GDAY =    pathlib.Path(RAWDATA_DIR, "Chronic Food Consumption Grams per day gday - Consumers only.xlsx")
CHRONIC_ALL_GKGDAY =        pathlib.Path(RAWDATA_DIR, "Chronic Food Consumption Grams per kilogram of body weight per day gkg bw per day - All Subjects.xlsx")
CHRONIC_CONSUMERS_GKGDAY =  pathlib.Path(RAWDATA_DIR, "Chronic Food Consumption Grams per kilogram of body weight per day gkg bw per day - Consumers only.xlsx")


class ConsumptionData:
    def __init__(self, abrv, filepath, IDENTIFIER_COLUMNS, DATA_COLUMNS):
        # Store the three input arguments as instance variables
        self.abrv = abrv
        self.filepath = filepath
        self.IDENTIFIER_COLUMNS = IDENTIFIER_COLUMNS
        self.DATA_COLUMNS = DATA_COLUMNS
        self.df = pd.read_excel(self.filepath, skiprows=2)
        self.df = basic_processing(self.df)
        self.new_names = []

    def basic_processing(self, df) -> pd.DataFrame:
        # trim data and set to lowercase
        df = df_trim(df)
        df = df_tolower(df)
        
        # simplify column names
        df.columns = cleancolumns(df.columns)
        
        return df

    def check_dups(self):
        dups = sum(self.df.duplicated(subset=self.IDENTIFIER_COLUMNS))

        if dups > 0:
            print(f"THERE ARE DUPLICATES FOR {self.abrv}")
        else:
            print(f"No duplicates found for {self.abrv}")

    def rename_datacolumns(self):
        
        rename_dict = {
            "5th_percentile": "percentile_5" + "_" + self.abrv,
            "10th_percentile": "percentile_10" + "_" + self.abrv,
            "95th_percentile": "percentile_95" + "_" + self.abrv,
            "975th_percentile": "percentile_975" + "_" + self.abrv,
            "99th_percentile": "percentile_99" + "_" + self.abrv,
            "mean": "mean" + "_" + self.abrv,
            "standard_deviation": "standard_deviation" + "_" + self.abrv,
            "comment": "comment" + "_" + self.abrv,
                
        }
        self.df = self.df.rename(columns=rename_dict)
        
        self.new_names = list(rename_dict.values())
        #for col in self.DATA_COLUMNS:
        #    self.new_names[col] = col + "_" + self.abrv
            
        #self.df = self.df.rename(columns=self.new_names)
      
    def select_datacolumns(self):
        self.df = self.df[self.IDENTIFIER_COLUMNS + self.new_names]

    def shape(self):
        print(f"shape: {self.df.shape}")


"""
COLUMN GROUPINGS
"""
IDENTIFIER_COLUMNS = ['surveys_country', 'survey_start_year', 'survey_name', 'population_group_l2', 'exposure_hierarchy_l7']
DATA_COLUMNS = ['mean', 'standard_deviation', '5th_percentile', '10th_percentile',
                'median', '95th_percentile', '975th_percentile', '99th_percentile',
                'comment']

"""
IMPORT DATA AND BASIC PROCESSING
"""
cag = ConsumptionData('cag', CHRONIC_ALL_GDAY, IDENTIFIER_COLUMNS, DATA_COLUMNS)
ccg = ConsumptionData('ccg', CHRONIC_CONSUMERS_GDAY, IDENTIFIER_COLUMNS, DATA_COLUMNS)
cagkg = ConsumptionData('cagkg', CHRONIC_ALL_GKGDAY, IDENTIFIER_COLUMNS, DATA_COLUMNS)
ccgkg = ConsumptionData('ccgkg', CHRONIC_CONSUMERS_GKGDAY, IDENTIFIER_COLUMNS, DATA_COLUMNS)


"""
CHECK DUPLICATES
"""
cag.check_dups()
ccg.check_dups()
cagkg.check_dups()
ccgkg.check_dups()


"""
RENAME COLUMNS
"""
cag.rename_datacolumns()
ccg.rename_datacolumns()
cagkg.rename_datacolumns()
ccgkg.rename_datacolumns()


"""
SELECT COLUMNS FOR THREE OUT OF THE FOUR
"""
ccg.select_datacolumns()
cagkg.select_datacolumns()
ccgkg.select_datacolumns()


"""
SIZE OF DATAFRAMES
"""
cag.shape()
ccg.shape()
cagkg.shape()
ccgkg.shape()


"""
MERGE
"""
df = cag.df.merge(ccg.df, on=IDENTIFIER_COLUMNS, how='inner')
df = df.merge(cagkg.df, on=IDENTIFIER_COLUMNS, how='inner')
df = df.merge(ccgkg.df, on=IDENTIFIER_COLUMNS, how='inner')

print(df.shape)
print(df.columns)


"""
ADD COUNTRY ID USING EFSA COUNTRY CATALOGUE
"""
with PostgresDatabase(DB_NAME, DB_USER, DB_PASSWORD, HOST) as db:
    df_country = db.querydf("SELECT id AS survey_country_id, termextendedname AS surveys_country FROM ontologies_efsa.country;")

# Check matching
missing_countries = set(df['surveys_country']) - set(df_country['surveys_country'])
if len(missing_countries) > 0:
    print(f"THERE ARE {len(missing_countries)} MISSING COUNTRIES")
    print(missing_countries)

# add survey_country_id to df and check result
rows_before = len(df)
df = df.merge(df_country, on=['surveys_country'], how='inner')
rows_after = len(df)

if rows_before != rows_after:
    print('SOMETHING WENT WRONG WITH MERGING COUNTRIES')
    

"""
ADD MTX ID USING EFSA MTX CATALOGUE
"""
with PostgresDatabase(DB_NAME, DB_USER, DB_PASSWORD, HOST) as db:
    df_mtx = db.querydf("SELECT id AS mtx_id, termextendedname AS exposure_hierarchy_l7 FROM ontologies_efsa.mtx;")

# Check matching
missing_mtx = set(df['exposure_hierarchy_l7']) - set(df_mtx['exposure_hierarchy_l7'])
if len(missing_mtx) > 0:
    print(f"THERE ARE {len(missing_mtx)} MISSING MTX PRODUCTS")
    print(missing_mtx)

# add survey_country_id to df and check result
rows_before = len(df)
df = df.merge(df_mtx, on=['exposure_hierarchy_l7'], how='left')
rows_after = len(df)

if rows_before != rows_after:
    print('SOMETHING WENT WRONG WITH MERGING COUNTRIES')
    
df['mtx_id'] = df['mtx_id'].astype('Int64')


"""
DROP surveys_country column
"""
df = df.drop(['surveys_country'], axis=1)


"""
SAVE AS CSV
"""
df.to_csv(FINAL_SAVEPATH, index=False)


"""
PRINT \copy statements for SQL import
"""
copy_query = f"\copy efsa_consumption.chronic({','.join(df.columns)}) from '{FINAL_SAVEPATH}' (header true, delimiter ',', format csv, encoding 'UTF-8');"
print(copy_query)

    