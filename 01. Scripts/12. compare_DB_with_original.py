"""
Compares data from DB with original CSV files and checks for differences
"""

import random
from typing import List
import numpy as np
import pandas as pd
import pathlib
import os
from utils import cleancolumns, df_tolower, df_trim
from DBconnection import PostgresDatabase
from DBcredentials import DB_USER, DB_PASSWORD, DB_NAME, HOST


"""
SETTINGS
"""
import warnings
warnings.filterwarnings('ignore')


"""
GLOBALS
"""
WORKDIR = "../"

FULLPATH_MAIN_DIR = os.getcwd()
FULLPATH_MAIN_DIR = os.path.abspath(os.path.join(FULLPATH_MAIN_DIR ,".."))

META_DATA_DIR = pathlib.Path(WORKDIR, "06. Meta data")
FILE_DATA_DIR = pathlib.Path(WORKDIR, "07. Data Files")

FILE_COLUMNS = "column-meta-info.xlsx"
FILEPATH_COLUMNS = pathlib.Path(META_DATA_DIR, FILE_COLUMNS)

FILEPATH_COMPARISON =  pathlib.Path(FILE_DATA_DIR, "comparison_results.csv")

NR_TESTS = 5


"""
HELPER FUNCTIONS
"""
def basic_processing(df: pd.DataFrame) -> pd.DataFrame:

    # all data to lower and trim
    df = df_tolower(df)
    df = df_trim(df)
     
    # clean columnsnames (lowercase, remove special characters etc)
    df.columns = cleancolumns(df.columns)
    
    # return df
    return df

def datatypeToStr(df: pd.DataFrame) -> pd.DataFrame:
    """Takes in a dataframe and converts datatypes to str"""
    for col in df.columns:
        try:
            df[col] = df[col].astype(float)
        except:
            df[col] = df[col].astype(str)
       
    # convert text 'nan' to np.nan
    df = df.replace("nan", np.nan)
    
    return df

def processValues(tmp_df: pd.DataFrame) -> pd.DataFrame:
    # replace N_A with np.nan
    tmp_df = tmp_df.replace("n_a", np.nan) 

    # replace 0 with np.nan
    columns = list(tmp_df.columns)
    if 'sampmatinfoprody' in columns:
        tmp_df['sampmatinfoprody'] = tmp_df['sampmatinfoprody'].replace(0, np.nan)        
    if 'sampmatinfoprodm' in columns:
        tmp_df['sampmatinfoprodm'] = tmp_df['sampmatinfoprodm'].replace(0, np.nan)
    if 'sampmatinfoprodd' in columns:
        tmp_df['sampmatinfoprodd'] = tmp_df['sampmatinfoprodd'].replace(0, np.nan)
    if 'analysisy' in columns:
        tmp_df['analysisy'] = tmp_df['analysisy'].replace(0, np.nan)
    if 'analysism' in columns:
        tmp_df['analysism'] = tmp_df['analysism'].replace(0, np.nan)
    if 'analysisd' in columns:
        tmp_df['analysisd'] = tmp_df['analysisd'].replace(0, np.nan)
    
    return tmp_df
            
def renameColumns(df: pd.DataFrame) -> pd.DataFrame:
    
    # rename columns
    df.columns = [x.replace('_a', '') for x in df.columns]
    df = df.rename(columns={
        'prodcode':'sampmatcodebasebuilding',
        'labsampcode':'sampid',
        'expiryd':'sampmatinfoexpiryd',
        'expirym':'sampmatinfoexpirym',
        'expiryy':'sampmatinfoexpiryy',
        'fatperc':'exprrespercfatperc',
        'labcode':'labid',
        'localorg':'localorgid',
        'moistperc':'exprrespercmoistperc',
        'prodbrandname':'sampmatinfobrandname',
        'prodcom':'sampmatinfocom',
        'prodd':'sampmatinfoprodd',
        'prodm':'sampmatinfoprodm',
        'prodmanuf':'sampmatinfomanuf',
        'prodprodmeth':'sampmatcodeprod',
        'prodtext':'sampmattext',
        'prody':'sampmatinfoprody',
        'progcode':'progid',
        'progsampstrategy':'sampstrategy',
        'resevaluation':'evalcode',
        'paramcode':'paramcodebaseparam',
        'resultcode':'resid',
        'anmethcode':'anmethcodebasemeth',
        'reslegallimittype':'evallimittype',
        'exprres':'exprrestype',        
        'prodcode':'sampmatcodebasebuilding',
        'labsampcode':'sampid',
        'expiryd':'sampmatinfoexpiryd',
        'expirym':'sampmatinfoexpirym',
        'expiryy':'sampmatinfoexpiryy',
        'fatperc':'exprrespercfatperc',
        'labcode':'labid',
        'localorg':'localorgid',
        'moistperc':'exprrespercmoistperc',
        'prodbrandname':'sampmatinfobrandname',
        'prodcom':'sampmatinfocom',
        'prodd':'sampmatinfoprodd',
        'prodm':'sampmatinfoprodm',
        'prodmanuf':'sampmatinfomanuf',
        'prodprodmeth':'sampmatcodeprod',
        'prodtext':'sampmattext',
        'prody':'sampmatinfoprody',
        'progcode':'progid',
        'progsampstrategy':'sampstrategy'})
    
    return df
                
def compareColumns(df_columns, db_columns) -> None:
    """COMPARE COLUMNS"""
    print("columns in original and not in db")
    print(set(df_columns) - set(db_columns))
    
    print("columns in db and not in original")
    print(set(db_columns) - set(df_columns))
    

def compareValues(df1: pd.DataFrame, df2: pd.DataFrame, idvars: List[str]) -> pd.DataFrame:
    """Compares the values of two dataframes"""
    df1 = df1.melt(id_vars=idvars)
    df1 = df1.dropna(subset=['value'])

    df2 = df2.melt(id_vars=idvars)
    df2 = df2.dropna(subset=['value'])    
        
    # merge and compare values
    df = df1.merge(df2, on=idvars+['variable'])
    del df1
    del df2
    return df.query("value_x != value_y")
    
def getListRandomFiles(datadir: pathlib.Path, nr: int) -> List[pathlib.Path]:

    listFiles = [x for x in list(datadir.rglob("*.ZIP"))]
    listFiles = [x for x in listFiles if 'contaminants' not in x.name and 'pesticides' not in x.name and 'veterinary' not in x.name]
    randomSample = random.sample(listFiles, nr)
    return randomSample
        
    
"""
GET COLUMN META INFO
"""
df_cols = pd.read_excel(FILEPATH_COLUMNS, sheet_name="column_metaInfo")
df_cols = basic_processing(df_cols)

# get datatype dict
COLUMNS_DICT = dict(zip(df_cols.columnname, df_cols.datatype))

# get sample columns that unique identify sample
df_cols_unique = df_cols.query("unique_identifier == 1")
UNIQUE_IDENTIFIER_COLUMNS = list(df_cols_unique.columnname)


def main():
    
    """
    GET LIST OF FILES FOR TESTING
    """
    listFiles = getListRandomFiles(FILE_DATA_DIR, NR_TESTS)
        
    # loop over each file and compare DB with original
    for FILE in listFiles:
        
        FILENAME = FILE.name.lower()

        print("------------------------------------------------")
        print(FILENAME)
        
        """
        GET ORIGINAL DATA FROM DISK
        """        
        df = pd.read_csv(FILE, compression='zip')
        df = basic_processing(df)
        
        # replace N_A with np.nan
        df = df.replace("n_a", np.nan)

        # drop columns that are all empty, except columns in UNIQUE_IDENTIFIER_COLUMNS
        df = df.dropna(axis=1, how='all')

        # rename columns
        df = renameColumns(df)

        # process some values
        df = processValues(df)
        
        # set datatypes
        df = datatypeToStr(df)
        
        
        """
        GET DATA FROM DB
        """        
        query_sample_rest = ("SELECT sr.sample_core_id, cm.columnname, sr.value " +
            "FROM efsa.sample_core sc " +
            "JOIN efsa.files f " +
            "ON sc.file_id = f.id " +
            "JOIN efsa.sample_rest sr " +
            "ON sc.id = sr.sample_core_id " +
            "JOIN efsa.column_metainfo cm " +
            "ON sr.column_metainfo_id = cm.id " +
            f"WHERE f.filename = '{FILENAME}';")
        
        query_measurement_rest = ("SELECT mr.file_id, mr.measurement_nr, mr.columnname, mr.value " +
            "FROM efsa.measurement_rest mr " +
            "JOIN efsa.files f " +
            "ON mr.file_id = f.id " +
            f"WHERE f.filename = '{FILENAME}';")
        
        with PostgresDatabase(DB_NAME, DB_USER, DB_PASSWORD, HOST) as db:
            dfcore = db.querydf(f"SELECT * FROM efsa.vw_sample_measurement_core_termcode WHERE filename = '{FILENAME}';")
            df_sample_rest = db.querydf(query_sample_rest)
            df_measurement_rest = db.querydf(query_measurement_rest)
        
        # if measurement rest data not yet in database, then skip this iteration
        if df_measurement_rest.shape[0] == 0:
            print(f"SKIPPED {FILENAME}")
            continue
        
        # replace None with np.nan
        dfcore = dfcore.fillna(value=np.nan)
        
        # drop columns that are all empty, except colums in UNIQUE_IDENTIFIER_COLUMNS
        # dfcore = dfcore.dropna(axis=1, how='all')

        """
        RECREATE DATAFRAME
        """  
        # sample_rest from long to wide
        df_sample_rest = df_sample_rest.pivot(index="sample_core_id", 
                                              columns="columnname", 
                                              values="value").reset_index()
        
        # merge with dfcore
        dfcore = dfcore.merge(df_sample_rest, on='sample_core_id', how='left')
        
        del df_sample_rest
        
        # df_measurement_rest from long to wide
        df_measurement_rest = df_measurement_rest.pivot(index=["file_id", "measurement_nr"],
                                                          columns="columnname", 
                                                          values="value").reset_index()
        
        # merge with dfcore
        dfcore = dfcore.merge(df_measurement_rest, on=["file_id", "measurement_nr"], how='left')
        
        del df_measurement_rest

        # set datatypes
        dfcore = datatypeToStr(dfcore)
        
        # drop extra columns
        dfcore = dfcore.drop(['filename', 'file_id', 'sample_core_id', 'measurement_nr'], axis=1)
       

        """
        COMPARE ORIGINAL WITH DB
        """ 
        # compare columns
        compareColumns(df.columns, dfcore.columns)
        
        # compare values
        UNIQUE_IDENTIFIER_COLUMNS_in_df = [x for x in df.columns if x in UNIQUE_IDENTIFIER_COLUMNS or x == 'resid']
        diffs = compareValues(df, dfcore, UNIQUE_IDENTIFIER_COLUMNS_in_df)

        if diffs.shape[0] > 0:
            print(diffs.head())
            print(diffs['variable'].value_counts())
            diffs['filename'] = FILENAME
            diffs.to_csv(FILEPATH_COMPARISON, mode='a', index=False)
            print(f"\nDIFFERENCES FOUND in {FILENAME}. ABORTED!")
            return
        else:
            print(f"No differences found for {FILENAME}")
            
        del dfcore
        del df
        del diffs

if __name__ == "__main__":
    main()