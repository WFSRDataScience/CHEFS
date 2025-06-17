"""
Creates csv files with sample rest data for import into DB
"""

import numpy as np
import pandas as pd
import pathlib
import os
from utils import cleancolumns, df_tolower, df_trim


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

FILEPATH_COPYQUERIES_REST_SAMPLE = pathlib.Path(FILE_DATA_DIR, "copyqueries_core_sample_rest.txt")

CHUNK_SIZE = 1000000


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
    

"""
GET RELEVANT COLUMNS
"""
df_cols = pd.read_excel(FILEPATH_COLUMNS, sheet_name="column_metaInfo")
df_cols = basic_processing(df_cols)
df_cols = df_cols.query("sample_measurement == 's'")

# for sample table
SAMPLE_COLUMNS_DICT = dict(zip(df_cols.columnname, df_cols.datatype))
SAMPLE_COLUMNS_LIST = list(df_cols.columnname)

# get sample core columns
df_cols_core = df_cols.query("core_sample == 1")
CORE_SAMPLE_COLUMNS = list(df_cols_core.columnname)

df_cols_uniques = df_cols.query("unique_identifier == 1")
UNIQUE_IDENTIFIER_COLUMNS = list(df_cols_uniques.columnname) + ['file']

CORE_SAMPLE_COLUMNS_NOT_UNIQUE = [x for x in CORE_SAMPLE_COLUMNS if x not in UNIQUE_IDENTIFIER_COLUMNS]


"""
GET ALL DIRECTORIES IN WORKDIR
"""
DIRECTORIES = [{'country': dI, 'path': pathlib.Path(FILE_DATA_DIR,dI)} for dI in os.listdir(FILE_DATA_DIR) if os.path.isdir(os.path.join(FILE_DATA_DIR,dI))]   
DIRECTORIES = [x for x in DIRECTORIES if len(x['country']) == 2]


"""
READ IN ALL SAMPLE CSV FILES AND CONCAT THEM INTO 1 FILE
"""
def main():
    for DIR in DIRECTORIES:
        
        COUNTRY = DIR['country']

        print("------------------------------------------------")
        print(COUNTRY)
        
        # get sample dir
        sample_dir = pathlib.Path(DIR['path'], 'sample')
        
        # get sample csv in sample dir
        CSV_PATH = pathlib.Path(sample_dir, f"sample_{COUNTRY}.csv")
    
        # skip country if a sample csv file does not exist
        fileExist = os.path.exists(CSV_PATH)
        if not fileExist:
            print(f"Skipped {COUNTRY}. sample file does not exist")
            continue
        
        # read in sample csv file
        i = 0
        for tmp_df in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):
            
            # remove CORE COLUMNS NOT PART OF UNIQUE_IDENTIFIER_COLUMNS
            tmp_df = tmp_df.drop(CORE_SAMPLE_COLUMNS_NOT_UNIQUE, axis=1)

            # create empty columns in case UNIQUE_IDENTIFIER_COLUMNS are not present
            missing_identifier_cols = [x for x in UNIQUE_IDENTIFIER_COLUMNS if x not in tmp_df.columns]
            for col in missing_identifier_cols:
                print(f"adding unique identifier column {col}")
                tmp_df[col] = np.nan
            
            # from wide to long
            tmp_df = tmp_df.melt(id_vars=UNIQUE_IDENTIFIER_COLUMNS)
            
            # remove rows with no value
            tmp_df = tmp_df.dropna(subset=['value'])
            
            # basic processing
            tmp_df = basic_processing(tmp_df)
            
            # file to lower
            tmp_df['file'] = tmp_df['file'].str.lower()
            
            # sort columns by name
            tmp_df_columns = list(tmp_df.columns)
            tmp_df_columns.sort()
            tmp_df = tmp_df[tmp_df_columns]
            
            # construct save Path
            FILENAME_SAVE = f"sample_rest_{COUNTRY}_{i}.csv"
            full_sample_dir_path = pathlib.Path(FULLPATH_MAIN_DIR, str(sample_dir)[3:])
            full_sample_file_path = pathlib.Path(full_sample_dir_path, FILENAME_SAVE)            
            CSV_SAVE_PATH = pathlib.Path(sample_dir, FILENAME_SAVE)
            
            tmp_df['analysisd'] = tmp_df['analysisd'].astype(float)
            tmp_df['analysism'] = tmp_df['analysism'].astype(float)
            tmp_df['anportseq'] = tmp_df['anportseq'].astype(float)
            tmp_df['repyear'] = tmp_df['repyear'].astype(float)

            # write to csv
            tmp_df.to_csv(CSV_SAVE_PATH, index=False)
            
            # sql query
            copy_query = f"\copy efsa.tmp_sample_rest({','.join(tmp_df.columns)}) from '{full_sample_file_path}' (header true, delimiter ',', format csv, encoding 'UTF-8');"
            with open(FILEPATH_COPYQUERIES_REST_SAMPLE, 'a') as f:
                f.write(copy_query + "\n")
            
            del tmp_df
            i = i + 1
        
        
if __name__ == "__main__":
    main()