"""
Creates csv files with all samples for country
"""

import numpy as np
import pandas as pd
import pathlib
import os
from zipfile import ZipFile
from utils import cleancolumns, df_tolower, df_trim


"""
SETTINGS
"""
pd.set_option("future.no_silent_downcasting", True)
from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
simplefilter(action="ignore", category=pd.errors.DtypeWarning)


"""
GLOBALS
"""
OVERWRITE = True

WORKDIR = "../"
META_DATA_DIR = pathlib.Path(WORKDIR, "06. Meta data")

FILE_COLUMNS = "column-meta-info.xlsx"
FILEPATH_COLUMNS = pathlib.Path(META_DATA_DIR, FILE_COLUMNS)
FILEPATH_DUPLICATES_BETWEEN_FILES = pathlib.Path(WORKDIR, "duplicates_between_files.csv")

FILE_DATA_DIR = pathlib.Path(WORKDIR, "07. Data Files")


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

def exceptionVMPR2017PT(df: pd.DataFrame) -> pd.DataFrame:    
    """" This file has a few duplicate rows in which sampsizeunit sometimes has a value and sometimes does not. The values are always the same, hence the solution"""
    # find duplicates
    cols = [x for x in df.columns if x in UNIQUE_IDENTIFIER_COLUMNS]    
    #cols = getColumnsRemoveDuplicates(df.columns)
    
    tmp_df_sample_duplicates = df[df.duplicated(cols)]
    dupIds = list(tmp_df_sample_duplicates['sampid'])
    
    # get df with dubs
    dfDups = df.query("sampid == @dupIds")    
    dfDups['sampsizeunit'] = 'g050a'
    
    # get df without dubs
    dfNoDups = df.query("sampid != @dupIds")
    
    # concat together
    df = pd.concat([dfNoDups, dfDups])
    
    return df

def replaceZero(df: pd.DataFrame, c: str) -> pd.DataFrame:
    if c in df.columns:
        df[c] = df[c].replace(0, np.nan)
    return df
    

"""
GET META DATA
"""
print("start processing create_country_sample_files")
print("Getting meta data..")

df_cols = pd.read_excel(FILEPATH_COLUMNS, sheet_name="column_metaInfo")
df_cols = basic_processing(df_cols)

# get only sample columns
df_cols = df_cols.query("sample_measurement == 's'")[["columnname", "datatype", "unique_identifier"]]

# for sample table
SAMPLE_COLUMNS_DICT = dict(zip(df_cols.columnname, df_cols.datatype))
SAMPLE_COLUMNS_LIST = list(df_cols.columnname)

# get core sample columns
df_cols_core = df_cols.query("unique_identifier == 1")
UNIQUE_IDENTIFIER_COLUMNS = list(df_cols_core.columnname)


"""
GET ALL DIRECTORIES IN WORKDIR
"""
DIRECTORIES = [{'country': dI, 'path': pathlib.Path(FILE_DATA_DIR,dI)} for dI in os.listdir(FILE_DATA_DIR) if os.path.isdir(os.path.join(FILE_DATA_DIR,dI))]   
DIRECTORIES = [x for x in DIRECTORIES if len(x['country']) == 2]


"""
LOOP OVER ALL DIRECTORIES, OPEN ALL ZIPFILES IN THAT DIRECTORY, CONCAT INTO DF AND SAVE AS CSV
"""
print("start main processing\n")

for DIR in DIRECTORIES:

    print("------------------------------------------------")
    print(DIR['country'])
        
    # create dir if not exists
    sample_dir = pathlib.Path(DIR['path'], 'sample')
    dirExist = os.path.exists(sample_dir)
    if not dirExist:
        os.makedirs(sample_dir)
        print("Directory created!")     
        
    # check if there already is a sample file, if so and OVERWRITE is not True, then continue to next iteration
    SAVE_PATH = pathlib.Path(sample_dir, f"sample_{DIR['country']}.csv")
    fileExist = os.path.exists(SAVE_PATH)
    if fileExist:
        if not OVERWRITE:
            print(f"Skipped {DIR['country']}. sample file already exists")
            continue
            
    # loop over zip files in directory, open them, rename column, select columns, append to list_dfs and then concat all to single df
    list_dfs = []
    zipfile_list = list(DIR['path'].rglob("*.ZIP"))

    if len(zipfile_list) > 0:
      for zipfile in zipfile_list:
          
          # skip downloaded ZIP file
          if 'contaminants' in zipfile.name:
              continue
          
          if 'pesticides' in zipfile.name:
              continue
          
          if 'veterinary' in zipfile.name:
              continue
          
          # in case AMR data is saved in the same folder, skip that for now (you need another script to process AMR data)
          if 'AMR' in zipfile.name:
              continue
          
          print("\t - " + zipfile.name)
          
          # read data
          zf = ZipFile(zipfile)
          csv_in_zf = [csv_file.filename for csv_file in zf.infolist() if csv_file.filename.lower().endswith('.csv')]
          tmp_df = pd.read_csv(zf.open(csv_in_zf[0]))
      
          #tmp_df = pd.read_csv(zipfile, compression='zip')
          
          # clean column names
          tmp_df.columns = cleancolumns(tmp_df.columns)
          
          # rename columns to harmonize column names over all files
          tmp_df.columns = [x.replace('_a', '') for x in tmp_df.columns]
          tmp_df = tmp_df.rename(columns={
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

          # select only sample columns (columns with sample information)
          sample_columns_in_tmp_df = [x for x in tmp_df.columns if x in SAMPLE_COLUMNS_LIST]
          tmp_df = tmp_df[sample_columns_in_tmp_df]
          
          # all data to lower and trim and clean columnnames
          tmp_df = basic_processing(tmp_df)

          # replace N_A with np.nan
          tmp_df = tmp_df.replace("n_a", np.nan) 

          # replace 0 with np.nan
          tmp_df['sampmatinfoprody'] = tmp_df['sampmatinfoprody'].replace(0, np.nan)
          tmp_df['sampmatinfoprodm'] = tmp_df['sampmatinfoprodm'].replace(0, np.nan)
          tmp_df['sampmatinfoprodd'] = tmp_df['sampmatinfoprodd'].replace(0, np.nan)
          tmp_df['analysisy'] = tmp_df['analysisy'].replace(0, np.nan)
          tmp_df['analysism'] = tmp_df['analysism'].replace(0, np.nan)
          tmp_df['analysisd'] = tmp_df['analysisd'].replace(0, np.nan)

          # convert columns to correct datatype based on the datatype definition in column-meta-info.xlsx
          sample_datatype_dict = {key: SAMPLE_COLUMNS_DICT[key] for key in sample_columns_in_tmp_df}
          for k, v in sample_datatype_dict.items():
              if 'number' in v:
                  if 'int' not in str(tmp_df[k].dtypes):
                      tmp_df[k] = tmp_df[k].astype(float)
              elif 'text' in v or 'catalogue' in v:
                  tmp_df[k] = tmp_df[k].astype(str)
              else:
                  print(f"no datatype for column {k}")
          
          # convert text 'nan' to np.nan (this might be introduced in the datatype conversion above)
          tmp_df = tmp_df.replace("nan", np.nan)

          # EXCEPTION FOR 'VMPR_2017_PT.ZIP'
          if zipfile.name == 'VMPR_2017_PT.ZIP':
              tmp_df = exceptionVMPR2017PT(tmp_df) 
              
          # group on everything to keep only 1 row per sampid. Go to next DIR iteration if duplicates still exist
          tmp_df = tmp_df.drop_duplicates(subset = tmp_df.columns)    
          
          # reduce samples to 1 row by removing duplicates based on set of columns
          # if duplicates based on the unique identifiers remain, print a warning and skip that file
          columns_to_remove_duplicates = [x for x in tmp_df.columns if x in UNIQUE_IDENTIFIER_COLUMNS]
          tmp_df_sample_duplicates = tmp_df[tmp_df.duplicated(columns_to_remove_duplicates)]
          if tmp_df_sample_duplicates.shape[0] > 0:
              print("Duplicates remain in sample table. MAKE SURE TO RESOLVE THIS!!")
              tmp_df_sample_duplicates[columns_to_remove_duplicates].to_excel(pathlib.Path(sample_dir, f"{zipfile.name} dubs.xlsx"), index=False)
              continue
          
          # ADD FILENAME
          zipname = zipfile.name
          tmp_df['file'] = zipname.lower()
          
          # append to list_dfs
          list_dfs.append(tmp_df)
      
      # create 1 dataframe. This dataframe will have all sample information for a country
      del tmp_df
      df = pd.concat(list_dfs)    
      
      # convert some values
      df = replaceZero(df, 'isolinfoisoly')
      df = replaceZero(df, 'isolinfoisolm')
      df = replaceZero(df, 'isolinfoisold')
      df = replaceZero(df, 'isolinfoarrivalyisol')
      df = replaceZero(df, 'isolinfoarrivalmisol')
      df = replaceZero(df, 'isolinfoarrivaldisol')
      df = replaceZero(df, 'sampaninfocompy')
      df = replaceZero(df, 'sampaninfocompm')
      df = replaceZero(df, 'sampaninfocompd')
      df = replaceZero(df, 'sampmatinfoprody')
      df = replaceZero(df, 'sampmatinfoprodm')
      df = replaceZero(df, 'sampmatinfoprodd')
      df = replaceZero(df, 'sampmatinfoexpiryy')
      df = replaceZero(df, 'sampmatinfoexpirym')
      df = replaceZero(df, 'sampmatinfoexpiryd')
      df = replaceZero(df, 'sampeventinfoslaughtery')
      df = replaceZero(df, 'sampeventinfoslaughterm')
      df = replaceZero(df, 'sampeventinfoslaughterd')
      df = replaceZero(df, 'sampinfoarrivaly')
      df = replaceZero(df, 'sampinfoarrivalm')
      df = replaceZero(df, 'sampinfoarrivald')

      """
      df['isolinfoisoly'] = df['isolinfoisoly'].replace(0, np.nan)
      df['isolinfoisolm'] = df['isolinfoisolm'].replace(0, np.nan)
      df['isolinfoisold'] = df['isolinfoisold'].replace(0, np.nan)
      df['isolinfoarrivalyisol'] = df['isolinfoarrivalyisol'].replace(0, np.nan)
      df['isolinfoarrivalmisol'] = df['isolinfoarrivalmisol'].replace(0, np.nan)
      df['isolinfoarrivaldisol'] = df['isolinfoarrivaldisol'].replace(0, np.nan)    
      df['sampaninfocompy'] = df['sampaninfocompy'].replace(0, np.nan)
      df['sampaninfocompm'] = df['sampaninfocompm'].replace(0, np.nan)
      df['sampaninfocompd'] = df['sampaninfocompd'].replace(0, np.nan)
      df['sampmatinfoprody'] = df['sampmatinfoprody'].replace(0, np.nan)
      df['sampmatinfoprodm'] = df['sampmatinfoprodm'].replace(0, np.nan)
      df['sampmatinfoprodd'] = df['sampmatinfoprodd'].replace(0, np.nan)
      df['sampmatinfoexpiryy'] = df['sampmatinfoexpiryy'].replace(0, np.nan)
      df['sampmatinfoexpirym'] = df['sampmatinfoexpirym'].replace(0, np.nan)
      df['sampmatinfoexpiryd'] = df['sampmatinfoexpiryd'].replace(0, np.nan)
      df['sampeventinfoslaughtery'] = df['sampeventinfoslaughtery'].replace(0, np.nan)
      df['sampeventinfoslaughterm'] = df['sampeventinfoslaughterm'].replace(0, np.nan)
      df['sampeventinfoslaughterd'] = df['sampeventinfoslaughterd'].replace(0, np.nan)
      df['sampinfoarrivaly'] = df['sampinfoarrivaly'].replace(0, np.nan)
      df['sampinfoarrivalm'] = df['sampinfoarrivalm'].replace(0, np.nan)
      df['sampinfoarrivald'] = df['sampinfoarrivald'].replace(0, np.nan)
      """

      # group on everything to keep only 1 row per sampid. Go to next DIR iteration if duplicates exist
      # this duplication step removes duplicates between files (instead of within files)
      df = df.drop_duplicates(subset = df.columns)
      
      # repeating check for duplicates
      columns_to_remove_duplicates_in_df = [x for x in df.columns if x in UNIQUE_IDENTIFIER_COLUMNS]
      df_sample_duplicates = df[df.duplicated(columns_to_remove_duplicates_in_df + ['file'])]
      if df_sample_duplicates.shape[0] > 0:
          print("Duplicates remain in sample table")
      
      # save to csv
      df.to_csv(SAVE_PATH, index=False)

      if len(df_sample_duplicates) > 0:
        SAVE_PATH_DUPLICATES = pathlib.Path(sample_dir, f"duplicates_{DIR['country']}.csv")
        df_sample_duplicates.to_csv(SAVE_PATH_DUPLICATES, index=False)

    else:
        print("No zipfiles for processing")