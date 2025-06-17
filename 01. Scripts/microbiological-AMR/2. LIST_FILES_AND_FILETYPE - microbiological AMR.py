# -*- coding: utf-8 -*-
"""
Created on Wed Mar 13 14:20:53 2024

@author: hoend008
"""

import xlrd
import csv
import pandas as pd
import pathlib
from csv import writer
import re
from zipfile import ZipFile

import sys
import os

sys.path.insert(1, 'W:/WFSR/Projects/72250_Statistics_NP_Feed/11. Libs')
from WH_data_wrangling_functions import cleancolumns, df_tolower, df_trim
from PostgresDatabasev2 import PostgresDatabase


"""
GLOBALS
"""
# set output path
#WORKDIR = pathlib.Path('W:/WFSR/Projects/74141-HOLiFOODWP1/Data/EFSA (zenodo)/')
WORKDIR = pathlib.Path("C:/Users/hoend008/HOLIFOOD")
FILEPATH_LISTFILES = pathlib.Path(WORKDIR, "listFiles-AMR.csv")

DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')


"""
HELPER FUNCTIONS
"""
def getFileType(string: str) -> str:
    if "AMR" in string:
        return 'amr'
    else:
        return ''
    
def basic_processing(df: pd.DataFrame) -> pd.DataFrame:

    # all data to lower and trim
    df = df_tolower(df)
    df = df_trim(df)
     
    # clean columnsnames (lowercase, remove special characters etc)
    df.columns = cleancolumns(df.columns)
    
    # return df
    return df


def createDataframe(p: pathlib.Path):
    
    workbook = xlrd.open_workbook(p)
    sheet = workbook.sheet_by_index(0)

def csv_from_excel(p: pathlib.Path) -> pathlib.Path:
    wb = xlrd.open_workbook(p)
    sh = wb.sheet_by_index(0)
    csvPath = pathlib.Path(p.parent, p.stem + ".csv")
    your_csv_file = open(csvPath, 'w', encoding='utf8')
    wr = csv.writer(your_csv_file, quoting=csv.QUOTE_ALL)

    for rownum in range(sh.nrows):
        wr.writerow(sh.row_values(rownum))

    your_csv_file.close()
    
    return csvPath
    
    
def main():  
    
    """
    LIST FILETYPE PER FILE
    - loop over all AMR zipfiles, extract the content (either csv or xlsx), and convert that to a 
      proper working csv file with a correct name (delete the original file)
    """
    # get all zip files with 'amr' in the filename
    for zipfile in list(WORKDIR.rglob("*AMR_PUB*.zip")):        
        
        print(zipfile)
        
        # determine final csv name and skip procedure if that file already exists
        finalCSVpath = pathlib.Path(zipfile.parent, zipfile.name.replace('.zip', '.csv'))
        if os.path.exists(finalCSVpath):
            continue

        print(zipfile)
            
        filename = str(zipfile.name)
        countrycode = filename[:2]
        year = int(''.join([str(i) for i in filename if i.isdigit()]))
        
        with ZipFile(zipfile, 'r') as zObject: 

            # extract zipfile into DIR
            DIR = zipfile.parent
            zObject.extractall(path=DIR)            
            
            # rename extracted file
            for zip_info in zObject.infolist():
                filename = zip_info.filename
                print(filename)
                if filename.split('.')[1] == 'zip':
                    continue
                fileExtension = filename.split('.')[1]
                oldName = pathlib.Path(DIR, filename)
                newName = pathlib.Path(DIR, f"{countrycode}_AMR_PUB_{year}.{fileExtension}")
                
                # if new file not yet exists
                if not os.path.exists(newName):
                    os.rename(oldName, newName)
        
        # convert xlsx to csv because xlsx cannot be opened in pandas due to an error I do not understand
        # then read in data
        if fileExtension == 'xlsx':
            csvPath = csv_from_excel(newName)
            # delete old xlsx file
            os.remove(newName)
            
            df = pd.read_csv(csvPath)
        else:
            csvPath = newName
            df = pd.read_csv(csvPath, delimiter='\t')
            df.to_csv(csvPath, index=False)
            
        # determine number of columns
        nr_cols = df.shape[1]
        nr_rows = df.shape[0]
        
        # write to excel
        with open(FILEPATH_LISTFILES, 'a', newline='') as f:
            writer_object = writer(f, delimiter=',')
            writer_object.writerow([newName, year, countrycode, nr_cols, nr_rows])
        

def toDB():
    """
    IMPORT FILES IN DB
    """
    df_files = pd.read_csv(FILEPATH_LISTFILES)
    df_files = basic_processing(df_files)
    
    df_files = df_files.rename(columns={'file': 'filename'})
    
    # check uniqueness of termcode
    print('duplicates\n')
    print(df_files[df_files.duplicated(['filename'])])
    
    # to DB
    # ---------------- search for differences
    with PostgresDatabase('owfsr', DB_USER, DB_PASSWORD) as db:
        diffs = db.explore_diffs(df_files.copy(), 
                                 'efsa.files', 
                                 identifiers=['filename'])
    
    print(diffs)
    
        
    # ---------------- import in DB
    with PostgresDatabase('owfsr', DB_USER, DB_PASSWORD) as db:
        db.insert_update(df_files.copy(), 
                         'efsa.files', 
                         update_existing=['filename'])

if __name__ == "__main__":
    main()