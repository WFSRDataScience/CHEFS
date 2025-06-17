"""
Convert .7z files to .zip files
"""

import py7zr
import numpy as np
import pandas as pd
import pathlib
import os
import zipfile 


"""
GLOBALS
"""
#TYPE = 'contaminants'
TYPE = 'pesticides'
#TYPE = 'veterinary'

WORKDIR = "../"
FILE_DATA_DIR = pathlib.Path(WORKDIR, "07. Data Files")

FULLPATH_MAIN_DIR = os.getcwd()
FULLPATH_MAIN_DIR = os.path.abspath(os.path.join(FULLPATH_MAIN_DIR ,".."))
FULLPATH_DATA_DIR = pathlib.Path(FULLPATH_MAIN_DIR, "07. Data Files")


"""
GET ALL DIRECTORIES IN WORKDIR
"""
DIRECTORIES = [{'country': dI, 'path': pathlib.Path(FULLPATH_DATA_DIR,dI)} for dI in os.listdir(FULLPATH_DATA_DIR) if os.path.isdir(os.path.join(FULLPATH_DATA_DIR,dI))]   
DIRECTORIES = [x for x in DIRECTORIES if len(x['country']) == 2]


"""
LOOP OVER ALL DIRECTORIES, EXTRACT 7z FILES, CONVERT TO ZIP
"""

# loop over directories in WORKDIR
for DIR in DIRECTORIES:
    
    print( f"------------------ {DIR['country']} ------------------")
    
    # set current working directory to path of country
    folderPath = DIR['path']
    os.chdir(folderPath)

    # skip extracting .7z files for pesticides. This does not seem to work for those .7z files
    if TYPE == 'pesticides':
        pass
    else:
        # # loop over all 7z files
        for zFile in list(folderPath.rglob("*.7z")):
            print(zFile.name)
                    
            # extract .7z file
            with py7zr.SevenZipFile(zFile.name, mode='r') as z:
                z.extractall(folderPath)
        
    # loop over all .csv files in WORKDIR and add to .zip files
    for csvFile in list(folderPath.rglob("*.csv")):

        zipname = csvFile.name.lower().replace('.csv', '.zip').upper()
        
        # zip the csv file
        with zipfile.ZipFile(zipname, 'w', compression=zipfile.ZIP_DEFLATED) as new_zipfile:
            new_zipfile.write(csvFile.name)
            
        # delete the csvFile
        os.remove(csvFile)

    # remove .7z files for pesticides (in order to save some space, these files are big)
    if TYPE == 'pesticides':
        for zFile in list(folderPath.rglob("*.7z")):
            os.remove(zFile)
    
    
    