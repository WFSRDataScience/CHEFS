# -*- coding: utf-8 -*-
"""
Created on Mon Aug 19 14:56:04 2024

@author: hoend008
"""

import pathlib
import pandas as pd   
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import os


"""
GLOBALS
"""
WORKDIR = "C:/Users/hoend008/HOLIFOOD"
EXCEL_PATH = pathlib.Path(WORKDIR, "zonodo url file.xlsx")


"""
GET LIST OF FILES
"""
df = pd.read_excel(EXCEL_PATH, sheet_name = "microbiological")

# check
print(df[df.duplicated(subset=['url'])])
print(df[df.duplicated(subset=['countrycode', 'year'])])



"""
LOOP OVER FILE LIST AND DOWNLOAD ZIPFILES AND GIVE THEM PROPER NAMES
"""
for i, row in df.iterrows():

    countrycode = row['countrycode']
    year = row['year']
    url = row['url']
    countrydir = f"C:/Users/hoend008/HOLIFOOD/{countrycode}/"
    
    zipurl = url.replace("records/", 'api/records/') + "/files-archive"

    with urlopen(zipurl) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zfile:
            for zip_info in zfile.infolist():
                filename = zip_info.filename
                oldName = pathlib.Path(countrydir, filename)
                newName = pathlib.Path(countrydir, f"{countrycode}_AMR_PUB_{year}.zip")
            if not os.path.exists(oldName) and not os.path.exists(newName):
                zfile.extractall(countrydir)
                print(countrycode, " ", year, " new file")
            else:
                print(countrycode, " ", year, " file already exists")
            
            if not os.path.exists(newName):
                os.rename(oldName, newName)