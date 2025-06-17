"""
Downloads files from zenodo
"""

import pandas as pd
import pathlib
import os
import requests
from zipfile import ZipFile 


"""
SETTINGS
"""
import warnings
warnings.filterwarnings('ignore')


"""
GLOBALS
"""
WORKDIR = "../"

META_DATA_DIR = pathlib.Path(WORKDIR, "06. Meta data")
FILE_DATA_DIR = pathlib.Path(WORKDIR, "07. Data Files")

FILEPATH_URL_FILE = pathlib.Path(META_DATA_DIR, "zenodo_recent_urls.xlsx")

#TYPE = 'contaminants'
TYPE = 'pesticides'
#TYPE = 'veterinary'


"""
READ URL FILE
"""
df = pd.read_excel(FILEPATH_URL_FILE, sheet_name=TYPE)

df_countries = df.countrycode.value_counts().reset_index()


"""
CREATE FOLDER STRUCTURE FOR COUNTRIES
"""
for i, row in df_countries.iterrows():
	countrycode = row['countrycode']
	countrypath = pathlib.Path(FILE_DATA_DIR, countrycode)

	# check if dir exists, if not then create it
	if os.path.isdir(countrypath):
		print(f"Folder {countrycode} already exists")
	else:
		print(f"Folder {countrycode} created")
		os.makedirs(countrypath)
	

"""
GET ALL DIRECTORIES OF COUNTRIES IN WORKDIR
"""
DIRECTORIES = [{'country': dI, 'path': pathlib.Path(FILE_DATA_DIR,dI)} for dI in os.listdir(FILE_DATA_DIR) if os.path.isdir(os.path.join(FILE_DATA_DIR,dI))]   
DIRECTORIES = [x for x in DIRECTORIES if len(x['country']) == 2]


"""
LOOP OVER DIRECTORIES, DOWNLOAD FILES, STORE IN FOLDER AND UNZIP
"""
for DIR in DIRECTORIES:
	
	# get country
	country = DIR['country']
	print("------------------------------------------------")
	print(country)

	# get path and create path for zipfile
	path = DIR['path']
	pathZipfile = pathlib.Path(path, country + f"_{TYPE}.zip")

	# get url(s)
	urls = df.query("countrycode == @country")['url']
	urls = urls.to_list()

	for url in urls:
		# if there is a url, then download, otherwise do nothing
		if url == url:
			url_id = url.replace('https://zenodo.org/records/', '').replace('/latest', '')
			URL = "https://zenodo.org/api/records/" + url_id + "/files-archive"
			print(url_id, "   ", URL)

			# download files
			r = requests.get(URL)
			with open(pathZipfile, "wb") as fd:
				fd.write(r.content)
			
			# unzip downloaded file
			with ZipFile(pathZipfile, 'r') as zObject: 
				zObject.extractall(path=path)
				print(f"Unzipped files for {country}")
		else:
			print(f"No files for {country}")
