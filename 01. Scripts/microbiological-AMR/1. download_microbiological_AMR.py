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
WORKDIR = "../../"

META_DATA_DIR = pathlib.Path(WORKDIR, "06. Meta data")
FILE_DATA_DIR = pathlib.Path(WORKDIR, "07. Data Files")

FILEPATH_URL_FILE = pathlib.Path(META_DATA_DIR, "zenodo url file.xlsx")


"""
READ URL FILE
"""
df = pd.read_excel(FILEPATH_URL_FILE, sheet_name="microbiological")

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

	# get path
	path = DIR['path']

	# get url(s) and years
	df_country = df.query("countrycode == @country")
	
	for i, row in df_country.iterrows():
		url = row['url']
		year = row['year']

		# create path for zipfile
		pathZipfile = pathlib.Path(path, country + f"{year}_AMR.zip")
		print(pathZipfile)

		# if there is a url, then download, otherwise do nothing
		if url == url:
			URL = url.replace("records/", 'api/records/') + "/files-archive"

			# download files
			r = requests.get(URL)
			with open(pathZipfile, "wb") as fd:
				fd.write(r.content)
			
			print(pathZipfile)
			# unzip downloaded file
			with ZipFile(pathZipfile, 'r') as zObject:
		    
				# get old and new filename
				for file in zObject.infolist():
					oldName = pathlib.Path(path, file.filename)
					newName = pathlib.Path(path, f"{country}_AMR_PUB_{year}.zip")

				# extract
				zObject.extractall(path=path)				

				# rename file
				os.rename(oldName, newName)

			# remove old zip
			os.remove(pathZipfile)

			print(f"Unzipped files for {country}")

		else:
			print(f"No files for {country}")
