# CHEFS

## About this repository

This respository contains everything required to create a local version of the CHEFS database, further discussed in the accompanying article: "Trends from 300 million European food safety monitoring measurements: the CompreHensive European Food Safety (CHEFS) Database". The repository is structured as follows:

-   01. Scripts : collection of scripts for database setup, data download and data processing
-   02. Prevalence Data : Folder to store microbiological Prevalence data (empty for now) 
-   03. AMR data : Folder to store AMR data (empty for now)
-   04. Exports : Folder for data exports (empty for now)
-   05. Zoonose data : Folder to store microbiological Prevalence data (empty for now) 
-   06. Meta data : Folder that stores some meta information in Excel files (needed for some of the scripts in 01. Scripts)
-   07. Data Files : destination folder for downloaded data files
-   08. EFSA Catalogues/version v9 : All EFSA catalogues (needed for some scripts in the 01. Scripts)
-   09. Query & Visualization Examples : Collection of python notebooks that include example SQL queries and visualizations from the article. See the README.md inside for more information
-   README.md
-   requirements.txt 

## How to get started?

### Installation instructions

#### Software
The CHEFS database processing pipeline has been developed on computer with a Windows operation system. Supplementary software included:
- python v3.12
- postgresql v14
- git

#### Hardware
The data processing scripts have been developed and run on a computer with an Intel Xeon CPU 2400 MHz, 32 GB RAM, and 100+ GB of storage space

#### Installation steps: setting up a python environment, an empty database, and the database connection
To set up the CHEFS database processing pipeline, follow these steps:
1. Download and install [Python](https://www.python.org/downloads/)
2. Download and install [PostgreSQL](https://www.postgresql.org/download/)
3. Download and install [git](https://git-scm.com/downloads)
4. Create a directory on your computer where you want to store all files related to the processing pipeline
5. Open up the terminal
6. cd into directory created at step 3 (see above)
7. Clone content of this repository into the directory: `git clone https://git.wur.nl/wouter.hoenderdaal/chefs.git`
8. Create python virtual environment by typing in the following commands in the terminal in this order:
- `pip install virtualenv` (if you don't already have virtualenv installed)
- `virtualenv venv`
- `.\venv\Scripts\activate`
- `pip install -r requirements.txt`
Note that the last step will install all python packages required for the CHEFS database processing pipeline. The packages will be installed inside the newly created environment "venv".
9. Setting up the database: Download and install [pgAdmin](https://www.pgadmin.org/download/) and create an empty database. Inside this database, run the following commands:
- `CREATE SCHEMA efsa;`
- `CREATE SCHEMA ontologies_efsa;`
These schemas will store the EFSA data and the EFSA catalogues.
10. Configure database connection: Having cloned this repository into the directory, go to the directory "01. Scripts" and open the file "DBcredentials.py". In this file, either use environment variables or (not recommended) hardcode the DB host/name/username/password here.

You have now sucessfully set up the python environment, the database with two empty schemas, and the database connection.


### Running the data processing scripts
Below the data processing steps are summarized. In the other 2 sections more details are presented. Please read everything, otherwise you will run into problems.

#### Python vs SQL scripts
Note that there are .py (Python) and .sql (PostgreSQL) scripts in the 01. Scripts folder. The .py files should be run on the command line from the virtual environment. The .sql scripts, however, should be run from the pqsl Postgres terminal. You can enter this terminal either via the pgAdmin tool. How to use psql and get it started, please see the [documentation](https://www.postgresql.org/docs/current/app-psql.html) and the pgAdmin [documentation](https://www.pgadmin.org/docs/pgadmin4/development/psql_tool.html) on psql

#### Summary Steps
1. Run the "0." scripts to create tables in the database
2. Choose a type of data, e.g. start with "contaminants"
3. Run script 1 to 13
4. Run some queries in the database for testing (optional)
5. Delete all folders and files in the "07. Datafiles" folder
6. Repeat steps starting from step 2.

#### Step 1: Initialising the database
To run the processing scripts, go to the terminal and type the following to reach the 01. Scripts folder:
- `cd "01. Scripts"`

In the "01 Scripts" folder, you find a series of python and SQL scripts numbered from 0 to 13.

**Important** The scripts starting with "0" should only be executed once! These are:

- 0a. EFSA CATALOGUE.py
- 0b. efsa - TABLES CATALOGUE.sql
- 0c. efsa - TABLES.sql
- 0d. meta-info in DB.sql

These scripts create the tables and content of the EFSA catalogues, and creates the tables and views where the EFSA zenodo data will be stored.

You can run a scripts by typing in the terminal (after having activated your python virtual environment): `python '.\0a. EFSA CATALOGUE.py'`

Tip: type in "python 0" and then press the TAB key on your keyword. This will auto-complete the filename and give you `python '.\0a. EFSA CATALOGUE.py'`

#### Steps 2-5: Data processing 
After intialising the database in Step 1, you can proceed with steps 2-5. These steps download the datafiles from Zenodo, to process these files, create intermediary csv files, and import those into the database.
**Note** that there are three types of data that can be imported into the database, namely:
- Contaminants
- Pesticides
- Veterinary

If you wish to get access to the entire database, all scripts (from "1. download_zonodo_files.py" to "13. CLEAN TABLES"), with the exception of the scripts listed under nr 0, should be run for each data type.

You select the data type by changing the TYPE variable in the scripts 1. download_zonodo_files.py (lines 26-28) and 2. convert 7z to zip.py (lines 16-18) to the desired type.
For example, if you start with "contaminants" (which is recommended because the file sizes are smallest compared to the other two), you need to first change the variable TYPE to TYPE = "contaminants". 

Then you start running the scripts 1 to 13 in the order that they are numbered.
In the scripts, especially the .sql scripts, additional detailed information is provided to explain what you need to do.

**EXCEPTION** Note that for pesticides, the script "2. convert 7z to zip.py" requires you to first manually unpack all the .7z files in all the country folders in "07. Data Files". Unfortunatelly, these pesticide .7z files could not be automatically unzipped using Python. So that is why you need to do this manually.


Some of the data files are very large, and processing is extensive, so do not be surprised if running a script like "4. create_country_sample_files.py" takes many hours (up to 6 hours on my computer for veterinary data).
The scripts do give you feedback that shows on which file or for which country they are currently working.


## Querying the data in the database
After all data processing is done, you can rely on the following views (and their underlying tables) to query the data:
- efsa.vw_sample_core
- efsa.vw_measurement_core
- efsa.vw_sample_measurement_core

Note that the database is not yet optimized for fast performance. Some steps, e.g. partitioning is implemented to facilitate faster query speed, but feel free to implement additional indexes and other things to boost performance. Because these things can be very specific to what type of queries you are running, we leave it up to you to make decisions regarding performance optimization.


## Authors and acknowledgment
Rosan Hobe, Wouter Hoenderdaal