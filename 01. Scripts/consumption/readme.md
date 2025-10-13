# EFSA CONSUMPTION DATA

## DOWNLOAD
You can download the files here: https://www.efsa.europa.eu/en/microstrategy/foodex2-level-7

As you can see, there are 4 different types, namely
- Chronic
- Chronic by gender
- Acute
- Acute by gender

Each has a corresponding data processing script, see the .py files

Make sure to do the following:
- In the data folder ("10. Consumption Data"), create four subfolders, namely "Chronic", "Chronic Gender", "Acute", and "Acute Gender". Inside each of these subfolders, create a "final" subfolder.
-  Move the downloaded files to their corresponding folder

## Data Processing
Before running the scripts, make sure to do the following in each processing (.py) script:
- On line 115, fill in your working directory
- Make sure you have set up a proper database connection with working environment variables (see lines 22-25)
- Make sure you have the PostgreSQL database set up. For that you need to run the "create tables.sql" file.

You can now run the scripts one by one. Each time, a csv file will be saved in the "final" folder that you created before.
Each processing scripts will print the SQL \copy statement to import the data into the database table at the end of the script. You can run this SQL statement in a psql terminal.

## Useful links
Here you can read more about the EFSA food consumption data itself:
https://www.efsa.europa.eu/en/data-report/food-consumption-data
