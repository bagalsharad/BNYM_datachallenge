Exercise 1:

You have data on passive mutual funds and ETFs from the Morningstar database for a specific date (snapshot as of October 31, 2017): Fund Data (1).csv and Fund Data (2).csv

1.	Start by inspecting and cleaning the data. Produce a brief “Data Quality Report” outlining what you find (maximum 1 page in Word). 
2.	Next, make a database using the cleaned data (any type of DB you want), justify your choice of DB and schema, and write a query of some kind to answer the following question:
•	For funds with more than 1 share class, what is the average 5-year information ratio?
3.	Use the data and your favorite visualization techniques to describe the risk/return characteristics of the different asset classes. Create a presentation or webpage (or any other medium you choose) to present the results. 



Exercise 2:
The results.json file contains logs of users accessing an app. 
•	Create a python script (or document your solution in the tool of your choosing) to convert the file to .csv format. 
	Only the following fields are needed: lastopenedat, objectid, appname, email, localeidentifier, devicetype, timezone, installationid, devicetoken, latitude and longitude.
•	Load the csv file to an sql database of your choice
•	Submit 2-3 SQL queries to analyze the data
•	Bonus points: the original log file was a hundred times bigger. Do your scripts to convert and load the json file still work?
