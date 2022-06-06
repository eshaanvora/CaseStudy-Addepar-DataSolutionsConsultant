#Eshaan Vora
#Addepar Case Study
#04/06/2022


# Referencing (https://www.earthdatascience.org/courses/intro-to-earth-data-science/scientific-data-structures-python/pandas-dataframes/import-csv-files-pandas-dataframes/#:~:text=Using%20the%20read_csv()%20function,pd%20to%20call%20pandas%20functions)
# Import packages
import os
import matplotlib.pyplot as plt
import pandas as pd

from sqlalchemy import create_engine
import pymysql

import csv

import mysql.connector
from mysql.connector import errorcode

#Set credentials for Database connection
hostName = "localhost"
dbName = "AddeparCaseStudy"
userName = "root"
password = "Password"


# Import data from .csv file
fname1 = "/Users/eshaan/Downloads/CASE_STUDY/FilesReceived/AccountTransactions.csv"
fname2 = "/Users/eshaan/Downloads/CASE_STUDY/ReferenceFiles/TransactionTypeMappings.csv"

fname3 = "/Users/eshaan/Downloads/CASE_STUDY/FilesReceived/PositionDetails.csv"
fname4 = "/Users/eshaan/Downloads/CASE_STUDY/ReferenceFiles/SecurityTypeMappings.csv"

#Read CSV into Pandas Dataframe
transactions_df = pd.read_csv(fname1)
reference_transactions_df = pd.read_csv(fname2)

position_details_df = pd.read_csv(fname3)
reference_security_mapping_df = pd.read_csv(fname4)

#Remove spaces in the column names of the dataframe
transactions_df.columns = transactions_df.columns.str.replace(' ','_')
reference_transactions_df.columns = reference_transactions_df.columns.str.replace(' ','_')

position_details_df.columns = position_details_df.columns.str.replace(' ','_')
reference_security_mapping_df.columns = reference_security_mapping_df.columns.str.replace(' ','_')


###########################################
#Create SQL Connection to upload data into MySQL Server
sqlEngine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=hostName, db=dbName, user=userName, pw=password))

dbConnection = sqlEngine.connect()

try:
    frame = transactions_df.to_sql("Transactions", dbConnection, if_exists='fail');
    frame = reference_transactions_df.to_sql("TransactionTypeMappings", dbConnection, if_exists='fail');
    frame = position_details_df.to_sql("PositionDetails", dbConnection, if_exists='fail');
    frame = reference_security_mapping_df.to_sql("SecurityTypeMappings", dbConnection, if_exists='fail');

except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Tables created successfully.");
finally:
    dbConnection.close()

###########################################
#Establish connection to MySQLServer Platform
#If error occurs, check error type, then exit program
try:
   AddeparConnection = mysql.connector.connect(
   user=userName,
   password=password,
   #Public IP address: '35.192.72.137' Port: 3306
   host=hostName,
   database=dbName)

   #Create cursor for cpsc408 database connection
   dbCursor = AddeparConnection.cursor()
   print("Database connection was successful")

except mysql.connector.Error as err:
   if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print('Invalid credentials')
      exit()
   elif err.errno == errorcode.ER_BAD_DB_ERROR:
      print('Database not found')
      exit()
   else:
      print('Cannot connect to database:', err)
      exit()

def write_csv_prompt(results):
    choice = input("Save results to CSV? (y/n) ")
    while choice.isdigit() == True:
        print("Incorrect option. Try again")
        choice = input("Enter choice number: ")
    if choice[0].upper() == "Y":
        with open("ExportBin/Results.csv", 'w') as csvfile:
            writeFile = csv.writer(csvfile)
            writeFile.writerows(results)
            csvfile.close()


query = """SELECT Transactions.index, Security, temp.Addepar_Security_Type,
TransactionTypeMappings.Addepar_Transaction_Type,
Trade_Date, Quantity, Value, Currency
FROM Transactions
INNER JOIN TransactionTypeMappings
ON Transactions.Transaction_Type = TransactionTypeMappings.Transaction_Type
INNER JOIN (
SELECT PositionDetails.CUSIP, SecurityTypeMappings.Addepar_Security_Type
FROM PositionDetails
INNER JOIN SecurityTypeMappings
ON PositionDetails.Asset_Type = SecurityTypeMappings.Security_Type) temp
ON Transactions.CUSIP = temp.CUSIP;"""

dbCursor.execute(query)
results = dbCursor.fetchall()

write_csv_prompt(results)

AddeparConnection.close()
