"""
Author: Adel Cheah

Date: 2 Aug 2021

AUTOMATED DATA LOADER (ADL) - modules
This script produces 5 key modular functions which assists with the
autmated ingestion of flat files from a source folder location
into a target SQL Server database location

Variables:

Folder location with source flat files
Tick box for the option to traverse subfolders (optional)
Extension name to look for
Destination Server name
Destimation DB name
Idea of steps:

Create base ETL log tables if they don't already exist in dest db
log all scanned source files and MD5 hash of each
decide which files have not been loaded before and needs to be loaded
(based on MD5 hashes of prev loaded files)
for each file to be loaded, load each file into a separate table
store logs of each file loaded
reporting - perform stats summary of each file that's been loaded
Add try catch capability to opening files (MD5) and loading files functions.
Parse through error messages into logging tables.
"""

##############################################################################
### IMPORT all required libraries
##############################################################################
import hashlib # used for MD5 hash check
import pandas as pd
import os # used for file list exploration
import time, datetime

#libraries for SQL connections
import urllib
from sqlalchemy import create_engine

# for RAW table creation - sets as NVARCHAR type
from sqlalchemy.types import NVARCHAR

#for breaking out of script (optional future improvements)
#import sys

##############################################################################
### 0) Standard functions used throughout other modules in this script
##############################################################################

def create_server_connection(DBserver, DBname):
	"""
	Returns a DB engine via sqlalchemy that can be used for SQL queries

	Inputs: DBserver - the db servername to connect to
			DBname - the db name to connect to

	Output: connection - the sql engine that can be used for SQl queries
			error_message - the error message if an error occurs
			(blank string if no error)
	"""
	try:
		params = urllib.parse.quote_plus("Driver={SQL Server Native Client 11.0};"
										 "Server="+DBserver+";"
										 "Database="+DBname+";"
										 "Trusted_Connection=yes;")
										 #'UID=' + creds['user'] + ';' \
										 #'PWD=' + creds['passwd'] + ';' \
										 #'PORT=' + str(creds['port']) + ';')


		connection = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
		error_message = ''

		print("SQL Server DB engine config set successful!")

	except Exception as err:
		print(f"SQL Server DB engine config FAILED!!!\nError:\n'{err}'")
		connection = None
		error_message = err

	finally:
		return connection, error_message


def sql_query_execute(query, engine):
	"""
	Performs SQL query based on the DB connection that has been passed

	Inputs: query - sql query to perform on target SQL db connection engine
			engine - the target SQL DB connection that the above passed query is to be performed on

	Output: result - the SQL Query result based on the input query performed
			error_message - the error message if an error occurs (blank string if no error)
	"""

	try:
		result = engine.execute(query)
		error_message = ''

		print("SQL query executed successfully!")

	except Exception as err:
		print(f"SQL query execution FAILED!!!\nError:\n'{err}'")
		result = None
		error_message = err

	finally:
		return result, error_message

