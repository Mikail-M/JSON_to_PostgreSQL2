This Python program dynamically makes a PostgreSQL table from a given JSON file, and fills the PostgreSQL table with the data from that JSON file after. 
The JSON file i used contains two strings and WKT data for geospatial data.

Please be sure to include an .env file in thesame directory as this Python program.
This .env file is needed to secure a connection to PostgreSQL. It should contain values for:

USER
PASSWORD
HOST
PORT
DATABASE

