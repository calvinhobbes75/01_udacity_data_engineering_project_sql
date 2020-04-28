
---
[UDACITY DATA ENGINEERING NANODEGREE](https://classroom.udacity.com/nanodegrees)

Project: Data Modeling with Postgres
---

# **Introduction**

A startup called **Sparkify** wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project will create a Postgres Database Schema and ETL pipeline to optimize queries for song play analysis.

# **Project Description**

In this project, we will create a data model with Postgres and build and ETL pipeline using Python :
-  On the database side, we need to define fact and dimension tables for a Star Schema
- an ETL pipeline to transfer data from files located in two local directories into these tables in Postgres using Python and SQL

# **Schema for Song Play Analysis** (see figure)

**Fact Table**

- **songplays** records in log data associated with song plays

**Dimension Tables**

- **users** in the app

- **songs** in music database

- **artists** in music database

- **time**: timestamps of records in songplays broken down into specific units

**Sparkify Database - Fact and Dimensions Tables - Star Schema :**
![alt text](./img/sparkify-database-schema.png "Sparkify Database Star Schema")

# **Project Design**

Database Design is very optimized because with a few number of tables and doing specific join, we can get the most information and do analysis

ETL Design is also simplified have to read json files and parse accordingly to store the tables into specific columns and proper formatting

# **Database Script**

Writing "python create_tables.py" command in terminal, it is easier to create and recreate tables

# **Jupyter Notebook**

etl.ipynb, a Jupyter notebook is given for verifying each command and data as well and then using those statements and copying into etl.py and running it into terminal using "python etl.py" and then running test.ipynb to see whether data has been loaded in all the tables

# **Projetc Files**

- **docker-compose.yml** configuration file to run Postgres and pgAdmin from Docker containers

- **requirements.txt** PIP install requirements to install necessary python packages

- **sql_queries.py** containg all sql queries to drop, create and import data to sparkify database

- **create_tables.py** script to drop and to create tables in sparkify database. This script must be ran to reset 

- **etl.ipynb** Jupyter notebook to read and process a single file from song_data and log_data and loads into your tables in Jupyter 

- **etl.py**  script to process batch of files from song_data and log_data and loads into tables in sparkify database

- **test.ipnb** Jupyter notebook to test sparkify database and display the first few rows of each table

# Running code

Create a conda python environment
```python
conda create -n udacity python=3.7
```
Active conda environment and install python package environment
```python
conda activate udacity
pip install -r requirements.txt
```
Run Postgres and pgAdmin using Docker on local pc (Linux, MacOS or Windows using WSL)
```docker
docker-compose up -d
```
Postgres will run on **localhost:5432**  
pgAdmin will run on **localhost:5555**

See docker-compose.yml for details


For the project, before running any script/notebook, reset the sparkify database if necessary
```
python create_tables.py
```
Use **etl.ipynb** notebook to test and design the ETL steps  
  
To insert songs and logs data to sparkify database :
```python
python etl.py
```

Use **test.ipynb** to check sparkify database tables contents and test useful queries