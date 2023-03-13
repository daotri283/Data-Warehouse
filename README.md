 Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This repo is the ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs  users are listening to.

# Project Repository files: 
create_tables.py: drops and creates your tables. 
etl.py: reads and processes files from song_data and log_data and loads them into Redshift. 
sql_queries.py: contains all your sql queries, and is imported into the last two files above.
README.md: provides discussion on the project.
dwh.cfg: contains config for AWS resources 
Create_AWS_Resources.ipynb: Create the neccessary resources on AWS for the project and delete those resources when done

# Database design: 
    Fact Table:
    1. songplays - records in log data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

    Dimension Tables:
    1. users - users in the app
    user_id, first_name, last_name, gender, level
    2. songs - songs in music database
    song_id, title, artist_id, year, duration
    3. artists - artists in music database
    artist_id, name, location, latitude, longitude
    4. time - timestamps of records in songplays broken down into specific units
    start_time, hour, day, week, month, year, weekday


# Execution step

## Prerequisite: 
- In order to run this project we need to create an IAM role, an S3 bucket and a Redshift database on AWS

## How to run the project 
Step 1: Create tables by running create_tables.py
Step 2: Build ETL Pipeline by running etl.py to load data from S3 to staging tables on Redshift and to load data from staging tables to analytics tables on Redshift.