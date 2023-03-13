import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config['IAM_ROLE']['ARN']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(event_id int IDENTITY(0,1),
                                                                          artist varchar, 
                                                                          auth varchar ,
                                                                          firstName varchar,
                                                                          gender varchar, 
                                                                          iteminSession int,
                                                                          lastName varchar, 
                                                                          length float,
                                                                          level varchar,
                                                                          location varchar, 
                                                                          method varchar,
                                                                          page varchar ,
                                                                          registration varchar,
                                                                          sessionId int ,
                                                                          song varchar ,
                                                                          status int ,
                                                                          ts numeric,
                                                                          userAgent varchar,
                                                                          userId int 
                                                                         )
                            """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(num_songs int, 
                                                                          artist_id varchar ,
                                                                          artist_latitude float , 
                                                                          artist_longitude float ,
                                                                          artist_location varchar ,
                                                                          artist_name varchar , 
                                                                          song_id varchar , 
                                                                          title varchar , 
                                                                          duration float , 
                                                                          year int )
                            """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays( 
                                                                    songplay_id int IDENTITY(0,1) PRIMARY KEY,
                                                                    start_time timestamp NOT NULL, 
                                                                    user_id int NOT NULL , 
                                                                    level varchar NULL,
                                                                    song_id varchar NULL, 
                                                                    artist_id varchar NULL, 
                                                                    session_id int NULL, 
                                                                    location varchar NULL, 
                                                                    user_agent varchar NULL)
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS 
                        users (userId int PRIMARY KEY , 
                        firstName varchar NULL, 
                        lastName varchar NULL, 
                        gender varchar NULL, 
                        level varchar NULL);
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS 
                        songs (song_id varchar PRIMARY KEY, 
                        title varchar NULL, 
                        artist_id varchar NULL, 
                        year int NULL, 
                        duration float NULL);
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY ,
                                                                artist_name varchar NULL, 
                                                                artist_location varchar NULL, 
                                                                artist_latitude float NULL, 
                                                                artist_longitude float NULL);
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time timestamp PRIMARY KEY , 
                                                        hour int NULL, 
                                                        day int NULL, 
                                                        week int NULL, 
                                                        month int NULL, 
                                                        year int NULL, 
                                                        weekday varchar NULL);
                    """)

# STAGING TABLES

staging_events_copy = """copy staging_events from {}
                         credentials 'aws_iam_role={}'
                         format as json {}
                         STATUPDATE ON
                         region 'us-west-2';
                        """.format(config['S3']['LOG_DATA'], DWH_ROLE_ARN,config['S3']['LOG_JSONPATH'])


staging_songs_copy = """copy staging_songs from {}
                        credentials 'aws_iam_role={}'
                        format as json 'auto'
                        STATUPDATE ON
                        region 'us-west-2';
                    """.format(config['S3']['SONG_DATA'], DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level,song_id , artist_id, session_id, location, user_agent) 
                            SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'   AS start_time,
                                            se.userId                   AS user_id,
                                            se.level                    AS level,
                                            ss.song_id                  AS song_id,
                                            ss.artist_id                AS artist_id,
                                            se.sessionId                AS session_id,
                                            se.location                 AS location,
                                            se.userAgent                AS user_agent
                            FROM staging_events AS se
                            JOIN staging_songs ss ON se.song = ss.title and se.artist = ss.artist_name
                            WHERE se.page = 'NextSong';
                        """)

user_table_insert = ("""INSERT INTO users (userId,firstName,lastName,gender,level) 
                        SELECT DISTINCT se.userID as userId,
                                        se.firstName as firstName,
                                        se.lastName as lastName,
                                        se.gender as gender,
                                        se.level as level
                        FROM staging_events as se
                        WHERE se.page = 'NextSong';
                    """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                        SELECT DISTINCT ss.song_id as song_id,
                                        ss.title as title,
                                        ss.artist_id as artist_id,
                                        ss.year as year,
                                        ss.duration as duration
                        FROM staging_songs as ss;
                    """)

artist_table_insert = ("""INSERT INTO artists (artist_id , artist_name , artist_location , artist_latitude , artist_longitude) 
                          SELECT DISTINCT ss.artist_id as artist_id,
                                          ss.artist_name as artist_name,
                                          ss.artist_location as artist_location,
                                          ss.artist_latitude as artist_latitude,
                                          ss.artist_longitude as artist_longitude
                          FROM staging_songs as ss;
                        """)

time_table_insert = ("""INSERT INTO time (start_time,hour,day,week,month,year,weekday) 
                        SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'   AS start_time,
                                        EXTRACT(hour FROM start_time) as hour,
                                        EXTRACT(day FROM start_time) as day,
                                        EXTRACT(week FROM start_time) as week,
                                        EXTRACT(month FROM start_time) as month,
                                        EXTRACT(year FROM start_time) as year,
                                        EXTRACT(weekday FROM start_time) as weekday
                        FROM staging_events as se
                        WHERE se.page = 'NextSong';
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
