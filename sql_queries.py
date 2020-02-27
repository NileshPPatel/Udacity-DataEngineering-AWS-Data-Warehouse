import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN') 
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES 

# Staging Tables
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (\
    event_id bigint IDENTITY(0,1), artist varchar distkey, auth varchar,\
    firstName varchar, gender varchar, itemInSession int, lastName varchar,\
    length float, level varchar, location varchar, method varchar,\
    page varchar sortkey, registration varchar, sessionId int, song varchar,\
    status varchar, ts bigint, userAgent varchar, userId int)""")

#artist_location, artist_name, and title columns to extra large (VARCHAR(500)).

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (\
    num_songs int, artist_id varchar, artist_latitude varchar,\
    artist_longitude varchar, artist_location varchar,\
    artist_name varchar distkey, song_id varchar, title varchar,\
    duration float, year int)""")

# Fact Table
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (\
    songplay_id bigint IDENTITY(0,1) PRIMARY KEY, start_time timestamp NOT NULL,\
    user_id int NOT NULL, level varchar, song_id varchar, artist_id\
    varchar distkey, session_id int, location varchar, user_agent varchar)""")

# Dimension Tables
user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int\
    PRIMARY KEY, first_name varchar, last_name varchar, gender varchar,\
    level varchar) diststyle all""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar distkey\
    PRIMARY KEY, title varchar, artist_id varchar NOT NULL, year int,\
    duration float)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar\
    PRIMARY KEY, name varchar, location varchar, latitude varchar,\
    longitude varchar) diststyle all""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp\
    PRIMARY KEY, hour int2, day int2, week int2, month int2, year int,\
    weekday int2) diststyle all""")


# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json {} compupdate off region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json 'auto' compupdate off region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES
#
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id,\
    level, song_id, artist_id, session_id, location, user_agent) \
    select distinct \
    timestamp 'epoch' + e.ts/1000 * INTERVAL '1 second',\
    e.userId,\
    e.level,\
    s.song_id,\
    s.artist_id,\
    e.sessionId,\
    e.location,\
    e.userAgent \
    from staging_events e \
    JOIN staging_songs s ON e.artist = s.artist_name \
    and e.song = s.title WHERE e.page = 'NextSong' """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name,\
    gender, level) \
    select distinct \
    userId,\
    firstName,\
    lastName,\
    gender,\
    level \
    from staging_events where page = 'NextSong' """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year,\
    duration) \
    select distinct \
    song_id,\
    title,\
    artist_id,\
    year,\
    duration \
    from staging_songs """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location,\
    latitude, longitude) \
    select distinct \
    artist_id,\
    artist_name,\
    artist_location,\
    artist_latitude,\
    artist_longitude \
    from staging_songs """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month,\
    year, weekday) \
    select distinct \
    timestamp 'epoch' + ts/1000 * interval '1 second' as start_time,\
    extract(hour from start_time),\
    extract(day from start_time),\
    extract(week from start_time),\
    extract(month from start_time),\
    extract(year from start_time),\
    extract(dayofweek from start_time) \
    from staging_events where page = 'NextSong' """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
