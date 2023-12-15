import configparser
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Define Global variables from the configuration file
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")

# SQL queries for table creation, data copying, and data insertion
# CREATE TABLES
                     
songplay_table_create = ( 
    """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1),
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    );
    """ )
user_table_create = (    
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR
    );
    """)
song_table_create = (    
    """
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INT,
        duration FLOAT
    );
    """)
artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    );
    """)
time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    );
    """ )
    
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAr,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts numeric,
    userAgent VARCHAR,
    userId INTEGER
);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
);""") 
                      
#STAGING TABLES
staging_songs_copy = (f"""
    COPY staging_songs FROM {SONG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ROLE}'
    REGION 'us-east-1'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS
    BLANKSASNULL
    EMPTYASNULL
""")
#STAGING EVENTS
staging_events_copy = ("""
 
    copy staging_events from {bucket}
    credentials 'aws_iam_role={role}'
    region      'us-west-2'
    format       as JSON {path}
    timeformat   as 'epochmillisecs'
 
""").format(bucket=LOG_DATA, role=IAM_ROLE, path=LOG_PATH)
# DROP TABLES


staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;",
user_table_drop = "DROP TABLE IF EXISTS users;",
song_table_drop = "DROP TABLE IF EXISTS songs;",
artist_table_drop = "DROP TABLE IF EXISTS artists;",
time_table_drop = "DROP TABLE IF EXISTS time;"

#insert tables

songplay_table_insert = (
    """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT e.ts AS start_time,
           e.userId AS user_id,
           e.level,
           s.song_id,
           s.artist_id,
           e.sessionId AS session_id,
           e.location,
           e.userAgent AS user_agent
    FROM staging_events e
    JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong';
    """)
user_table_insert = (
    """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId AS user_id,
                    firstName AS first_name,
                    lastName AS last_name,
                    gender,
                    level
    FROM staging_events
    WHERE userId IS NOT NULL;
    """)
song_table_insert =(
    """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id,
           title,
           artist_id,
           year,
           duration
    FROM staging_songs;
    """)
artist_table_insert = (
    """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id,
           artist_name AS name,
           artist_location AS location,
           artist_latitude AS latitude,
           artist_longitude AS longitude
    FROM staging_songs;
    """)
time_table_insert = (
    """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT ts AS start_time,
           EXTRACT(HOUR FROM ts) AS hour,
           EXTRACT(DAY FROM ts) AS day,
           EXTRACT(WEEK FROM ts) AS week,
           EXTRACT(MONTH FROM ts) AS month,
           EXTRACT(YEAR FROM ts) AS year,
           EXTRACT(DOW FROM ts) AS weekday
    FROM staging_events
    WHERE page = 'NextSong';
    """)

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop ] 
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]                      
