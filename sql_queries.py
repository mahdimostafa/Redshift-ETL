import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS song_play"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events 
(
   artist varchar,
   auth varchar,
   firstName varchar,
   gender varchar,
   itemInSession int,
   lastName varchar,
   length numerical,
   level varchar,
   location varchar,
   method varchar,
   page varchar,
   registration varchar,
   sessionId int,
   song varchar,
   status int,
   ts bigint,
   userAgent varchar,
   userId int
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs int,
    artist_id varchar,
    artist_latitude numerical,
    artist_longitude numerical,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration numerical,
    year int
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(
    songplay_id PRIMARY KEY identity(0,1),
    start_time timestamp NOT NULL,
    user_id varchar NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar  
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
(
    user_id int,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
);    
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
(
    song_id varchar,
    title varchar,
    artist_id varchar,
    year int,
    duration numerical
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
(
    artist_id varchar,
    name varchar,
    location varchar,
    latitude numerical,
    logitude numerical
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
(
    start_time varchar,
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from s3://udacity-dend/log_data               
""").format(ARN, SONG_DATA)

staging_songs_copy = ("""
COPY staging_songs from s3://udacity-dend/song_data
""").format(ARN, LOG_DATA)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT date_part(epoch, ts) as start_time,
    se.userId as user_id,
    se.level, 
    ss.song_id,
    ss.artist_id,
    se.sessionId as session_id,
    se.location,
    se.userAgent as user_agent
    from staging_events se
    inner join staging_songs ss 
    on se.artist = ss.artist_name
    
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId as user_id,
               firstName as first_name,
               lastName as last_name,
               gender as gender,
               level as level
    FROM staging_events
    WHERE userId IS NOT NULL;
""")


song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, 
    title, 
    artist_id, 
    year, 
    duration 
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT artist_id, 
           artist_name as name, 
           artist_location as location, 
           artist_latitude as latitude,
           artist_longitude as longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
    ts as start_time
    EXTRACT(h FROM ts) as hour
    EXTRACT(d FROM ts) as day
    EXTRACT(w FROM ts) as week
    EXTRACT(mon FROM ts) as month
    EXTRACT(weekday FROM ts) as weekday
    FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
