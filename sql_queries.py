import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_songs_table_create= ("""CREATE TABLE IF NOT EXISTS staging_songs(
num_songs int,
artist_id varchar(20),
artist_latitude float(5),
artist_longitude float(5),
artist_location varchar(100),
artist_name varchar(100),
song_id varchar(20),
title varchar(100),
duration float,
year int
)
""")


staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events(
artist varchar(100),
auth varchar(100),
first_name VARCHAR(100),
gender char(1),
item_in_session int,
last_name VARCHAR(100),
length float,
level varchar(10),
location varchar(100),
method varchar(20),
page varchar(20),
registration float,
session_id int,
song VARCHAR,
status int,
ts BIGINT,
user_agent varchar(150),
user_id int
);
""")


songplay_table_create = ("""
CREATE TABLE if not exists songplay(
                         songplay_id serial primary key,
                         start_time TIMESTAMP,
                         user_id INT not null,
                         level varchar(10) not null,
                         song_id varchar(20),
                         artist_id varchar(20),
                         session_id int,
                         location varchar(50),
                         user_agent varchar(150),
                         primary key (songplay_id)
                         );
                         """)



user_table_create =("""
CREATE TABLE IF NOT EXISTS users(
user_id int,
first_name varchar(100),
last_name varchar(100),
gender char(1),
level varchar(10) not null,
primary key (user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id varchar(20),
title varchar(100),
artist_id varchar(20),
year int,
duration float,
primary key(song_id)
);
""")

artist_table_create = ("""
CREATE TABLE if not exists artists(
artist_id varchar(20) primary key,
name varchar(100) not null,
location varchar(100),
latitude float(5),
longitude float(5),
);
""")


time_table_create = ("""
CREATE TABLE if not exists time(
start_time timestamp primary key,
hour int,
day int,
week int,\
month int, 
year int,
weekday int
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_events
FROM {}
iam_role {}
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays(songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
select distinct ss.song_id,
se.ts,
se.level,
ss.artist_id,
se.user_id,
se.session_id,
se.location,
se.user_agent 
from staging_events se
inner join staging_songs ss on (ss.title=se.song and se.artist=ss.artist_name)
and se.page='NextSong';
""")

        
user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
select distinct se.user_id,
se.first_name,
se.last_name,
se.gender,
se.level from staging_events se
where se.user_id is not null
""")

song_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
select distinct se.user_id,
se.first_name,
se.last_name,
se.gender,
se.level from staging_events se
where se.user_id is not null
""")



artist_table_insert = ("""
INSERT INTO artist(artist_id, name, location, lattitude, longitude)
select distinct ss.artist_id,
ss.artist_name,
ss.artist_location,
ss.artist_latitude,
ss.longitude from staging_songs ss
where ss.artist_id is not null
""")

time_table_insert = ("""
INSERT INTO TIME(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
       TIMESTAMP 'epoch' + (se.ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events se;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
