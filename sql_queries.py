import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES IF THEY DO NOT EXIST

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_songs_table_create= ("""
  CREATE TABLE staging_songs(num_songs int, artist_id varchar, artist_latitude decimal, artist_longitude decimal, artist_location varchar, song_id varchar, artist_name varchar, title varchar, duration decimal, year int)
""")

staging_events_table_create = ("""
  CREATE TABLE staging_events(artist varchar, auth varchar, firstName varchar, gender char, itemInSession int, lastName varchar, length decimal, level varchar, location varchar, method varchar, page varchar, registration decimal, sessionId int, song varchar, status int, ts bigint, userAgent varchar, userId int)
""")

# Create a distribution key because this is a dimension table. Create a sortkey on timestamp because queries against this table will filter on start_time.
songplay_table_create = ("""
  CREATE TABLE songplays(songplay_id int IDENTITY(0,1) distkey, start_time timestamp sortkey, user_id varchar, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar) DISTSTYLE KEY
""")

# Do not create a distribution key on this table because it is a fact table. Create a sortkey on user_id to improve run time of joining the fact table (songplay) to this table.
user_table_create = ("""
  CREATE TABLE users(user_id varchar primary key not null sortkey, first_name varchar, last_name varchar, gender char, level varchar) DISTSTYLE ALL
""")

# Do not create a distribution key because this is a fact table. Create a sortkey on song_id to improve run time of joining the fact table (songplay) to this table.
song_table_create = ("""
  CREATE TABLE songs(song_id varchar primary key not null sortkey, title varchar, artist_id varchar, year int, duration decimal) DISTSTYLE ALL
""")

# Do not create a distribution key on this table because it is a fact table. Create a sortkey on artist_id to improve run time of joining the fact table (songplay) to this table.
artist_table_create = ("""
  CREATE TABLE artists(artist_id varchar primary key not null sortkey, name varchar, location varchar, latitude decimal, longitude decimal) DISTSTYLE ALL
""")

# Do not create a distribution key because it is a fact table. Create a sortkey on start_time to improve run time of joining the fact table (songplay) to this table.
time_table_create = ("""
  CREATE TABLE time(start_time timestamp primary key not null sortkey, hour smallint, day smallint, week smallint, month smallint, year smallint, weekday smallint) DISTSTYLE ALL
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {} CREDENTIALS {} JSON {} REGION 'us-west-2'
""").format(config['S3']['log_data'], config['IAM_ROLE']['arn'], config['S3']['log_jsonpath'])

staging_songs_copy = ("""COPY staging_songs FROM {} CREDENTIALS {} JSON 'auto' REGION 'us-west-2';
""").format(config['S3']['song_data'], config['IAM_ROLE']['arn'])

# FINAL TABLES

songplay_table_insert = ("""
  INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + (se.ts / 1000 * INTERVAL '1 second'), se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events se
    JOIN staging_songs ss ON se.song = ss.title
    WHERE se.song IS NOT NULL and ss.title IS NOT NULL
  """)

user_table_insert = ("""
  INSERT INTO users
  SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events se
  WHERE se.userID is NOT NULL
""")

song_table_insert = ("""
  INSERT INTO songs
  SELECT song_id, title, artist_id, year, duration FROM staging_songs ss      
  WHERE ss.song_id is NOT NULL
""")

artist_table_insert = ("""
  INSERT INTO artists
  SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
  FROM staging_songs ss
  WHERE ss.artist_id is NOT NULL
""")

time_table_insert = ("""
  INSERT INTO time
    WITH convert_to_timestamp AS (
      SELECT TIMESTAMP 'epoch' + (ts / 1000 * INTERVAL '1 second') AS ts
      FROM staging_events se
      WHERE se.userId IS NOT NULL
      )          
  SELECT DISTINCT ts,
  EXTRACT(hour FROM ts) AS hour,
  EXTRACT(day FROM ts) AS day,
  EXTRACT(week FROM ts) AS week,
  EXTRACT(month FROM ts) AS month,
  EXTRACT(year FROM ts) AS year,            
  EXTRACT(dow FROM ts) AS weekday
  FROM convert_to_timestamp                
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

copy_table_queries = [staging_events_copy, staging_songs_copy]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]