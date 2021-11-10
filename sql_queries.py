# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays_fct;"
song_play_drop = "DROP TABLE IF EXISTS song_play;"
user_table_drop = "DROP TABLE IF EXISTS users_dim;"
song_table_drop = "DROP TABLE IF EXISTS songs_dim;"
artist_table_drop = "DROP TABLE IF EXISTS artists_dim;"
time_table_drop = "DROP TABLE IF EXISTS time_dim;"

###########################################################
# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(songplay_id SERIAL PRIMARY KEY
            , start_time timestamp NOT NULL, user_id int NOT NULL
            , level text, song_id text NOT NULL, artist_id text NOT NULL
            , session_id int NOT NULL, location text, user_agent text
            , CONSTRAINT natural_pk unique(user_id, session_id, start_time));
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(user_id int PRIMARY KEY, first_name text
            , last_name text , gender char, level text);
""")


song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(song_id text PRIMARY KEY, title text
            , artist_id text NOT NULL, year int, duration numeric);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(artist_id text PRIMARY KEY, name text
            , location text, latitude int, longitude int);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(start_time timestamp PRIMARY KEY, hour int
            , day int, week int, month int , year int, weekday int);
""")

song_play_create = ("""
    CREATE TABLE IF NOT EXISTS song_play(artist text, auth text
            , firstName text, gender text, itemInSession numeric
            , lastName text, length numeric, level text, location text
            , method text, page text, registration numeric, sessionId numeric
            , song text, status numeric, start_ts timestamp, userAgent text
            , userId text);
""") 

###########################################################
# INSERT RECORDS
songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level
            , song_id, artist_id, session_id, location, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT natural_pk DO UPDATE SET
                    (level, song_id, artist_id, location, user_agent)
                        = (EXCLUDED.level, EXCLUDED.song_id, EXCLUDED.artist_id
                            , EXCLUDED.location, EXCLUDED.user_agent)
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                    (first_name, last_name, gender, level)
                        = (EXCLUDED.first_name, EXCLUDED.last_name
                            , EXCLUDED.gender, EXCLUDED.level)
""")

song_table_insert = """
    INSERT INTO songs(song_id, title, artist_id, year, duration)
            VALUES (%s, %s, %s, %s, %s) 
            ON CONFLICT (song_id) DO UPDATE SET 
                    (title, artist_id, year, duration) 
                        = (EXCLUDED.title, EXCLUDED.artist_id, EXCLUDED.year 
                            , EXCLUDED.duration)
"""

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (artist_id) DO UPDATE SET
                    (name, location, latitude, longitude)
                        = (EXCLUDED.name, EXCLUDED.location, EXCLUDED.latitude
                            , EXCLUDED.longitude)
""")


time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
            VALUES (%s, %s, %s, %s, %s, %s, %s )
            ON CONFLICT (start_time) DO NOTHING
""")


###########################################################
# FIND SONGS

song_select = ("""
    SELECT s.song_id, s.artist_id
    FROM songs s JOIN artists a ON s.artist_id = a.artist_id
    WHERE (s.title, a.name, s.duration) = (%s, %s, %s)
""")

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, song_play_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, song_play_drop]