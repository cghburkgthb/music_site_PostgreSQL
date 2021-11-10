import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json
from pandas.io.json import json_normalize


def process_song_file(cur, filepath):
    """
    Read, parse and store song info into songs and artists table for one song file
    
    Args:
        (DB cursor) cur - cursor of open DB connection where tables exist
        (str) filepath  - directory and name of song file

    Returns:
        N/A
    """

    with open(filepath) as song_stream:    
        song_json = json.load(song_stream)
        song_df = pd.io.json.json_normalize(song_json)

    # insert song record
    artist_id = song_df.values[0][0]
    song_id = song_df.values[0][7]
    title = song_df.values[0][8]
    duration = song_df.values[0][5]
    year = song_df.values[0][9]

    song_data = [song_id, title, artist_id, year, duration]
    
    try: 
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e: 
        print("Error: Inserting record into song table")
        print (e)   
    
    # insert artist record
    artist_id = song_df.values[0][0]
    latitude = song_df.values[0][1]
    location = song_df.values[0][2]
    longitude = song_df.values[0][3]
    name = song_df.values[0][4]
    
    artist_data = [artist_id, name, location, latitude, longitude]
    
    try: 
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e: 
        print("Error: Inserting record into artist table")
        print (e)
    


def process_log_file(cur, filepath):
    """
    Read, parse and store song site log info into time, user and song play 
    tables for one file.
    
    Args:
        (DB cursor) cur - cursor of open DB connection where tables exist
        (str) filepath  - directory and name of song Web site log file

    Returns:
        N/A
    """
    
    # open log file
    df = pd.read_json(filepath, orient='columns', lines = True)

    # filter by NextSong action
    df = df.query('page == "NextSong"')

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month
                    , t.dt.year, t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year'
                        , 'weekday']
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        try: 
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e: 
            print("Error: Inserting record into time table")
            print (e)
            return
        

    # load user table
    user_data = [df['userId'], df['firstName'], df['lastName'], df['gender']
                    , df['level']]
                    
    usr_data_col_lbl = ['user_id', 'first_name', 'last_name', 'gender', 'level']
    user_df = pd.DataFrame.from_dict(dict(zip(usr_data_col_lbl, user_data)))
    #user_df.drop_duplicates(inplace = True)
    user_df = user_df.dropna()
    
    # insert user records
    for i, row in user_df.iterrows():
        try: 
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e: 
            print("Error: Inserting record into users table")
            print (e)
            return

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        #print("lookup: ", results)
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        start_ts = pd.to_datetime(row.ts, unit='ms')
        songplay_data = (start_ts, row.userId, row.level, songid, artistid
                            , row.sessionId, row.location, row.userAgent)
                            
        try: 
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e: 
            print("Error: Inserting record into songplays table")
            print (e)
            return


def process_data(cur, conn, filepath, func):
    """
    Determine file paths and call processing function for each path found.
    
    Args:
        (DB cursor) cur - cursor of open DB connection where tables exist
        (DB connection) conn - open database where the tables exist    
        (str) filepath  - directory and name of song Web site log file
        (function) - pointer to function use to process data files

    Returns:
        N/A
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student \
                                password=student")
    cur = conn.cursor()
    conn.set_session(autocommit = True)
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()