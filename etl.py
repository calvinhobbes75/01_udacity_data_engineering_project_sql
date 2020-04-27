import os
import glob
import psycopg2
import pandas as pd
from io import StringIO
from sql_queries import *


def process_song_file(cur, filepath):
    """This function reads JSON files and read information of song and artist data and saves into song_data and artist_data

    :param cur: Database Cursor
    :type cur: psycopg2.extensions.cursor
    :param filepath: path of JSON files for songs and artits data
    :type filepath: str
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """This function reads Log files and reads information of time, user and songplay data and saves into time, user, songplay tables

    :param cur: Database Cursor
    :type cur: psycopg2.extensions.cursor
    :param filepath: path of JSON files for logs data
    :type filepath: str
    """
  
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records

    time_df = pd.DataFrame({'start_time' : t,
                            'hour' : t.dt.hour,
                            'day' : t.dt.day,
                            'week' : t.dt.week,
                            'month' : t.dt.month,
                            'year' : t.dt.year,
                            'weekday' : t.dt.weekday})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    

    # load user table
    user_df = df[['userId','firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records

    # define empty list to dump songplays data
    songplays_dump_data = list()

    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # define songplays records
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId,row.location, row.userAgent)
        
        # append row data to songplays_dump_data_list
        songplays_dump_data.append(songplay_data)

    # define a pandas dataframe ans write CSV to a StrinIO file objetc
    df_songplays = pd.DataFrame(songplays_dump_data, columns=['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id','location', 'user_agent'])
    f = StringIO()
    df_songplays.to_csv(f,header=None, index=None, sep=',')

    # copy dumped data from StringIO to songplays table
    f.seek(0)
    cur.copy_expert(songplay_table_copy, f)


def process_data(cur, conn, filepath, func):
    """This function gets all files matching extension from directory and process them using the function passed as argument

    :param cur: Database Cursor
    :type cur: psycopg2.extensions.cursor
    :param conn: Connection to existing Postgres DB using psycopg2
    :type conn: psycopg2.extensions.connection
    :param filepath: folder path where to parse exiting JSON files for LOGS and SONGS information
    :type filepath: str
    :param func: processing function for songs files of logs file 
    :type func: function
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()