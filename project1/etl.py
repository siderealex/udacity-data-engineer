import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # Prepare & insert songs: drop duplicates, and select only relevant columns
    song_df = df.drop_duplicates(subset='song_id')
    song_df = song_df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_df.values.tolist()
    for song in song_data:
        cur.execute(song_table_insert, song)

    # Prepare & insert artists: drop duplicates,
    # and select only relevant columns
    artist_df = df.drop_duplicates(subset='artist_id')
    artist_df = artist_df[[
        'artist_id',
        'artist_name',
        'artist_location',
        'artist_latitude',
        'artist_longitude'
    ]]
    artist_data = artist_df.values.tolist()
    for artist in artist_data:
        cur.execute(artist_table_insert, artist)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [
        t,
        t.dt.hour,
        t.dt.day,
        t.dt.week,
        t.dt.month,
        t.dt.year,
        t.dt.weekday
    ]
    column_labels = [
        'start_time',
        'hour',
        'day',
        'week',
        'month',
        'year',
        'weekday'
    ]

    # Create rows for time table
    time_list = []
    for n in range(len(time_data[0].index)):
        i = time_data[0].index[n]
        time_dict = {
            'start_time': time_data[0][i],
            'hour': time_data[1][i].item(),
            'day': time_data[2][i].item(),
            'week': time_data[3][i].item(),
            'month': time_data[4][i].item(),
            'year': time_data[5][i].item(),
            'weekday': time_data[6][i].item()
        }

    time_list.append(time_dict)

    for row in time_list:
        cur.execute(time_table_insert, list(row.values()))
    # time_df =

    # for i, row in time_df.iterrows():
    #     cur.execute(time_table_insert, list(row))

    # Filter for the columns in the user table,
    # and drop duplicates based on unique userId
    user_df = df[[
        'userId',
        'firstName',
        'lastName',
        'gender',
        'level'
    ]].drop_duplicates(subset='userId')

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        song_id, artist_id = results if results else None, None

        # Insert songplay record
        songplay_data = (
            pd.to_datetime(row['ts'], unit='ms'),
            row['userId'],
            row['level'],
            song_id,
            artist_id,
            row['sessionId'],
            row['location'],
            row['userAgent']
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=udacity_project2")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
