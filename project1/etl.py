import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from io import StringIO


def process_data(cur, conn, filepath, func):
    '''Iterates over all data files, processing and inserting into database.'''

    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # Get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def process_song_file(cur, filepath):
    '''Reads in given song file
       and inserts song/artist information into database
    '''

    # Open song file
    df = pd.read_json(filepath, lines=True)

    _insert_songs(cur, df)
    _insert_artists(cur, df)


def process_log_file(cur, filepath):
    # Open log file
    df = pd.read_json(filepath, lines=True)

    # Filter by NextSong action
    df = df[df['page'] == 'NextSong']

    _insert_time(cur, df)
    _insert_users(cur, df)
    _insert_songplays(cur, df)


def _insert_songs(cur, df):
    '''Prepare & insert songs: drop duplicates,
       and select only relevant columns
    '''

    song_df = df.drop_duplicates(subset='song_id')
    song_df = song_df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_df.values.tolist()
    for song in song_data:
        cur.execute(song_table_insert, song)


def _insert_artists(cur, df):
    '''Prepare & insert artists: drop duplicates,
       and select only relevant columns
    '''

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


def _insert_time(cur, df):
    '''Prepare and insert time rows'''

    # Convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # Insert time data records
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


def _insert_users(cur, df):
    '''Prepare and insert user rows'''

    # Filter for the columns in the user table,
    # and drop duplicates based on unique userId
    user_df = df[[
        'userId',
        'firstName',
        'lastName',
        'gender',
        'level'
    ]].drop_duplicates(subset='userId')

    # Insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)


def _insert_songplays(cur, df):
    '''Prepare and insert songplays rows.
       Strategy:
       1. Aggregate songplays info into Python list.
       2. Convert list to csv string
       3. use PostGreSQL COPY to bulk insert rows.
    '''

    # Aggregating songplays
    songplays_list = []
    for index, row in df.iterrows():

        # Get songid and artistid from song and artist tables
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
        songplays_list.append(songplay_data)

    # Convert to csv string
    column_labels = [
        'start_time',
        'user_id',
        'level',
        'song_id',
        'artist_id',
        'session_id',
        'location',
        'user_agent'
    ]
    songplays_df = pd.DataFrame(songplays_list, columns=column_labels)
    songplays_csv = songplays_df.to_csv(header=False, index=False, sep='\t')

    # # COPY into DB
    cur.copy_from(
        StringIO(songplays_csv),
        'songplays',
        columns=column_labels,
        sep='\t'
    )


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=udacity_project2")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
