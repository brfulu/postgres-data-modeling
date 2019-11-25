import os
import glob
import psycopg2
import pandas as pd
from scripts.sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # Round the duration to the closest second
    df['duration'] = round(df['duration'])

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = tuple(song_data.values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = tuple(artist_data.values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [(round(dt.timestamp()), dt.hour, dt.day, dt.week, dt.month, dt.year, dt.day_name()) for dt in t]
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df[user_df['userId'].astype(bool)]
    user_df = user_df.drop_duplicates(subset='userId')

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get song_id and artist_id from song and artist tables
        cur.execute(song_select, (row.song, row.artist, round(row.length)))
        result = cur.fetchone()
        (song_id, artist_id) = (result if result else (None, None))

        # uncomment if we only want records with valid song_id and artist_id
        # if song_id is None or artist_id is None:
        #     continue

        # insert songplay record
        songplay_data = (round(row.ts / 1000.0), row.userId, row.level, song_id, \
                         artist_id, row.sessionId, row.location, row.userAgent)
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='../data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='../data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
