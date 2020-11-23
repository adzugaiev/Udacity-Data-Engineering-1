import os
import glob
import psycopg2
import pandas as pd
from io import StringIO
from sql_queries import *

def get_files(filepath, filemask = '*.*', exclude = ''):
    '''
    Returns the list of files under a given filepath, mask, and excluding specified system subfolders.

            Parameters:
                    filepath (str): Path to start with and look into subfolders;
                    filemask (str): Files to look at, e.g. "*.json";
                    exclude  (str): Subfolder(s) name (or tuple) to exclude from the list.

            Returns:
                    all_files (list): A list of files matching the specified conditions.
    '''    
    all_files = []
    for root, dirs, filez in os.walk(filepath): # 'files' looks unsafe here because of the same local variable
        if exclude and root.endswith(exclude):
            files = []
        else:
            files = glob.glob(os.path.join(root, filemask))

        for f in files:
            all_files.append(os.path.abspath(f))
    
    return all_files

def copy_from_stringio(conn, df, table, which_columns = None):
    '''
    Save the dataframe in memory and use copy_from() to copy it to the table.
    based on https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/

    Parameters:
        conn (connection): From psycopg2.connect();
        df (DataFrame): DataFrame to be copied into the table;
        table (str): Subfolder(s) name (or tuple) to exclude from the list;
        which_columns (iterable): Column names, use to omit serial or defaulting attributes. If not specified, it is assumed that the entire table matches the file structure.
    '''    
    buffer = StringIO()
    df.to_csv(buffer, index = False, header = False, doublequote = False, sep = '\t', na_rep = 'NULL', escapechar = '"')
    buffer.seek(0)
    
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep = "\t", null = 'NULL', columns = which_columns)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, typ = 'series')

    # insert song record
    song_data = (df['song_id'], df['title'], df['artist_id'], df['year'], df['duration'])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = (df['artist_id'], df['artist_name'], df['artist_location'], df['artist_latitude'], df['artist_longitude'])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df.page.eq('NextSong')]

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    # Level is not a permanent attribute of a user, I think it's incorrect to set this dimension based on the log_data.
    # I can include timestamp to users_df and sort it descending, so at least we have the latest mentioned level of a user.
    user_df = df[["ts", "userId", "firstName", "lastName", "gender", "level"]].sort_values(by = ["ts"], ascending = False)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row[1:]) #[1:] excludes the timestamp

    # insert songplay records
    songplay_cols = ['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent']
    songplay_df = pd.DataFrame(columns = songplay_cols)

    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, str(row.length)))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_row = [pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent.strip('"')]
        songplay_df = songplay_df.append(dict(zip(songplay_cols, songplay_row)), ignore_index=True)

    copy_from_stringio(cur.connection, songplay_df, 'songplays', songplay_cols)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = get_files(filepath, '*.json', exclude = '.ipynb_checkpoints')

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