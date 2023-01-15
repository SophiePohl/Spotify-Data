from bs4 import BeautifulSoup
import urllib.error, urllib.parse, urllib.request
import ssl
import sqlite3
import json

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('spotify_data')
c = conn.cursor()

# Delete tables if needed
c.execute('DROP TABLE IF EXISTS tracks ')
c.execute('DROP TABLE IF EXISTS artist ')
c.execute('DROP TABLE IF EXISTS stream ')

# Create tables
c.execute( '''
    CREATE TABLE IF NOT EXISTS artist
	([artist_id] INTEGER PRIMARY KEY,
	[artist_name] TEXT)'''
)
c.execute('''
    CREATE TABLE IF NOT EXISTS track
	([track_id] INTEGER PRIMARY KEY,
	[track_name] TEXT,
	[artist_id] TEXT)'''
)
c.execute('''
    CREATE TABLE IF NOT EXISTS stream
	([stream_id] INTEGER PRIMARY KEY,
	[stream_time] INTEGER,
	[stream_ms_played] INTEGER,
	[track_id] INTEGER)'''
)
conn.commit()

# Open data
fname = 'StreamingHistory3.json'
file = open(fname, encoding='cp437')
json_data = json.load(file)


# Insert data into database
index = 0
for entry in json_data:

    number = json_data[index]
    artist_name = number["artistName"]
    track_name = number["trackName"]
    stream_ms_played = number["msPlayed"]
    index += 1

    c.execute('''INSERT INTO artist (artist_name)
        VALUES ( ? )''', ( artist_name, ) )
    c.execute('SELECT artist_id FROM artist WHERE artist_name = ? ', (artist_name, ))
    artist_id = c.fetchone()[0]

    c.execute('''INSERT INTO track (track_name, artist_id)
        VALUES ( ?, ? )''', ( track_name, artist_id, ))
    c.execute('SELECT track_id FROM track WHERE track_name = ? ', (track_name, ))
    track_id = c.fetchone()[0]

    c.execute('''INSERT INTO stream (stream_ms_played, track_id)
            VALUES ( ?, ? )''', ( stream_ms_played, track_id, ) )

    conn.commit()

