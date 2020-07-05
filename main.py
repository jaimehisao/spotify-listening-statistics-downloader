import sys
import os
import spotipy
import pprint
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util
import psycopg2
import types
import uuid
import schedule
import time
from dotenv import load_dotenv
import sentry_sdk
import pymongo

load_dotenv()

sentry_id = os.getenv("SENTRY_SDK")
sentry_sdk.init(sentry_id)


def current_user_recently_played(self, limit=1):
    return self._get('me/player/recently-played', limit=limit)


def add_artist_if_non_existent(artist, conn, cursor):
    artist_name_track = str(artist['name'])
    artist_id_track = str(artist['id'])
    artist_uri_track = str(artist['uri'])

    # Checks if the artist has been added to the DB
    cursor.execute('SELECT * FROM artists WHERE id = %s', (artist_id_track,))
    if len(cursor.fetchall()) == 0:
        # Add the artist to the DB since it does not exist yet.
        cursor.execute('INSERT INTO artists(id, name, uri) VALUES (%s, %s, %s)',
                       (artist_id_track, artist_name_track, artist_uri_track))
        print('Added a new artist: ' + artist_name_track)
    conn.commit()


def add_album_if_non_existent(album, conn, cursor):
    # Album Information
    album_id = str(album['id'])
    album_name = str(album['name'])
    album_release_date = str(album['release_date'])
    album_release_date_precision = str(album['release_date_precision'])
    album_total_tracks = str(album['total_tracks'])
    album_uri = str(album['uri'])

    if album_release_date_precision == 'year':
        album_release_date = album_release_date + '-01-01 00:00:00.000000'

    if album_release_date_precision == 'month':
        album_release_date = album_release_date + '-01 00:00:00.000000'

    cursor.execute('INSERT INTO albums(id, name, release_date, release_date_precision, total_tracks, '
                   'uri) VALUES(%s, %s, %s, %s, %s, %s)',
                   (album_id, album_name, album_release_date, album_release_date_precision,
                    album_total_tracks, album_uri))
    print('Added a new album: ' + album_name)

    # Add artist information from Album
    for artist in album['artists']:
        # Checks if the artist has been added to the DB
        cursor.execute('SELECT * FROM artists WHERE id = %s', (str(artist['id']),))
        if len(cursor.fetchall()) == 0:
            add_artist_if_non_existent(artist, conn, cursor)
            cursor.execute('INSERT INTO album_artist(album_id, artist_id) VALUES (%s, %s)',
                           (album_id, str(artist['id'])))
            print('Added a new artist: ' + str(artist['name']))
            conn.commit()
    conn.commit()


def add_track_if_non_existant(track, conn, cursor):
    # Add artist information from Track
    for artist in track['artists']:
        # Checks if the artist has been added to the DB
        cursor.execute('SELECT * FROM artists WHERE id = %s', (str(artist['id']),))
        if len(cursor.fetchall()) == 0:
            add_artist_if_non_existent(artist, conn, cursor)

    # Track Information
    track_name = str(track['name'])
    track_id = str(track['id'])
    track_popularity = str(track['popularity'])
    track_duration_ms = str(track['duration_ms'])
    track_explicit = False
    if str(track['explicit']):
        track_explicit = True

    cursor.execute('INSERT INTO track(id, name, duration_ms, explicit, popularity) '
                   'VALUES (%s, %s, %s, %s, %s) ',
                   (track_id, track_name, track_duration_ms, track_explicit, track_popularity))
    print('Added new track: ' + track_name)
    cursor.execute('INSERT INTO track_artist(track_id, artist_id) VALUES (%s, %s)',
                   (track_id, str(artist['id'])))


def query():
    # Log into Postgres database
    conn = psycopg2.connect(user="spotifyu",
                            password="spotifyT343432434@",
                            host="services.hisao.org",
                            port="5432",
                            database="spotify")
    cursor = conn.cursor()

    print(conn)

    client_id = os.getenv("SPOTIPY_CLIENT_ID")
    client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")
    redirect = os.getenv("SPOTIPY_REDIRECT_URI")

    # Spotify Login Object
    scope = 'user-library-read user-read-recently-played'
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id,
                                                          client_secret=client_secret)
    token = util.prompt_for_user_token("jaimehisao", scope)
    # sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    # sp = spotipy.client.Spotify(auth = token, client_credentials_manager=client_credentials_manager)
    sp = spotipy.Spotify(auth=token)
    print('HERE')
    # Insert to PostgreSQL database
    sp.current_user_recently_played = types.MethodType(current_user_recently_played, sp)
    recent = sp.current_user_recently_played(limit=50)

    for i, item in enumerate(recent['items']):
        # Checks if the album has been added to the DB
        cursor.execute('SELECT id FROM albums WHERE id = %s', (str(item['track']['album']['id']),))
        if len(cursor.fetchall()) == 0:
            add_album_if_non_existent(item['track']['album'], conn, cursor)

        # Check if the track information has been added to the DB
        cursor.execute('SELECT id FROM track WHERE id = %s', (str(item['track']['id']),))
        if len(cursor.fetchall()) == 0:
            # Add the track information since it's not in the DB
            add_track_if_non_existant(item['track'], conn, cursor)

        # Check if the record of listening to the track exists in the DB
        cursor.execute('SELECT track_id FROM track_history WHERE track_id = %s  and played_at = %s',
                       (str(item['track']['id']), str(item['played_at'])))
        if len(cursor.fetchall()) == 0:
            # If there is no track, add it, else move on
            cursor.execute('INSERT INTO track_history(track_id, played_at) VALUES (%s, %s)',
                           (str(item['track']['id']), str(item['played_at'])))
            print('Added new track history : ' + str(item['track']['name']) + ' ' + str(item['played_at']))
    conn.commit()


def mongo_to_postgres():
    # Log into Postgres database
    conn = psycopg2.connect(user="spotifyu",
                            password="spotifyT343432434@",
                            host="services.hisao.org",
                            port="5432",
                            database="spotify")
    cursor = conn.cursor()

    mongoClient = pymongo.MongoClient("mongodb://services.hisao.org:27017/")

    # Select MongoDB Database and Collection
    mydb = mongoClient["music"]
    statsCol = mydb["listeningStats"]

    recordsRemoved = 0

    conn.rollback()

    tracks = statsCol.find({})

    for track in tracks:
        cursor.execute('SELECT * FROM track WHERE id = %s', (track['trackId'],))
        if len(cursor.fetchall()) != 0:
            recordsRemoved += 1
            print(track['trackId'] + ' ' + track['timestamp'])
            cursor.execute('SELECT * FROM track_history WHERE track_id = %s and played_at = %s',
                           (track['trackId'], track['timestamp']))
            if len(cursor.fetchall()) == 0:
                cursor.execute('INSERT INTO track_history(track_id, played_at) VALUES (%s, %s)',
                               (track['trackId'], track['timestamp']))
            statsCol.delete_one({'_id': track['_id']})
            conn.commit()
            print(recordsRemoved)
    conn.close()
    mongoClient.close()

print('Starting Spotify Downloader')
print("Hello? Anyone there?", flush=True)
schedule.every().hour.at(":16").do(query)
schedule.every().hour.at(":02").do(mongo_to_postgres)

while True:
    schedule.run_pending()
    time.sleep(1)
