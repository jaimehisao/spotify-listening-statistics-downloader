import os
import pprint

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util
import psycopg2
import types
import schedule
import time
from dotenv import load_dotenv
import sentry_sdk
import pymongo

load_dotenv()

sentry_id = os.getenv("SENTRY_SDK")
sentry_sdk.init(sentry_id)


def current_user_recently_played(self, limit=1):
    return self._get("me/player/recently-played", limit=limit)


def add_artist_if_non_existent(artist, conn, cursor) -> None:
    """
    Adds an artist object to the database if it didn't exist. Since its a requirement to add an album and track.
    :param artist: Artist JSON from Spotify API (only the artist part)
    :param conn: PostgreSQL Connection Object
    :param cursor: PostgreSQL Cursor Object
    :return: None
    """
    artist_name_track = str(artist["name"])
    artist_id_track = str(artist["id"])
    artist_uri_track = str(artist["uri"])

    # Checks if the artist has been added to the DB
    cursor.execute("SELECT * FROM artists WHERE id = %s", (artist_id_track,))
    if len(cursor.fetchall()) == 0:
        # Add the artist to the DB since it does not exist yet.
        cursor.execute(
            "INSERT INTO artists(id, name, uri) VALUES (%s, %s, %s)",
            (artist_id_track, artist_name_track, artist_uri_track),
        )
        print("Added a new artist: " + artist_name_track)
    conn.commit()


def add_album_if_non_existent(album, conn, cursor) -> None:
    """
    Adds an album object to the database if it didn't exist. Since its a requirement to add a track.
    :param album: Album JSON from Spotify API (only the album part)
    :param conn: PostgreSQL Connection Object
    :param cursor: PostgreSQL Cursor Object
    :return: None
    """
    # Album Information
    album_id = str(album["id"])
    album_name = str(album["name"])
    album_release_date = str(album["release_date"])
    album_release_date_precision = str(album["release_date_precision"])
    album_total_tracks = str(album["total_tracks"])
    album_uri = str(album["uri"])

    if album_release_date_precision == "year":
        album_release_date = album_release_date + "-01-01 00:00:00.000000"

    if album_release_date_precision == "month":
        album_release_date = album_release_date + "-01 00:00:00.000000"

    cursor.execute(
        "INSERT INTO albums(id, name, release_date, release_date_precision, total_tracks, "
        "uri) VALUES(%s, %s, %s, %s, %s, %s)",
        (
            album_id,
            album_name,
            album_release_date,
            album_release_date_precision,
            album_total_tracks,
            album_uri,
        ),
    )
    print("Added a new album: " + album_name)

    # Add artist information from Album
    for artist in album["artists"]:
        # Checks if the artist has been added to the DB
        cursor.execute("SELECT * FROM artists WHERE id = %s", (str(artist["id"]),))
        if len(cursor.fetchall()) == 0:
            add_artist_if_non_existent(artist, conn, cursor)
            cursor.execute(
                "INSERT INTO album_artist(album_id, artist_id) VALUES (%s, %s)",
                (album_id, str(artist["id"])),
            )
            print("Added a new artist: " + str(artist["name"]))
            conn.commit()
    conn.commit()


def add_track_if_non_existent(track, conn, cursor) -> None:
    """
    Adds a track information to the database if non existant. Requirement for adding a track_history
    :param track: Track JSON object from Spotify API
    :param conn: PostgreSQL connection object
    :param cursor: PostgreSQL cursor object
    :return: None
    """
    # Add artist information from Track
    for artist in track["artists"]:
        # Checks if the artist has been added to the DB
        cursor.execute("SELECT * FROM artists WHERE id = %s", (str(artist["id"]),))
        if len(cursor.fetchall()) == 0:
            add_artist_if_non_existent(artist, conn, cursor)

    # Track Information
    track_name = str(track["name"])
    track_id = str(track["id"])
    track_popularity = str(track["popularity"])
    track_duration_ms = str(track["duration_ms"])
    track_album_id = str(track["album"]["id"])
    track_explicit = False
    if str(track["explicit"]) == "True":
        track_explicit = True

    cursor.execute(
        "INSERT INTO track(id, name, duration_ms, explicit, popularity, album_id) "
        "VALUES (%s, %s, %s, %s, %s, %s) ",
        (
            track_id,
            track_name,
            track_duration_ms,
            track_explicit,
            track_popularity,
            track_album_id,
        ),
    )
    try:
        print("Added new track: " + track_name)
    except UnicodeEncodeError:
        print("Error when printing debug output")
    cursor.execute(
        "INSERT INTO track_artist(track_id, artist_id) VALUES (%s, %s)",
        (track_id, str(artist["id"])),
        )

def query() -> None:
    """
    Method that calls the Spotify API and retrieves the user's last 50 songs and adds them to the database in Postgres.
    :return: None
    """
    # Log into Postgres database
    conn = psycopg2.connect(
        user="apps",
        password="apps_for_postgres_prod",
        host="100.105.109.22",
        port="5432",
        database="spotify",
    )
    cursor = conn.cursor()

    client_id = os.getenv("SPOTIPY_CLIENT_ID")
    client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")
    redirect = os.getenv("SPOTIPY_REDIRECT_URI")

    # Spotify Login Object
    scope = "user-library-read user-read-recently-played"
    token = util.prompt_for_user_token("jaimehisao", scope)
    sp = spotipy.Spotify(auth=token)
    sp.current_user_recently_played = types.MethodType(current_user_recently_played, sp)
    recent = sp.current_user_recently_played(limit=50)

    for i, item in enumerate(recent["items"]):
        # Checks if the album has been added to the DB
        cursor.execute(
            "SELECT id FROM albums WHERE id = %s", (str(item["track"]["album"]["id"]),)
        )
        if len(cursor.fetchall()) == 0:
            add_album_if_non_existent(item["track"]["album"], conn, cursor)

        # Check if the track information has been added to the DB
        cursor.execute(
            "SELECT id FROM track WHERE id = %s", (str(item["track"]["id"]),)
        )
        if len(cursor.fetchall()) == 0:
            # Add the track information since it's not in the DB
            add_track_if_non_existent(item["track"], conn, cursor)

        # Check if the record of listening to the track exists in the DB
        cursor.execute(
            "SELECT track_id FROM track_history WHERE track_id = %s  and played_at = %s",
            (str(item["track"]["id"]), str(item["played_at"])),
        )
        if len(cursor.fetchall()) == 0:
            # If there is no track, add it, else move on
            cursor.execute(
                "INSERT INTO track_history(track_id, played_at) VALUES (%s, %s)",
                (str(item["track"]["id"]), str(item["played_at"])),
            )
            try:
                print(
                    "Added new track history : "
                    + str(item["track"]["name"])
                    + " "
                    + item["played_at"]
                )
            except Exception as e:
                print(e)
                print("Error printing track history addition")
    print("Finished adding new track history", flush=True)
    conn.commit()


def mongo_to_postgres() -> None:
    """
    Method that moves songs from the previous MongoDB to the new PostgreSQL database. Taking into account the database
    constraints. As tracks are added to Postgres, this method scans MongoDB and adds that track history to the new DB
    and removes the records from the old database.
    :return: None
    """
    # Log into Postgres database
    conn = psycopg2.connect(
        user="apps",
        password="apps_for_postgres_prod",
        host="100.105.109.22",
        port="5432",
        database="spotify",
    )
    cursor = conn.cursor()

    mongoClient = pymongo.MongoClient("mongodb://100.105.109.22:27017/")

    # Select MongoDB Database and Collection
    mydb = mongoClient["music"]
    statsCol = mydb["listeningStats"]

    recordsRemoved = 0

    conn.rollback()

    tracks = statsCol.find({})

    for track in tracks:
        cursor.execute("SELECT * FROM track WHERE id = %s", (track["trackId"],))
        if len(cursor.fetchall()) != 0:
            recordsRemoved += 1
            print(track["trackId"] + " " + track["timestamp"])
            cursor.execute(
                "SELECT * FROM track_history WHERE track_id = %s and played_at = %s",
                (track["trackId"], track["timestamp"]),
            )
            if len(cursor.fetchall()) == 0:
                cursor.execute(
                    "INSERT INTO track_history(track_id, played_at) VALUES (%s, %s)",
                    (track["trackId"], track["timestamp"]),
                )
            statsCol.delete_one({"_id": track["_id"]})
            conn.commit()
            print(recordsRemoved)
    conn.close()
    mongoClient.close()
    print("Finished Mongo -> Postgres", flush=True)


print("Starting Spotify Downloader")
print("Hello? Anyone there?", flush=True)
query()