# spotify-listening-statistics-downloader
A program that automatically queries the Spotify API and downloads the user's last 50 songs and adds them to a PostgreSQL database.

## Usage
When using this program, it can either be ran using a Docker container or as a standalone Python application.
Using the docker container requires an additional one time step of running the app out of a container locally and getting
the tokens for Spotify. This due to Spotipy needing to do OAuth authentication. After running the app outside of the container,
the .cache-username file has to be passed as a volume to the Docker container. After that the container will run normally.
Whenever the token needs to refresh itself, it'll do this automatically.

## Notes
This program is more of a small personal project. A lot of values are hardcoded, including the database address. Still
my code is available to serve as a guide or modify for your own personal use.