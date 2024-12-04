from django.shortcuts import render
from django.db import connection
from plugins.spot_api import SpotifyAPI
from dotenv import load_dotenv
import os


def load_home(request):

    return render(request, 'home/home.html')


def track_playlist(request):

    # load_dotenv()

    spot_api = SpotifyAPI(os.getenv('SPOTIFY_CLIENT_ID'), os.getenv('SPOTIFY_CLIENT_SECRET'))

    query = request.GET.get('q', '')

    if query:

        plist_search_result = spot_api.search_spotify(query, search_type="playlist")  # returns JSON response
        plist_id = plist_search_result['playlists']['items'][0]['id']
        plist_name = plist_search_result['playlists']['items'][0]['name']
        playlist_data = (plist_id, plist_name)

        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO playlist_dim (playlist_spotify_id, playlist_name) VALUES (%s, %s)",
                           playlist_data)

        return render(request, 'track-playlist.html', {'query': query, 'results': [playlist_data]})


