import logging
import time
import requests


class SpotifyAPI:

    """
    Class that uses the Spotify Web API to retrieve playlist, artist, and track metadata
    """

    def __init__(self, client_id, client_secret):

        self.client_id = client_id  # Spotify client ID
        self.client_secret = client_secret  # Spotify client secret
        self.headers = self.get_authorization_header()  # grab access token / authorization header for future requests

    def get_access_token(self):

        """
        Obtains API access token
        """

        url = 'https://accounts.spotify.com/api/token'
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret}

        try:
            response = requests.post(url=url,
                                     headers=headers,
                                     data=data)
            response.raise_for_status()
            json_response = response.json()
            logging.info("Successful API request - get access token")
            return json_response.get("access_token")
        except requests.exceptions.RequestException as err:
            logging.info(f"UNSUCCESSFUL API REQUEST - GET ACCESS TOKEN:\n {err}")

    def get_authorization_header(self):

        token = self.get_access_token()
        return {"Authorization": f"Bearer {token}"}

    def search_spotify(self, search_term, search_type="playlist"):

        """
        Searches the Spotify app for an item of interest

        :param search_term: the keyword you'd like to search
        :param search_type: specifies what you're looking for (playlist, artist, track, etc.)
        :return: json object containing data associated with the 1st result returned by the search
        """

        url = 'https://api.spotify.com/v1/search'
        headers = self.headers
        params = {'q': search_term,
                  'type': search_type,
                  'limit': 1}  # only return the first search result
        try:
            response = requests.get(url=url,
                                    headers=headers,
                                    params=params)
            response.raise_for_status()
            json_response = response.json()
            logging.info(f"Successful API request - search spotify, term = {search_term}, search type = {search_type}")
            return json_response
        except requests.exceptions.RequestException as err:
            logging.exception(f"UNSUCCESSFUL API REQUEST - SEARCH SPOTIFY:\n{err}")

    def get_playlist_items(self, playlist_id, offset_val):

        """
        Grabs the data for up to 50 items (tracks) from a playlist

        :param playlist_id: the unique ID for a Spotify playlist
        :param offset_val: the index value of the first playlist item you'd like to return
        :return: json object containing metadata for up to 50 tracks from a playlist
        """

        url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
        headers = self.headers
        params = {'offset': offset_val}

        try:
            response = requests.get(url=url,
                                    headers=headers,
                                    params=params)
            response.raise_for_status()
            json_response = response.json()
            logging.info(f"Successful API request - get playlist items, plist id = {playlist_id}, off = {offset_val}")
            return json_response
        except requests.exceptions.RequestException as err:
            logging.exception(f"UNSUCCESSFUL API REQUEST - GET PLAYLIST ITEMS: \n{err}")

    def get_all_playlist_items(self, plist_id):

        """
        Grabs and organizes the data for ALL tracks on a Spotify playlist of interest

        :param plist_id: unique ID for a Spotify playlist
        :return: 4 lists containing cleaned metadata for each track. Each list is comprised of data that will later be
        stored in separate tables within a data warehouse.
        """

        items_processed = 0
        off = 0  # offset value to be passed into the API request
        all_track_data = []
        all_artist_data = []
        all_track_artist_data = []
        all_track_playlist_data = []

        while True:

            json_response = self.get_playlist_items(plist_id, off)

            playlist_total_items = json_response['total']
            request_total_items = len(json_response['items'])
            json_clean = [json_response['items'][i] for i in range(len(json_response['items'])) if
                          json_response['items'][i]['track'] is not None]

            # 'track' data
            track_ids = [i['track']['id'] for i in json_clean]
            track_names = [i['track']['name'] for i in json_clean]
            track_durations = [i['track']['duration_ms'] for i in json_clean]
            track_isrcs = [i['track']['external_ids']['isrc'] for i in json_clean]
            track_album_positions = [i['track']['track_number'] for i in json_clean]
            album_ids = [i['track']['album']['id'] for i in json_clean]
            album_names = [i['track']['album']['name'] for i in json_clean]
            album_release_dates = [
                i['track']['album']['release_date'] + '-01-01' if len(i['track']['album']['release_date']) == 4 else
                i['track']['album']['release_date'] for i in json_clean]  # handle irregular date formats
            album_types = [i['track']['album']['album_type'] for i in json_clean]
            album_total_tracks = [i['track']['album']['total_tracks'] for i in json_clean]
            track_data = list(zip(track_ids, track_names, track_durations, track_isrcs, track_album_positions,
                                  album_ids, album_names, album_release_dates, album_types, album_total_tracks))

            # 'track_playlist' data
            track_plist_positions = [off + i + 1 for i in range(len(json_clean))]
            track_playlist_data = track_plist_positions.copy()

            # 'artist' data
            artist_ids = [j['id'] for i in json_clean for j in i['track']['artists']]
            artist_names = [j['name'] for i in json_clean for j in i['track']['artists']]
            artist_data = list(zip(artist_ids, artist_names))

            # 'track_artist' data
            track_artist_data = [(track['track']['id'], artist['id']) for track in json_clean for
                                 artist in track['track']['artists']]

            for i in range(len(track_data)):
                all_track_data.append(track_data[i])
                all_track_playlist_data.append(track_playlist_data[i])

            for artist in artist_data:
                all_artist_data.append(artist)

            for track_artist in track_artist_data:
                all_track_artist_data.append(track_artist)

            items_processed += request_total_items

            if items_processed == playlist_total_items:
                logging.info(f"All {playlist_total_items} tracks processed successfully, plist id = {plist_id}")
                break

            off += request_total_items
            time.sleep(1)

        all_artist_data = list(set(all_artist_data))  # remove potential duplicate artists

        return all_track_data, all_artist_data, all_track_artist_data, all_track_playlist_data

    def prepare_playlist_data(self, playlist_id):

        """
        Obtains and organizes additional data related to each track on a playlist of interest. Calls both the
        'get_all_playlist_items' + 'process_all_ids' helper methods to do this.

        :param playlist_id: unique Spotify playlist ID
        :return: four updated lists that contain information that will be stored later in a data warehouse
        """

        track_data, artist_data, track_artist_data, track_playlist_data = self.get_all_playlist_items(playlist_id)

        # update 'track' data
        album_ids = [i[5] for i in track_data]
        more_track_data = self.process_all_ids(album_ids, id_type="album")  # grab tracks' album UPC + label info
        all_track_data = [i + j for i, j in zip(track_data, more_track_data)]

        # update 'track_playlist' data
        track_ids = [i[0] for i in track_data]
        playlist_ids = [playlist_id for i in range(len(track_ids))]
        track_playlist_playlist_positions = track_playlist_data
        all_track_playlist_data = list(zip(track_ids, playlist_ids, track_playlist_playlist_positions))

        # update 'track_artist' data
        ta_track_ids, ta_artist_ids = zip(*track_artist_data)  # rename to Spotify IDs
        track_artist_track_ids = list(ta_track_ids)
        track_artist_artist_ids = list(ta_artist_ids)
        all_track_artist_data = list(zip(track_artist_track_ids, track_artist_artist_ids))

        return all_track_data, artist_data, all_track_artist_data, all_track_playlist_data

    def get_ids_data(self, id_list, id_type="tracks"):

        """
        Grabs additional data related to a list of Spotify IDs

        :param id_list: String containing a comma-separated list of unique Spotify IDs. Can provide up to 50 track or
        artist IDs at a time. Can supply a maximum of 20 album IDs in a single request.
        :param id_type: Specifies whether the list contains track, album, or artist IDs
        :return: Json object containing data associated with list of IDs
        """

        url = f"https://api.spotify.com/v1/{id_type}"
        headers = self.headers
        params = {'ids': id_list}

        try:
            response = requests.get(url=url,
                                    headers=headers,
                                    params=params)
            response.raise_for_status()
            json_response = response.json()
            logging.info("Successful API request - get ids data")
            return json_response
        except requests.exceptions.RequestException as err:
            logging.exception(f"API REQUEST ERROR - get ids data:\n{err}")

    def organize_album_data(self, album_ids):

        """
        Organizes album metadata that's returned after calling the 'get_ids_data' method

        :param album_ids: list of unique album IDs
        :return: list containing cleaned album metadata associated with tracks of interest
        """

        logging.info("Getting the tracks' UPC and label information...")
        json_response = self.get_ids_data(album_ids, id_type="albums")
        albums_arr = json_response['albums']
        total_albums = len(albums_arr)

        album_upcs = [albums_arr[i]['external_ids']['upc'] for i in range(total_albums)]
        album_labels = [albums_arr[i]['label'] for i in range(total_albums)]
        organized_album_data = list(zip(album_upcs, album_labels))

        return organized_album_data

    def get_track_pop_scores(self, track_ids):

        """
        Grabs the current popularity scores associated with a list of Spotify tracks

        :param track_ids: list of Spotify track IDs
        :return: list of popularity scores for each of the provided tracks
        """

        logging.info("Getting track popularity scores...")
        json_response = self.get_ids_data(track_ids, id_type="tracks")
        track_arr = json_response['tracks']
        total_tracks = len(track_arr)

        track_pop_scores = [track_arr[i]['popularity'] for i in range(total_tracks)]

        return track_pop_scores

    def get_artist_metrics(self, artist_ids):

        """
        Organizes Spotify performance metrics associated with a list of artists

        :param artist_ids: list of artist IDs
        :return: list containing artists' follower counts and popularity scores
        """

        logging.info("Getting artists' popularity metrics...")
        json_response = self.get_ids_data(artist_ids, id_type="artists")
        artists_arr = json_response['artists']
        total_artists = len(artists_arr)

        artist_followers = [artists_arr[i]['followers']['total'] for i in range(total_artists)]
        artist_pop_scores = [artists_arr[i]['popularity'] for i in range(total_artists)]
        artist_pop_metrics = list(zip(artist_pop_scores, artist_followers))

        return artist_pop_metrics

    def process_all_ids(self, id_list, id_type="album"):

        """
        Grabs performance metrics or metadata for a larger volume of albums, tracks, or artists

        :param id_list: list of unique Spotify IDs
        :param id_type: string specifying whether the list contains album, artist, or track IDs
        :return:
        """

        total_ids = len(id_list)
        all_responses = []

        i = 0

        if id_type == "album":
            j = 20
        else:
            j = 50

        while i <= (total_ids - 1):

            sub_list = id_list[i: j]
            ids_str = ",".join(sub_list)

            if id_type == "album":
                data = self.organize_album_data(ids_str)
            elif id_type == "artist":
                data = self.get_artist_metrics(ids_str)
            else:
                data = self.get_track_pop_scores(ids_str)

            for element in data:
                all_responses.append(element)

            i = j
            if id_type == "album":
                j += 20
            else:
                j += 50

            time.sleep(1)

        return all_responses
