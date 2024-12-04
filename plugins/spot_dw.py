import logging
import time
import datetime
import pytz
import os
from dotenv import load_dotenv
from psycopg2 import IntegrityError
from spot_api import SpotifyAPI
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PlaylistDW:

    """
    Stores playlist data in a data warehouse
    """

    def __init__(self):

        # load_dotenv()

        self.hook = PostgresHook(os.getenv('POSTGRES_CONN_ID'))
        self.connection = self.hook.get_conn()  # connect to the dw
        self.cursor = self.connection.cursor()
        self.spot_api = SpotifyAPI(os.getenv('SPOTIFY_CLIENT_ID'),
                                   os.getenv('SPOTIFY_CLIENT_SECRET'))  # instantiate Spotify API object

    def create_tables(self):

        """
        Creates the data warehouse's fact and dimension tables

        """

        # create the dimensions
        create_date = """CREATE TABLE IF NOT EXISTS date_dim (
                            date_id char(8),
                            date DATE,
                            date_description varchar(17),
                            calendar_year char(4),
                            calendar_quarter SMALLINT,
                            calendar_month_num SMALLINT,
                            calendar_month_name varchar(9),
                            calendar_month_day_num SMALLINT,
                            day_of_week varchar(9),
                            constraint date_id_pk primary key (date_id)
                            )
                        """
        self.hook.run(create_date)

        create_playlist = """CREATE TABLE IF NOT EXISTS playlist_dim (
                                playlist_id SERIAL,
                                playlist_spotify_id char(22), 
                                playlist_name varchar(40), 
                                constraint plist_id_pk primary key (playlist_id),
                                constraint unique_playlist_spotify_id unique (playlist_spotify_id)
                                )
                            """
        self.hook.run(create_playlist)

        create_track = """CREATE TABLE IF NOT EXISTS track_dim (
                                track_id SERIAL,
                                track_spotify_id char(22),
                                track_name varchar(100),
                                track_duration_ms INTEGER,
                                track_isrc char(12),
                                track_album_position SMALLINT,
                                album_id char(22),
                                album_name varchar(100),
                                album_release_date DATE,
                                album_type varchar(20),
                                album_total_tracks SMALLINT,
                                album_upc varchar(20),
                                label_name varchar(100),
                                constraint track_id_pk primary key (track_id),
                                constraint unique_track_spotify_id unique (track_spotify_id)
                                )
                            """
        self.hook.run(create_track)

        create_artist = """CREATE TABLE IF NOT EXISTS artist_dim (
                                artist_id SERIAL,
                                artist_spotify_id char(22),
                                artist_name varchar(40),
                                constraint artist_id_pk primary key (artist_id),
                                constraint unique_artist_spotify_id unique (artist_spotify_id)
                                )
                        """
        self.hook.run(create_artist)

        # create the fact tables
        create_track_artist = """CREATE TABLE IF NOT EXISTS track_artist_fact (
                                        track_id INTEGER,
                                        artist_id INTEGER,
                                        date_id char(8),
                                        track_popularity SMALLINT,
                                        artist_popularity SMALLINT,
                                        artist_followers INT,
                                        constraint track_artist_pk primary key (track_id, artist_id, date_id),
                                        constraint track_artist_fk1 foreign key (track_id) references track_dim (track_id),
                                        constraint track_artist_fk2 foreign key (artist_id) references artist_dim (artist_id),
                                        constraint track_artist_fk3 foreign key (date_id) references date_dim (date_id))
                                    """
        self.hook.run(create_track_artist)

        create_track_playlist = """CREATE TABLE IF NOT EXISTS track_playlist_fact (
                                        track_id INTEGER,
                                        playlist_id INTEGER,
                                        date_id char(8),
                                        track_playlist_position SMALLINT,
                                        track_popularity SMALLINT,
                                        constraint track_playlist_pk primary key (track_id, playlist_id, date_id),
                                        constraint track_playlist_fk1 foreign key (track_id) references track_dim (track_id),
                                        constraint track_playlist_fk2 foreign key (playlist_id) references playlist_dim (playlist_id),
                                        constraint track_playlist_fk3 foreign key (date_id) references date_dim (date_id)
                                        )
                                    """
        self.hook.run(create_track_playlist)
        logging.info("Created the data warehouse's tables")

    def insert_record(self, insertion_statement, insertion_values, destination_table="date_dim"):

        """
        Inserts a new record into one of the data warehouse's tables

        :param insertion_statement: SQL insertion statement
        :param insertion_values: the row of values to be added to a table of interest
        :param destination_table: the fact or dimension table to be updated
        """

        try:
            self.hook.run(insertion_statement, parameters=insertion_values)
        except IntegrityError:
            logging.info(f"The following row is already in the {destination_table}:\n{insertion_values}")
        except Exception as e:
            logging.info(f"The following row was not added to the {destination_table}:\n{insertion_values}\n")
            logging.exception(f"ERROR TYPE - {e}")

    def extract_playlists_data(self, playlist_list):

        """
        Collects and cleans playlist data to be stored in the data warehouse

        :param playlist_list: list of Spotify playlist IDs
        :return: 4 lists containing cleaned data that will eventually be added to the data warehouse's tables
        """

        tracks = []
        artists = []
        track_artist_pairings = []
        track_playlist_pairings = []

        for playlist in playlist_list:

            track_data, artist_data, ta_data, tp_data = self.spot_api.prepare_playlist_data(playlist)

            tracks.append(track_data)
            artists.append(artist_data)
            track_artist_pairings.append(ta_data)
            track_playlist_pairings.append(tp_data)

            time.sleep(2)
            logging.info(f"\nFinished collecting initial playlist data for playlist with ID = {playlist}\n")

        logging.info(f"\nFinished collecting data for all tracked playlists\n")

        # flatten the lists
        tracks_flattened = [track for playlist in tracks for track in playlist]
        artists_flattened = [artist for playlist in artists for artist in playlist]
        track_artist_pairings_flattened = [tap for playlist in track_artist_pairings for tap in playlist]
        track_playlist_pairings_cleaned = [tpp for playlist in track_playlist_pairings for tpp in playlist]

        tracks_cleaned = list(set(tracks_flattened))
        artists_cleaned = list(set(artists_flattened))
        track_artist_pairings_cleaned = list(set(track_artist_pairings_flattened))

        return tracks_cleaned, artists_cleaned, track_artist_pairings_cleaned, track_playlist_pairings_cleaned

    def update_dimensions(self, track_data, artist_data):

        """
        Adds new rows to the dimension tables within the data warehouse

        :param track_data: list of cleaned track metadata that's been extracted from playlists of interest
        :param artist_data: list of cleaned artist metadata that's been extracted from playlists of interest
        """

        # update 'date_dim' table
        mountain_tz = pytz.timezone("America/Denver")
        todays_date = datetime.datetime.now(mountain_tz)
        calendar_year = todays_date.year
        calendar_month_num = f"{todays_date.month:02}"
        calendar_month_day_num = f"{todays_date.day:02}"
        date_full = f"{calendar_year}-{calendar_month_num}-{calendar_month_day_num}"
        date_id = date_full.replace("-", "")
        date_description = todays_date.strftime("%B %d, %Y")
        calendar_quarter = (todays_date.month - 1) // 3 + 1
        calendar_month_name = todays_date.strftime("%B")
        day_of_week = todays_date.strftime("%A")

        date_data = (date_id, date_full, date_description, calendar_year, calendar_quarter, calendar_month_num,
                     calendar_month_name, calendar_month_day_num, day_of_week)
        date_insert = """INSERT INTO date_dim (date_id, date, date_description, calendar_year, calendar_quarter, 
                                            calendar_month_num, calendar_month_name, calendar_month_day_num, 
                                            day_of_week)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
        self.insert_record(date_insert, date_data, destination_table="date_dim")
        logging.info("Added date information to 'date_dim' table")

        # update 'track_dim' table
        track_insert = """INSERT INTO track_dim (track_spotify_id, track_name, track_duration_ms, track_isrc, 
                                                track_album_position, album_id, album_name, album_release_date, 
                                                album_type, album_total_tracks, album_upc, label_name)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
        for track in track_data:
            self.insert_record(track_insert, track, destination_table="track_dim")
        logging.info(f"Added track data to 'track_dim' table")

        # update 'artist_dim' table
        artist_insert = "INSERT INTO artist_dim (artist_spotify_id, artist_name) VALUES (%s, %s)"
        for artist in artist_data:
            self.insert_record(artist_insert, artist, destination_table="artist_dim")
        logging.info(f"Added artist data to 'artist_dim' table")

    def organize_facts(self, new_track_artist_data, track_playlist_data):

        """
        Compiles all of the information needed for the data warehouse's fact tables. Combines existing
        playlist data with the dimension tables' foreign keys and the latest performance metrics for both new and
        existing tracks / artists that are already present inside the dw.

        :param track_artist_data: previously-extracted data that will be added to the 'track_artist_fact' table
        :param track_playlist_data: previously-extracted data that will be added to the 'track_playlist_fact' table
        :return: 2 lists of cleaned data that will be added to their respective fact tables in the dw
        """
        logging.info("Gathering and organizing additional data for both fact tables")

        # 'TRACK_ARTIST'

        # look up id info in the data warehouse that will be used as foreign keys in the fact tables
        track_id_query = "SELECT track_id, track_spotify_id from track_dim"
        self.cursor.execute(track_id_query)
        track_id_info = [track_info for track_info in self.cursor.fetchall()]

        artist_id_query = "SELECT artist_id, artist_spotify_id from artist_dim"
        self.cursor.execute(artist_id_query)
        artist_id_info = [artist_info for artist_info in self.cursor.fetchall()]

        mountain_tz = pytz.timezone("America/Denver")
        current_date = datetime.datetime.now(mountain_tz)
        date_values = f"{current_date.year}-{current_date.month:02}-{current_date.day:02}"
        date_query = """SELECT date_id
                            FROM date_dim
                            WHERE date = %s
                        """
        self.cursor.execute(date_query, (date_values,))
        date_foreign_key = self.cursor.fetchall()[0][0]

        # collect latest facts / measurements to add to 'track_artist_fact'
        track_ids = [i[0] for i in track_id_info]
        track_spotify_ids = [i[1] for i in track_id_info]
        track_pop_scores = self.spot_api.process_all_ids(track_spotify_ids, id_type="track")
        track_pop_dict = {i[0]: i[1] for i in zip(track_ids, track_pop_scores)}  # store in dict for future lookup

        artist_ids = [i[0] for i in artist_id_info]
        artist_spotify_ids = [i[1] for i in artist_id_info]
        artist_pop_metrics = self.spot_api.process_all_ids(artist_spotify_ids, id_type="artist")
        artist_pop_dict = {i[0]: i[1] for i in zip(artist_ids, artist_pop_metrics)}  # store pop + followers in dict

        # grab distinct artist / track pairings that are currently in 'track_artist_fact'
        track_artist_query = "SELECT DISTINCT track_id, artist_id from track_artist_fact"
        self.cursor.execute(track_artist_query)
        existing_track_artist_combinations = [ta_combo for ta_combo in self.cursor.fetchall()]
        # grab new artist / track pairings => use dicts look up the relevant ID info to add to 'track_artist_fact'
        track_id_dict = {track_id[1]: track_id[0] for track_id in track_id_info}
        artist_id_dict = {artist_id[1]: artist_id[0] for artist_id in artist_id_info}
        new_track_artist_combinations = [(track_id_dict[i[0]], artist_id_dict[i[1]]) for i in new_track_artist_data]
        unique_track_artist_combinations = list(set(existing_track_artist_combinations + new_track_artist_combinations))

        # combine the final data together before inserting into 'track_artist_fact'
        final_ta_track_ids = [i[0] for i in unique_track_artist_combinations]
        final_ta_artist_ids = [i[1] for i in unique_track_artist_combinations]
        final_ta_date_fks = [date_foreign_key for i in range(len(unique_track_artist_combinations))]
        final_ta_track_pops = [track_pop_dict[i] for i in final_ta_track_ids]  # KEY ERROR: 88
        final_ta_artist_pops = [artist_pop_dict[j][0] for j in final_ta_artist_ids]
        final_ta_artist_followers = [artist_pop_dict[j][1] for j in final_ta_artist_ids]
        final_track_artist_data = list(zip(final_ta_track_ids, final_ta_artist_ids, final_ta_date_fks,
                                           final_ta_track_pops, final_ta_artist_pops, final_ta_artist_followers))

        # 'TRACK_PLAYLIST'

        playlist_id_query = "SELECT playlist_id, playlist_spotify_id from playlist_dim"  # look up fk info
        self.cursor.execute(playlist_id_query)
        playlist_id_info = [playlist_info for playlist_info in self.cursor.fetchall()]
        playlist_id_dict = {playlist_id[1]: playlist_id[0] for playlist_id in playlist_id_info}

        final_tp_track_ids = [track_id_dict[i[0]] for i in track_playlist_data]
        final_tp_playlist_ids = [playlist_id_dict[i[1]] for i in track_playlist_data]
        final_tp_date_fks = [date_foreign_key for i in range(len(track_playlist_data))]
        final_tp_playlist_positions = [i[2] for i in track_playlist_data]
        final_tp_track_pops = [track_pop_dict[i] for i in final_tp_track_ids]

        # combine the final data for 'track_playlist_fact' table
        final_track_playlist_data = list(zip(final_tp_track_ids, final_tp_playlist_ids, final_tp_date_fks,
                                             final_tp_playlist_positions, final_tp_track_pops))

        return final_track_artist_data, final_track_playlist_data

    def update_fact_tables(self, cleaned_track_artist_data, cleaned_track_playlist_data):

        """
        Updates the data warehouse's 2 fact tables

        :param cleaned_track_artist_data: list containing rows of data to be added to the 'track_artist_fact' table
        :param cleaned_track_playlist_data: list containing rows of data to be added to the 'track_playlist_fact' table
        """

        # update 'track_artist_fact'
        track_artist_insert = """INSERT INTO track_artist_fact (
                                    track_id, artist_id, date_id, track_popularity, artist_popularity, artist_followers)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                """
        try:
            self.cursor.executemany(track_artist_insert, cleaned_track_artist_data)
            self.connection.commit()
            logging.info("Data successfully added to 'track_artist_fact' table")
        except Exception:
            logging.exception("Data NOT added to 'track_artist_fact' table")

        # update 'track_playlist_fact'
        track_playlist_insert = """INSERT INTO track_playlist_fact (
                                        track_id, playlist_id, date_id, track_playlist_position, track_popularity)
                                    VALUES (%s, %s, %s, %s, %s)
                                    """
        try:
            self.cursor.executemany(track_playlist_insert, cleaned_track_playlist_data)
            self.connection.commit()
            logging.info("Data added to 'track_playlist_fact' table")
        except Exception:
            logging.exception("Data NOT added to 'track_playlist_fact' table")

        logging.info("All playlist data successfully added to the DW")

