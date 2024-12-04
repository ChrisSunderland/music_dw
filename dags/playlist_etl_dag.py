from datetime import timedelta
import pendulum
from spot_dw import PlaylistDW
from airflow.decorators import dag, task

default_args = {'owner': 'cs',
                'retries': 5,
                'retry_delay': timedelta(minutes=5)}


@dag(dag_id='playlist_etl_v1',
     default_args=default_args,
     start_date=pendulum.datetime(2024, 11, 8, tz="UTC"),
     schedule_interval='30 19 * * Fri',
     catchup=False)  # run dag every Friday at 12:30 local time
def playlist_etl():

    dw = PlaylistDW()  # instantiate class that connects to and updates the data warehouse

    @task
    def get_tracked_playlists():

        dw.create_tables()  # set up the data warehouse if it hasn't already been created
        playlist_id_query = """SELECT playlist_spotify_id FROM playlist_dim WHERE playlist_id != 1"""
        dw.cursor.execute(playlist_id_query)
        tracked_playlists = [plist_id[0] for plist_id in dw.cursor.fetchall()]

        return tracked_playlists

    @task
    def extract_playlist_data(playlist_list):

        # collect, clean playlist data
        track, artist, track_artist, track_playlist = dw.extract_playlists_data(playlist_list)

        return track, artist, track_artist, track_playlist

    @task
    def load_playlist_data(playlist_data):

        track_data, artist_data, track_artist_data, track_playlist_data = playlist_data

        dw.update_dimensions(track_data, artist_data)  # add new rows to the data warehouse's dimensions

        final_track_artist_data, final_track_playlist_data = dw.organize_facts(track_artist_data, track_playlist_data)

        dw.update_fact_tables(final_track_artist_data, final_track_playlist_data)

    plist_list = get_tracked_playlists()

    plist_data = extract_playlist_data(playlist_list=plist_list)

    load_playlist_data(playlist_data=plist_data)


plist_etl = playlist_etl()  # instantiate the DAG

