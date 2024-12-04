from django.shortcuts import render
from django.db import connection
import ast
import pandas as pd
import plotly.express as px
from plotly.io import to_html
from collections import defaultdict


def labels(request):

    with connection.cursor() as cursor:
        cursor.execute("SELECT DISTINCT(label_name) FROM track_dim")
        labels = [label[0] for label in cursor.fetchall()]

    context = {'labels': labels}

    return render(request, 'releases/releases.html', context)


def artists(request):

    label = request.GET.get('label_search')

    with connection.cursor() as cursor:

        artist_query = """
            SELECT DISTINCT tpf.artist_id, artist_name
            FROM track_artist_fact tpf
            JOIN track_dim td ON td.track_id = tpf.track_id
            JOIN artist_dim ad ON ad.artist_id = tpf.artist_id
            WHERE label_name ILIKE %s
            ORDER BY artist_name ASC
        """
        cursor.execute(artist_query, [f"%{label}%"])
        artists = [artist for artist in cursor.fetchall()]
        artist_data = [(label, i[0], i[1]) for i in artists]

    context = {'artist_data': artist_data}

    return render(request, 'releases/partials/artists.html', context)


def releases(request):

    artist_data = request.GET.get('artist_dropdown')
    artist_data_tuple = ast.literal_eval(artist_data)

    label = artist_data_tuple[0]
    artist_id = artist_data_tuple[1]

    release_search = """
            SELECT DISTINCT tpf.track_id, track_name, artist_id
            FROM track_artist_fact tpf
            JOIN track_dim td ON td.track_id = tpf.track_id
            WHERE artist_id = %s and label_name ILIKE %s
        """

    with connection.cursor() as cursor:
        cursor.execute(release_search, [artist_id, f"%{label}%"])
        releases = [release for release in cursor.fetchall()]

    context = {'releases': releases}

    return render(request, 'releases/partials/tracks.html', context)


def prepare_plot_data(request):

    track_data = request.GET.get('track_dropdown')
    track_data = ast.literal_eval(track_data)

    track_id = track_data[0]
    artist_id = track_data[2]

    plot_data_search = """
            SELECT date, track_popularity, artist_popularity, artist_followers
            FROM track_artist_fact taf
            JOIN date_dim dd ON dd.date_id = taf.date_id
            JOIN artist_dim ad ON ad.artist_id = taf.artist_id
            WHERE track_id = %s AND taf.artist_id = %s
            ORDER by date ASC
        """

    with connection.cursor() as cursor:
        cursor.execute(plot_data_search, [track_id, artist_id])
        plot_data = [row for row in cursor.fetchall()]

    plot_df = pd.DataFrame(plot_data, columns=["Date", "Track Pop", "Artist Pop", "Artist Followers"])
    plot_df['Date'] = pd.to_datetime(plot_df['Date'])
    plot_df['Track Pop'] = plot_df['Track Pop'].astype(int)
    plot_df['Artist Pop'] = plot_df['Artist Pop'].astype(int)
    plot_df['Artist Followers'] = plot_df['Artist Followers'].astype(int)
    plot_df['Follower Growth'] = plot_df['Artist Followers'].diff()

    return plot_df


def create_time_series_plot(df, y, title, hover_cols=("Date",)):

    hover_dict = defaultdict(bool)

    for col in hover_cols:
        hover_dict[col] = True

    fig = px.line(df,
                  x="Date",
                  y=y,
                  title=title,
                  hover_data=hover_dict
                  )

    fig.update_layout(
        xaxis=dict(
            tickvals=df["Date"].unique(),
        ),
        yaxis=dict(
            autorange=True,
            tickformat="d",
        ),
        xaxis_title="Date",
        yaxis_title=y,
        title_x=0.5,
    )

    line_chart = to_html(fig, full_html=False)

    return line_chart


def display_plots(request):

    data = prepare_plot_data(request)

    # create the 3 plots to display on the page
    date_vs_track_pop = create_time_series_plot(data, "Track Pop", "Track Popularity over time",
                                                hover_cols=("Date", "Track Pop"))
    date_vs_artist_pop = create_time_series_plot(data, "Artist Pop", "Artist Popularity over time",
                                                 hover_cols=("Date", "Artist Pop", "Artist Followers"))
    date_vs_artist_followers = create_time_series_plot(data, "Follower Growth", "Artist Follower Growth over time",
                                                       hover_cols=("Date", "Follower Growth","Artist Followers"))

    context = {'date_vs_track': date_vs_track_pop,
               'date_vs_artist_pop': date_vs_artist_pop,
               'date_vs_artist_foll': date_vs_artist_followers}

    return render(request, 'releases/partials/track_plot.html', context)



