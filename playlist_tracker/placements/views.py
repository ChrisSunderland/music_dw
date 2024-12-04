from django.shortcuts import render
from django.db import connection
import ast
import pandas as pd
import plotly.express as px
from plotly.io import to_html


def get_playlists(request):

    with connection.cursor() as cursor:
        cursor.execute("SELECT playlist_id, playlist_name FROM playlist_dim")
        playlist_data = [playlist for playlist in cursor.fetchall()]

    context = {'playlist_data': playlist_data}

    return render(request, 'placements/placements.html', context)


def get_start(request):

    selected_playlist = request.GET.get('playlist_dropdown')
    selected_playlist_data = ast.literal_eval(selected_playlist)
    playlist_id = selected_playlist_data[0]

    with connection.cursor() as cursor:

        date_query = """
            SELECT DISTINCT dd.date_id, date
            FROM date_dim dd
            JOIN track_playlist_fact tpf on tpf.date_id = dd.date_id
            WHERE playlist_id = %s
        """
        cursor.execute(date_query, [playlist_id])
        dates = [date for date in cursor.fetchall()]
        date_data = [(playlist_id, i[0], i[1]) for i in dates]

    context = {'date_data': date_data}

    return render(request, 'placements/partials/start_date.html', context)


def get_end(request):

    selected_start = request.GET.get('start_date')
    selected_start = selected_start[1:-1]
    selected_start = selected_start.split(", ", 2)

    playlist_id = selected_start[0].strip()
    start_date = selected_start[2]
    start_date = start_date.replace("datetime.date", "").strip("()")
    start_date = start_date.strip()
    start_date = start_date.replace(", ", "-")

    with connection.cursor() as cursor:

        date_query = """
            SELECT DISTINCT dd.date_id, date
            FROM date_dim dd
            JOIN track_playlist_fact tpf on tpf.date_id = dd.date_id
            WHERE playlist_id = %s and date >= %s
        """

        cursor.execute(date_query, [playlist_id, start_date])
        end_dates = [date for date in cursor.fetchall()]
        end_dates_data = [(playlist_id, start_date, i[1]) for i in end_dates]

    context = {'end_dates_data': end_dates_data}

    return render(request, 'placements/partials/end_date.html', context)


def organize_placement_data(request):

    selected_end = request.GET.get('end_date')
    selected_end = selected_end[1:-1]
    selected_end = selected_end.split(", ", 2)

    playlist_id_clean = selected_end[0].strip("\'\"")
    start_date_clean = selected_end[1].strip("\'\"")
    end_date_clean = selected_end[2]
    end_date_clean = end_date_clean.replace("datetime.date", "").strip("()")
    end_date_clean = end_date_clean.strip()
    end_date_clean = end_date_clean.replace(", ", "-")

    with connection.cursor() as cursor:

        query = """
                SELECT
                    label_name,
                    count(track_playlist_position) as tracks_placed,
                    round(avg(track_playlist_position), 2) as average_track_position    
                FROM
                    track_playlist_fact tpf
                JOIN
                    date_dim dd ON tpf.date_id = dd.date_id
                JOIN
                    track_dim td ON tpf.track_id = td.track_id
                WHERE
                    playlist_id = %s AND date BETWEEN %s AND %s
                GROUP BY label_name
                ORDER BY tracks_placed DESC, average_track_position ASC
        """
        cursor.execute(query, [playlist_id_clean, start_date_clean, end_date_clean])
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)

    return df


def plot_label_placements(df):

    plot_df = df.nlargest(10, "tracks_placed")  # only display the top 10 in the plot
    plot_df.sort_values(by=["tracks_placed", "average_track_position"], ascending=[True, False], inplace=True)

    fig = px.bar(
        plot_df,
        x="tracks_placed",
        y="label_name",
        orientation="h",
        labels={"average_track_position": "average track position",
                "tracks_placed": "tracks placed",
                "label_name": "label"},
        title="Total playlist placements (top 10 labels)",
        hover_data={"label_name": False,
                    "tracks_placed": False,
                    "average_track_position": False}
    )

    fig.update_traces(
        marker_color="rgba(144, 238, 144, 0.7)",
        marker_line_color="rgba(0, 128, 0, 1)",
        marker_line_width=1.5
    )

    fig.update_layout(
        title={
            "x": 0.5,
            "xanchor": "center",
        }
    )

    label_bar_chart = to_html(fig, full_html=False)

    return label_bar_chart


def display_placement_summary(request):

    data = organize_placement_data(request)

    bar_chart = plot_label_placements(data)

    headers = list(data.columns)
    rows = data.to_dict(orient="records")

    context = {'bar_chart': bar_chart,
               'headers': headers,
               'rows': rows}

    return render(request, 'placements/partials/placement_display.html', context)
