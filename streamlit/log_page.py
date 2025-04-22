import pandas as pd
import plotly.express as px

def success_rate_choropleth_map(db, selected_api, selected_country):
    """
    Creates and displays a Choropleth map of success rates for a given
    country and given API.

    Args:
        db (DatabaseConnector)
        selected_api (str): The name of the selected API.
        selected_country(str): The name of the selected country.

    Returns:
        fig (plotly.graph_objects.Figure)
    """

    query = """
        SELECT
            a.api_name AS api,
            c.name AS country,
            c.code AS country_code,
            c.latitude,
            c.longitude,
            ROUND(SUM(CASE WHEN log.code_response = 200 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate
        FROM extract.api_import_log AS log
        JOIN extract.api_info AS a ON log.api_id = a.id
        JOIN extract.country AS c ON log.country_id = c.id
        GROUP BY log.api_id, a.api_name, c.name, c.code, c.latitude, c.longitude
        ORDER BY api, country;
    """
    data = db.fetch_rows(query)
    df = pd.DataFrame(data, columns=["api", "country", "country_code",
                                     "latitude", "longitude", "success_rate"])

    df = df[df["api"] == selected_api]
    center_lat = None
    center_lon = None
    zoom_scope = True
    if selected_country != "All countries":
        country_data = df[df["country"] == selected_country].iloc[0]
        center_lat = country_data["latitude"]
        center_lon = country_data["longitude"]
        zoom_scope = False

    bins = [0,10,20,30,40,50,60,70,80,90,100]
    labels = ['0-10%', '10-20%', '20-30%', '30-40%', '40-50%',
              '50-60%', '60-70%', '70-80%', '80-90%', '90-100%']
    df['success_bin'] = pd.cut(df['success_rate'], bins=bins, labels=labels)
    df['success_bin'] = df['success_bin'].astype(str)

    color_map = {
        '0-10%': '#d73027',
        '10-20%': '#f46d43',
        '20-30%': '#fdae61',
        '30-40%': '#fee08b',
        '40-50%': '#ffffbf',
        '50-60%': '#d9ef8b',
        '60-70%': '#a6d96a',
        '70-80%': '#66bd63',
        '80-90%': '#1a9850',
        '90-100%': '#006837'
    }

    fig = px.choropleth(
        df,
        locations="country_code",
        color="success_bin",
        hover_name="country",
        hover_data={"success_rate": ":.2f"},
        title=f"Success Rate for {selected_api}",
        color_discrete_map=color_map
    )
    fig.update_layout(
        geo=dict(
            showland=True,
            landcolor="lightgray",
            showocean=True,
            oceancolor="lightblue",
            projection_type="natural earth",
            center={"lat": center_lat, "lon": center_lon} if not zoom_scope else None,
            scope="world" if zoom_scope else None,
            projection_scale=2.5 if not zoom_scope else 1
        ),
        coloraxis_colorbar=dict(title="Success Rate (%)"),
        legend_title=dict(text="Success Rate Range")
    )
    fig.update_traces(
        hovertemplate=(
            "<b>%{hovertext}</b><br><br>" +
            "Success Rate: %{customdata[0]:.2f}%<br>" +
            "Range: %{customdata[1]}<br>" +
            "<extra></extra>"
        ),
        hovertext=df["country"],
        customdata=df[["success_rate", "success_bin"]]
    )
    return fig

def display_summary_statistics(db, selected_country):
    """
    Prepares summary statistics about API import logs and
    creates and displays a pie chart of the code responses
    for the API calls.

    Information includes:
        total calls, average extraction time in seconds,
        number of successful and failed calls.

    Args:
        db (DatabaseConnector object)
        selected_country (str): The name of the selected country.

    Returns:
        df (DataFrame): A DataFrame containing the above statistics.
        fig (plotly.graph_objects.Figure)
    """

    query = """
        SELECT
            COUNT(*) AS total_calls,
            AVG(EXTRACT(EPOCH FROM (end_time - start_time))) AS avg_extraction_time,
            SUM(CASE WHEN code_response = 200 THEN 1 ELSE 0 END) AS successful_calls,
            SUM(CASE WHEN code_response != 200 THEN 1 ELSE 0 END) AS failed_calls
        FROM extract.api_import_log log
        JOIN extract.country c ON log.country_id = c.id
        WHERE c.name = %s;
    """
    values = (selected_country,)
    smry_data = db.fetch_rows(query, values)
    df = pd.DataFrame(smry_data, columns=["Total Calls", "Avg Extraction Time (s)",
                                     "Successful Calls", "Failed Calls"])

    query = """
        SELECT
            code_response,
            COUNT(*) AS total_calls
        FROM extract.api_import_log log
        JOIN extract.country c ON log.country_id = c.id
        WHERE c.name = %s
        GROUP BY code_response
        ORDER BY total_calls DESC;
    """
    calls_data = db.fetch_rows(query, values)
    df_response = pd.DataFrame(calls_data, columns=["code_response", "total_calls"])
    df_response["hover_label"] = df_response.apply(
        lambda row: f"Code: {row['code_response']}<br>Total Calls: {row['total_calls']:,}", axis=1
    )

    pie_fig = px.pie(df_response, values='total_calls', names='code_response')
    pie_fig.update_traces(
        hoverinfo="label+percent+value",
        hovertemplate="<b>%{label}</b><br>" +
                    "Total Calls: %{value:,}<br>" +
                    "Percentage: %{percent}<br>" +
                    "<extra></extra>"
    )
    return df, pie_fig

def rolling_average_rows(db, selected_country):
    query = """
        SELECT
        batch_date,
        c.name AS country,
        SUM(row_count) AS daily_rows_imported,
        AVG(SUM(row_count)) OVER (
            PARTITION BY c.name
            ORDER BY batch_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_rows
        FROM extract.import_log log
        JOIN extract.country c ON log.country_id = c.id
        WHERE c.name = %s
        GROUP BY batch_date, c.name
        ORDER BY batch_date;
    """
    values = (selected_country,)
    data = db.fetch_rows(query, values)
    df = pd.DataFrame(data, columns=["batch_date", "country",
                                     "daily_rows_imported", "rolling_avg_rows"])

    df["batch_date"] = pd.to_datetime(df["batch_date"], errors="coerce")
    df = df.dropna(subset=["batch_date"])
    df = df.sort_values(by="batch_date")

    fig = px.bar(
        df,
        x="batch_date",
        y="daily_rows_imported",
        labels={"daily_rows_imported": "Daily Rows Imported", "batch_date": "Date"},
        title=f"Daily Rows Imported for {selected_country}",
        opacity=0.7
    )
    fig.add_scatter(
        x=df["batch_date"],
        y=df["rolling_avg_rows"],
        mode="lines+markers",
        name="7 Day Rolling Average",
        line=dict(color="red", width=3, dash="solid"),
        marker=dict(size=7, color="red", symbol="circle", line=dict(color="darkred")),
        hoverinfo="y",
        hovertemplate="<b>7-Day Avg:</b> %{y:,.2f}<extra></extra>"
    )
    fig.update_traces(
        hoverinfo="y",
        hovertemplate="<b>Rows Imported:</b> %{y:,}<extra></extra>",
        selector=dict(type='bar')
    )
    fig.update_layout(
        hovermode="x unified"
    )
    return fig


def daily_api_time(db, start_date, end_date):
    """
    Calculated and displays the daily API time for all calls
    for every single day in the given date range.

    Args:
        db (DatabaseConnector)
        start_date (str): The start date of the date range.
        end_date (str): The end date of the date range.

    Returns:
        fig (plotly.graph_objects.Figure)
    """

    query = """
        SELECT
            DATE(start_time) AS api_date,
            COUNT(*) AS total_calls,
            SUM(EXTRACT(EPOCH FROM (end_time - start_time))) AS daily_api_time
        FROM extract.api_import_log
        WHERE start_time >= %s AND start_time <= %s
        GROUP BY api_date
        ORDER BY api_date;
    """
    values = (start_date, str(end_date) + " 23:59:59")
    data = db.fetch_rows(query, values)
    df = pd.DataFrame(data, columns=["api_date", "total_calls", "daily_api_time"])
    df["api_date"] = pd.to_datetime(df["api_date"], errors="coerce")

    full_date_range = pd.date_range(start=start_date, end=end_date)
    full_df = pd.DataFrame(full_date_range, columns=["api_date"])
    df = pd.merge(full_df, df, on="api_date", how="left")
    df = df.fillna({"daily_api_time": 0, "total_calls": 0})
    df = df.sort_values(by="api_date")

    fig = px.line(
        df,
        x="api_date",
        y="daily_api_time",
        labels={"daily_api_time": "Daily API Time (s)", "api_date": "Date"},
        title="Daily API Time Over Selected Date Range",
        range_y = (-2, max(df["daily_api_time"].max()*2, 2)),
        range_x = (start_date, end_date)
    )
    fig.update_traces(
        mode="lines+markers",
        line=dict(color="#1f77b4", width=2),
        marker=dict(
            size=8,
            color="#1f77b4",
            line=dict(width=2, color="white")
        ),
        hovertemplate="<b>Date:</b> %{x|%Y-%m-%d}<br>" +
                    "<b>API Time:</b> %{y:.2f} seconds<br>" +
                    "<b>Total Calls:</b> %{customdata:,}<extra></extra>",
        customdata=df[["total_calls"]],
        fill="tozeroy"
    )
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Daily API Time (seconds)",
        hovermode="x unified",
        plot_bgcolor="white",
        margin=dict(t=80, l=50, r=30, b=50)
    )
    return fig

def transformation_rates_by_day_type(db):
    """
    Creates and displays a bar chart of the transformation rates of the raw files
    from the extract process of the ETL.

    Args:
        db (DatabaseConnector)

    Returns:
        fig (plotly.graph_objects.Figure)
    """

    query = """
        SELECT
            CASE
                WHEN EXTRACT(DOW FROM batch_date) IN (0, 6) THEN 'Weekend'
                ELSE 'Workday'
            END AS day_type,
            COUNT(*) AS total_transformations,
            SUM(CASE WHEN status = 'processed' THEN 1 ELSE 0 END) AS successful_transformations,
            SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS failed_transformations
        FROM transform.transform_log
        WHERE batch_date IS NOT NULL
        GROUP BY day_type
        ORDER BY day_type;
    """
    data = db.fetch_rows(query)
    df = pd.DataFrame(
        data,
        columns=[
            "day_type",
            "total_transformations",
            "successful_transformations",
            "failed_transformations"
        ]
    )
    df = df.rename(columns={
    "successful_transformations": "Successful Transformations",
    "failed_transformations": "Failed Transformations"
    })

    fig = px.bar(
        df,
        x="day_type",
        y=["Successful Transformations", "Failed Transformations"],
        labels={
            "value": "Number of Transformations",
            "day_type": "Day Type",
            "variable": "Transformation Status",
        },
        title="Transformation Success and Failure Rates on Weekends vs. Workdays",
        text_auto=True,
    )
    fig.update_layout(
        barmode="stack",
        xaxis_title="Day Type",
        yaxis_title="Number of Transformations",
        legend_title="Transformation Status",
        hovermode = "x unified"
    )
    for trace in fig.data:
        trace_name = trace.name
        trace.hovertemplate = (
            "<b>Day Type:</b> %{x}<br>" +
            f"<b>{trace_name}:</b> %{{y:,}}<extra></extra>"
        )
        trace.textposition = "inside"
        trace.marker.line = dict(color="black", width=1)
    return fig
