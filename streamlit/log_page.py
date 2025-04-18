import pandas as pd
import plotly.express as px

def success_rate_choropleth_map(db, selected_api, selected_country):
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

    filtered_df = df[df["api"] == selected_api].copy()

    if selected_country != "All":
        country_data = filtered_df[filtered_df["country"] == selected_country].iloc[0]
        center_lat = country_data["latitude"]
        center_lon = country_data["longitude"]
        zoom_scope = False
    else:
        center_lat = None
        center_lon = None
        zoom_scope = True

    bins = [0,10,20,30,40,50,60,70,80,90,100]
    labels = ['0-10%', '10-20%', '20-30%', '30-40%', '40-50%',
              '50-60%', '60-70%', '70-80%', '80-90%', '90-100%']
    filtered_df['success_bin'] = pd.cut(filtered_df['success_rate'], bins=bins, labels=labels)
    filtered_df['success_bin'] = filtered_df['success_bin'].astype(str)

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
        filtered_df,
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
    return fig

def display_summary_statistics(db, selected_country):
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
    data = db.fetch_rows(query, values)
    df = pd.DataFrame(data, columns=["Total Calls", "Avg Extraction Time (s)",
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
    data = db.fetch_rows(query, values)
    df_response = pd.DataFrame(data, columns=["code_response", "total_calls"])
    pie_fig = px.pie(df_response, values='total_calls', names='code_response')
    return df, pie_fig

    # col1, col2 = st.columns(2)
    # with col1:
    #     st.subheader(f"Summary Statistics for {selected_country}")
    #     st.metric("Total API Calls", f"{df['Total Calls'][0]:,}")
    #     st.metric("Successful Calls", f"{df['Successful Calls'][0]:,}")
    #     st.metric("Failed Calls", f"{df['Failed Calls'][0]:,}")
    #     st.metric("Average Extraction Time", f"{df['Avg Extraction Time (s)'][0]:.2f} seconds")
    # with col2:
    #     st.subheader("Response Codes Distribution")
    #     pie_fig = px.pie(df_response, values='total_calls', names='code_response')
    #     st.plotly_chart(pie_fig, key=f"pie_chart_{selected_country}")

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
    )
    fig.add_scatter(
        x=df["batch_date"],
        y=df["rolling_avg_rows"],
        mode="lines+markers",
        name="7 day rolling average",
        line=dict(color="red"),
    )
    fig.update_traces(hovertemplate=None)
    fig.update_layout(
        hovermode = "x unified")
    return fig


def daily_api_time(db, start_date, end_date):
    query = """
        SELECT
            DATE(start_time) AS api_date,
            COUNT(*) AS total_calls,
            SUM(EXTRACT(EPOCH FROM (end_time - start_time))) AS daily_api_time
        FROM extract.api_import_log
        WHERE start_time >= %s AND start_time <= %s 23:59:59'
        GROUP BY api_date
        ORDER BY api_date;
    """
    values = (start_date, end_date)
    data = db.fetch_rows(query, values)
    df = pd.DataFrame(data, columns=["api_date", "total_calls", "daily_api_time"])
    df["api_date"] = pd.to_datetime(df["api_date"], errors="coerce")

    full_date_range = pd.date_range(start=start_date, end=end_date)
    full_df = pd.DataFrame(full_date_range, columns=["api_date"])
    df = pd.merge(full_df, df, on="api_date", how="left").fillna({"daily_api_time": 0, "total_calls": 0})
    df = df.sort_values(by="api_date")

    fig = px.line(
        df,
        x="api_date",
        y="daily_api_time",
        labels={"daily_api_time": "Daily API Time (s)", "api_date": "Date"},
        title="Daily API Time Over Selected Date Range",
        range_y = (-2, max(df["daily_api_time"].max()*2, 2)),
                hover_data={"api_date": "|%Y-%m-%d", "daily_api_time": ":.2f", "total_calls": ":,"}
    )
    fig.update_traces(mode="lines+markers")
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Daily API Time (s)",
        hovermode="x unified",
        legend_title_text="API Time"
    )
    fig.update_traces(hovertemplate=None)
    fig.update_layout(
        hovermode = "x unified")
    return fig

def transformation_rates_by_day_type(db):
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
        GROUP BY day_type
        ORDER BY day_type;
    """

    data = db.fetch_rows(query)
    df = pd.DataFrame(data, columns=["day_type", "total_transformations", "successful_transformations", "failed_transformations"])
    df_renamed = df.rename(columns={
    "successful_transformations": "Successful Transformations",
    "failed_transformations": "Failed Transformations"
    })

    fig = px.bar(
        df_renamed,
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
    )
    fig.update_traces(hovertemplate=None)
    fig.update_layout(
        hovermode = "x unified")
    return fig