import pandas as pd
import plotly.express as px

def covid_vs_weather(db, selected_country):
    query = """
        SELECT
            d.date,
            c.country_code,
            w.mean_temperature,
            w.relative_humidity,
            f.confirmed_cases,
            f.deaths
        FROM load.fact_covid_data f
        JOIN load.dim_country c ON f.country_id = c.country_id
        JOIN load.fact_weather_data w ON f.country_id = w.country_id AND f.date_id = w.date_id
        JOIN load.dim_date d ON f.date_id = d.date_id
        WHERE w.mean_temperature IS NOT NULL
        AND f.confirmed_cases IS NOT NULL
        ORDER BY d.date, c.country_code;
    """

    data = db.fetch_rows(query)
    df = pd.DataFrame(data, columns=["date", "country_code", "mean_temperature",
                                     "relative_humidity", "confirmed_cases", "deaths"])

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "mean_temperature", "confirmed_cases"])
    df["confirmed_cases"] = df["confirmed_cases"].apply(lambda x: max(x, 0))

    if selected_country != "All countries":
        df = df[df["country_code"] == selected_country]
    else:
        df = df.groupby(["mean_temperature", "relative_humidity"]).agg(
            confirmed_cases=("confirmed_cases", "sum"),
            deaths=("deaths", "sum")
        ).reset_index()

    min_val = df["confirmed_cases"].min()
    max_val = df["confirmed_cases"].max()
    mid_val = df["confirmed_cases"].mean()

    fig = px.density_heatmap(
        df,
        x="mean_temperature",
        y="relative_humidity",
        z="confirmed_cases",
        color_continuous_scale="rdylgn_r",
        labels={
            "mean_temperature": "Mean Temperature (°C)",
            "relative_humidity": "Relative Humidity (%)",
            "confirmed_cases": "Confirmed Cases",
        },
        title="Heatmap: COVID-19 Cases vs Weather Conditions",
        range_color=[df["confirmed_cases"].min(), df["confirmed_cases"].max()],
        color_continuous_midpoint=df["confirmed_cases"].mean(),
        hover_data=["confirmed_cases", "deaths"]
    )
    fig.update_traces(hovertemplate=None)
    fig.update_layout(
        hovermode = "x unified",
        xaxis_title="Mean Temperature (°C)",
        yaxis_title="Relative Humidity (%)",
        coloraxis_colorbar=dict(
            title="Confirmed Cases",
            tickvals=[min_val, mid_val, max_val],
            ticktext=[f"Low ({min_val:,})", f"Avg ({int(mid_val):,})", f"High ({max_val:,})"]
        )
    )
    return fig

def covid_and_weather_summary_stats(db, selected_country):
    query = """
        WITH sum_stats AS (
            SELECT
                MIN(d.date) AS start_date,
                MAX(d.date) AS end_date,
                AVG(w.mean_temperature) AS avg_temperature,
                AVG(w.relative_humidity) AS avg_humidity,
                SUM(f.confirmed_cases) AS total_confirmed_cases,
                SUM(f.deaths) AS total_deaths,
                SUM(f.recovered) AS total_recovered_cases
            FROM load.fact_covid_data f
            JOIN load.dim_country c ON f.country_id = c.country_id
            JOIN load.fact_weather_data w ON f.country_id = w.country_id AND f.date_id = w.date_id
            JOIN load.dim_date d ON f.date_id = d.date_id
            WHERE c.country_code = %s
        ),
        most_frequent_weather AS (
            SELECT
                w.weather_code,
                wc.description,
                COUNT(*) AS frequency,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank
            FROM load.fact_weather_data w
            JOIN load.dim_weather_code wc ON w.weather_code = wc.weather_code
            JOIN load.dim_country c ON w.country_id = c.country_id
            WHERE c.country_code = %s
            GROUP BY w.weather_code, wc.description
        )
        SELECT
            s.start_date,
            s.end_date,
            s.avg_temperature,
            s.avg_humidity,
            s.total_confirmed_cases,
            s.total_deaths,
            s.total_recovered_cases,
            mf.weather_code,
            mf.description
        FROM sum_stats s
        LEFT JOIN most_frequent_weather mf ON mf.rank = 1;
    """
    values = (selected_country, selected_country)
    data = db.fetch_rows(query, values)
    df = pd.DataFrame(data, columns=[
        "start_date", "end_date", "avg_temperature", "avg_humidity",
        "total_confirmed_cases", "total_deaths", "total_recovered_cases",
        "weather_code", "weather_description"
    ])
    return df

def peak_of_new_cases(db):
    query = """
        WITH ranked_peaks AS (
            SELECT
                dc.country_code,
                dd.date,
                fcd.confirmed_cases,
                LAG(fcd.confirmed_cases) OVER (
                    PARTITION BY dc.country_code ORDER BY dd.date
                ) AS prev_day
            FROM load.fact_covid_data AS fcd
            JOIN load.dim_country AS dc ON fcd.country_id = dc.country_id
            JOIN load.dim_date AS dd ON fcd.date_id = dd.date_id
        ),
        daily_new_cases AS (
            SELECT
                country_code,
                date,
                confirmed_cases,
                ABS(confirmed_cases) - ABS(prev_day) AS new_cases
            FROM ranked_peaks
        ),
        country_peaks AS (
            SELECT
                country_code,
                date AS peak_date,
                new_cases,
                ROW_NUMBER() OVER (
                    PARTITION BY country_code ORDER BY new_cases DESC NULLS LAST
                ) AS row_num
            FROM daily_new_cases
            WHERE new_cases IS NOT NULL
        ),
        top_peaks AS (
            SELECT
                country_code,
                peak_date,
                new_cases
            FROM country_peaks
            WHERE row_num = 1
        )
        SELECT
            *,
            RANK() OVER (ORDER BY new_cases DESC) AS peak_rank
        FROM top_peaks
        ORDER BY peak_rank
    """
    data = db.fetch_rows(query)
    df = pd.DataFrame(data, columns=["country_code", "peak_date", "new_cases", "peak_rank"])
    df["peak_date"] = pd.to_datetime(df["peak_date"], errors="coerce")
    df = df.dropna(subset=["peak_date"])
    df = df.sort_values(by="peak_rank")
    df["peak_date"] = df["peak_date"].dt.strftime("%Y-%m-%d")
    df["new_cases"] = df["new_cases"].apply(lambda x: max(x, 0))

    fig = px.bar(
        df,
        x="country_code",
        y="new_cases",
        color="peak_date",
        text="new_cases",
        title="Worst Single Day Spike of New COVID-19 Cases",
        labels={"new_cases": "New Cases", "country_code": "Country", "peak_date": "Date"},
        color_discrete_sequence=px.colors.sequential.Reds[::-1],
        hover_data={"new_cases": ":,", "peak_date": True, "country_code": True}
    )
    fig.update_traces(
        texttemplate="%{text:,}",
        textposition="outside",
        hovertemplate="<b>Country:</b> %{x}<br>" +
                      "<b>New Cases:</b> %{y:,}<br>" +
                      "<b>Peak Date:</b> %{customdata[0]}<extra></extra>",
        marker=dict(line=dict(color="black", width=1))
    )
    fig.update_layout(
        hovermode="x unified",
        xaxis_title="Country",
        yaxis_title="New Cases (Peak)",
        legend_title="Peak Date"
    )
    return fig

def covid_vs_date(db,selected_country, start_date, end_date):
    query = """
        SELECT
            dc.country_code,
            dd.date,
            dd.is_weekend,
            fcd.deaths
        FROM load.fact_covid_data fcd
        JOIN load.dim_country dc ON fcd.country_id = dc.country_id
        JOIN load.dim_date dd ON fcd.date_id = dd.date_id
        WHERE dd.date BETWEEN %s AND %s;
    """
    values = (start_date, end_date)
    data = db.fetch_rows(query, values)
    df = pd.DataFrame(data, columns=["country_code", "date", "is_weekend", "deaths"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.sort_values(by="date")

    if selected_country != "All countries":
        df = df[df["country_code"] == selected_country]
    else:
        df = df.groupby(["date", "is_weekend"]).agg(
            deaths=("deaths", "sum")
        ).reset_index()

    df["weekend_label"] = df["is_weekend"].map({True: "Weekend", False: "Weekday"})
    df = df.sort_values(by="date")

    fig = px.line(
        df,
        x="date",
        y="deaths",
        title=f"COVID-19 deaths for {selected_country}",
        color="weekend_label",
        labels={"weekend_label": "Day Type", "deaths": "Deaths"},
        markers=True,
        hover_data={"date": False, "deaths": False}
    )
    fig.update_traces(
        line=dict(width=2),
        marker=dict(size=6, line=dict(width=1, color="black")),
        hovertemplate="<b>Date:</b> %{x|%Y-%m-%d}<br>" +
                      "<b>Deaths:</b> %{y:,}<br>" +
                      "<b>Day Type:</b> %{fullData.name}<extra></extra>",
    )
    fig.update_layout(
            hovermode="x unified",
            xaxis_title="Date",
            yaxis_title="Number of Deaths",
            legend_title="Day Type",
            template="plotly_white"
    )
    return fig
