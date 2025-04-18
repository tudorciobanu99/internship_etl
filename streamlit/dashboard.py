import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
from log_page import (success_rate_choropleth_map, display_summary_statistics,
                      rolling_average_rows, daily_api_time, transformation_rates_by_day_type)
from data_page import (covid_and_weather_summary_stats, covid_vs_date,
                       covid_vs_weather, peak_of_new_cases)
from common.database_connector import DatabaseConnector
import streamlit as st

def connect_to_database(**db_config):
    db = DatabaseConnector(**db_config)
    return db

def centered_title(title):
    st.markdown(
        f"""
        <h1 style="text-align: center; color: black;">{title}</h1>
        """,
        unsafe_allow_html=True
    )

def override_dark_mode():
    st.markdown(
            """
            <style>
            /* Override Streamlit's dark mode */
            html, body, [class*="css"]  {
                background-color: white !important;
                color: black !important;
            }
            </style>
            """,
            unsafe_allow_html=True
    )

def weather_description_format(df):
    st.markdown(
        f"""
        <div style="text-align: left;">
            <h4>Most Frequent Weather Description</h4>
            <p style="font-size: 16px; word-wrap: break-word;">
                {df["weather_description"][0]} ({df["weather_code"][0]})
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )

def page_config():
    st.set_page_config(layout="wide")
    st.sidebar.title("Navigation")
    with st.sidebar.expander("Menu", expanded=True):
        page = st.selectbox("Select a page:", ["API and Logs", "Weather and COVID data"])
    return page

def api_selector(db):
    query = """
        SELECT api_name AS api FROM extract.api_info
    """
    data = db.fetch_rows(query)
    df = pd.DataFrame(data, columns=["api"])
    api_options = df["api"].unique()
    selected_api = st.selectbox("Select API:", options=api_options, index=0)
    return selected_api

def country_selector(db):
    query = """
        SELECT name FROM extract.country
    """
    data = db.fetch_rows(query)
    df = pd.DataFrame(data, columns=["country"])
    country_options = df["country"].unique()
    selected_country = st.selectbox("Select Country:",
                                options=["All"] + list(country_options), index=0)
    return selected_country

def date_slider(past=None):
    today = datetime.today().date()
    if past:
        today = today.replace(year=2022)
    one_month = today - timedelta(days=30)
    st.subheader("Daily API time")

    start_date, end_date = st.slider(
        "Select date range:",
        min_value=one_month,
        max_value=today,
        value=(one_month, today),
        format="YYYY-MM-DD",
    )
    return start_date, end_date

def api_and_logs(db):
    centered_title("API and Logs from the ETL Process")
    col1, col2 = st.columns(2)
    with col1:
        selected_api = api_selector(db)
        selected_country = country_selector(db)

        fig = success_rate_choropleth_map(db, selected_api, selected_country)
        st.plotly_chart(fig, key=f"choro_{selected_country}_{selected_api}")
        if selected_country != "All":
            df, pie_fig = display_summary_statistics(db, selected_country)
            col1, col2 = st.columns(2)
            with col1:
                st.subheader(f"Summary Statistics for {selected_country}")
                st.metric("Total API Calls", f"{df['Total Calls'][0]:,}")
                st.metric("Successful Calls", f"{df['Successful Calls'][0]:,}")
                st.metric("Failed Calls", f"{df['Failed Calls'][0]:,}")
                st.metric("Average Extraction Time",
                          f"{df['Avg Extraction Time (s)'][0]:.2f} seconds")
            with col2:
                st.subheader("Response Codes Distribution")
                st.plotly_chart(pie_fig, key=f"pie_chart_{selected_country}")
    with col2:
        selected_country = country_selector(db)
        if selected_country != "All":
            fig = rolling_average_rows(db, selected_country)
            st.plotly_chart(fig)
        else:
            st.warning("Please select a specific country to view the rolling average.")

    col3, col4 = st.columns(2)
    with col3:
        start_date, end_date = date_slider()
        daily_api_time(db, start_date, end_date)
    with col4:
        transformation_rates_by_day_type(db)

def covid_and_weather(db):
    centered_title("Weather and COVID Data")
    col1, col2 = st.columns(2)
    selected_country = "All"
    with col1:
        selected_country = country_selector(db)
        fig = covid_vs_weather(db, selected_country)
        st.plotly_chart(fig)
    with col2:
        st.subheader(f"Summary Statistics for {selected_country}:")
        df = covid_and_weather_summary_stats(db, selected_country)

        if selected_country != "All":
            col1, col2 = st.columns(2)
            with col1:
                st.metric("First Collection Date", df["start_date"][0].strftime("%Y-%m-%d"))
                st.metric("Average Temperature (°C)", f"{df['avg_temperature'][0]:.2f}")
                st.metric("Average Humidity (%)", f"{df['avg_humidity'][0]:.2f}")
            with col2:
                st.metric("Last Collection Date", df["end_date"][0].strftime("%Y-%m-%d"))
                st.metric("Total Confirmed Cases", f"{df['total_confirmed_cases'][0]:,}")
                st.metric("Total Deaths", f"{df['total_deaths'][0]:,}")
            weather_description_format(df)
        else:
            st.warning("Please select a specific country to view summary statistics!")
    col3, col4 = st.columns(2)
    with col3:
        start_date, end_date = date_slider(past=True)
        fig = covid_vs_date(db, selected_country, start_date, end_date)
        st.plotly_chart(fig)
    with col4:
        fig = peak_of_new_cases(db)
        st.plotly_chart(fig)

if __name__ == '__main__':
    load_dotenv('database_password.env')
    db_config = {
        "dbname": "etl",
        "user": "postgres",
        "host": "localhost",
        "password": os.environ.get("db_password"),
        "port": 5432,
    }
    db = connect_to_database(**db_config)

    page = page_config()

    if page == "API and Logs":
        api_and_logs(db)
    elif page == "Weather and COVID data":
        covid_and_weather(db)
