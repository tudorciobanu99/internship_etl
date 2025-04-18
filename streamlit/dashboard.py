import os
from datetime import datetime, timedelta
import plotly.express as px
from dotenv import load_dotenv
import pandas as pd
import streamlit as st
from common.database_connector import DatabaseConnector
from log_page import (success_rate_choropleth_map, display_summary_statistics,
                      rolling_average_rows, daily_api_time, transformation_rates_by_day_type)

load_dotenv('database_password.env')
db_config = {
    "dbname": "etl",
    "user": "postgres",
    "host": "localhost",
    "password": os.environ.get("db_password"),
    "port": 5432,
}
db = DatabaseConnector(**db_config)

st.set_page_config(layout="wide")
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

def centered_title(title_text):
    st.markdown(
        f"""
        <h1 style="text-align: center; color: black;">{title_text}</h1>
        """,
        unsafe_allow_html=True
    )

st.sidebar.title("Navigation")
with st.sidebar.expander("Menu", expanded=True):
    page = st.selectbox("Select a page:", ["API and Logs", "Weather and COVID data"])

if page == "API and Logs":
    centered_title("API and Logs from the ETL Process")

    col1, col2 = st.columns(2)
    with col1:
        query = """SELECT api_name AS api FROM extract.api_info"""
        data = db.fetch_rows(query)
        df = pd.DataFrame(data, columns=["api"])
        api_options = df["api"].unique()
        selected_api = st.selectbox("Select API:", options=api_options, index=0)

        query = """SELECT name FROM extract.country"""
        data = db.fetch_rows(query)
        df = pd.DataFrame(data, columns=["country"])
        country_options = df["country"].unique()
        selected_country = st.selectbox("Select Country:",
                                    options=["All"] + list(country_options), index=0)
        success_rate_choropleth_map(selected_api, selected_country)
        if selected_country != "All":
            display_summary_statistics(selected_country)
    with col2:
        query = """SELECT name FROM extract.country"""
        data = db.fetch_rows(query)
        df = pd.DataFrame(data, columns=["country"])
        country_options = df["country"].unique()
        selected_country = st.selectbox("Select Country:",
                                    options=["All"] + list(country_options), index=0, key="col_2")
        if selected_country != "All":
            rolling_average_rows(selected_country)
        else:
            st.warning("Please select a specific country to view the rolling average.")

    col3, col4 = st.columns(2)
    with col3:
        today = datetime.today().date()
        one_month = today - timedelta(days=30)

        st.subheader("Daily API time")

        start_date, end_date = st.slider(
            "Select date range:",
            min_value=one_month,
            max_value=today,
            value=(one_month, today),
            format="YYYY-MM-DD",
        )
        daily_api_time(start_date, end_date)
    with col4:
        transformation_rates_by_day_type()

elif page == "Weather and COVID data":
    centered_title("Weather and COVID Data")

    col1, col2 = st.columns(2)
    selected_country = "All"
    with col1:
        st.subheader("COVID-19 cases and weather conditions")
        query = """SELECT country_code AS country FROM load.dim_country"""
        data = db.fetch_rows(query)
        df = pd.DataFrame(data, columns=["country"])
        country_options = df["country"].unique()
        selected_country = st.selectbox("Select Country:",
                                    options=["All"] + list(country_options), index=0, key="col_2")
        covid_vs_weather(selected_country)
    with col2:
        st.subheader(f"Summary Statistics for {selected_country}:")
        covid_and_weather_summary_stats(selected_country)
    col3, col4 = st.columns(2)
    with col3:
        today = datetime.today().date()
        today = today.replace(year=2022)
        one_month = today - timedelta(days=30)

        start_date, end_date = st.slider(
            "Select date range:",
            min_value=one_month,
            max_value=today,
            value=(one_month, today),
            format="YYYY-MM-DD",
        )
        covid_vs_date(selected_country, start_date, end_date)
    with col4:
        peak_of_new_cases()