-- This script creates the schema and tables for the transform process.
CREATE SCHEMA transform;

CREATE TABLE transform.transform_log(
    id SERIAL PRIMARY KEY,
    batch_date DATE,
    country_id INT,
    processed_directory_name VARCHAR(100),
    processed_file_name VARCHAR(100),
    row_count INT,
    status VARCHAR(50) NOT NULL
);

CREATE TABLE transform.weather_data_import(
    id SERIAL PRIMARY KEY,
    country_id INT NOT NULL,
    date DATE NOT NULL,
    weather_code VARCHAR(10) NOT NULL,
    weather_description VARCHAR(255) NOT NULL,
    mean_temperature DECIMAL(5,2) NOT NULL,
    mean_surface_pressure DECIMAL(6,2) NOT NULL,
    precipitation_sum DECIMAL(5,2) NOT NULL,
    relative_humidity DECIMAL(5,2) NOT NULL,
    wind_speed DECIMAL(5,2) NOT NULL,
    FOREIGN KEY (country_id) REFERENCES extract.country(id)
);

CREATE TABLE transform.covid_data_import(
    id SERIAL PRIMARY KEY,
    country_id INT NOT NULL,
    date DATE NOT NULL,
    confirmed_cases INT NOT NULL,
    deaths INT NOT NULL,
    recovered INT NOT NULL,
    FOREIGN KEY (country_id) REFERENCES extract.country(id)
);