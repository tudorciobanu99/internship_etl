-- This script creates the schema and tables for the load process
CREATE SCHEMA load;

CREATE TABLE fact_weather_data (
    id SERIAL PRIMARY KEY,
    country_id INT NOT NULL,
    date_id INT NOT NULL,
    weather_code INT,
    mean_temperature NUMERIC(5, 2),
    mean_surface_pressure NUMERIC(7, 2),
    precipitation_sum NUMERIC(5, 2),
    relative_humidity NUMERIC(5, 2),
    wind_speed NUMERIC(5, 2),
    createdAt TIMESTAMP,
    updatedAt TIMESTAMP,
    FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
    -- FOREIGN KEY (weather_code) REFERENCES dim_weather_code(weather_code),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

CREATE TABLE dim_country (
    country_id SERIAL PRIMARY KEY,
    country_code VARCHAR(10) NOT NULL,
    latitude NUMERIC(9, 6) NOT NULL,
    longitude NUMERIC(9, 6) NOT NULL
);

-- schimba 
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
);

-- CREATE TABLE dim_weather_code (
--     weather_code INT PRIMARY KEY,
--     description VARCHAR(255) NOT NULL
-- );

CREATE TABLE fact_covid_data (
    id SERIAL PRIMARY KEY,
    country_id INT NOT NULL,
    date DATE NOT NULL,
    confirmed_cases INT,
    deaths INT,
    recovered INT,
    createdAt TIMESTAMP,
    updatedAt TIMESTAMP,
    FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
    FOREIGN KEY (date) REFERENCES dim_date(date_id)
);






