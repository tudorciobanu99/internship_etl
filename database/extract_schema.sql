-- This script creates the schema and tables for the extract process
CREATE SCHEMA extract;

CREATE TABLE extract.country (
	id SERIAL PRIMARY KEY,
    code VARCHAR(10) NOT NULL, -- (ISO code)
    name VARCHAR(100) NOT NULL
    latitude DECIMAL(9,6) NOT NULL,
    latitude DECIMAL(9,6) NOT NULL,
    capital VARCHAR(50) NOT NULL
)

CREATE TABLE extract.import_log (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    country_id INT NOT NULL,
    import_directory_name VARCHAR(100),
    import_file_name VARCHAR(100),
    file_created_date DATE,
    file_last_modified_date DATE,
    row_count INT,
    FOREIGN KEY (country_id) REFERENCES extract.country(id)
)

CREATE TABLE extract.api_info (
    id SERIAL PRIMARY KEY,
    api_name VARCHAR(100) NOT NULL,
    api_base_url VARCHAR(255) NOT NULL
)

CREATE TABLE extract.api_import_log (
    id SERIAL PRIMARY KEY,
    country_id INT NOT NULL,
    api_id INT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    code_response INT,
    error_message TEXT,
    FOREIGN KEY (country_id) REFERENCES extract.country(id),
    FOREIGN KEY (api_id) REFERENCES extract.api_info(id)
)

-- Inserting the api information
INSERT INTO extract.api_info (api_name, api_base_url)
VALUES ('Weather API', 'https://historical-forecast-api.open-meteo.com/v1/forecast'),
	   ('COVID API', 'https://covid-api.com/api/reports/total');

INSERT INTO extract.country (code, name, latitude, longitude, capital)
VALUES ('USA', 'United States of America', 38.8951, -77.0364, 'Washington D.C.'),
	   ('DEU', 'Germany', 52.5200, 13.4050, 'Berlin'),
	   ('JPN', 'Japan', 35.6895, 139.6917, 'Tokyo');


