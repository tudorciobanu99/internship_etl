# AMDARIS 2025 Data Engineering Internship project

The project was completed as part of the 2025 Data Engineering Internship at AMDARIS, under the supervision of Mentor Marius Purici. It implements an ETL (Extract, Transform, Load) pipeline designed to store and process COVID-19 and Weather data for a given country and date. It is organized into modular components and is intended to be maintainable and scalable.

## 📖 Overview
1) 📁 [Project structure](#-project-structure)
2) ⚙️ [Tools](#-tools)
3) 🌐 [API Details](#-api-details)
4) 🗃️ [Database](#-database)
5) 🔄 [ETL Overview](#-etl-overview)
6) ▶️ [Running the ETL](#-running-the-etl)

## 📁 Project Structure
<pre>
📁 internship_etl/
├── 📁 common/
│   ├── database_connector.py - Super class that handles the connection to the database
│   └── utils.py - Common functions reused in other modules
├── 📁 data/ - Storage for all data files
│   ├── 📁 raw/ - Files extracted from APIs
│   │   ├── 📁 covid_data/
│   │   └── 📁 weather_data/
│   ├── 📁 processed/ - Successfully transformed files
│   │   ├── 📁 covid_data/
│   │   └── 📁 weather_data/
│   └── 📁 error/ - Files that failed during processing
│       ├── 📁 covid_data/
│       └── 📁 weather_data/
├── 📁 database/ - SQL files and DB-related scripts
│   ├── 🗃️ extract_schema.sql - Creates the extract schema and related tables
│   ├── 🗃️ transform_schema.sql - Creates the transform schema and related tables
│   └── 🗃️ load_schema.sql - Creates the load schema and related tables
├── 📁 extract/
│   ├── 📄 covid_api.py - API wrapper class that handles the extraction of COVID-19 data
│   ├── 📄 data_extractor.py - Inherits the DatabaseConnector class and handles additional logic
│   │                          for the interaction with data in the extract schema
│   ├── 📄 extract.py - Handles the extract routine of the ETL
│   └── 📄 weather_api.py - API wrapper class that handles the extraction of weather data
├── 📁 load/
│   ├── 📄 data_loader.py - Inherits the DatabaseConnector class and handles additional logic
│   │                       for the interaction with data in the load schema
│   └── 📄 load.py - Handles the load routine of the ETL
├── 📁 streamlit/ - Data visualization with Streamlit
│   ├── 📄 dashboard.py - Page configuration and UI
│   ├── 📄 data_page.py - Generates visual representations related to COVID-19 and Weather data
│   └── 📄 log_page.py - Generates visual representations related to import and transform logs
├── 📁 transform/
│   ├── 📄 data_transformer.py - Inherits the DatabaseConnector class and handles additional logic
│   │                            for the interaction with data in the transform schema
│   └── 📄 transform.py - Handles the transform routine of the ETL
├── 📁 weather_description/
│   └── 🧾 wmo_code_4677.csv - Provides the description for WMO 4677 codes
├── 🔒 .gitignore
├── 🗒️ README.md - Project documentation
├── 📄 etl.py - Entrypoint to run the full pipeline
└── 🗒️ requirements.txt - Python dependencies
</pre>

## ⚙️ Tools
Whilst the project is built primarily on the Python Standard Library, several additional tools were used to support development, data processing, and visualization. Below is a list of the key tools used.
- [PostgreSQL](https://www.postgresql.org/) - The relational database management system of choice.
- [psycopg2](https://www.psycopg.org/) - The most popular PostgreSQL adapter for Python.
- [requests](https://requests.readthedocs.io/) - The HTTP library for Python.
- [pandas](https://pandas.pydata.org/) - Data analysis and manipulation tool.
- [Streamlit](https://streamlit.io/) - Transforms Python scripts into interactive web apps to build data dashboards.
- [Plotly](https://plotly.com/python/plotly-express/) - Graphing lirary for interactive charts.


