# AMDARIS 2025 Data Engineering Internship project

The project was completed as part of the 2025 Data Engineering Internship at AMDARIS, under the supervision of Marius Purici. It implements an ETL (Extract, Transform, Load) pipeline designed to store and process COVID-19 and Weather data for a given country and date. It is organized into modular components and is intended to be maintainable and scalable.

## 📖 Overview
1) 📁 [Project structure](#-project-structure)
2) ⚙️ [Tools](#-tools)
3) 🌐 [API Details](#-api-details)
4) 🗃️ [Database](#-database)
5) 🔄 [ETL Overview](#-etl-overview)
6) ▶️ [Running the ETL](#-running-the-etl)

## 📁 Project Structure
📁 **internship_etl/**
├── 📁 **data/** - Storage for all data files
│   ├── 📁 **raw/** - Files extracted from APIs
│   │   ├── 📁 covid_data/
│   │   └── 📁 weather_data/
│   ├── 📁 **processed/** - Successfully transformed files
│   │   ├── 📁 covid_data/
│   │   └── 📁 weather_data/
│   └── 📁 **error/** - Files that failed during processing
│       ├── 📁 covid_data/
│       └── 📁 weather_data/
│
├── 📁 **db/** - SQL files and DB-related scripts
│   ├── create_tables.sql
│   └── seed_data.sql
│
├── 📁 **src/** - Source code for the ETL
│   ├── extract.py
│   ├── transform.py
│   └── load.py
│
├── 📁 **tests/** - Unit tests
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
│
├── 📄 **main.py** - Entrypoint to run the full pipeline
├── 📄 **requirements.txt** - Python dependencies
└── 📄 **README.md** - Project documentation

