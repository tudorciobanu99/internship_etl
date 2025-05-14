import os
import argparse
from datetime import datetime
from dotenv import load_dotenv
from extract.weather_api import WeatherAPI
from extract.covid_api import CovidAPI
from extract.extract import e_routine
from extract.data_extractor import DataExtractor
from transform.transform import t_routine
from transform.data_transformer import DataTransformer
from load.load import l_routine
from load.data_loader import DataLoader

def initialize_database_objects(**db_config):
    """
    Initializes a DataExtractor, DataTransformer and DataLoader object.

    Args:
        **db_config (dict): PostgreSQL database connection parameters.
                Keys should include:
                    dbname (str): The name of the database.
                    user (str): The username for authentication.
                    host (str): The database server host.
                    password (str): The password for authentication.
                    port (int): The port number.
    """

    e_db = DataExtractor(**db_config)
    t_db = DataTransformer(**db_config)
    l_db = DataLoader(**db_config)
    return e_db, t_db, l_db

def add_countries(e_db:DataExtractor):
    """
    Asks the user if a new country is to be added.
    It follows the following scheme:
        1) The user is asked if a new country is to be added.
            Answering n would skip adding new countries.
        2) The user is asked to provide the ISO code, name and
            coordinates. If the coordinates are in an invalid
            format, the user is asked whether to be repeat the
            entire process or simply abort the operation.
        3) If the user provides legitimate values for a new
            country, the user is again asked if a new country is
            to be added, or skip adding more countries.

    Args:
        e_db (DataExtractor object)
    """

    add = input("Do you want to add a country? (y/n): ").strip().lower()
    if add != 'y':
        print("Skipping country addition.\n")
        return

    print("\n-- Add New Countries --")
    while True:
        code = input("Enter ISO code (e.g., MDA): ").strip()
        name = input("Enter country name: ").strip()
        try:
            latitude = float(input("Enter latitude (e.g. 47.0036): "))
            longitude = float(input("Enter longitude (e.g. 28.9071): "))
        except ValueError:
            print("Invalid coordinates!")
            cancel = input("Do you want to cancel the operation? (y/n): ").strip().lower()
            if cancel == 'y':
                print("Abort country addition process.\n")
                return
            else:
                print("Let's try again.\n")
                continue

        e_db.add_country((code, name, latitude, longitude))
        print(f"Added: {name} ({code})\n")

        cont = input("Add another? (y/n): ").strip().lower()
        if cont != 'y':
            print("Finished adding countries.\n")
            break

def main():
    """"
    Completes the entire ETL routine or specific modules.
    A parser takes care of selecting the processes that are to be run
    by providing --process (extract, transform, load or all)
    By default, the entire ETL routine will be executed.
    Another parser determines the date for which the data from the API
    must be extracted, by providing --date. If the format is invalid or
    the date is not provided, the date is defaulted to the given date of
    execution of the script but in 2022.
    Depending on the choices of the parser, the function will execute
    the corresponding actions.
    """

    parser = argparse.ArgumentParser(description="-- Run ETL modules --")
    parser.add_argument(
        "--process",
        choices = ["extract", "transform", "load", "all"],
        default = "all",
        help = "Specify which ETL module to run: extract, transform, load, or all."
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Specify the date in the YYYY-MM-DD format: "
    )
    args = parser.parse_args()

    batch_date = datetime.now().strftime("%Y-%m-%d")

    try:
        if args.date:
            date = datetime.strptime(args.date, "%Y-%m-%d").strftime("%Y-%m-%d")
        else:
            date = batch_date.replace("2025", "2022")
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD. Defaults to today's date but in 2022!")

    # Load the PostgreSQL database connection parameters from a .env file,
    # located in the root directory of the project.
    load_dotenv('database.env')

    # Constructing a db_config dictionary from the extracted parameters from
    # the .env file.
    db_config = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "host": os.environ.get("HOST"),
        "password": os.environ.get("PASSWORD"),
        "port": int(os.environ.get("PORT")),
    }

    # Initialize the required database objects for the ETL.
    e_db, t_db, l_db = initialize_database_objects(**db_config)

    # Prompts the user to add more countries if necessary
    # add_countries(e_db)

    print('ETL process begins...')

    if args.process in ("extract", "all"):
        print("Starting extract process...")

        # Fetches the information about the APIs.
        api_info = e_db.fetch_api_information()

        # Initialize the respective weather API object.
        weather_api_info = api_info[api_info["api_name"] == "Weather API"]
        weather_api = WeatherAPI(
            api_id=weather_api_info["id"].values[0],
            base_url=weather_api_info["api_base_url"].values[0]
        )

        # Initialize the respective COVID-19 API object.
        covid_api_info = api_info[api_info["api_name"] == "COVID API"]
        covid_api = CovidAPI(
            api_id=covid_api_info["id"].values[0],
            base_url=covid_api_info["api_base_url"].values[0]
        )

        # Fetch the countries that are going to be used for data extraction.
        countries = e_db.fetch_countries()

        # The extract process of the ETL.
        e_routine(weather_api, covid_api, e_db, countries, date)
        print("Extract process completed.")

    if args.process in ("transform", "all"):
        print("Starting transform process...")
        countries = t_db.fetch_countries()

         # The transform process of the ETL.
        t_routine(countries, t_db)
        print("Transform process completed.")

    if args.process in ("load", "all"):
        print("Starting load process...")

         # The load process of the ETL.
        l_routine(l_db)
        print("Load process completed.")

    print('ETL process has been completed!')

if __name__ == "__main__":
    main()
