import os
from databaseconnection import databaseconnection
from dotenv import load_dotenv

def routine(db):
    # Merge data into dimension tables
    db.merge_dim_country()
    db.merge_dim_date()
    db.merge_dim_weather_description()

    # Merge data into fact tables
    db.merge_fact_covid()
    db.merge_fact_weather()

    # Close the database connection
    db.close_connection()

if __name__ == "__main__":
    load_dotenv('../database_password.env')

    my_db = databaseconnection(
        dbname="etl",
        user="postgres",
        host="localhost",
        password=os.environ.get("db_password"),
        port=5432,
    )

    # Execute the routine
    routine(my_db)
    print('Routine completed successfully!')

    