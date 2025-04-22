from load.data_loader import DataLoader

def l_routine(db: DataLoader):
    """
    Attempts to complete the load part of the ETL.
    The process follows the scheme:
        1) The dimension tables are first merged.
        2) The fact tables are then merged.

    Args:
        db (DataLoader object)
    """

    db.merge_dim_country()
    db.merge_dim_date()
    db.merge_dim_weather_description()

    db.merge_fact_covid()
    db.merge_fact_weather()

    db.close_connection()
