from load.data_loader import DataLoader

def l_routine(db: DataLoader):
    db.merge_dim_country()
    db.merge_dim_date()
    db.merge_dim_weather_description()

    db.merge_fact_covid()
    db.merge_fact_weather()

    db.close_connection()
