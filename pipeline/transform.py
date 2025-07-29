import pandas as pd
from sqlalchemy import create_engine

def transform_flight_data(db_path):
    engine = create_engine(f"sqlite:///{db_path}")
    
    df = pd.read_sql("SELECT * FROM raw_flights", con=engine)

    df['flight_date'] = pd.to_datetime(df[['year', 'month', 'day']])
    df['dep_delay'] = df['dep_delay'].fillna(0)
    df['arr_delay'] = df['arr_delay'].fillna(0)
    df['is_delayed'] = df['arr_delay'] > 15
    df = df.dropna(subset=['origin', 'dest', 'carrier', 'flight'])

    df.to_sql("clean_flights", con=engine, if_exists="replace", index=False)
    print("✅ Transformation complete.")
