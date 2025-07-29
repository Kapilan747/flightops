import pandas as pd
from sqlalchemy import create_engine

def ingest_csv_to_sqlite(csv_path, db_path):
    engine = create_engine(f"sqlite:///{db_path}")
    
    chunksize = 10000
    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        chunk.to_sql("raw_flights", con=engine, if_exists="append", index=False)
    
    print("Ingestion complete.")
