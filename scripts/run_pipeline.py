import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline.ingest import ingest_csv_to_sqlite
from pipeline.transform import transform_flight_data
from pipeline.analytics import run_analytics

csv_path = "data/flights.csv"
db_path = "db/flight_data.db"

ingest_csv_to_sqlite(csv_path, db_path)
transform_flight_data(db_path)
run_analytics(db_path)
