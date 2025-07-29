import argparse
from pipeline.ingest import ingest_csv_to_sqlite
from pipeline.transform import transform_flight_data
from pipeline.analytics import run_analytics

def main():
    parser = argparse.ArgumentParser(description="FlightOps Data Pipeline CLI")
    parser.add_argument("stage", choices=["ingest", "transform", "analyze", "all"], help="Pipeline stage to run")
    args = parser.parse_args()

    csv_path = "data/flights.csv"
    db_path = "db/flight_data.db"

    if args.stage == "ingest":
        ingest_csv_to_sqlite(csv_path, db_path)
    elif args.stage == "transform":
        transform_flight_data(db_path)
    elif args.stage == "analyze":
        run_analytics(db_path)
    elif args.stage == "all":
        ingest_csv_to_sqlite(csv_path, db_path)
        transform_flight_data(db_path)
        run_analytics(db_path)

if __name__ == "__main__":
    main()
