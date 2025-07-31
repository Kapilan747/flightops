import pandas as pd
from sqlalchemy import create_engine

def run_analytics(db_path):
    engine = create_engine(f"sqlite:///{db_path}")

    print("\nTop 5 Most Delayed Routes:")
    q1 = """
    SELECT origin, dest, ROUND(AVG(arr_delay), 2) AS avg_arr_delay, COUNT(*) as flights
    FROM clean_flights
    GROUP BY origin, dest
    ORDER BY avg_arr_delay DESC
    LIMIT 5
    """
    print(pd.read_sql(q1, engine))

    print("\nTop Airlines by Delay %:")
    q2 = """
    SELECT carrier, COUNT(*) as total_flights,
           SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS delay_percent
    FROM clean_flights
    GROUP BY carrier
    ORDER BY delay_percent DESC
    LIMIT 5
    """
    print(pd.read_sql(q2, engine))
