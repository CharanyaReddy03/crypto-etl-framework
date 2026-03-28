import pandas as pd
import logging
from sqlalchemy import create_engine, text

# Airflow logger
logger = logging.getLogger("airflow.task")

def run_bitcoin_transformation():
    input_path = "/usr/local/airflow/include/data/bitstamp.csv"
    
    logger.info("Step 1: Extracting data from CSV...")
    df = pd.read_csv(input_path)
    print(df.columns)
    df.columns = df.columns.str.strip()
    logger.info("Step 2: Transforming data...")

    # Convert timestamp
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

    # Fill missing values
    df = df.ffill()

    # Drop null rows
    df.dropna(inplace=True)

    logger.info(f"Transformation complete. Row count: {len(df)}")

    # Step 3: Load to Postgres
    conn_string = "postgresql://postgres:postgres@postgres:5432/postgres"
    engine = create_engine(conn_string)

    load_df = df.tail(100000)

    min_ts = load_df['Timestamp'].min()
    max_ts = load_df['Timestamp'].max()

    logger.info(f"Step 4: Syncing data from {min_ts} to {max_ts}...")

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM cleaned_bitcoin WHERE \"Timestamp\" BETWEEN :min AND :max"),
            {"min": min_ts, "max": max_ts}
        )

        load_df.to_sql("cleaned_bitcoin", conn, if_exists='append', index=False)

    logger.info("Successfully synced data to database without duplicates.")