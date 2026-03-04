import duckdb
import os
import time

MINIO_ENDPOINT   = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
DB_PATH          = os.getenv('DUCKDB_PATH', '/opt/airflow/warehouse/data/warehouse.duckdb')

# Retry connection up to 5 times in case of lock conflicts
con = None
for attempt in range(5):
    try:
        con = duckdb.connect(DB_PATH)
        print(f"Connected to DuckDB at {DB_PATH}")
        break
    except duckdb.IOException as e:
        if 'lock' in str(e).lower() and attempt < 4:
            print(f"DuckDB locked, retrying in 10s... (attempt {attempt + 1}/5)")
            time.sleep(10)
        else:
            raise

try:
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{MINIO_ENDPOINT.replace('http://', '')}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    # See the exact filenames
    files = con.execute("SELECT * FROM glob('s3://bronze/*.parquet')").fetchall()
    print(f"Files in bucket: {len(files)}")

    if len(files) == 0:
        print("No parquet files found in MinIO bronze bucket. Exiting.")
        exit(0)

    # Create or update raw_logistics table
    table_exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_name = 'raw_logistics'
    """).fetchone()[0]

    if not table_exists:
        print("First run — creating raw_logistics table and loading all data...")
        con.execute("""
            CREATE TABLE raw_logistics AS
            SELECT 
                src.*,
                current_localtimestamp() AS ingestion_timestamp
            FROM read_parquet('s3://bronze/*.parquet') AS src;
        """)
        print("Bronze table created.")
    else:
        print("Table exists — checking for new rows...")
        new_rows = con.execute("""
            INSERT INTO raw_logistics
            SELECT 
                src.*,
                current_localtimestamp() AS ingestion_timestamp
            FROM read_parquet('s3://bronze/*.parquet') AS src
            WHERE src.event_id NOT IN (
                SELECT event_id FROM raw_logistics
            );
        """).rowcount
        print(f"{new_rows} new row(s) inserted into raw_logistics.")

    print("Done.")
    print(con.execute("SELECT COUNT(*) FROM raw_logistics").fetchall())

finally:
    if con:
        con.close()
        print("DuckDB connection closed.")