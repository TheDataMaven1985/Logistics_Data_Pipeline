import duckdb

# Connect to (or create) your local database file
con = duckdb.connect('data/warehouse.duckdb')

# Install and load the httpfs extension to talk to S3/MinIO
con.execute("INSTALL httpfs; LOAD httpfs;")

# Configure DuckDB to talk to your local MinIO
# Since it's local, we disable SSL and use 'path' style
con.execute("""
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

# See the exact filenames
files = con.execute("SELECT * FROM glob('s3://bronze/*.parquet')").fetchall()
print(f"Files in bucket: {len(files)}")

# create the table and load all existing data
# Subsequent runs: only insert rows that haven't been ingested yet
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

print("DuckDB is connected to MinIO. Bronze table 'raw_logistics' created.")
print(con.execute("SELECT COUNT(*) FROM raw_logistics").fetchall())