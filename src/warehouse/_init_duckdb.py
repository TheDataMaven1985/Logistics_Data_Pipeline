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

# Create a View that points to ALL parquet files in the bronze bucket
# This is the "Bronze" layer: raw data, no changes.
con.execute("""
    CREATE OR REPLACE VIEW raw_logistics AS
    SELECT *,
            current_localtimestamp() as ingestion_timestamp
    FROM read_parquet('s3://bronze/*.parquet');
""")

print("DuckDB is connected to MinIO. Bronze view 'raw_logistics' created.")
print(con.execute("SELECT COUNT(*) FROM raw_logistics").fetchall())