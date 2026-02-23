import duckdb
import sys
from datetime import datetime, timedelta

def run_dq_checks():
    # Connect to the same database file
    import os
    # os.makedirs('src/warehouse/data', exist_ok=True)
    con = duckdb.connect('src/warehouse/data/warehouse.duckdb')
    
    # Configure S3 access (same as your init script)
    con.execute("LOAD httpfs;")
    con.execute("""
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    print("--- Starting Data Quality Checks on Bronze Layer ---")

    # First, check if any files exist
    try:
        file_count = con.execute("""
            SELECT COUNT(*) FROM glob('s3://bronze/*.parquet')
        """).fetchone()[0]
        
        if file_count == 0:
            print("No parquet files found in s3://bronze/")
            print("Waiting for consumer to upload data...")
            sys.exit(0)
        
        print(f"Found {file_count} parquet files in bronze bucket\n")
    except Exception as e:
        print(f"Could not connect to MinIO: {e}")
        sys.exit(1)

    # Get total record count
    try:
        total_records = con.execute("""
            SELECT COUNT(*) FROM read_parquet('s3://bronze/*.parquet')
        """).fetchone()[0]
        print(f"Total records in bronze: {total_records}\n")
    except Exception as e:
        print(f"Could not read parquet files: {e}")
        sys.exit(1)

    if total_records == 0:
        print("Bronze layer is empty. No records to validate.")
        sys.exit(0)

    print("Running validation checks...\n")

    errors = 0
    warnings = 0

    # Test 1: Check for Null Order IDs (Critical)
    try:
        null_orders = con.execute("""
            SELECT COUNT(*) FROM read_parquet('s3://bronze/*.parquet')
            WHERE order_id IS NULL OR order_id = ''
        """).fetchone()[0]

        if null_orders > 0:
            print(f"Found {null_orders} rows with NULL/empty Order IDs")
            errors += 1
        else:
            print(f"No NULL order IDs found ({total_records} records checked)")
    except Exception as e:
        print(f"Failed to check order IDs: {e}")
        errors += 1

    # Test 2: Check for unrealistic weights (Business Logistics Logic)
    try:
        invalid_weights = con.execute("""
            SELECT COUNT(*) FROM read_parquet('s3://bronze/*.parquet')
            WHERE weight_kg IS NULL OR weight_kg <= 0 OR weight_kg > 5000
        """).fetchone()[0]

        if invalid_weights > 0:
            print(f"Found {invalid_weights} rows with invalid weights (≤0 or >5000kg)")
            errors += 1
        else:
            print(f"All weights are valid (0 < weight ≤ 5000kg)")
    except Exception as e:
        print(f"Failed to check weights: {e}")
        errors += 1

    # Test 3: Check for malformed timestamps
    try:
        malformed_dates = con.execute("""
            SELECT COUNT(*) FROM read_parquet('s3://bronze/*.parquet')
            WHERE timestamp IS NULL OR TRY_CAST(timestamp AS TIMESTAMP) IS NULL
        """).fetchone()[0]

        if malformed_dates > 0:
            print(f"Found {malformed_dates} rows with malformed timestamps")
            errors += 1
        else:
            print(f"All timestamps are valid")
    except Exception as e:
        print(f"Failed to check timestamps: {e}")
        errors += 1

    # Test 4: Invalid Status Values (ENUM VALIDATION)
    try:
        valid_statuses = ["Order Created", "Picked Up", "In Transit", "Out for Delivery", "Delivered", "Delayed"]
        status_placeholders = ", ".join([f"'{s}'" for s in valid_statuses])
        
        invalid_status = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('s3://bronze/*.parquet')
            WHERE status NOT IN ({status_placeholders})
        """).fetchone()[0]

        if invalid_status > 0:
            print(f"Found {invalid_status} rows with invalid status values")
            errors += 1
        else:
            print(f"All status values are valid")
    except Exception as e:
        print(f"Failed to check statuses: {e}")
        errors += 1

    # Test 5: Coordinate Validation (GEOGRAPHY)
    try:
        invalid_coords = con.execute("""
            SELECT COUNT(*) FROM read_parquet('s3://bronze/*.parquet')
            WHERE latitude IS NULL OR longitude IS NULL 
               OR latitude < -90 OR latitude > 90 
               OR longitude < -180 OR longitude > 180
        """).fetchone()[0]

        if invalid_coords > 0:
            print(f"Found {invalid_coords} rows with invalid coordinates")
            errors += 1
        else:
            print(f"All coordinates are valid (lat: -90 to 90, lon: -180 to 180)")
    except Exception as e:
        print(f"Failed to check coordinates: {e}")
        errors += 1

    # Test 6: Duplicate Event IDs
    try:
        duplicate_events = con.execute("""
            SELECT COUNT(*) as cnt FROM (
                SELECT event_id, COUNT(*) as dup_count 
                FROM read_parquet('s3://bronze/*.parquet')
                WHERE event_id IS NOT NULL
                GROUP BY event_id
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]

        if duplicate_events > 0:
            print(f"Found {duplicate_events} duplicate event IDs")
            warnings += 1
        else:
            print(f"No duplicate event IDs found")
    except Exception as e:
        print(f"Failed to check duplicates: {e}")
        errors += 1

    # Test 7: Delivery Date Validation ---
    try:
        future_deliveries = con.execute("""
            SELECT COUNT(*) FROM read_parquet('s3://bronze/*.parquet')
            WHERE TRY_CAST(estimated_delivery AS DATE) IS NULL
               OR TRY_CAST(estimated_delivery AS DATE) < CURRENT_DATE
        """).fetchone()[0]

        if future_deliveries > 0:
            print(f"Found {future_deliveries} rows with past/invalid delivery dates")
            warnings += 1
        else:
            print(f"All delivery dates are valid and in future")
    except Exception as e:
        print(f"Failed to check delivery dates: {e}")
        errors += 1

    # Summary of results
    print("\n--- Data Quality Check Summary ---")
    print(f"Total Records Checked: {total_records}")
    print(f"Errors Found: {errors}")
    print(f"Warnings Found: {warnings}")
    print()

    if errors > 0:
        print("Data Quality checks FAILED")
        print("Pipeline halted. Fix data quality issues before proceeding.")
        con.close()
        sys.exit(1)

    elif warnings > 0:
        print("Data Quality checks PASSED with WARNINGS")
        print("Proceeding with caution. Review warnings above.")
        con.close()
        sys.exit(0)

    else:
        print("All Data Quality checks PASSED")
        print("Proceeding to dbt transformations...")
        con.close()
        sys.exit(0)

if __name__ == "__main__":
    run_dq_checks()