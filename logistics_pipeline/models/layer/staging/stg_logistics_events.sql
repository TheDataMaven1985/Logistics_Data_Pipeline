with 
raw AS(
    SELECT *
    FROM {{ source('logistics_pipeline', 'raw_logistics') }}
),

casted AS (
    SELECT 
        TRIM(CAST(event_id AS STRING)) AS event_id,
        TRIM(CAST(order_id AS STRING)) AS order_id,
        TRIM(CAST(timestamp AS STRING)) AS timestamp,
        TRIM(CAST(estimated_delivery AS STRING)) AS estimated_delivery,
        TRIM(CAST(status AS STRING)) AS status,
        TRIM(CAST(origin AS STRING)) AS origin,
        TRIM(CAST(destination AS STRING)) AS destination,
        TRIM(CAST(carrier_name AS STRING)) AS carrier_name,
        CAST(latitude AS FLOAT) AS latitude,
        CAST(longitude AS FLOAT) AS longitude,
        CAST(weight_kg AS FLOAT) AS weight_kg,
        current_localtimestamp() AS ingestion_timestamp
    
    FROM raw
),

cleaned AS  (
    SELECT *
    FROM casted
    WHERE event_id IS NOT NULL
      AND order_id IS NOT NULL
      -- Guard against empty-string IDs after trimming
      AND event_id <> ''
      AND order_id <> ''
)

SELECT * FROM cleaned