WITH
staged AS (
    SELECT * FROM {{ ref('stg_logistics_events') }}
),

flagged AS (
    SELECT
        *,

        -- Boolean flags (TRUE = rule violated)
        (weight_kg IS NULL OR weight_kg <= 0)                            AS is_invalid_weight,
        (latitude IS NULL OR longitude IS NULL)                          AS is_missing_coordinates,
        (estimated_delivery IS NULL
            OR estimated_delivery < timestamp)                           AS is_invalid_delivery,
        (status IS NULL OR status = '')                                  AS is_missing_status,
        (origin IS NULL OR origin = ''
            OR destination IS NULL OR destination = '')                  AS is_missing_location

    FROM staged
),

invalid_rows AS (

    SELECT *
    FROM flagged
    WHERE
        is_invalid_weight
        OR is_missing_coordinates
        OR is_invalid_delivery
        OR is_missing_status
        OR is_missing_location

),

labelled AS (

    SELECT
        event_id,
        order_id,
        estimated_delivery,
        status,
        origin,
        destination,
        carrier_name,
        latitude,
        longitude,
        weight_kg,
        ingestion_timestamp,

        -- Derive reason via priority CASE
        CASE
            WHEN is_invalid_weight        THEN 'INVALID_WEIGHT'
            WHEN is_missing_coordinates   THEN 'MISSING_COORDINATES'
            WHEN is_invalid_delivery      THEN 'INVALID_ESTIMATED_DELIVERY'
            WHEN is_missing_status        THEN 'MISSING_STATUS'
            WHEN is_missing_location      THEN 'MISSING_LOCATION'
        END AS invalid_reason

    FROM invalid_rows

)

SELECT * FROM labelled
