WITH

invalid AS (
    SELECT
        CAST(ingestion_timestamp AS DATE) AS event_date,
        invalid_reason,
        event_id
    FROM {{ ref('int_invalid_logistics_events') }}
),

all_events AS (
    SELECT
        CAST(ingestion_timestamp AS DATE) AS event_date,
        COUNT(event_id)               AS total_events
    FROM {{ ref('stg_logistics_events') }}
    GROUP BY CAST(ingestion_timestamp AS DATE)
),

invalid_counts AS (

    SELECT
        event_date,
        invalid_reason,
        COUNT(event_id) AS invalid_count
    FROM invalid
    GROUP BY
        event_date,
        invalid_reason

),

summary AS (

    SELECT
        ic.event_date,
        ic.invalid_reason,
        ic.invalid_count,
        ae.total_events,

        -- Round to 2 decimal places; multiply by 100 for a readable percentage
        ROUND(
            CAST(ic.invalid_count AS FLOAT) / NULLIF(ae.total_events, 0) * 100,
            2
        ) AS pct_of_total_events

    FROM invalid_counts AS ic
    LEFT JOIN all_events AS ae
        ON ic.event_date = ae.event_date

)

SELECT
    event_date,
    invalid_reason,
    invalid_count,
    total_events,
    pct_of_total_events
FROM summary
ORDER BY
    event_date,
    invalid_reason