WITH

timestamps AS (

    SELECT DISTINCT timestamp AS full_timestamp
    FROM {{ ref('int_valid_logistics_events') }}

),

expanded AS (
    SELECT
        full_timestamp,
        CAST(full_timestamp AS TIMESTAMP)                                    AS ts,
        CAST(full_timestamp AS DATE)                                         AS date,
        EXTRACT(YEAR  FROM CAST(full_timestamp AS TIMESTAMP))                AS year,
        EXTRACT(MONTH FROM CAST(full_timestamp AS TIMESTAMP))                AS month,
        EXTRACT(DAY   FROM CAST(full_timestamp AS TIMESTAMP))                AS day,
        EXTRACT(HOUR  FROM CAST(full_timestamp AS TIMESTAMP))                AS hour,
        EXTRACT(DOW   FROM CAST(full_timestamp AS TIMESTAMP))                AS day_of_week
    FROM timestamps
),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['full_timestamp']) }} AS time_sk,
        full_timestamp,
        date,
        year,
        month,
        day,
        hour,
        day_of_week
    FROM expanded

)

SELECT * FROM final