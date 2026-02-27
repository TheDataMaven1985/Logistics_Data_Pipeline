WITH

origins AS (

    SELECT
        origin      AS location_name,
        latitude,
        longitude
    FROM {{ ref('int_valid_logistics_events') }}
    WHERE origin IS NOT NULL AND origin <> ''

),

destinations AS (

    SELECT
        destination AS location_name,
        latitude,
        longitude
    FROM {{ ref('int_valid_logistics_events') }}
    WHERE destination IS NOT NULL AND destination <> ''

),

all_locations AS (

    SELECT location_name, latitude, longitude FROM origins
    UNION ALL
    SELECT location_name, latitude, longitude FROM destinations

),

deduped AS (

    SELECT
        location_name,
        MIN(latitude)  AS latitude,
        MIN(longitude) AS longitude
    FROM all_locations
    GROUP BY location_name

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['location_name']) }} AS location_sk,
        location_name,
        latitude,
        longitude
    FROM deduped

)

SELECT * FROM final