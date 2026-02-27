WITH

valid_events AS (

    SELECT * FROM {{ ref('int_valid_logistics_events') }}

),

orders AS (
    SELECT
        order_id,
        ANY_VALUE(destination)      AS destination,
        ANY_VALUE(origin)           AS origin,
        MIN(weight_kg)              AS weight_kg,
        MIN(timestamp)              AS first_event_timestamp,
        MAX(timestamp)              AS latest_event_timestamp
    FROM valid_events
    GROUP BY order_id
),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_sk,
        order_id,
        destination,
        origin,
        weight_kg,
        first_event_timestamp,
        latest_event_timestamp
    FROM orders

)

SELECT * FROM final