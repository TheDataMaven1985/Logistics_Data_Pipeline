WITH

valid_events AS (

    SELECT * FROM {{ ref('int_valid_logistics_events') }}

),

-- Pull in dimension surrogate keys to make downstream analysis easier and more performant
dim_order AS (

    SELECT order_sk, order_id, latest_event_timestamp
    FROM {{ ref('dim_order') }}

),

dim_time AS (

    SELECT time_sk, full_timestamp
    FROM {{ ref('dim_time') }}

),

dim_status AS (

    SELECT status_sk, status_name
    FROM {{ ref('dim_status') }}

),

dim_location AS (

    SELECT location_sk, location_name
    FROM {{ ref('dim_location') }}

),

dim_carrier AS (

    SELECT carrier_sk, carrier_name
    FROM {{ ref('dim_carrier') }}

),

-- Join all dimension keys onto valid events
joined AS (

    SELECT
        e.event_id,
        e.timestamp,
        e.estimated_delivery,
        e.weight_kg,

        -- Dimension surrogate keys
        ord.order_sk,
        tim.time_sk,
        sta.status_sk,
        loc_orig.location_sk   AS origin_location_sk,
        loc_dest.location_sk   AS destination_location_sk,
        car.carrier_sk,

        -- For late delivery calculation: compare estimated_delivery vs
        -- the latest event timestamp recorded for the order
        ord.latest_event_timestamp

    FROM valid_events AS e

    -- Order dimension
    LEFT JOIN dim_order AS ord
        ON e.order_id = ord.order_id

    -- Time dimension (match on exact timestamp)
    LEFT JOIN dim_time AS tim
        ON e.timestamp = tim.full_timestamp

    -- Status dimension
    LEFT JOIN dim_status AS sta
        ON e.status = sta.status_name

    -- Origin location
    LEFT JOIN dim_location AS loc_orig
        ON e.origin = loc_orig.location_name

    -- Destination location
    LEFT JOIN dim_location AS loc_dest
        ON e.destination = loc_dest.location_name

    -- Carrier dimension
    LEFT JOIN dim_carrier AS car
        ON e.carrier_name = car.carrier_name
),

-- Compute derived measures + surrogate key
final AS (

    SELECT
        -- Fact surrogate key
        {{ dbt_utils.generate_surrogate_key(['event_id']) }} AS event_sk,

        event_id,

        -- Foreign keys to dimensions
        order_sk,
        time_sk,
        status_sk,
        origin_location_sk,
        destination_location_sk,
        carrier_sk,

        -- Measures
        weight_kg,
        estimated_delivery,

        -- is_late_delivery: TRUE when the estimated delivery was before
        -- the latest event timestamp recorded for the same order.
        -- This signals the delivery likely ran late.
        CASE
            WHEN estimated_delivery < latest_event_timestamp
            THEN TRUE
            ELSE FALSE
        END AS is_late_delivery

    FROM joined

)

SELECT * FROM final