with 
staged AS (
    SELECT * FROM {{ ref('stg_logistics_events') }}
),

validated AS (

    SELECT *
    FROM staged
    WHERE
        -- Rule 1: positive weight
        weight_kg > 0

        -- Rule 2: coordinates present
        AND latitude  IS NOT NULL
        AND longitude IS NOT NULL

        -- Rule 3: delivery cannot be before the event
        AND estimated_delivery >= timestamp

        -- Rule 4: status present
        AND status IS NOT NULL
        AND status <> ''

        -- Rule 5: origin & destination present
        AND origin      IS NOT NULL AND origin      <> ''
        AND destination IS NOT NULL AND destination <> ''

)

SELECT * FROM validated