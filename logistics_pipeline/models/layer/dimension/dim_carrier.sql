WITH

carriers AS (

    SELECT DISTINCT carrier_name
    FROM {{ ref('int_valid_logistics_events') }}
    WHERE carrier_name IS NOT NULL
      AND carrier_name <> ''
),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['carrier_name']) }} AS carrier_sk,
        carrier_name
    FROM carriers

)

SELECT * FROM final
