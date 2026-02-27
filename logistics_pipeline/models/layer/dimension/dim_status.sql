WITH

statuses AS (

    SELECT DISTINCT status AS status_name
    FROM {{ ref('int_valid_logistics_events') }}
    WHERE status IS NOT NULL
      AND status <> ''

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['status_name']) }} AS status_sk,
        status_name
    FROM statuses

)

SELECT * FROM final