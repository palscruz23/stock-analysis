{{ config(materialized='table') }}

SELECT
    *
FROM   
    {{ ref('int_price_features') }}
LIMIT 1