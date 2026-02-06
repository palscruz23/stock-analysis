{{ config(materialized='table') }}

SELECT
    *
FROM   
    {{ ref('int_price_features') }}
ORDER BY DATETIME DESC 
LIMIT 1