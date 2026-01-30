{{ config(materialized='table') }}

SELECT
    *,
    LEAD(CLOSE, 1) OVER (PARTITION BY TICKER ORDER BY DATETIME) AS NEXT_PRICE
FROM   
    {{ ref('int_price_features') }}