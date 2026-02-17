{{
  config(
    materialized='table',
    file_format='delta',
  )
}}

SELECT
  *
FROM
  delta.`/opt/spark/work-dir/data/bronze/inventory`

