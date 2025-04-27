{{ config(
   materialized='table',
   schema='datamart'
) }}

SELECT *
FROM {{ source('classicmodels', 'customers') }}