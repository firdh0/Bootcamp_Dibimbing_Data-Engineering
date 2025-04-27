{{ config(
   materialized='view',
   schema='datamart'
) }}

SELECT
   orderDate,
   status,
   comments,
   customername,
   productname,
   quantityOrdered,
   priceEach * quantityOrdered AS totalSales
FROM {{ ref('fact_sales') }} AS s
LEFT JOIN {{ ref('dim_customers') }} AS c
   ON s.customerNumber = c.customerNumber
LEFT JOIN {{ ref('dim_products') }} AS p
   ON s.productCode = p.productCode


