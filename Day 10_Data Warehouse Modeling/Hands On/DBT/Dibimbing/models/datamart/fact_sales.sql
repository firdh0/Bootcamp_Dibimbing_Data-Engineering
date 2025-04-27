{{ config(
   materialized='table',
   schema='datamart'
) }}

SELECT
   o.orderNumber,
   o.orderDate,
   o.requiredDate,
   o.shippedDate,
   o.status,
   o.comments,
   o.customerNumber,
   od.productCode,
   od.quantityOrdered,
   od.priceEach,
   od.orderLineNumber
FROM {{ source('classicmodels', 'orderdetails') }} AS od
LEFT JOIN {{ source('classicmodels', 'orders') }} AS o
   ON od.orderNumber = o.orderNumber

