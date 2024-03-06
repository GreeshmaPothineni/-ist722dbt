WITH f_order_fulfillment AS (
    SELECT * FROM {{ ref('fact_order_fulfillment') }}
),
d_customer AS (
    SELECT * FROM {{ ref('dim_customer') }}
),
d_employee AS (
    SELECT * FROM {{ ref('dim_employee') }}
),
d_date AS (
    SELECT * FROM {{ ref('dim_date') }}
)
SELECT
    d_customer.*, 
    d_employee.*, 
    d_date.*,
    f.orderid, f.orderdatekey, f.shippeddatekey, f.requireddatekey, f.shipname, f.shipaddress, f.shipcity, f.shipregion, f.shippostalcode, f.shipcountry,
    f.freight, f.shipvia, f.shippercompanyname, f.quantityonorder, f.totalorderamount, f.daysfromordertoshipped, f.daysfromordertorequired, f.shippedtorequireddelta, f.shippedontime
FROM f_order_fulfillment f
LEFT JOIN d_customer ON f.customerkey = d_customer.customerkey
LEFT JOIN d_employee ON f.employeekey = d_employee.employeekey
LEFT JOIN d_date ON f.orderdatekey = d_date.datekey

