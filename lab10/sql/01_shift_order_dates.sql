-- Shift AdventureWorksLT sample order dates into the range of gold.dim_date (2020-2030).
-- The WHERE clause makes the script safe to re-run.

UPDATE SalesLT.SalesOrderHeader
SET OrderDate = DATEADD(YEAR, 17, OrderDate),
    DueDate   = DATEADD(YEAR, 17, DueDate),
    ShipDate  = DATEADD(YEAR, 17, ShipDate)
WHERE YEAR(OrderDate) < 2020;