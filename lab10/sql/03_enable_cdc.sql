-- Enable SQL Server CDC on the database and on SalesLT.Customer.
-- Password-related and technical columns are excluded so they never reach the lakehouse.
EXEC sys.sp_cdc_enable_db;

EXEC sys.sp_cdc_enable_table
    @source_schema         = N'SalesLT',
    @source_name           = N'Customer',
    @role_name             = NULL,
    @supports_net_changes  = 0,
    @captured_column_list  = N'CustomerID, Title, FirstName, MiddleName, LastName, Suffix,
                               CompanyName, SalesPerson, EmailAddress, Phone, ModifiedDate';

-- Change tables live in the cdc schema created above; the federation user needs read access to it.
GRANT SELECT ON SCHEMA::cdc TO databricks_reader;