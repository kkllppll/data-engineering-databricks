-- Read-only user for the Databricks Lakehouse Federation connection (least privilege).
-- The password is stored in Key Vault kvua5816bd as elina-sharabura-sql-reader-password;

CREATE USER databricks_reader WITH PASSWORD = '<password from Key Vault>';
ALTER ROLE db_datareader ADD MEMBER databricks_reader;