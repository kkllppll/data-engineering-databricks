-- Changes the shop application would normally make. Run after the initial load.

-- new customer (password columns are required by the table, values are not used)
INSERT INTO SalesLT.Customer (Title, FirstName, LastName, CompanyName, SalesPerson, EmailAddress, Phone, PasswordHash, PasswordSalt)
VALUES ('Mr.', 'Marcus', 'Lindqvist', 'Northern Trail Cycles', 'adventure-works\jae0', 'marcus0@adventure-works.com', '425-555-0187', 'not-used', 'not-used');

-- one update
UPDATE SalesLT.Customer
SET EmailAddress = 'orlando.gee@adventure-works.com', ModifiedDate = GETDATE()
WHERE CustomerID = 1;

-- two updates of the same customer before the next merge
-- the pause keeps the two versions from getting the same timestamp
UPDATE SalesLT.Customer
SET Phone = '170-555-0199', ModifiedDate = GETDATE()
WHERE CustomerID = 2;

WAITFOR DELAY '00:00:02';

UPDATE SalesLT.Customer
SET CompanyName = 'Progressive Sports Ltd', ModifiedDate = GETDATE()
WHERE CustomerID = 2;

-- delete, addresses first because of the foreign key
DELETE FROM SalesLT.CustomerAddress WHERE CustomerID = 3;
DELETE FROM SalesLT.Customer WHERE CustomerID = 3;