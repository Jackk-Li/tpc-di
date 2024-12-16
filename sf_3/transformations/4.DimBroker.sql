-- DimBroker
INSERT INTO master.DimBroker (IsCurrent, EffectiveDate, EndDate, BatchID, BrokerID, ManagerID, FirstName, LastName, MiddleInitial, Branch, Office, Phone)
SELECT
    TRUE AS IsCurrent,
    (SELECT MIN(DateValue) FROM master.DimDate) AS EffectiveDate,
    '9999-12-31' AS EndDate,
    1 AS BatchID,
    EmployeeID AS BrokerID,
    ManagerID AS ManagerID,
    EmployeeFirstName AS FirstName,
    EmployeeLastName AS LastName,
    EmployeeMI AS MiddleInitial,
    EmployeeBranch AS Branch,
    EmployeeOffice AS Office,
    EmployeePhone AS Phone
FROM staging.HR
WHERE EmployeeJobCode = 314;
