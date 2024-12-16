-- DimBroker
INSERT INTO master.DimBroker (
    sk_brokerid,
    IsCurrent, 
    EffectiveDate, 
    EndDate, 
    BatchID, 
    BrokerID, 
    ManagerID, 
    FirstName, 
    LastName, 
    MiddleInitial, 
    Branch, 
    Office, 
    Phone
)
SELECT
    ROW_NUMBER() OVER (ORDER BY EmployeeID) AS sk_brokerid,
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