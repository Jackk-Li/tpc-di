USE master;
-- create index for prospect and customer
CREATE INDEX idx_prospect_name_addr ON master.prospect(lastname, firstname, addressline1, addressline2, postalcode);
CREATE INDEX idx_customer_name_addr ON master.dimcustomer(lastname, firstname, addressline1, addressline2, postalcode);

-- use temporary table to update prospect
CREATE TEMPORARY TABLE temp_matches AS
SELECT DISTINCT 
    p.lastname,
    p.firstname,
    p.addressline1,
    p.addressline2,
    p.postalcode
FROM master.prospect p
INNER JOIN master.dimcustomer c ON
    p.lastname = c.lastname AND
    p.firstname = c.firstname AND
    COALESCE(p.addressline1,'') = COALESCE(c.addressline1,'') AND
    COALESCE(p.addressline2,'') = COALESCE(c.addressline2,'') AND
    COALESCE(p.postalcode,'') = COALESCE(c.postalcode,'')
WHERE c.status = 'ACTIVE'
AND c.iscurrent = TRUE;

-- create index for temporary table
CREATE INDEX idx_temp_matches ON temp_matches(lastname, firstname, addressline1, addressline2, postalcode);

-- update prospect
UPDATE master.prospect p
INNER JOIN temp_matches m ON
    p.lastname = m.lastname AND
    p.firstname = m.firstname AND
    COALESCE(p.addressline1,'') = COALESCE(m.addressline1,'') AND
    COALESCE(p.addressline2,'') = COALESCE(m.addressline2,'') AND
    COALESCE(p.postalcode,'') = COALESCE(m.postalcode,'')
SET p.iscustomer = TRUE;

-- drop temporary table and index
DROP TABLE temp_matches;
DROP INDEX idx_prospect_name_addr ON master.prospect;
DROP INDEX idx_customer_name_addr ON master.dimcustomer;