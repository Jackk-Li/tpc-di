-- Update the prospect table to set iscustomer = TRUE for active customers
UPDATE master.prospect p
JOIN (
    SELECT 
        p.lastname,
        p.firstname,
        p.addressline1,
        p.addressline2,
        p.postalcode
    FROM master.prospect p
    INNER JOIN master.dimcustomer c
        ON UPPER(c.lastname) = UPPER(p.lastname)
        AND UPPER(c.firstname) = UPPER(p.firstname)
        AND UPPER(c.addressline1) = UPPER(p.addressline1)
        AND UPPER(c.addressline2) = UPPER(p.addressline2)
        AND UPPER(c.postalcode) = UPPER(p.postalcode)
    WHERE c.status = 'ACTIVE'
      AND c.iscurrent = TRUE
) AS cac
    ON UPPER(p.lastname) = UPPER(cac.lastname)
    AND UPPER(p.firstname) = UPPER(cac.firstname)
    AND UPPER(p.addressline1) = UPPER(cac.addressline1)
    AND UPPER(p.addressline2) = UPPER(cac.addressline2)
    AND UPPER(p.postalcode) = UPPER(cac.postalcode)
SET p.iscustomer = TRUE;
