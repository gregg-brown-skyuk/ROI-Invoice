CREATE OR REPLACE TABLE
  `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union_backup` AS
SELECT
  *
FROM
  `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union`;
  
CREATE OR REPLACE TABLE
  `skyuk-uk-csgbillanalysis-dev.roi_rental.2_roi_siro_union_backup` AS
SELECT
  *
FROM
  `skyuk-uk-csgbillanalysis-dev.roi_rental.2_roi_siro_union`;