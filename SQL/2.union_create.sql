DECLARE
  TableSuffix DEFAULT LOWER(FORMAT_DATE("%b_%y", CURRENT_DATE));
CREATE OR REPLACE TABLE
  `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union`
PARTITION BY
  advanced_billing_date AS
SELECT
  advanced_billing_date,
  reseller_name,
  internal_number,
  order_number,
  original_ptt_no,
  new_ptt_no,
  record_type,
  notification_type,
  billing_point_date,
  billing_end_date,
  number_of_clis,
  notification_item,
  lead_cli,
  reference_cli,
  notification_item_description,
  chargeable_quantity,
  number_of_days,
  daily_amount,
  full_period_amount,
  signed_number
FROM
  `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union`
UNION ALL
SELECT
  DATE(advanced_billing_date),
  reseller_name,
  internal_number,
  order_number,
  original_ptt_no,
  new_ptt_no,
  record_type,
  notification_type,
  DATE(billing_point_date),
  DATE(billing_end_date),
  number_of_clis,
  notification_item,
  CAST(lead_cli AS string),
  CAST(reference_cli AS string),
  notification_item_description,
  chargeable_quantity,
  number_of_days,
  daily_amount,
  full_period_amount,
  signed_number
FROM
  `skyuk-uk-csgbillanalysis-dev.roi_rental.roi_invoice_*`
WHERE
  _TABLE_SUFFIX = TableSuffix;
CREATE OR REPLACE TABLE
  `skyuk-uk-csgbillanalysis-dev.roi_rental.2_roi_siro_union`
PARTITION BY
  advanced_billing_date AS
SELECT
  advanced_billing_date,
  invoice,
  detail,
  order_detail,
  record_type,
  notification_type,
  billing_point_date,
  billing_point_end,
  notification_item,
  directory_number,
  notification_item_description,
  quantity,
  days,
  cost
FROM
  `skyuk-uk-csgbillanalysis-dev.roi_rental.2_roi_siro_union`
UNION ALL
SELECT
  advanced_billing_date,
  invoice,
  detail,
  order_detail,
  record_type,
  notification_type,
  billing_point_date,
  billing_point_end,
  notification_item,
  directory_number,
  notification_item_description,
  quantity,
  days,
  cost
FROM
  `skyuk-uk-csgbillanalysis-dev.roi_rental.siro_*`
WHERE
  _TABLE_SUFFIX = TableSuffix
