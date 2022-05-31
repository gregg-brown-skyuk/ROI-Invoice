SELECT
  CAST(ADVANCED_BILLING_DATE AS DATE) AS ADVANCED_BILLING_DATE,
  Record_Type,
  CASE
    WHEN Notification_Item_Description IN ('CPS Sign up Single Line Account', 'SB-WLR Sign Up Single Line Account') THEN 'CPS Sign / SB-WLR up Single Line Account'
  ELSE
  Notification_Item_Description
END
  AS Notification_Item_Description,
  CASE
    WHEN Notification_Item_Description IN ('WLR VL Line Rental Fee', 'WLR PSTN Part Period Rental Arrears') THEN 'Partial Rental'
    WHEN Notification_Item_Description IN ('SB-WLR Cease Orders Single Line Account') THEN 'Cease'
    WHEN Notification_Item_Description IN ('WLR PSTN PI Activation Fee', 'WLR PSTN PN Activation Fee', 'WLR PSTN PP Activation Fee', 'SB-WLR Sign Up Single Line Account', 'CPS Sign up Single Line Account') THEN 'Connection'
    WHEN Notification_Item_Description IN ('WLR PSTN Credit For Unused Service') THEN 'Credit'
    WHEN Notification_Item_Description IN ('Missed Appointment Charge', 'Number Porting Charge', 'PSTN Change Number Fee', 'PSTN PRIORITY SLA') THEN 'Event'
    WHEN Notification_Item_Description IN ('Call Waiting', 'Call Answering', 'Call Barring ALL', 'Three Way Calling', 'Call Barring Premium', 'Route To Call Centre', 'Variable Call Forwarding', 'Call Barring Premium & Mobile', 'Call Barring Premium & International', 'Calling Line Identification Restriction', 'Calling Line Identification Presentation', 'Call Barring Premium International & Mobile', 'Call Barring Premium & International (excluding UK)', 'Family Mailbox', 'Premium Mobile Barred') THEN 'Call Feature'
    WHEN Notification_Item_Description IN ('Basic Phone1', 'Private Meter', 'SWICONN', 'Basic Phone', 'Ampliphone') THEN 'Hardware Rental'
    WHEN Notification_Item_Description IN ('WLR PSTN Part Period Rental Arrears') THEN 'Part Month Rental'
    WHEN Notification_Item_Description IN ('WLR PSTN Line Rental Fee') THEN 'Rental'
  ELSE
  'Check'
END
  AS CHARGE_TYPE,
  COUNT(DISTINCT( Reference_CLI)) AS CUSTOMER,
  SUM(Signed_Number) AS VALUE
FROM
  `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union`
WHERE
  Record_Type IN ('WLR Line',
    'WLR ORD',
    'WLR',
    'WLR RSLA',
    'WLR AS',
    'WLR CPE')
AND
  ADVANCED_BILLING_DATE = DATE_TRUNC(CURRENT_DATE, MONTH)
GROUP BY
  ADVANCED_BILLING_DATE,
  Record_Type,
  Notification_Item_Description,
  CHARGE_TYPE
HAVING
  Value <> 0