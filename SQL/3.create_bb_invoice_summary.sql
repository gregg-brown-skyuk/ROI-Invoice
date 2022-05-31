DECLARE
  ADVANCED_BILLING DATE DEFAULT DATE_TRUNC(CURRENT_DATE, MONTH);
SELECT
  ADVANCED_BILLING_DATE AS BILLING_DATE,
  TYPE,
  CHARGE_TYPE,
  Notification_Item,
  CASE
    WHEN CHARGE_TYPE = 'Connection' THEN COUNT(CUSTOMER)
    ELSE COUNT(DISTINCT(CUSTOMER)) 
  END AS CUST_COUNT,
  ROUND(SUM(VALUE), 2) AS BILLED_VALUE,
  ROUND(SUM(VALUE)/COUNT(DISTINCT CUSTOMER), 2) AS AVERAGE
FROM (
  SELECT
    ADVANCED_BILLING_DATE,
    CASE
      WHEN Notification_Item IN ('ADSL_GLOW', 'ADSL_KINDLE', 'ADSL_TORCH') THEN 'Rural'
      WHEN Notification_Item IN ('WBP', 'WBMAX', 'WBS') THEN 'Urban'
      WHEN Notification_Item IN ('WBC', 'LNB', 'WBC_Fault') THEN 'Fibre'
      WHEN Notification_Item IN ('WBPN', 'WBPN150', 'WBPN150', 'WBPN300', 'WBPN500', 'WBPN1G') THEN 'WBPN'
      WHEN Notification_Item IN ('WBCN') THEN 'WBCN'
      WHEN Notification_Item IN ('VOIP') THEN 'VOIP'
      WHEN Notification_Item IN ('NFIB') THEN 'NBI'
    ELSE
    'CHECK'
  END
    AS TYPE,
    CASE
      WHEN Notification_Type = 'A' AND Notification_Item_Description = 'Missed Appointment Charge' THEN 'Event'
      WHEN Notification_Type = 'L' THEN 'Rental'
      WHEN Notification_Type = 'C' AND Notification_Item_Description IN ('BT VoIP Cease', 'Broadband Cessation Fee', 'Cease Minimum Term Charge', 'Cease Broadband Minimum Term Charge') THEN 'Terminate'
      WHEN Notification_Type = 'C' AND Notification_Item_Description IN ('BT VoIP Part Period Adjustment', 'Broadband Part Period Adjustment', 'Broadband ISP Part Period Adjustment', 'Amortised Broadband Part Period Adjustment', 'Missed Appointment Supplier Credit', 'Broadband Part Period Adjustment') THEN 'Credit / Adjustment'
      WHEN Notification_Type = 'P' AND Notification_Item_Description IN ('Broadband Network Activation Fee', 'Broadband Transfer Fee', 'FTTH Connection Charge', 'Standard Install of Data Port Extension', 'NFIB Connection Charge') THEN 'Connection'
      WHEN Notification_Type = 'P' AND Notification_Item_Description IN ('Amortised Broadband Part Period Rental Arrears', 'BT VoIP Part Period Rental Arrears', 'Broadband ISP Part Period Rental Arrears', 'Broadband Part Period Rental Arrears', 'Siro Part Period Rental Arrears', 'Broadband Part Period Rental Arrears') THEN 'Rental Partial'
      WHEN Notification_Type = 'P' AND Notification_Item_Description IN ('Number Transfer', 'BT VoIP Change Number', 'Broadband Downgrade Fee', 'PreOrder Cancellation', 'Order Cancellation (Late Cancellation)', 'Broadband Speed Charge') THEN 'Event'
      WHEN Notification_Type = 'P' AND Notification_Item_Description = 'Broadband Network Activation Fee Promotion Credit' THEN 'Credit / Adjustment'
    ELSE
    'Check'
  END
    AS CHARGE_TYPE,
    CASE
      WHEN Notification_Item = 'NFIB' THEN internal_number
    ELSE
    Reference_CLI
  END
    AS CUSTOMER,
    CASE
      WHEN Notification_Item = 'NFIB' THEN Notification_Item_Description
    ELSE
    Notification_Item
  END
    AS Notification_Item,
    SUM(Signed_Number) AS VALUE
  FROM
    `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union`
  WHERE
    Record_Type IN ('Bitstream',
      'Line Share',
      'VOIP Service',
      'NBI')
    AND CAST(ADVANCED_BILLING_DATE AS DATE) = ADVANCED_BILLING
  GROUP BY
    ADVANCED_BILLING_DATE,
    TYPE,
    CHARGE_TYPE,
    Notification_Item,
    notification_item_description,
    Reference_CLI,
    customer )
GROUP BY
  BILLING_DATE,
  CHARGE_TYPE,
  Notification_Item,
  TYPE
ORDER BY
  CHARGE_TYPE,
  Notification_Item