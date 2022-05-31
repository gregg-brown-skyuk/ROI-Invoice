SELECT
  ADVANCED_BILLING_DATE,
  Record_Type,
  Notification_Item_Description,
  CHARGE_GROUPING,
  CHARGE_TYPE,
  SUM(ORDER_COUNT) AS ORDERCOUNT,
  SUM(DN_COUNT) AS CUSTOMER,
  SUM(BILL) AS VALUE
FROM (
  SELECT
    CAST(ADVANCED_BILLING_DATE AS DATE) AS ADVANCED_BILLING_DATE,
    Record_Type,
    Notification_Type,
    Notification_Item_Description,
    CASE
      WHEN (Notification_Type IN ('P', 'L') AND (COST = 95.0 OR COST = 40.0)) OR Notification_Item_Description = 'SIROCPE Installation' THEN 'Connection'
      WHEN Notification_Type = 'L' AND Notification_Item_Description = 'SIROLightStream GPON Access' THEN 'Rental'
      WHEN Notification_Type = 'L' AND Notification_Item_Description = 'SIROCancellation Charge' THEN 'Cease'
      WHEN Notification_Type IN ('A', 'L') AND Notification_Item_Description IN ('SIROModify Charge', 'SIROPenalty Charge') THEN 'Event'
    ELSE
    "CHECK"
  END
    AS CHARGE_TYPE,
    CASE
      WHEN Notification_Type = 'P' AND COST = 95.0 THEN 'SIROLightStream GPON Access (€95)'
      WHEN Notification_Type = 'P' AND COST = 40.0 THEN 'SIROLightStream GPON Access (€40)'
      WHEN Notification_Type = 'L' AND Notification_Item_Description = 'SIROLightStream GPON Access' THEN 'SIROLightStream GPON Access'
      WHEN Notification_Type = 'L' AND Notification_Item_Description = 'SIROCancellation Charge' THEN 'SIROCancellation Charge'
      WHEN Notification_Type = 'A' AND Notification_Item_Description = 'SIROModify Charge' THEN 'SIROModify Charge'
      WHEN Notification_Type = 'A' AND Notification_Item_Description = 'SIROPenalty Charge' THEN 'SIROPenalty Charge'
      WHEN Notification_Type IN ( 'P', 'L') THEN Notification_Item_Description
    ELSE
    "CHECK"
  END
    AS CHARGE_GROUPING,
    SUM( Quantity) AS CUSTOMER,
    SUM( Cost) AS BILL,
    COUNT(DISTINCT ORDER_DETAIL) OVER (PARTITION BY ORDER_DETAIL) AS ORDER_COUNT,
    COUNT(DISTINCT DIRECTORY_NUMBER) OVER (PARTITION BY DIRECTORY_NUMBER) AS DN_COUNT
  FROM
    `skyuk-uk-csgbillanalysis-dev.roi_rental.2_roi_siro_union`
  WHERE
    ADVANCED_BILLING_DATE = DATE_TRUNC(CURRENT_DATE, MONTH)
  GROUP BY
    Record_Type,
    Notification_Type,
    Notification_Item_Description,
    CHARGE_GROUPING,
    ADVANCED_BILLING_DATE,
    CHARGE_TYPE,
    DIRECTORY_NUMBER,
    ORDER_DETAIL )
GROUP BY
  Record_Type,
  Notification_Item_Description,
  CHARGE_GROUPING,
  ADVANCED_BILLING_DATE,
  CHARGE_TYPE