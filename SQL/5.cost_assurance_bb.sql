SELECT
  TYPE,
  CRM_STATUS,
  CASE
    WHEN SERVICE_START_DATE = '9999-09-09' THEN 'ACTIVE'
    WHEN SERVICE_START_DATE <= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 2 month) THEN 'ACTIVE MORE 60 DAYS'
    WHEN SERVICE_START_DATE > DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 2 month) THEN 'ACTIVE WITH 60 DAYS'
END
  SERVICE_START_DATE_FLAG,
  CASE
    WHEN SERVICE_CEASE_DATE = '9999-09-09' THEN 'ACTIVE'
    WHEN SERVICE_CEASE_DATE <= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 2 month) THEN 'CEASED MORE 60 DAYS'
    WHEN SERVICE_CEASE_DATE > DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 2 month)THEN 'CEASED WITH 60 DAYS'
END
  CEASE_FLAG,
  CASE
    WHEN DESCRIPTION IN ('Basic Pricing: Broadband free for 3 months - Existing Customers', 'Free FTTH Subscription', 'Sky Broadband Essential ¤21 off for 12 Months - New ROI Customers', 'Sky Broadband Essential ¤21 off for 12 Months - ROI Customers', 'Sky Broadband Essential ¤21 off for 12 Months (12M New Min Term)', 'Sky Broadband Superfast or Connect ¤25 off for 12 Months - New ROI Customers', 'Sky Broadband Superfast or Connect ¤25 off for 12 Months - ROI Customers', 'Sky Broadband Superfast or Connect ¤25 off for 12 Months (12M New Min Term)', 'Sky VIP Reward - Sky Broadband Essential ¤21 off for 12 Months (12M New Min Term)', 'Sky VIP Reward - Sky Broadband Superfast or Connect ¤25 off for 12 Months (12M New Min Term)', 'Superfast or Connect Unlimited Free for 12 Months with Line Rental - ROI Customer', 'Basic Pricing: Broadband free for 3 months - Existing Customers', 'Sky Broadband Superfast and Connect Fibre ?25 Off for 12 Months - New ROI SABB Customers', 'Sky Broadband Essential ?21 Off for 12 Months - New ROI SABB Customers', 'Sky Broadband Ultrafast Max ?40 off for 12 Months (12M New Min Term)', 'Sky Broadband Superfast ?25 Off for 12 Months with Sky Signature (12M New Min Term)', 'Sky Broadband Superfast or Connect ¤25 off for 12 Months - ROI Customers', 'Sky Broadband Essential ?21 Off for 12 Months for SABB Customers (12M New Min Term)', 'Sky Broadband Essential ?21 Off for 12 Months with Sky Signature (12M New Min Term)', 'Sky Broadband Superfast ?25 Off for 12 Months for SABB Customers (12M New Min Term)', 'Sky Broadband Superfast ?25 Off for 12 Months with Sky Signature - New ROI Customers', 'Sky Ultrafast Plus 100% Off for 6 Months - ROI Customers ', 'Sky Broadband Superfast and Connect Fibre Â¤20.00 Off for 12 Months - ROI Customers') THEN 'yes billed'
    WHEN LAST_BILL_DATE IS NULL THEN 'not billed'
  ELSE
  'yes billed'
END
  billed_flag,
  ORDER_STATUS_CODE,
  ORDER_TYPE_CODE,
  CASE
    WHEN CRM_STATUS_START_DATE = '9999-09-09' THEN 'ACTIVE'
    WHEN CRM_STATUS_START_DATE <= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 2 month) THEN 'ACTIVE MORE 60 DAYS'
    WHEN CRM_STATUS_START_DATE > DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 2 month) THEN 'ACTIVE WITH 60 DAYS'
END
  CRM_STATUS_DATE,
  CASE
    WHEN CRM_STATUS_FIRST_DATE = '9999-09-09' THEN 'ACTIVE'
    WHEN CRM_STATUS_FIRST_DATE <= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 2 month) THEN 'ACTIVE MORE 60 DAYS'
    WHEN CRM_STATUS_FIRST_DATE > DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 2 month) THEN 'ACTIVE WITH 60 DAYS'
END
  CRM_FIRST_DATE,
  CASE
    WHEN ACCOUNT_NUMBER IS NULL THEN 'NO ACCOUNT NUMBER'
  ELSE
  'YES ACCOUNT NUMBER'
END
  ACCOUNT_FLAG,
  COUNT(DISTINCT( Reference_CLI)) AS CUSTOMER,
  SUM(VALUE) AS BILLED_VALUE
FROM
  `skyuk-uk-csgbillanalysis-dev.roi_rental.roi_bb_rental` 
GROUP BY
  TYPE,
  CRM_STATUS,
  CEASE_FLAG,
  ORDER_STATUS_CODE,
  ORDER_TYPE_CODE,
  billed_flag,
  CRM_STATUS_DATE,
  CRM_FIRST_DATE,
  ACCOUNT_FLAG,
  SERVICE_START_DATE_FLAG