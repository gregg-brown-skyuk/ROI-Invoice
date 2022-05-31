DECLARE
  ADVANCED_BILLING DATE DEFAULT DATE_TRUNC(CURRENT_DATE, MONTH);
DECLARE
  BILLING_START DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 2 MONTH);
DECLARE
  BILLING_END DATE DEFAULT DATE_TRUNC(CURRENT_DATE, MONTH);
CREATE OR REPLACE TABLE
  `skyuk-uk-csgbillanalysis-dev.roi_rental.roi_#TABLEMONTH#_bb_rentals` AS --- *************** NEED TO CHANGE THIS INVOICE ***************
WITH
  INVOICE AS (
  SELECT
    Reference_CLI AS Reference_CLI,
    INVOICE_TYPE,
    Notification_Item_Description,
    Notification_TYPE,
    Order_Number,
    Notification_Item,
    New_PTT_No
  FROM (
    SELECT
      roi_union.Internal_Number,
      coalesce(roi_union.Order_Number,
        ord_union.Order_Number) AS Order_Number,
      roi_union.Original_PTT_No,
      roi_union.New_PTT_No,
      roi_union.Record_Type,
      roi_union.Notification_Type,
      roi_union.Billing_Point_Date,
      roi_union.Billing_End_Date,
      CONCAT(0,roi_union.Reference_CLI) AS Reference_CLI,
      roi_union.Notification_Item_Description,
      roi_union.Chargeable_Quantity,
      roi_union.Number_of_Days,
      roi_union.Daily_Amount,
      'BTI_DATA' AS INVOICE_TYPE,
      roi_union.Notification_Item
    FROM
      `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union` roi_union
    LEFT OUTER JOIN
      `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union` ord_union
    ON
      ord_union.reference_cli = roi_union.reference_cli
    WHERE
      (CAST(roi_union.Reference_CLI AS STRING) NOT LIKE '888%'
        AND CAST(roi_union.Reference_CLI AS STRING) NOT LIKE '777%')
      AND CAST(roi_union.ADVANCED_BILLING_DATE AS DATE) = ADVANCED_BILLING
      AND roi_union.Notification_Type = 'L'
      AND roi_union.Record_Type IN ('Bitstream',
        'Line Share',
        'VOIP Service',
        'Siro',
        'NBI'))
  UNION ALL
  SELECT
    CONCAT(SUBSTR(CAST(Reference_CLI AS STRING),0, 4),'-',SUBSTR(CAST(Reference_CLI AS STRING),5, 7)),
    INVOICE_TYPE,
    Notification_Item_Description,
    Notification_TYPE,
    Order_Number,
    Notification_Item,
    New_PTT_No
  FROM (
    SELECT
      roi_union.Internal_Number,
      coalesce(roi_union.Order_Number,
        ord_union.Order_Number) AS Order_Number,
      roi_union.Original_PTT_No,
      roi_union.New_PTT_No,
      roi_union.Record_Type,
      roi_union.Notification_Type,
      roi_union.Billing_Point_Date,
      roi_union.Billing_End_Date,
      roi_union.Reference_CLI,
      roi_union.Notification_Item_Description,
      roi_union.Chargeable_Quantity,
      roi_union.Number_of_Days,
      roi_union.Daily_Amount,
      'BTI_DATA' AS INVOICE_TYPE,
      roi_union.Notification_Item,
    FROM
      `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union` roi_union
    LEFT OUTER JOIN
      `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union` ord_union
    ON
      ord_union.reference_cli = roi_union.reference_cli
    WHERE
      CAST(roi_union.Reference_CLI AS STRING) LIKE '888%'
      AND CAST(roi_union.ADVANCED_BILLING_DATE AS DATE) = ADVANCED_BILLING
      AND roi_union.Notification_Type = 'L'
      AND roi_union.Record_Type IN ('Bitstream',
        'Line Share',
        'VOIP Service',
        'Siro',
        'NBI') )
  UNION ALL
  SELECT
    Internal_Number,
    INVOICE_TYPE,
    Notification_Item_Description,
    Notification_TYPE,
    Order_Number,
    Notification_Item,
    New_PTT_No
  FROM (
    SELECT
      roi_union.Internal_Number,
      coalesce(roi_union.Order_Number,
        ord_union.Order_Number) AS Order_Number,
      roi_union.Original_PTT_No,
      roi_union.New_PTT_No,
      roi_union.Record_Type,
      roi_union.Notification_Type,
      roi_union.Billing_Point_Date,
      roi_union.Billing_End_Date,
      roi_union.Reference_CLI,
      roi_union.Notification_Item_Description,
      roi_union.Chargeable_Quantity,
      roi_union.Number_of_Days,
      roi_union.Daily_Amount,
      'BTI_DATA' AS INVOICE_TYPE,
      roi_union.Notification_Item,
    FROM
      `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union` roi_union
    LEFT OUTER JOIN
      `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union` ord_union
    ON
      ord_union.Internal_Number = roi_union.Internal_Number
    WHERE
      CAST(roi_union.Reference_CLI AS STRING) LIKE '777%'
      AND CAST(roi_union.ADVANCED_BILLING_DATE AS DATE) = ADVANCED_BILLING
      AND roi_union.Notification_Type = 'L'
      AND roi_union.Record_Type IN ('Bitstream',
        'Line Share',
        'VOIP Service',
        'Siro',
        'NBI') )
  UNION ALL
  SELECT
    Directory_Number,
    INVOICE_TYPE,
    Notification_Item_Description,
    'SIRO',
    ORDER_DETAIL,
    Notification_Item,
    'Null' AS New_PTT_No
  FROM (
    SELECT
      Directory_Number,
      DETAIL,
      -1,
      '-2',
      Record_Type,
      CAST(Billing_Point_Date AS STRING),
      CAST(Billing_Point_End AS STRING),
      01234,
      Notification_Item_Description,
      Quantity,
      Days,
      Cost,
      'SIRO_DATA' AS INVOICE_TYPE,
      ORDER_DETAIL,
      Notification_IteM
    FROM
      `skyuk-uk-csgbillanalysis-dev.roi_rental.2_roi_siro_union`
    WHERE
      Notification_Type = 'L'
      AND Record_Type IN ('Bitstream',
        'Line Share',
        'VOIP Service',
        'Siro')
      AND ADVANCED_BILLING_DATE = ADVANCED_BILLING )),
  BILL_VALUE AS (
  SELECT
    CONCAT(0,Reference_CLI) AS Reference_CLI2,
    SUM(Signed_Number) VALUE
  FROM
    `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union`
  WHERE
    (CAST(Reference_CLI AS STRING) NOT LIKE '888%'
      AND CAST(Reference_CLI AS STRING) NOT LIKE '777%')
    AND CAST(ADVANCED_BILLING_DATE AS DATE) = ADVANCED_BILLING
    AND Notification_Type = 'L'
    AND Record_Type IN ('Bitstream',
      'Line Share',
      'VOIP Service',
      'Siro',
      'NBI')
  GROUP BY
    Reference_CLI
  UNION ALL
  SELECT
    CONCAT(SUBSTR(CAST(Reference_CLI AS STRING),0, 4),'-',SUBSTR(CAST(Reference_CLI AS STRING),5, 7)) AS Reference_CLI2,
    SUM(Signed_Number) VALUE
  FROM
    `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union`
  WHERE
    CAST(Reference_CLI AS STRING) LIKE '888%'
    AND CAST(ADVANCED_BILLING_DATE AS DATE) = ADVANCED_BILLING
    AND Notification_Type = 'L'
    AND Record_Type IN ('Bitstream',
      'Line Share',
      'VOIP Service',
      'Siro',
      'NBI')
  GROUP BY
    Reference_CLI
  UNION ALL
  SELECT
    internal_number AS Reference_CLI2,
    SUM(Signed_Number) VALUE
  FROM
    `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union`
  WHERE
    CAST(Reference_CLI AS STRING) LIKE '777%'
    AND CAST(ADVANCED_BILLING_DATE AS DATE) = ADVANCED_BILLING
    AND Notification_Type = 'L'
    AND Record_Type IN ('Bitstream',
      'Line Share',
      'VOIP Service',
      'Siro',
      'NBI')
  GROUP BY
    Reference_CLI2
  UNION ALL
  SELECT
    Directory_Number,
    SUM(Cost)
  FROM
    `skyuk-uk-csgbillanalysis-dev.roi_rental.2_roi_siro_union`
  WHERE
    Notification_Type = 'L'
    AND Record_Type IN ('Bitstream',
      'Line Share',
      'VOIP Service',
      'Siro')AND ADVANCED_BILLING_DATE = ADVANCED_BILLING
  GROUP BY
    Directory_Number ),
  OM AS (
  SELECT
    RESOURCE_PROVIDER_ORDER_ID,
    SERVICE_ID,
    ACCOUNT_NUMBER,
    ACTUAL_DIRECTORY_NUMBER,
    CREATED
  FROM (
    SELECT
      ROW_NUMBER() OVER (PARTITION BY ACTUAL_DIRECTORY_NUMBER ORDER BY CREATED DESC) LATEST_ORDER,
      RESOURCE_PROVIDER_ORDER_ID,
      SERVICE_ID,
      ACCOUNT_NUMBER,
      ACTUAL_DIRECTORY_NUMBER,
      CREATED
    FROM
      `skyuk-uk-csgbillanalysis-dev.Sandpit.OM_UPDATE_ROI` )
  WHERE
    LATEST_ORDER = 1),
  CUSTOMER AS (
  SELECT
    ACCOUNT_NUMBER,
    BO_ACCOUNT,
    SERVICE_ID,
    DIRECTORY_NUMBER,
    SERVICE_START_DATE,
    SERVICE_CEASE_DATE
  FROM
    `skyuk-uk-csgbillanalysis-dev.billing_core.customer_by_circuit_id`
  WHERE
    PRODUCT_FLAG = 'Broadband DSL Line'
  GROUP BY
    ACCOUNT_NUMBER,
    BO_ACCOUNT,
    SERVICE_ID,
    DIRECTORY_NUMBER,
    SERVICE_START_DATE,
    SERVICE_CEASE_DATE
  UNION ALL
  SELECT
    ACCOUNT_NUMBER,
    CAST(BO_ACCOUNT AS INT64),
    CAST(SERVICE_ID AS INT64),
    DIRECTORY_NUMBER,
    SERVICE_START_DATE,
    SERVICE_CEASE_DATE
  FROM
    `skyuk-uk-csgbillanalysis-dev.billing_core.customer_by_phone_number`
  WHERE
    ACCOUNT_NUMBER IS NOT NULL
  GROUP BY
    ACCOUNT_NUMBER,
    BO_ACCOUNT,
    SERVICE_ID,
    DIRECTORY_NUMBER,
    SERVICE_START_DATE,
    SERVICE_CEASE_DATE
  UNION ALL
  SELECT
    OM.ACCOUNT_NUMBER,
    BO.BO_ACCOUNT,
    SER.SERVICE_ID,
    CIRCUIT_REFERENCE_NUMBER,
    DATE(ser.SERVICE_START_DATE) AS SERVICE_START_DATE,
    DATE(ser.SERVICE_CEASE_DATE) AS SERVICE_CEASE_DATE
  FROM
    `skyuk-uk-csgbillanalysis-dev.uk_inp_provisioning_ic.ODW_PENFOLD_DSL_SERVICE_DIM` ser
  LEFT OUTER JOIN
    `skyuk-uk-csgbillanalysis-dev.Sandpit.OM_UPDATE_ROI` OM
  ON
    (OM.SERVICE_ID = CAST(SER.SERVICE_ID AS STRING))
  LEFT OUTER JOIN
    `skyuk-uk-csgbillanalysis-dev.billing_core.customer_by_circuit_id` BO
  ON
    (OM.ACCOUNT_NUMBER = BO.ACCOUNT_NUMBER)
  WHERE
    OM.ACCOUNT_NUMBER IS NOT NULL
  GROUP BY
    ACCOUNT_NUMBER,
    BO.BO_ACCOUNT,
    CIRCUIT_REFERENCE_NUMBER,
    SERVICE_ID,
    SERVICE_START_DATE,
    SERVICE_CEASE_DATE
  UNION ALL
  SELECT
    BO.ACCOUNT_NUMBER,
    BO.BO_ACCOUNT,
    SER.SERVICE_ID,
    CIRCUIT_REFERENCE_NUMBER,
    DATE(ser.SERVICE_START_DATE) AS SERVICE_START_DATE,
    DATE(ser.SERVICE_CEASE_DATE) AS SERVICE_CEASE_DATE
  FROM
    `skyuk-uk-csgbillanalysis-dev.uk_inp_provisioning_ic.ODW_PENFOLD_DSL_SERVICE_DIM` ser
  LEFT OUTER JOIN
    `skyuk-uk-csgbillanalysis-dev.billing_core.customer_by_circuit_id` BO
  ON
    (ser.SERVICE_ID = BO.SERVICE_ID)
  WHERE
    BO.ACCOUNT_NUMBER IS NOT NULL
  GROUP BY
    ACCOUNT_NUMBER,
    BO.BO_ACCOUNT,
    CIRCUIT_REFERENCE_NUMBER,
    SERVICE_ID,
    SERVICE_START_DATE,
    SERVICE_CEASE_DATE ),
  account_by_uan AS (
  SELECT
    RANK() OVER(PARTITION BY service.SERVICE_ID ORDER BY SERVICE_START_DATE DESC) AS rank,
    service.SERVICE_ID,
    UAN,
    account_number
  FROM
    `skyuk-uk-csgbillanalysis-dev.uk_inp_provisioning_ic.ODW_PENFOLD_DSL_SERVICE_DIM` service
  LEFT OUTER JOIN
    `skyuk-uk-csgbillanalysis-dev.Sandpit.Account_to_Service_id` acc
  ON
    CAST(service.SERVICE_ID AS string) = acc.SERVICE_ID
  GROUP BY
    SERVICE_START_DATE,
    service.SERVICE_ID,
    UAN,
    account_number),
  account_by_order AS (
  SELECT
    ACCOUNT_NUMBER,
    ORDER_ID,
    BT_REFERENCE,
    BT_CIRCUIT,
    DELIVERY_PHONE_NUMBER,
    SERVICE_ID
  FROM
    `skyuk-uk-csgbillanalysis-dev.Sandpit.all_orders`
  WHERE
    PARENT_ORDER_TYPE_CODE = 'ROI'
    AND ACCOUNT_NUMBER IS NOT NULL
  GROUP BY
    ACCOUNT_NUMBER,
    ORDER_ID,
    BT_REFERENCE,
    BT_CIRCUIT,
    DELIVERY_PHONE_NUMBER,
    SERVICE_ID ),
  KEENAN_VALUE AS (
  SELECT
    INCURRING_CUST_ACCOUNT_NO,
    CREATED_DT
  FROM (
    SELECT
      ROW_NUMBER() OVER (PARTITION BY CAST(INCURRING_CUST_ACCOUNT_NO AS STRING)
      ORDER BY
        CREATED_DT DESC) firstrow,
      INCURRING_CUST_ACCOUNT_NO,
      CREATED_DT
    FROM
      `skyuk-uk-csgbillanalysis-dev.billing_core.product_charges_rc`
    WHERE
      DATE(CREATED_DT) BETWEEN BILLING_START
      AND BILLING_END
      AND CHARGE_GROUPING_ID IN (27,
        1004)
    GROUP BY
      INCURRING_CUST_ACCOUNT_NO,
      CREATED_DT )
  WHERE
    firstrow = 1 ),
  CHARGE_VALUE AS (
  SELECT
    INCURRING_CUST_ACCOUNT_NO,
    CREATED_DT,
    SUM( SUM) AS CHARGE_VALUE
  FROM
    `skyuk-uk-csgbillanalysis-dev.billing_core.product_charges_rc`
  WHERE
    DATE(CREATED_DT) BETWEEN BILLING_START
    AND BILLING_END
    AND CHARGE_GROUPING_ID IN (27,
      1004)
  GROUP BY
    INCURRING_CUST_ACCOUNT_NO,
    CREATED_DT ),
  BILLED_DATA AS (
  SELECT
    BO_ACCOUNT_NO,
    DATE(CREATED_DT) AS created_dt
  FROM (
    SELECT
      ROW_NUMBER() OVER (PARTITION BY BO_ACCOUNT_NO ORDER BY CREATED_DT DESC) firstrow,
      BO_ACCOUNT_NO,
      CREATED_DT
    FROM
      `skyuk-uk-csgbillanalysis-dev.uk_inp_trading_ic.WH_CHARGE_GROUP_BALANCE`
    WHERE
      DATE(CREATED_DT) BETWEEN BILLING_START
      AND BILLING_END
      AND CHARGE_GROUPING_ID IN (27,
        1004)
    GROUP BY
      BO_ACCOUNT_NO,
      CREATED_DT )
  WHERE
    firstrow =1 ),
  BILLED_VALUE AS (
  SELECT
    BO_ACCOUNT_NO,
    DATE(CREATED_DT) AS CREATED_DT,
    NEW_CHARGES_AT_BILLING AS BILLED_VALUE
  FROM
    `skyuk-uk-csgbillanalysis-dev.uk_inp_trading_ic.WH_CHARGE_GROUP_BALANCE`
  WHERE
    DATE(CREATED_DT) BETWEEN BILLING_START
    AND BILLING_END
    AND CHARGE_GROUPING_ID IN (27,
      1004)
  GROUP BY
    BO_ACCOUNT_NO,
    CREATED_DT,
    NEW_CHARGES_AT_BILLING ),
  SUBS AS (
  SELECT
    SECOND_ROW,
    ACCOUNT_NUMBER,
    SUBSCRIPTION_ID,
    FIRST_DATE,
    DATE(EFFECTIVE_FROM_DT) AS EFFECTIVE_FROM_DT,
    STATUS,
    SUBSCRIPTION_SUB_TYPE,
    PREV_STATUS,
    PREV_STATUS_START_DT
  FROM (
    SELECT
      ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NUMBER ORDER BY FIRST_DATE DESC) AS SECOND_ROW,
      FIRSTROWID,
      ACCOUNT_NUMBER,
      SUBSCRIPTION_ID,
      FIRST_DATE,
      EFFECTIVE_FROM_DT,
      STATUS,
      SUBSCRIPTION_SUB_TYPE,
      PREV_STATUS,
      PREV_STATUS_START_DT
    FROM (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NUMBER, SUBSCRIPTION_ID ORDER BY EFFECTIVE_FROM_DT DESC) FIRSTROWID,
        ACCOUNT_NUMBER,
        EFFECTIVE_FROM_DT,
        STATUS,
        SUBSCRIPTION_SUB_TYPE,
        SUBSCRIPTION_ID,
        DATE(FIRST_ACTIVATION_DT) AS FIRST_DATE,
        PREV_STATUS,
        DATE(PREV_STATUS_START_DT) AS PREV_STATUS_START_DT
      FROM
        `skyuk-uk-csgbillanalysis-dev.uk_inp_trading_ic.WH_PH_SUBS_HIST`
      WHERE
        EFFECTIVE_TO_DT = '9999-09-09'
        AND DATE(EFFECTIVE_FROM_DT) <= ADVANCED_BILLING
        AND SUBSCRIPTION_SUB_TYPE IN ('NOW_TV_2.0_BROADBAND_LINE',
          'Broadband DSL Line')
        AND DATE(FIRST_ACTIVATION_DT) <> '9999-09-09')
    WHERE
      FIRSTROWID = 1
    GROUP BY
      FIRSTROWID,
      ACCOUNT_NUMBER,
      SUBSCRIPTION_ID,
      FIRST_DATE,
      EFFECTIVE_FROM_DT,
      STATUS,
      SUBSCRIPTION_SUB_TYPE,
      PREV_STATUS,
      PREV_STATUS_START_DT)
  WHERE
    UPPER(STATUS) IN ('ACTIVE',
      'RESTRICTED',
      'PENDING CANCEL',
      'ACTIVE BLOCKED')
    AND SECOND_ROW = 1 ),
  SUBS_ALTERNITIVE AS (
  SELECT
    SECOND_ROW,
    ACCOUNT_NUMBER,
    SUBSCRIPTION_ID,
    FIRST_DATE,
    DATE(EFFECTIVE_FROM_DT) AS EFFECTIVE_FROM_DT,
    STATUS,
    SUBSCRIPTION_SUB_TYPE,
    PREV_STATUS,
    PREV_STATUS_START_DT
  FROM (
    SELECT
      ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NUMBER ORDER BY EFFECTIVE_FROM_DT DESC) AS SECOND_ROW,
      FIRSTROWID,
      ACCOUNT_NUMBER,
      SUBSCRIPTION_ID,
      FIRST_DATE,
      EFFECTIVE_FROM_DT,
      STATUS,
      SUBSCRIPTION_SUB_TYPE,
      PREV_STATUS,
      PREV_STATUS_START_DT
    FROM (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NUMBER, SUBSCRIPTION_ID ORDER BY EFFECTIVE_FROM_DT DESC) FIRSTROWID,
        ACCOUNT_NUMBER,
        EFFECTIVE_FROM_DT,
        STATUS,
        SUBSCRIPTION_SUB_TYPE,
        SUBSCRIPTION_ID,
        DATE(FIRST_ACTIVATION_DT) AS FIRST_DATE,
        PREV_STATUS,
        DATE(PREV_STATUS_START_DT) AS PREV_STATUS_START_DT
      FROM
        `skyuk-uk-csgbillanalysis-dev.uk_inp_trading_ic.WH_PH_SUBS_HIST`
      WHERE
        EFFECTIVE_TO_DT = '9999-09-09'
        AND DATE(EFFECTIVE_FROM_DT) <= ADVANCED_BILLING
        AND SUBSCRIPTION_SUB_TYPE IN ('NOW_TV_2.0_BROADBAND_LINE',
          'Broadband DSL Line')
        AND DATE(FIRST_ACTIVATION_DT) <> '9999-09-09')
    WHERE
      FIRSTROWID = 1
    GROUP BY
      FIRSTROWID,
      ACCOUNT_NUMBER,
      SUBSCRIPTION_ID,
      FIRST_DATE,
      EFFECTIVE_FROM_DT,
      STATUS,
      SUBSCRIPTION_SUB_TYPE,
      PREV_STATUS,
      PREV_STATUS_START_DT)
  WHERE
    UPPER(STATUS) NOT IN ('ACTIVE',
      'RESTRICTED',
      'PENDING CANCEL',
      'ACTIVE BLOCKED',
      'CANCELLED')
    AND SECOND_ROW = 1 ),
  SUBS_CANCELLED AS (
  SELECT
    SECOND_ROW,
    ACCOUNT_NUMBER,
    SUBSCRIPTION_ID,
    FIRST_DATE,
    DATE(EFFECTIVE_FROM_DT) AS EFFECTIVE_FROM_DT,
    STATUS,
    SUBSCRIPTION_SUB_TYPE,
    PREV_STATUS,
    PREV_STATUS_START_DT
  FROM (
    SELECT
      ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NUMBER ORDER BY EFFECTIVE_FROM_DT DESC) AS SECOND_ROW,
      FIRSTROWID,
      ACCOUNT_NUMBER,
      SUBSCRIPTION_ID,
      FIRST_DATE,
      EFFECTIVE_FROM_DT,
      STATUS,
      SUBSCRIPTION_SUB_TYPE,
      PREV_STATUS,
      PREV_STATUS_START_DT
    FROM (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY ACCOUNT_NUMBER, SUBSCRIPTION_ID ORDER BY EFFECTIVE_FROM_DT DESC) FIRSTROWID,
        ACCOUNT_NUMBER,
        EFFECTIVE_FROM_DT,
        STATUS,
        SUBSCRIPTION_SUB_TYPE,
        SUBSCRIPTION_ID,
        DATE(FIRST_ACTIVATION_DT) AS FIRST_DATE,
        PREV_STATUS,
        DATE(PREV_STATUS_START_DT) AS PREV_STATUS_START_DT
      FROM
        `skyuk-uk-csgbillanalysis-dev.uk_inp_trading_ic.WH_PH_SUBS_HIST`
      WHERE
        EFFECTIVE_TO_DT = '9999-09-09'
        AND DATE(EFFECTIVE_FROM_DT) <= ADVANCED_BILLING
        AND SUBSCRIPTION_SUB_TYPE IN ('NOW_TV_2.0_BROADBAND_LINE',
          'Broadband DSL Line')
        AND DATE(FIRST_ACTIVATION_DT) <> '9999-09-09')
    WHERE
      FIRSTROWID = 1
    GROUP BY
      FIRSTROWID,
      ACCOUNT_NUMBER,
      SUBSCRIPTION_ID,
      FIRST_DATE,
      EFFECTIVE_FROM_DT,
      STATUS,
      SUBSCRIPTION_SUB_TYPE,
      PREV_STATUS,
      PREV_STATUS_START_DT)
  WHERE
    UPPER(STATUS) = ('CANCELLED')
    AND SECOND_ROW = 1 ),
  LATEST_ORDER AS (
  SELECT
    Service_ID,
    ORDER_ID,
    DATE(ORDER_STATUS_SET_DATE) AS ORDER_STATUS_SET_DATE,
    ORDER_TYPE_CODE,
    ORDER_STATUS_CODE
  FROM (
    SELECT
      ROW_NUMBER() OVER (PARTITION BY Service_ID ORDER BY CREATED_DATE DESC) LATEST_ORDER_DETAIL,
      Service_ID,
      ORDER_ID,
      CREATED_DATE,
      BT_REFERENCE,
      Account_number,
      DELIVERY_PHONE_NUMBER,
      ORDER_STATUS_CODE,
      ORDER_STATUS_SET_DATE,
      ORDER_TYPE_CODE,
      PARENT_ORDER_TYPE_CODE,
      ACQUISITION_TYPE,
      PROVIDE_REQUEST_TYPE
    FROM
      `skyuk-uk-csgbillanalysis-dev.Sandpit.all_orders`
    GROUP BY
      Service_ID,
      ORDER_ID,
      CREATED_DATE,
      BT_REFERENCE,
      Account_number,
      DELIVERY_PHONE_NUMBER,
      ORDER_STATUS_CODE,
      ORDER_STATUS_SET_DATE,
      ORDER_TYPE_CODE,
      PARENT_ORDER_TYPE_CODE,
      ACQUISITION_TYPE,
      PROVIDE_REQUEST_TYPE)
  WHERE
    LATEST_ORDER_DETAIL = 1 ),
  BO_ACC AS (
  SELECT
    ACCOUNT_NUMBER,
    bo_account_number,
    portfolio_id
  FROM
    `skyuk-uk-csgbillanalysis-dev.billing_core.fo_account_mapping`
  GROUP BY
    ACCOUNT_NUMBER,
    bo_account_number,
    portfolio_id ),
  service_instance AS(
  SELECT
    portfolio_id,
    account_number,
    service_instance_id
  FROM
    `skyuk-uk-customer-pres-prod.uk_pub_cust_spine_subs_is.fact_subscription`
  WHERE
    type = 'Broadband'
    AND active_flag = TRUE
  GROUP BY
    portfolio_id,
    account_number,
    service_instance_id),
  offer AS (
  SELECT
    portfolio_id AS portfolioid,
    service_instance_id AS serviceinstanceid,
    account_number,
    start_dt,
    end_dt AS effective_to_dt,
    active_flag AS offer_status,
    description,
    bill_name
  FROM
    `skyuk-uk-customer-pres-prod.uk_pub_customer_spine_offer_is.fact_offer`
  WHERE
    active_flag = TRUE
    AND DATE(start_dt) < DATE_TRUNC(CURRENT_DATE, MONTH)
    AND DATE(end_dt )> DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 2 month)
    AND bill_name IN ('Broadband Subscription Discounted',
      'Broadband Discounted',
      'In-Contract Discount',
      'Sky VIP Reward - Broadband Subscription Discounted')
    AND DESCRIPTION NOT IN ('Smart Connectivity Fee for 10 EUR with Sky TV and Sky Broadband Superfast (SFTC)- New Customer',
      'Smart Connectivity for €10 with Fibre to the Home, Hub 4 and Sky Q - ROI Customers',
      'Free FTTH Installation',
      'Free Appointed Fibre Activation - ROI Customers',
      'FTTH Activation for €20 for existing BB Customers - Existing ROI Customers',
      'FTTH Appointed Activation for ?50 without TV - New ROI Customers',
      'Free Fibre Broadband Activation - ROI Customers',
      'Free Sky Hub 2.0',
      'Free Sky Hub',
      'Standalone Broadband Activation Fee €20 with Pre-Active TV',
      'Sky Q Smart Connectivity for 10 EUR - Existing Sky Q customers taking BB',
      'Broadband Activation Fee for free when moving from ADSL to FTTC - Existing ROI customers',
      'Sky Q Smart Connectivity for 5 EUR with Triple Play (ROI) - New Customers',
      'Broadband Activation Fee for €20 with TV - New ROI customers',
      'Broadband Activation Fee for €20 for existing BB Customers - Existing ROI customers',
      'Free FTTH install for Siro only - ROI Customers',
      'Free Broadband Hardware Delivery Charge with Sky Q (ROI Customers)',
      'Sky Broadband Buddy Free with Sky Broadband (ROI) (12M New Min Term)',
      'Free Fibre Activation with Sky Broadband Superfast- ROI Customers',
      'Sky Broadband Superfast or Fibre Unlimited 16 EUR off for 12 Months (12M New Min Term)',
      'Replacement Broadband Hardware Delivery Charge Free (ROI Customers)',
      'Broadband Activaion Fee Free when currently on Broadband and moving to ADSL or FTTC',
      'Broadband Activation Fee Free',
      'Sky Broadband Essential, Superfast or Connect €8 off for 12 Months - Existing ROI Customers',
      'Free Extension Kit',
      'Sky VIP Reward - Free Sky Q Hub Smart Connectivity',
      'FTTH Activation for €20 with TV - New ROI Customers',
      'Free Broadband Activation - ROI Customer',
      'Hub 3.0 Standard 10 Euro for existing Sky TV customers taking new broadband service',
      'Free Sky Q Booster - Staff',
      'Sky Fibre Activation Fee Free with Sky Broadband Superfast - ROI Staff'))
SELECT
  ALL_DATA.ACCOUNT_NUMBER,
  bo_account_number AS BO_ACCOUNT,
  service_instance_id,
  portfolio_id,
  Reference_CLI,
  New_PTT_No,
  TYPE,
  ALL_DATA.SERVICE_ID,
  DIRECTORY_NUMBER,
  SERVICE_START_DATE,
  SERVICE_CEASE_DATE,
  INVOICE_TYPE,
  VALUE,
  coalesce(DATE(KEENAN_VALUE.CREATED_DT),
    BILLED_DATA.created_dt) AS LAST_BILL_DATE,
  BILLED_VALUE.BILLED_VALUE,
  CHARGE_VALUE.CHARGE_VALUE,
  coalesce(SUBS.STATUS,
    SUBS_ALTERNITIVE.STATUS,
    SUBS_CANCELLED.STATUS) AS CRM_STATUS,
  coalesce(SUBS.EFFECTIVE_FROM_DT,
    SUBS_ALTERNITIVE.EFFECTIVE_FROM_DT,
    SUBS_CANCELLED.EFFECTIVE_FROM_DT) AS CRM_STATUS_START_DATE,
  coalesce(SUBS.FIRST_DATE,
    SUBS_ALTERNITIVE.FIRST_DATE,
    SUBS_CANCELLED.FIRST_DATE) AS CRM_STATUS_FIRST_DATE,
  ORDER_STATUS_CODE,
  ORDER_STATUS_SET_DATE,
  ORDER_ID,
  ORDER_TYPE_CODE,
  effective_to_dt,
  offer_status,
  portfolioid,
  serviceinstanceid,
  DESCRIPTION,
  BILL_NAME
FROM (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY SPINE.Reference_CLI ORDER BY SERVICE_CEASE_DATE DESC) LATEST_SERVICE,
    ROW_NUMBER() OVER (PARTITION BY SPINE.Reference_CLI ORDER BY SERVICE_START_DATE DESC) FIRST_SERVICE,
    SPINE.ACCOUNT_NUMBER,
    bo_account_number,
    coalesce(service_instance.service_instance_id,
      service_instance2.service_instance_id) service_instance_id,
    BO_ACC.portfolio_id,
    SPINE.Reference_CLI,
    SERVICE_ID,
    DIRECTORY_NUMBER,
    SERVICE_START_DATE,
    SERVICE_CEASE_DATE,
    SPINE.INVOICE_TYPE,
    SPINE.VALUE,
    TYPE,
    New_PTT_No
  FROM (
    SELECT
      coalesce(uan.account_number,
        account_by_order.account_number,
        OM.ACCOUNT_NUMBER,
        CUSTOMER.ACCOUNT_NUMBER) AS ACCOUNT_NUMBER,
      Reference_CLI,
      New_PTT_No,
      INVOICE_TYPE,
      CASE
        WHEN Notification_Item IN ('ADSL_GLOW', 'ADSL_KINDLE', 'ADSL_TORCH') THEN 'Rural'
        WHEN Notification_Item IN ('WBP', 'WBMAX', 'WBS') THEN 'Urban'
        WHEN Notification_Item IN ('WBC', 'LNB', 'WBC_Fault') THEN 'Fibre'
        WHEN Notification_Item IN ('WBPN', 'WBPN150', 'WBPN150', 'WBPN300', 'WBPN500', 'WBPN1G') THEN 'WBPN'
        WHEN Notification_Item IN ('WBCN') THEN 'WBCN'
        WHEN Notification_Item IN ('VOIP') THEN 'VOIP'
        WHEN Notification_Item IN ('SIRO_FH') THEN 'SIRO'
        WHEN Notification_Item IN ('NFIB') THEN 'NBI'
      ELSE
      'CHECK'
    END
      AS TYPE,
      VALUE
    FROM
      INVOICE
    INNER JOIN
      BILL_VALUE
    ON
      (INVOICE.Reference_CLI = BILL_VALUE.Reference_CLI2)
    LEFT OUTER JOIN
      account_by_uan uan
    ON
      (uan.uan = invoice.New_PTT_No)
    LEFT OUTER JOIN
      account_by_order
    ON
      CAST(account_by_order.order_id AS string) = invoice.order_number
    LEFT OUTER JOIN
      OM
    ON
      (OM.ACTUAL_DIRECTORY_NUMBER = INVOICE.Reference_CLI)
    LEFT OUTER JOIN
      CUSTOMER
    ON
      (CUSTOMER.DIRECTORY_NUMBER = INVOICE.Reference_CLI)
    WHERE
      Reference_CLI <> 'UNKNOWN'
    GROUP BY
      ACCOUNT_NUMBER,
      Reference_CLI,
      New_PTT_No,
      INVOICE_TYPE,
      VALUE,
      TYPE
    ORDER BY
      Reference_CLI )SPINE
  LEFT OUTER JOIN
    CUSTOMER
  ON
    (CUSTOMER.ACCOUNT_NUMBER = SPINE.ACCOUNT_NUMBER)
  LEFT OUTER JOIN
    BO_ACC
  ON
    (BO_ACC.ACCOUNT_NUMBER = SPINE.ACCOUNT_NUMBER)
  LEFT OUTER JOIN
    service_instance
  ON
    service_instance.portfolio_id = BO_ACC.portfolio_id
  LEFT OUTER JOIN
    service_instance service_instance2
  ON
    SPINE.ACCOUNT_NUMBER = service_instance2.ACCOUNT_NUMBER
  GROUP BY
    SPINE.ACCOUNT_NUMBER,
    BO_ACCOUNT,
    service_instance_id,
    BO_ACC.portfolio_id,
    SPINE.Reference_CLI,
    SPINE.INVOICE_TYPE,
    SPINE.VALUE,
    SERVICE_ID,
    DIRECTORY_NUMBER,
    SERVICE_START_DATE,
    SERVICE_CEASE_DATE,
    bo_account_number,
    TYPE,
    New_PTT_No )ALL_DATA
LEFT OUTER JOIN
  KEENAN_VALUE
ON
  (KEENAN_VALUE.INCURRING_CUST_ACCOUNT_NO = ALL_DATA.bo_account_number)
LEFT OUTER JOIN
  CHARGE_VALUE
ON
  (CHARGE_VALUE.INCURRING_CUST_ACCOUNT_NO = ALL_DATA.bo_account_number
    AND DATE(CHARGE_VALUE.CREATED_DT) = DATE(KEENAN_VALUE.CREATED_DT))
LEFT OUTER JOIN
  BILLED_DATA
ON
  (BILLED_DATA.BO_ACCOUNT_NO = ALL_DATA.bo_account_number)
LEFT OUTER JOIN
  BILLED_VALUE
ON
  (BILLED_VALUE.BO_ACCOUNT_NO = ALL_DATA.bo_account_number
    AND BILLED_VALUE.CREATED_DT = BILLED_DATA.CREATED_DT )
LEFT OUTER JOIN
  SUBS
ON
  (SUBS.ACCOUNT_NUMBER = all_data.ACCOUNT_NUMBER)
LEFT OUTER JOIN
  SUBS_ALTERNITIVE
ON
  (SUBS_ALTERNITIVE.ACCOUNT_NUMBER = all_data.ACCOUNT_NUMBER)
LEFT OUTER JOIN
  SUBS_CANCELLED
ON
  (SUBS_CANCELLED.ACCOUNT_NUMBER = all_data.ACCOUNT_NUMBER)
LEFT OUTER JOIN
  LATEST_ORDER
ON
  (LATEST_ORDER.SERVICE_ID = ALL_DATA.SERVICE_ID)
LEFT OUTER JOIN
  offer
ON
  offer.account_number = ALL_DATA.account_number
WHERE
  LATEST_SERVICE = 1
GROUP BY
  ACCOUNT_NUMBER,
  TYPE,
  bo_account_number,
  service_instance_id,
  portfolio_id,
  Reference_CLI,
  SERVICE_ID,
  DIRECTORY_NUMBER,
  SERVICE_START_DATE,
  SERVICE_CEASE_DATE,
  INVOICE_TYPE,
  VALUE,
  LAST_BILL_DATE,
  BILLED_VALUE,
  CHARGE_VALUE,
  CRM_STATUS_FIRST_DATE,
  CRM_STATUS_START_DATE,
  CRM_STATUS,
  ORDER_STATUS_CODE,
  ORDER_STATUS_SET_DATE,
  ORDER_ID,
  ORDER_TYPE_CODE,
  New_PTT_No,
  effective_to_dt,
  offer_status,
  portfolioid,
  serviceinstanceid,
  DESCRIPTION,
  BILL_NAME