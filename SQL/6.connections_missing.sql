/* code is needing cleaned */

WITH bill AS (
    SELECT ADVANCED_BILLING_DATE,
        New_PTT_No,
        order_number,
        CASE
            WHEN Notification_Item IN ('ADSL_GLOW', 'ADSL_KINDLE', 'ADSL_TORCH') THEN 'Rural'
            WHEN Notification_Item IN ('WBP', 'WBMAX', 'WBS') THEN 'Urban'
            WHEN Notification_Item IN ('WBC', 'LNB', 'WBC_Fault') THEN 'Fibre'
            WHEN Notification_Item IN (
                'WBPN',
                'WBPN150',
                'WBPN150',
                'WBPN300',
                'WBPN500',
                'WBPN1G'
            ) THEN 'WBPN'
            WHEN Notification_Item IN ('WBCN') THEN 'WBCN'
            WHEN Notification_Item IN ('VOIP') THEN 'VOIP'
            WHEN Notification_Item IN ('SIRO_FH') THEN 'SIRO'
            WHEN Notification_Item IN ('NFIB') THEN 'NBI'
            ELSE 'CHECK'
        END AS TYPE,
        case when Notification_Item = 'NFIB' then Notification_Item_Description else Notification_Item end as Notification_Item,
        Signed_Number
    FROM `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union`
    WHERE advanced_billing_date = DATE_TRUNC(CURRENT_DATE, MONTH)
        AND Record_Type IN (
            'Bitstream',
            'Line Share',
            'VOIP Service'
        )
        AND Notification_Type = 'P'
        AND Notification_Item_Description IN (
            'Broadband Network Activation Fee',
            'Broadband Transfer Fee',
            'FTTH Connection Charge',
            'Standard Install of Data Port Extension'
        )
),
orders AS (
    SELECT ORDER_ID
    FROM `skyuk-uk-csgbillanalysis-dev.Sandpit.all_orders`
    WHERE ORDER_TYPE_CODE IN (
            'Provide',
            'Migrate',
            'BulkMigrationOrder'
        )
        AND ORDER_STATUS_CODE = 'Completed'
    GROUP BY ORDER_ID
),
missing as 
(SELECT
    New_PTT_No
FROM (
        SELECT 
            bill.New_PTT_No,
            TYPE,
            Signed_Number,
            orders.ORDER_ID
        FROM bill
            LEFT OUTER JOIN orders ON SUBSTR(bill.order_number, 0, 9) = CAST(orders.order_id AS string)
    )
WHERE ORDER_ID IS  NULL
ORDER BY TYPE),
orders1 as
(SELECT
  o.*,
  i.new_ptt_no, type
FROM
  `skyuk-uk-csgbillanalysis-dev.Sandpit.all_orders` o
LEFT JOIN
  (SELECT
    DISTINCT CAST(order_number as INT) order_id, new_ptt_no,
    CASE
            WHEN Notification_Item IN ('ADSL_GLOW', 'ADSL_KINDLE', 'ADSL_TORCH') THEN 'Rural'
            WHEN Notification_Item IN ('WBP', 'WBMAX', 'WBS') THEN 'Urban'
            WHEN Notification_Item IN ('WBC', 'LNB', 'WBC_Fault') THEN 'Fibre'
            WHEN Notification_Item IN (
                'WBPN',
                'WBPN150',
                'WBPN150',
                'WBPN300',
                'WBPN500',
                'WBPN1G'
            ) THEN 'WBPN'
            WHEN Notification_Item IN ('WBCN') THEN 'WBCN'
            WHEN Notification_Item IN ('VOIP') THEN 'VOIP'
            WHEN Notification_Item IN ('SIRO_FH') THEN 'SIRO'
            WHEN Notification_Item IN ('NFIB') THEN 'NBI'
            ELSE 'CHECK'
        END AS TYPE
  FROM
    `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union`
  WHERE
    advanced_billing_date = "2022-06-01"
    AND notification_type = 'P'
    AND new_ptt_no IN (SELECT * from missing)
    AND billing_end_date IS NULL) i
ON o.ORDER_ID = i.order_id
WHERE
  PARENT_ORDER_TYPE_CODE = 'ROI'
  AND i.order_id is not null
ORDER BY
  o.ORDER_ID ASC,
  ORDER_STATUS_SET_DATE_TIME DESC)
SELECT * EXCEPT (status_row) FROM 
(SELECT
  row_number() over (partition by order_id order by order_status_set_date_time desc) as status_row,
  TYPE,
  ACCOUNT_NUMBER,
  ORDER_ID,
  new_ptt_no as UAN,
  DELIVERY_PHONE_NUMBER,
  ORDER_TYPE_CODE,
  ORDER_STATUS_CODE,
  CREATED_DATE,
  ORDER_STATUS_SET_DATE
FROM orders1)
WHERE status_row = 1
ORDER BY 1