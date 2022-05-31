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
        Notification_Item,
        Notification_Type,
        Lead_CLI,
        Reference_CLI,
        Notification_Item_Description,
        Signed_Number
    FROM `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union`
    WHERE Record_Type IN (
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
    GROUP BY ADVANCED_BILLING_DATE,
        New_PTT_No,
        order_number,
        Notification_Item,
        Notification_Type,
        Lead_CLI,
        Reference_CLI,
        Notification_Item_Description,
        Signed_Number,
        TYPE
),
orders AS (
    SELECT ORDER_ID,
        BT_REFERENCE,
        BT_CIRCUIT,
        DELIVERY_PHONE_NUMBER,
        SERVICE_ID,
        CREATED_DATE,
        ORDER_STATUS_CODE,
        ORDER_TYPE_CODE
    FROM `skyuk-uk-csgbillanalysis-dev.Sandpit.all_orders`
    WHERE ORDER_TYPE_CODE IN (
            'Provide',
            'Migrate',
            'BulkMigrationOrder'
        )
        AND ORDER_STATUS_CODE = 'Completed'
    GROUP BY ORDER_ID,
        BT_REFERENCE,
        BT_CIRCUIT,
        DELIVERY_PHONE_NUMBER,
        SERVICE_ID,
        CREATED_DATE,
        ORDER_STATUS_CODE,
        ORDER_TYPE_CODE
)
SELECT TYPE,
    COUNT(New_PTT_No) AS CUST_COUNT,
    CAST(ROUND(SUM(Signed_Number), 2) AS float64) AS BILLED_VALUE
FROM (
        SELECT ADVANCED_BILLING_DATE,
            SUBSTR(bill.order_number, 0, 9) order_number,
            bill.New_PTT_No,
            TYPE,
            Notification_Item,
            Notification_Type,
            Lead_CLI,
            bill.Reference_CLI,
            Notification_Item_Description,
            Signed_Number,
            orders.ORDER_ID
        FROM bill
            FULL OUTER JOIN orders ON SUBSTR(bill.order_number, 0, 9) = CAST(orders.order_id AS string)
        WHERE advanced_billing_date = DATE_TRUNC(CURRENT_DATE, MONTH)
        GROUP BY ADVANCED_BILLING_DATE,
            order_number,
            bill.New_PTT_No,
            TYPE,
            Notification_Item,
            Notification_Type,
            Lead_CLI,
            bill.Reference_CLI,
            Notification_Item_Description,
            Signed_Number,
            ORDER_ID
    )
WHERE ORDER_ID IS NOT NULL
GROUP BY TYPE
ORDER BY TYPE