WITH
  nbi_rentals AS (
  SELECT
    advanced_billing_date,
    CASE
      WHEN full_period_amount IN (26.0, 27.0) THEN '500Mb'
      WHEN full_period_amount IN (31.0, 32.0) THEN '1Gb'
    ELSE
    'Check'
  END
    AS PRODUCT,
    COUNT(lead_cli) AS CUST_COUNT,
    SUM(
      CASE
        WHEN full_period_amount IN (27.0, 32.0) THEN signed_number -1
      ELSE
      signed_number
    END
      ) AS BILLED_VALUE,
    SUM(
      CASE
        WHEN full_period_amount IN (27.0, 32.0) THEN 1
      ELSE
      0
    END
      ) AS VOIP_CHARGE
  FROM
    `skyuk-uk-csgbillanalysis-dev.roi_rental.1_roi_invoice_union`
  WHERE
    advanced_billing_date = date_trunc(current_date, month)
    and notification_item = 'NFIB'
    AND notification_type = 'L'
  GROUP BY
    advanced_billing_date,
    notification_item_description,
    PRODUCT)
SELECT
  *
FROM (
  SELECT
    advanced_billing_date,
    product,
    cust_count,
    billed_value
  FROM
    nbi_rentals)
UNION ALL
SELECT
  *
FROM (
  SELECT
    advanced_billing_date,
    'VOIP' AS product,
    SUM(VOIP_CHARGE) AS cust_count,
    SUM(CAST(VOIP_CHARGE AS float64)) AS billed_value
  FROM
    nbi_rentals
  GROUP BY
    1,
    2)
ORDER BY
  advanced_billing_date DESC,
  product ASC