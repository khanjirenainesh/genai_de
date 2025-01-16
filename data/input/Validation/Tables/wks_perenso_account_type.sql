
 --================statement 1================ 
SELECT
  acct_type_key AS "error value",
  'acct_type_key' AS "Validation Column",
  'Duplicate records for: ' || acct_type_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    acct_type_key,
    COUNT(*)
  FROM au_wks.wks_perenso_account_type
  GROUP BY
    acct_type_key
  HAVING
    COUNT(*) > 1
)
UNION
SELECT
  NULL AS "error value",
  'acct_type_key' AS "Validation Column",
  'acct_type_key is Null for dsp_order :' || dsp_order AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_account_type
  WHERE
    acct_type_key IS NULL
);
