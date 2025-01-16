
 --================statement 1================ 
SELECT
  acct_key AS "error value",
  'acct_key' AS "Validation Column",
  'Duplicate records in the source file: ' || field_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    field_key,
    acct_key,
    id,
    COUNT(*)
  FROM au_wks.wks_perenso_account_reln_id
  GROUP BY
    field_key,
    acct_key,
    id
  HAVING
    COUNT(*) > 1
)
UNION
SELECT
  NULL AS "error value",
  'field_key' AS "Validation Column",
  'field_key is Null for acct_key :' || acct_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_account_reln_id
  WHERE
    field_key IS NULL
)
UNION
SELECT
  NULL AS "error value",
  'acct_key' AS "Validation Column",
  'acct_key is Null for field_key :' || field_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_account_reln_id
  WHERE
    acct_key IS NULL
);
