
 --================statement 1================ 
SELECT
  acct_type_key AS "error value",
  'acct_type_key' AS "Validation Column",
  'Duplicate acct_type_key present for acct_key: ' || acct_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    acct_key,
    acct_type_key,
    COUNT(*)
  FROM au_wks.wks_perenso_account
  GROUP BY
    acct_key,
    acct_type_key
  HAVING
    COUNT(*) > 1
)
UNION
SELECT
  NULL AS "error value",
  'acct_key' AS "Validation Column",
  'acct_key is Null for acct_type_key :' || acct_type_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_account
  WHERE
    acct_key IS NULL
)
UNION
SELECT
  NULL AS "error value",
  'acct_type_key' AS "Validation Column",
  'acct_type_key is Null for acct_key :' || acct_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_account
  WHERE
    acct_type_key IS NULL
);
