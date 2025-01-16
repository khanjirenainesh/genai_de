
 --================statement 1================ 
SELECT
  acct_type_key AS "error value",
  'acct_type_key' AS "Validation Column",
  'Duplicate acct_type_key present for field_key: ' || field_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    field_key,
    acct_type_key,
    COUNT(*)
  FROM au_wks.wks_perenso_account_fields
  GROUP BY
    field_key,
    acct_type_key
  HAVING
    COUNT(*) > 1
)
UNION
SELECT
  NULL AS "error value",
  'field_key' AS "Validation Column",
  'field_key is Null for acct_type_key :' || acct_type_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_account_fields
  WHERE
    field_key IS NULL
)
UNION
SELECT
  NULL AS "error value",
  'acct_type_key' AS "Validation Column",
  'acct_type_key is Null for field_key :' || field_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_account_fields
  WHERE
    acct_type_key IS NULL
);
