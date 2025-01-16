
 --================statement 1================ 
SELECT
  acct_key AS "error value",
  'acct_key' AS "Validation Column",
  'Duplicate acct_key present for acct_grp_key: ' || acct_grp_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    acct_grp_key,
    acct_key,
    COUNT(*)
  FROM au_wks.wks_perenso_account_group_reln
  GROUP BY
    acct_grp_key,
    acct_key
  HAVING
    COUNT(*) > 1
)
UNION
SELECT
  NULL AS "error value",
  'acct_grp_key' AS "Validation Column",
  'acct_grp_key is Null for acct_key :' || acct_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_account_group_reln
  WHERE
    acct_grp_key IS NULL
)
UNION
SELECT
  NULL AS "error value",
  'acct_key' AS "Validation Column",
  'acct_key is Null for acct_grp_key :' || acct_grp_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_account_group_reln
  WHERE
    acct_key IS NULL
);
