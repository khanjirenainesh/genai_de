
 --================statement 1================ 
SELECT
  branch_key AS "error value",
  'branch_key' AS "Validation Column",
  'Duplicate branch_key present for acct_key: ' || acct_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    acct_key,
    branch_key,
    id,
    COUNT(*)
  FROM au_wks.wks_perenso_acct_dist_acct
  GROUP BY
    acct_key,
    branch_key,
    id
  HAVING
    COUNT(*) > 1
)
UNION
SELECT
  NULL AS "error value",
  'acct_key' AS "Validation Column",
  'acct_key is Null for branch_key:' || branch_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_acct_dist_acct
  WHERE
    acct_key IS NULL
)
UNION
SELECT
  NULL AS "error value",
  'branch_key' AS "Validation Column",
  'branch_key is Null for acct_key :' || acct_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_acct_dist_acct
  WHERE
    branch_key IS NULL
);
