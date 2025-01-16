
 --================statement 1================ 
SELECT
  branch_key AS "error value",
  'branch_key' AS "Validation Column",
  'Duplicate branch_key present for dist_key: ' || dist_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    dist_key,
    branch_key,
    COUNT(*)
  FROM au_wks.wks_perenso_distributor_detail
  GROUP BY
    dist_key,
    branch_key
  HAVING
    COUNT(*) > 1
)
UNION
SELECT
  NULL AS "error value",
  'dist_key' AS "Validation Column",
  'dist_key is Null for branch_key:' || branch_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_distributor_detail
  WHERE
    dist_key IS NULL
)
UNION
SELECT
  NULL AS "error value",
  'branch_key' AS "Validation Column",
  'branch_key is Null for dist_key :' || dist_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_distributor_detail
  WHERE
    branch_key IS NULL
);
