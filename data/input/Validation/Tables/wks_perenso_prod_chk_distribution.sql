
 --================statement 1================ 
SELECT
  acct_key AS "error value",
  'acct_key' AS "Validation Column",
  'Duplicate records present in file.' AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    acct_key,
    prod_key,
    start_date,
    end_date,
    COUNT(*)
  FROM au_wks.wks_perenso_prod_chk_distribution
  GROUP BY
    acct_key,
    prod_key,
    start_date,
    end_date
  HAVING
    COUNT(*) > 1
)
UNION
SELECT
  NULL AS "error value",
  'acct_key' AS "Validation Column",
  'acct_key is Null' AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_prod_chk_distribution
  WHERE
    acct_key IS NULL
)
UNION
SELECT
  NULL AS "error value",
  'prod_key' AS "Validation Column",
  'prod_key is Null' AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_prod_chk_distribution
  WHERE
    prod_key IS NULL
);
