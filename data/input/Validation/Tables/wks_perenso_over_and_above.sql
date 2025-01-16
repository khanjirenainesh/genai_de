
 --================statement 1================ 
SELECT
  over_and_above_key AS "error value",
  'over_and_above_key' AS "Validation Column",
  'Duplicate records present for over_and_above_key: ' || over_and_above_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    over_and_above_key,
    acct_key,
    todo_option_key,
    prod_grp_key,
    COUNT(*)
  FROM au_wks.wks_perenso_over_and_above
  GROUP BY
    over_and_above_key,
    acct_key,
    todo_option_key,
    prod_grp_key
  HAVING
    COUNT(*) > 1
)
UNION
SELECT
  NULL AS "error value",
  'over_and_above_key' AS "Validation Column",
  'over_and_above_key is Null' AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_over_and_above
  WHERE
    over_and_above_key IS NULL
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
  FROM au_wks.wks_perenso_over_and_above
  WHERE
    acct_key IS NULL
)
UNION
SELECT
  NULL AS "error value",
  'todo_option_key' AS "Validation Column",
  'todo_option_key is Null' AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_over_and_above
  WHERE
    todo_option_key IS NULL
)
UNION
SELECT
  NULL AS "error value",
  'prod_grp_key' AS "Validation Column",
  'prod_grp_key is Null' AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_over_and_above
  WHERE
    prod_grp_key IS NULL
);
