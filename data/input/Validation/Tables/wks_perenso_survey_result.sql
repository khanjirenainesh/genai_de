
 --================statement 1================ 
SELECT
  store_chk_hdr_key AS "error value",
  'store_chk_hdr_key' AS "Validation Column",
  'Duplicate records present for store_chk_hdr_key: ' || store_chk_hdr_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    store_chk_hdr_key,
    line_key,
    todo_key,
    prod_grp_key,
    COUNT(*)
  FROM au_wks.wks_perenso_survey_result
  GROUP BY
    store_chk_hdr_key,
    line_key,
    todo_key,
    prod_grp_key
  HAVING
    COUNT(*) > 1
)
UNION
SELECT
  NULL AS "error value",
  'store_chk_hdr_key' AS "Validation Column",
  'store_chk_hdr_key is Null' AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_survey_result
  WHERE
    store_chk_hdr_key IS NULL
)
UNION
SELECT
  NULL AS "error value",
  'line_key' AS "Validation Column",
  'line_key is Null' AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_survey_result
  WHERE
    line_key IS NULL
)
UNION
SELECT
  NULL AS "error value",
  'todo_key' AS "Validation Column",
  'todo_key is Null' AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_survey_result
  WHERE
    todo_key IS NULL
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
  FROM au_wks.wks_perenso_survey_result
  WHERE
    prod_grp_key IS NULL
);
