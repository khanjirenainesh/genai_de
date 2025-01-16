
 --================statement 1================ 
SELECT
  option_key AS "error value",
  'option_key' AS "Validation Column",
  'Duplicate records present for option_key: ' || option_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    option_key,
    todo_key,
    COUNT(*)
  FROM au_wks.wks_perenso_todo_option
  GROUP BY
    option_key,
    todo_key
  HAVING
    COUNT(*) > 1
)
UNION
SELECT
  NULL AS "error value",
  'option_key' AS "Validation Column",
  'option_key is Null' AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_todo_option
  WHERE
    option_key IS NULL
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
  FROM au_wks.wks_perenso_todo_option
  WHERE
    todo_key IS NULL
);
