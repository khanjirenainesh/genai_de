
 --================statement 1================ 
SELECT
  over_and_above_key AS "error value",
  'over_and_above_key' AS "Validation Column",
  'Duplicate records present for over_and_above_key: ' || over_and_above_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    over_and_above_key,
    store_chk_hdr_key,
    COUNT(*)
  FROM au_wks.wks_perenso_over_and_above_state
  GROUP BY
    over_and_above_key,
    store_chk_hdr_key
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
  FROM au_wks.wks_perenso_over_and_above_state
  WHERE
    over_and_above_key IS NULL
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
  FROM au_wks.wks_perenso_over_and_above_state
  WHERE
    store_chk_hdr_key IS NULL
);
