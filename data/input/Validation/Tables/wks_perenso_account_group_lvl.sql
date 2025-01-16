
 --================statement 1================ 
SELECT
  acct_grp_lev_key AS "error value",
  'acct_grp_lev_key' AS "Validation Column",
  'Duplicate acct_grp_lev_key present for field_key: ' || field_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    field_key,
    acct_grp_lev_key,
    COUNT(*)
  FROM au_wks.wks_perenso_account_group_lvl
  GROUP BY
    field_key,
    acct_grp_lev_key
  HAVING
    COUNT(*) > 1
)
UNION
SELECT
  NULL AS "error value",
  'field_key' AS "Validation Column",
  'field_key is Null for acct_grp_lev_key :' || acct_grp_lev_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_account_group_lvl
  WHERE
    field_key IS NULL
)
UNION
SELECT
  NULL AS "error value",
  'acct_grp_lev_key' AS "Validation Column",
  'acct_grp_lev_key is Null for field_key :' || field_key AS Validation,
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM au_wks.wks_perenso_account_group_lvl
  WHERE
    acct_grp_lev_key IS NULL
);
