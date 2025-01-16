
 --================statement 1================ 
DELETE FROM au_wks.wks_dstr_coles_sap_mapping
WHERE
  sap_code IN (
    SELECT
      sap_code
    FROM (
      SELECT
        sap_code,
        COUNT(*)
      FROM (
        SELECT
          sap_code,
          item_idnt
        FROM au_wks.wks_dstr_coles_sap_mapping
      )
      GROUP BY
        1
      HAVING
        COUNT(*) > 1
    )
  );

 --================statement 2================ 
DELETE FROM au_itg.itg_dstr_coles_sap_mapping
WHERE
  (LTRIM(sap_code, 0), LTRIM(item_idnt, 0)) IN (
    SELECT
      LTRIM(sap_code, 0),
      LTRIM(item_idnt, 0)
    FROM au_wks.wks_dstr_coles_sap_mapping
    WHERE
      NOT sap_code IS NULL OR sap_code <> ''
  );

 --================statement 3================ 
INSERT INTO au_itg.itg_dstr_coles_sap_mapping (
  sap_code,
  item_idnt,
  item_desc,
  crtd_dtmm
)
SELECT
  sap_code,
  item_idnt,
  item_desc,
  CURRENT_TIMESTAMP()
FROM au_wks.wks_dstr_coles_sap_mapping
WHERE
  NOT sap_code IS NULL OR sap_code <> '';
