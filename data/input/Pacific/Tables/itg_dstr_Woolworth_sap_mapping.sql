
 --================statement 1================ 
DELETE FROM au_wks.wks_dstr_Woolworth_sap_mapping
WHERE
  article_code IN (
    SELECT
      article_code
    FROM (
      SELECT
        article_code,
        COUNT(*)
      FROM (
        SELECT
          article_code,
          sap_code
        FROM au_wks.wks_dstr_Woolworth_sap_mapping
      )
      GROUP BY
        1
      HAVING
        COUNT(*) > 1
    )
  );

 --================statement 2================ 
DELETE FROM au_itg.itg_dstr_Woolworth_sap_mapping
WHERE
  article_code IN (
    SELECT DISTINCT
      article_code
    FROM au_wks.wks_dstr_Woolworth_sap_mapping
    WHERE
      NOT sap_code IS NULL OR sap_code <> ''
  );

 --================statement 3================ 
INSERT INTO au_itg.itg_dstr_Woolworth_sap_mapping (
  article_code,
  sap_code,
  article_name,
  crt_dttm
)
SELECT
  article_code,
  sap_code,
  article_name,
  CURRENT_TIMESTAMP()
FROM au_wks.wks_dstr_Woolworth_sap_mapping
WHERE
  NOT sap_code IS NULL OR sap_code <> '';
