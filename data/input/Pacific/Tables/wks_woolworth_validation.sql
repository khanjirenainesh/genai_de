
 --================statement 1================ 
DROP table IF EXISTS au_wks.wks_woolworth_validation;

 --================statement 2================ 
CREATE TABLE au_wks.wks_woolworth_validation AS
SELECT
  'WOOLWORTHS' AS dstr_nm,
  inv_date,
  inv.article_code,
  inv.articledesc,
  map.sap_code AS matl_id
FROM au_sdl.sdl_dstr_woolworth_inv AS inv
LEFT JOIN (
  SELECT DISTINCT
    article_code,
    sap_code
  FROM au_itg.itg_dstr_Woolworth_sap_mapping
  WHERE
    sap_code <> ''
) AS map
  ON LTRIM(inv.article_code, 0) = LTRIM(map.article_code, 0);
