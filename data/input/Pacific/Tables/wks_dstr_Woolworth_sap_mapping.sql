
 --================statement 1================ 
DELETE FROM au_wks.wks_dstr_Woolworth_sap_mapping;

 --================statement 2================ 
INSERT INTO au_wks.wks_dstr_Woolworth_sap_mapping (
  article_code,
  sap_code,
  article_name,
  crt_dttm
)
SELECT
  code,
  jnj_sap_code,
  prod_desc_ww,
  CURRENT_TIMESTAMP()
FROM au_sdl.sdl_mds_pacific_product_mapping_ww
WHERE
  NOT jnj_sap_code IS NULL OR jnj_sap_code <> '';
