
 --================statement 1================ 
DROP table IF EXISTS au_wks.wks_woolworth_validation;

 --================statement 2================ 
DROP table IF EXISTS au_wks.wks_dstr_woolworth_inv;

 --================statement 3================ 
CREATE TABLE au_wks.wks_dstr_woolworth_inv AS
SELECT
  SAP_PRNT_CUST_KEY,
  UPPER(SAP_PRNT_CUST_DESC) AS SAP_PRNT_CUST_DESC,
  inv_date,
  article_code,
  article_desc,
  CAST(soh_qty AS DECIMAL(16, 4)) AS soh_qty,
  soh_price
FROM (
  SELECT
    TO_DATE(inv.inv_date, 'DD/MM/YYYY') AS inv_date,
    inv.article_code,
    inv.articledesc AS article_desc,
    CAST(inv.soh_oms AS DECIMAL(10, 4)) * CAST(inv.om AS DECIMAL(10, 4)) AS soh_qty,
    CAST(soh_price AS DECIMAL(16, 4)) AS soh_price
  FROM au_sdl.sdl_dstr_woolworth_inv AS inv
  LEFT JOIN (
    SELECT DISTINCT
      article_code,
      sap_code
    FROM au_itg.itg_dstr_woolworth_sap_mapping
    WHERE
      sap_code <> ''
  ) AS map
    ON LTRIM(inv.article_code, 0) = LTRIM(map.article_code, 0)
) AS inv, (
  SELECT DISTINCT
    ECSD.PRNT_CUST_KEY AS SAP_PRNT_CUST_KEY,
    CDDES_PCK.CODE_DESC AS SAP_PRNT_CUST_DESC
  FROM RG_EDW.EDW_CUSTOMER_SALES_DIM AS ECSD, RG_EDW.EDW_CUSTOMER_BASE_DIM AS ECBD, RG_EDW.EDW_CODE_DESCRIPTIONS AS CDDES_PCK
  WHERE
    ECSD.CUST_NUM = ECBD.CUST_NUM
    AND ECSD.SLS_ORG IN ('3300', '330B', '330H', '3410', '341B')
    AND CDDES_PCK.CODE_TYPE() = 'Parent Customer Key'
    AND CDDES_PCK.CODE() = ECSD.PRNT_CUST_KEY
) AS cust
WHERE
  UPPER(TRIM(cust.SAP_PRNT_CUST_DESC)) = (
    SELECT
      TRIM(parameter_value)
    FROM rg_itg.itg_parameter_reg_inventory
    WHERE
      parameter_name = 'parent_desc_Grocery_WW'
  );
