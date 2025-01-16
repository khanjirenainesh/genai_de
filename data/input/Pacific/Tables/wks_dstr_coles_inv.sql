
 --================statement 1================ 
DROP table IF EXISTS au_wks.wks_dstr_coles_inv;

 --================statement 2================ 
CREATE TABLE au_wks.wks_dstr_coles_inv AS
SELECT
  SAP_PRNT_CUST_KEY,
  UPPER(SAP_PRNT_CUST_DESC) AS SAP_PRNT_CUST_DESC,
  inv_date,
  article_code,
  article_desc,
  soh_qty,
  soh_price
FROM (
  SELECT
    TO_DATE(TRIM(inv.inv_date), 'DD/MM/YYYY') AS inv_date,
    inv.order_item AS article_code,
    inv.order_item_desc AS article_desc,
    CAST(inv.closing_soh_qty_unit AS DECIMAL(16, 4)) AS soh_qty,
    CAST(closing_soh_nic AS DECIMAL(16, 4)) AS soh_price
  FROM au_sdl.sdl_dstr_coles_inv AS inv
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
  UPPER(TRIM(SAP_PRNT_CUST_DESC)) = (
    SELECT
      TRIM(parameter_value)
    FROM rg_itg.itg_parameter_reg_inventory
    WHERE
      parameter_name = 'parent_desc_Grocery'
  );
