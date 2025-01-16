
 --================statement 1================ 
DELETE FROM au_itg.itg_customer_grocery_inventory
WHERE
  (UPPER(sap_prnt_cust_desc), LTRIM(article_code, '0'), article_desc, inv_date) IN (
    SELECT
      UPPER(sap_prnt_cust_desc),
      LTRIM(article_code, '0'),
      article_desc,
      inv_date
    FROM au_wks.wks_dstr_coles_inv
  );

 --================statement 2================ 
INSERT INTO au_itg.itg_customer_grocery_inventory (
  sap_prnt_cust_key,
  sap_prnt_cust_desc,
  inv_date,
  article_code,
  article_desc,
  soh_qty,
  soh_price,
  crt_dttm
)
SELECT
  sap_prnt_cust_key,
  sap_prnt_cust_desc,
  inv_date,
  article_code,
  article_desc,
  soh_qty,
  soh_price,
  CURRENT_TIMESTAMP()
FROM au_wks.wks_dstr_coles_inv;
