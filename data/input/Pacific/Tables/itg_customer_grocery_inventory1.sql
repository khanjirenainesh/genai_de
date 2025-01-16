
 --================statement 1================ 
DELETE FROM au_itg.itg_customer_grocery_inventory
WHERE
  (UPPER(sap_prnt_cust_desc), LTRIM(article_code, '0'), article_desc, inv_date) IN (
    SELECT
      UPPER(sap_prnt_cust_desc),
      LTRIM(article_code, '0'),
      article_desc,
      inv_date
    FROM au_wks.wks_dstr_woolworth_inv
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
FROM au_wks.wks_dstr_woolworth_inv;

 --================statement 3================ 
INSERT INTO au_sdl.sdl_dstr_woolworth_inv_raw
SELECT
  inv_date,
  rank,
  article_code,
  articledesc,
  mm_code,
  mm_name,
  cm_code,
  cm_name,
  rm_code,
  rm_name,
  rep_code,
  replenisher,
  goods_supplier_code,
  goods_supplier_name,
  lt,
  dc_code,
  acd,
  rp_type,
  alc_status,
  om,
  vp,
  ti,
  hi,
  ww_stores,
  sl_perc,
  sl_missed_value,
  soh_oms,
  soo_oms,
  soh_price,
  demand_oms,
  issues_oms,
  not_supplied_oms,
  fairshare_oms,
  overlay_oms,
  awd_oms,
  avg_issues,
  dos_oms,
  due_date,
  reason,
  oos_comments,
  oos_28_days,
  cons_days_oos,
  total_wholesale_demand_om,
  total_wholesale_issue_om,
  wholesale_flag,
  CURRENT_TIMESTAMP()
FROM au_sdl.sdl_dstr_woolworth_inv;
