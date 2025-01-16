
 --================statement 1================ 
INSERT INTO au_sdl.sdl_dstr_coles_inv_raw
SELECT
  vendor,
  vendor_name,
  dc_state_shrt_desc,
  dc_state_desc,
  dc,
  dc_desc,
  category,
  category_desc,
  order_item,
  order_item_desc,
  ean,
  inv_date,
  closing_soh_nic,
  closing_soh_qty_ctns,
  closing_soh_qty_octns,
  closing_soh_qty_unit,
  dc_days_on_hand,
  CURRENT_TIMESTAMP()
FROM au_sdl.sdl_dstr_coles_inv;
