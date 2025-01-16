
 --================statement 1================ 
INSERT INTO au_itg.itg_perenso_product_fields
SELECT
  field_key,
  field_desc,
  field_type,
  active,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_product_fields;
