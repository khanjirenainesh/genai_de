
 --================statement 1================ 
DELETE FROM au_itg.itg_perenso_account_fields;

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_account_fields
SELECT
  field_key,
  field_desc,
  field_type,
  acct_type_key,
  active,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_account_fields;
