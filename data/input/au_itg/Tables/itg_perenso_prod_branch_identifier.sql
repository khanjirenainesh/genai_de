
 --================statement 1================ 
INSERT INTO au_itg.itg_perenso_prod_branch_identifier
SELECT
  prod_key,
  branch_key,
  identifier,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_prod_branch_identifier;
