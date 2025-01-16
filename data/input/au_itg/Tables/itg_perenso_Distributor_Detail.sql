
 --================statement 1================ 
DELETE FROM au_itg.itg_perenso_Distributor_Detail;

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_Distributor_Detail
SELECT
  dist_key,
  distributor,
  dist_id,
  branch_key,
  display_name,
  short_name,
  active,
  run_id,
  create_dt
FROM au_sdl.sdl_perenso_Distributor_Detail;
