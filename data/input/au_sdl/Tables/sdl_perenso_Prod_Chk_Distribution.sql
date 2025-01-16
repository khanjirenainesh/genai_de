
 --================statement 1================ 
DELETE FROM au_sdl.sdl_perenso_Prod_Chk_Distribution;

 --================statement 2================ 
INSERT INTO au_sdl.sdl_perenso_Prod_Chk_Distribution
SELECT
  *
FROM au_wks.wks_perenso_Prod_Chk_Distribution;

 --================statement 3================ 
UPDATE au_sdl.sdl_perenso_Prod_Chk_Distribution SET run_id = '"+context.run_id+"'
WHERE
  run_id IS NULL;

 --================statement 4================ 
INSERT INTO au_sdl.sdl_raw_perenso_Prod_Chk_Distribution
SELECT
  *
FROM au_sdl.sdl_perenso_Prod_Chk_Distribution;
