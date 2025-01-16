
 --================statement 1================ 
DELETE FROM au_sdl.sdl_perenso_Distributor_Detail;

 --================statement 2================ 
INSERT INTO au_sdl.sdl_perenso_Distributor_Detail
SELECT
  *
FROM au_wks.wks_perenso_Distributor_Detail;

 --================statement 3================ 
UPDATE au_sdl.sdl_perenso_Distributor_Detail SET run_id = '"+context.run_id+"'
WHERE
  run_id IS NULL;

 --================statement 4================ 
INSERT INTO au_sdl.sdl_raw_perenso_Distributor_Detail
SELECT
  *
FROM au_sdl.sdl_perenso_Distributor_Detail;
