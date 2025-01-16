
 --================statement 1================ 
DELETE FROM au_sdl.sdl_perenso_account_group;

 --================statement 2================ 
INSERT INTO au_sdl.sdl_perenso_account_group
SELECT
  *
FROM au_wks.wks_perenso_account_group;

 --================statement 3================ 
UPDATE au_sdl.sdl_perenso_account_group SET run_id = '"+context.run_id+"'
WHERE
  run_id IS NULL;

 --================statement 4================ 
INSERT INTO au_sdl.sdl_raw_perenso_account_group
SELECT
  *
FROM au_sdl.sdl_perenso_account_group;
