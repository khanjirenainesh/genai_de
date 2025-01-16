
 --================statement 1================ 
DELETE FROM au_sdl.sdl_perenso_over_and_above_state;

 --================statement 2================ 
INSERT INTO au_sdl.sdl_perenso_over_and_above_state
SELECT
  *
FROM au_wks.wks_perenso_Over_and_Above_State;

 --================statement 3================ 
UPDATE au_sdl.sdl_perenso_over_and_above_state SET run_id = '"+context.run_id+"'
WHERE
  run_id IS NULL;

 --================statement 4================ 
INSERT INTO au_sdl.sdl_raw_perenso_over_and_above_state
SELECT
  *
FROM au_sdl.sdl_perenso_over_and_above_state;
