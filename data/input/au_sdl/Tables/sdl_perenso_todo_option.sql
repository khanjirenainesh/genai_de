
 --================statement 1================ 
DELETE FROM au_sdl.sdl_perenso_todo_option;

 --================statement 2================ 
INSERT INTO au_sdl.sdl_perenso_todo_option (
  option_key,
  todo_key,
  option_desc,
  dsp_order,
  active,
  cascade_next_todo_key,
  cascadeon_answermode
)
SELECT
  option_key,
  todo_key,
  option_desc,
  dsp_order,
  active,
  cascade_next_todo_key,
  cascadeon_answermode
FROM au_wks.wks_perenso_todo_option;

 --================statement 3================ 
UPDATE au_sdl.sdl_perenso_todo_option SET run_id = '"+context.run_id+"'
WHERE
  run_id IS NULL;

 --================statement 4================ 
INSERT INTO au_sdl.sdl_raw_perenso_todo_option
SELECT
  *
FROM au_sdl.sdl_perenso_todo_option;
