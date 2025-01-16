
 --================statement 1================ 
DELETE FROM au_itg.itg_perenso_todo_option;

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_todo_option (
  option_key,
  todo_key,
  option_desc,
  dsp_order,
  active,
  run_id,
  create_dt,
  cascade_next_todo_key,
  cascadeon_answermode
)
SELECT
  option_key,
  todo_key,
  option_desc,
  dsp_order,
  active,
  run_id,
  create_dt,
  cascade_next_todo_key,
  cascadeon_answermode
FROM au_sdl.sdl_perenso_todo_option;
