
 --================statement 1================ 
DELETE FROM au_sdl.sdl_perenso_todo;

 --================statement 2================ 
INSERT INTO au_sdl.sdl_perenso_todo (
  todo_key,
  todo_type,
  todo_desc,
  work_item_key,
  start_date,
  end_date,
  dsp_order,
  ans_type,
  cascadeon_answermode,
  cascade_todo_key,
  cascade_next_todo_key
)
SELECT
  todo_key,
  todo_type,
  todo_desc,
  work_item_key,
  start_date,
  end_date,
  dsp_order,
  ans_type,
  cascadeon_answermode,
  cascade_todo_key,
  cascade_next_todo_key
FROM au_wks.wks_perenso_todo;

 --================statement 3================ 
UPDATE au_sdl.sdl_perenso_todo SET run_id = '"+context.run_id+"'
WHERE
  run_id IS NULL;

 --================statement 4================ 
INSERT INTO au_sdl.sdl_raw_perenso_todo (
  todo_key,
  todo_type,
  todo_desc,
  work_item_key,
  start_date,
  end_date,
  run_id,
  dsp_order,
  ans_type,
  cascadeon_answermode,
  cascade_todo_key,
  cascade_next_todo_key
)
SELECT
  todo_key,
  todo_type,
  todo_desc,
  work_item_key,
  start_date,
  end_date,
  run_id,
  dsp_order,
  ans_type,
  cascadeon_answermode,
  cascade_todo_key,
  cascade_next_todo_key
FROM au_sdl.sdl_perenso_todo;
