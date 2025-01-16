
 --================statement 1================ 
DELETE FROM au_itg.itg_perenso_todo;

 --================statement 2================ 
INSERT INTO au_itg.itg_perenso_todo (
  todo_key,
  todo_type,
  todo_desc,
  work_item_key,
  start_date,
  end_date,
  run_id,
  create_dt,
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
  CAST(TO_TIMESTAMP(SUBSTRING(start_date, 0, 23), 'DD/mm/yyyy HH:mi:ss %p') AS TIMESTAMPNTZ) AS start_date,
  CAST(TO_TIMESTAMP(SUBSTRING(end_date, 0, 23), 'DD/mm/yyyy HH:mi:ss %p') AS TIMESTAMPNTZ) AS end_date,
  run_id,
  create_dt,
  dsp_order,
  ans_type,
  cascadeon_answermode,
  cascade_todo_key,
  cascade_next_todo_key
FROM au_sdl.sdl_perenso_todo;
