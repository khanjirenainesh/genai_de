
 --================statement 1================ 
CREATE TABLE au_wks.wks_perenso_acct_hist_dim AS
WITH new_rec AS (
  SELECT DISTINCT
    acct_id
  FROM au_edw.edw_perenso_account_hist_dim
)
SELECT DISTINCT
  t.*,
  CASE
    WHEN new_rec.acct_id IS NULL
    THEN CAST('2019-01-01' AS DATE)
    WHEN h.start_date IS NULL
    THEN CURRENT_DATE
    ELSE h.start_date
  END AS start_date,
  CAST('2099-12-31' AS DATE) AS end_date,
  'N' AS hist_flg,
  CASE WHEN h.crt_dttm IS NULL THEN CURRENT_TIMESTAMP() ELSE h.crt_dttm END AS crt_dttm,
  CURRENT_TIMESTAMP() AS upd_dttm
FROM au_edw.edw_perenso_account_dim AS t
LEFT JOIN new_rec
  ON t.acct_id = new_rec.acct_id
LEFT JOIN (
  SELECT
    *
  FROM au_edw.edw_perenso_account_hist_dim
  WHERE
    hist_flg = 'N'
) AS h
  ON t.acct_id = h.acct_id AND t.acct_store_code = h.acct_store_code;
