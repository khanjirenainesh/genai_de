
 --================statement 1================ 
INSERT INTO au_edw.edw_px_term_plan_ext (
  cmp_id,
  channel_id,
  cust_id,
  matl_id,
  gltt_longname,
  time_period,
  px_term_proj_amt,
  year,
  gltt_rowid
)
SELECT
  '0000',
  '00',
  ac_attribute,
  sku_stockcode,
  gltt_longname,
  timeperiod,
  CASE timeperiod
    WHEN 1
    THEN asps_month1
    WHEN 2
    THEN asps_month2
    WHEN 3
    THEN asps_month3
    WHEN 4
    THEN asps_month4
    WHEN 5
    THEN asps_month5
    WHEN 6
    THEN asps_month6
    WHEN 7
    THEN asps_month7
    WHEN 8
    THEN asps_month8
    WHEN 9
    THEN asps_month9
    WHEN 10
    THEN asps_month10
    WHEN 11
    THEN asps_month11
    WHEN 12
    THEN asps_month12
  END AS px_term_proj_amt,
  bd_shortname,
  gltt_rowid
FROM (
  SELECT
    edw_px_term_plan.ac_code,
    edw_px_term_plan.ac_attribute,
    edw_px_term_plan.ac_longname,
    edw_px_term_plan.sku_stockcode,
    edw_px_term_plan.sku_attribute,
    edw_px_term_plan.sku_longname,
    edw_px_term_plan.gltt_longname,
    edw_px_term_plan.bd_shortname,
    edw_px_term_plan.bd_longname,
    edw_px_term_plan.asps_type,
    SUM(edw_px_term_plan.asps_month1) AS asps_month1,
    SUM(edw_px_term_plan.asps_month2) AS asps_month2,
    SUM(edw_px_term_plan.asps_month3) AS asps_month3,
    SUM(edw_px_term_plan.asps_month4) AS asps_month4,
    SUM(edw_px_term_plan.asps_month5) AS asps_month5,
    SUM(edw_px_term_plan.asps_month6) AS asps_month6,
    SUM(edw_px_term_plan.asps_month7) AS asps_month7,
    SUM(edw_px_term_plan.asps_month8) AS asps_month8,
    SUM(edw_px_term_plan.asps_month9) AS asps_month9,
    SUM(edw_px_term_plan.asps_month10) AS asps_month10,
    SUM(edw_px_term_plan.asps_month11) AS asps_month11,
    SUM(edw_px_term_plan.asps_month12) AS asps_month12,
    edw_px_term_plan.gltt_rowid
  FROM au_edw.edw_px_term_plan
  GROUP BY
    edw_px_term_plan.ac_code,
    edw_px_term_plan.ac_attribute,
    edw_px_term_plan.ac_longname,
    edw_px_term_plan.sku_stockcode,
    edw_px_term_plan.sku_attribute,
    edw_px_term_plan.sku_longname,
    edw_px_term_plan.gltt_longname,
    edw_px_term_plan.bd_shortname,
    edw_px_term_plan.bd_longname,
    edw_px_term_plan.asps_type,
    edw_px_term_plan.gltt_rowid
), (
  SELECT
    1 AS timeperiod
  UNION ALL
  SELECT
    2
  UNION ALL
  SELECT
    3
  UNION ALL
  SELECT
    4
  UNION ALL
  SELECT
    5
  UNION ALL
  SELECT
    6
  UNION ALL
  SELECT
    7
  UNION ALL
  SELECT
    8
  UNION ALL
  SELECT
    9
  UNION ALL
  SELECT
    10
  UNION ALL
  SELECT
    11
  UNION ALL
  SELECT
    12
) AS m
WHERE
  ac_attribute <> '  ';


cmp_id varchar(10),
channel_id varchar(20),
cust_id varchar(50),
matl_id varchar(15),
gltt_longname varchar(40),
time_period number(18,0),
px_term_proj_amt float,
year varchar(4),
gltt_rowid number(18,0)