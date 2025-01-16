
 --================statement 1================ 
delete from os_itg.itg_vn_dms_h_sellout_sales_fact
where
(dstrbtr_id, outlet_id, order_no) in (
select
dstrbtr_id,
outlet_id,
order_no
from os_wks.wks_sdl_vn_dms_h_sellout_sales_fact
);

 --================statement 2================ 
INSERT INTO os_itg.itg_vn_dms_h_sellout_sales_fact
SELECT
  dstrbtr_id,
  TRIM(cntry_code),
  outlet_id,
  CAST(order_date AS DATE) AS order_date,
  CAST(invoice_date AS DATE) AS invoice_date,
  order_no,
  invoice_no,
  CAST(sellout_afvat_bfdisc AS DECIMAL(15, 4)),
  CAST(total_discount AS DECIMAL(15, 4)),
  CAST(invoice_discount AS DECIMAL(15, 4)),
  CAST(sellout_afvat_afdisc AS DECIMAL(15, 4)),
  status,
  crtd_dttm,
  CONVERT_TIMEZONE('SGT', CURRENT_TIMESTAMP()) AS updt_dttm,
  run_id
FROM os_wks.wks_itg_vn_dms_h_sellout_sales_fact;
