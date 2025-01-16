
 --================statement 1================ 
SELECT
  dstrbtr_id,
  cntry_code,
  outlet_id,
  order_date,
  invoice_date,
  order_no,
  invoice_no,
  sellout_afvat_bfdisc,
  total_discount,
  invoice_discount,
  sellout_afvat_afdisc,
  status,
  run_id,
  curr_date
FROM os_sdl.sdl_vn_dms_h_sellout_sales_fact
WHERE
  (dstrbtr_id, outlet_id, order_no) IN (
    SELECT
      dstrbtr_id,
      outlet_id,
      order_no
    FROM os_sdl.sdl_vn_dms_h_sellout_sales_fact
    GROUP BY
      dstrbtr_id,
      outlet_id,
      order_no
    HAVING
      COUNT(*) > 1
  );
