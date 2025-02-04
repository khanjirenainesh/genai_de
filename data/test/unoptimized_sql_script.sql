CREATE OR REPLACE VIEW v_sales_analysis_problematic AS
WITH all_transactions AS (
    SELECT *, 
           CASE WHEN payment_method = 'CREDIT' THEN total_amount * 1.03 
                WHEN payment_method = 'DEBIT' THEN total_amount * 1.01 
                ELSE total_amount END as adjusted_amount,
           DATEDIFF('day', transaction_date, CURRENT_TIMESTAMP()) as days_since_transaction
    FROM sales_transactions
    WHERE created_at >= DATEADD(year, -5, CURRENT_TIMESTAMP())
),
customer_info AS (
    SELECT *,
           DATEDIFF('day', registration_date, CURRENT_DATE()) as customer_age_days,
           CASE WHEN DATEDIFF('day', last_purchase_date, CURRENT_DATE()) <= 90 THEN 'Active'
                WHEN DATEDIFF('day', last_purchase_date, CURRENT_DATE()) <= 180 THEN 'Semi-Active'
                ELSE 'Inactive' END as customer_status
    FROM customer_demographics
),
product_stats AS (
    SELECT product_id,
           AVG(stock_quantity) as avg_stock,
           MAX(last_restock_date) as last_restock,
           COUNT(*) as record_count
    FROM product_inventory
    GROUP BY product_id
),
store_metrics AS (
    SELECT s.store_id,
           s.store_name,
           s.region_id,
           COUNT(DISTINCT sr.sales_rep_id) as total_reps,
           SUM(CASE WHEN sr.termination_date IS NULL THEN 1 ELSE 0 END) as active_reps
    FROM store_locations s
    LEFT JOIN sales_representatives sr ON s.store_id = sr.store_id
    GROUP BY s.store_id, s.store_name, s.region_id
)
SELECT 
    t.transaction_id,
    c.first_name || ' ' || c.last_name || ' (' || c.loyalty_tier || ')' as customer_details,
    p.product_name,
    CASE 
        WHEN t.quantity * t.unit_price >= 1000 THEN 'High Value'
        WHEN t.quantity * t.unit_price >= 500 THEN 'Medium Value'
        WHEN t.quantity * t.unit_price >= 100 THEN 'Low Value'
        ELSE 'Very Low Value'
    END as transaction_tier,
    t.adjusted_amount * 
    CASE WHEN c.loyalty_tier = 'PLATINUM' THEN 0.95
         WHEN c.loyalty_tier = 'GOLD' THEN 0.97
         WHEN c.loyalty_tier = 'SILVER' THEN 0.98
         ELSE 1 END as final_amount,
    CASE WHEN t.transaction_date::DATE = CURRENT_DATE() THEN 'Today'
         WHEN t.transaction_date::DATE = DATEADD(day, -1, CURRENT_DATE()) THEN 'Yesterday'
         ELSE TO_CHAR(t.transaction_date::DATE, 'YYYY-MM-DD')
    END as transaction_date_display,
    ps.avg_stock,
    sm.total_reps,
    sm.active_reps,
    (SELECT COUNT(*) 
     FROM sales_transactions st2 
     WHERE st2.customer_id = t.customer_id 
     AND st2.transaction_date >= DATEADD(month, -6, t.transaction_date)) as customer_transactions_6m
FROM all_transactions t
LEFT JOIN customer_info c ON t.customer_id = c.customer_id
LEFT JOIN product_inventory p ON t.product_id = p.product_id
LEFT JOIN product_stats ps ON p.product_id = ps.product_id
LEFT JOIN store_metrics sm ON t.store_id = sm.store_id
WHERE t.transaction_status = 'COMPLETED'
AND t.total_amount > 0;