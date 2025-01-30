CREATE OR REPLACE VIEW inefficient_health_records_view AS
WITH cte1 AS (  
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY hospital ORDER BY date_of_admission DESC) AS rn 
    FROM health_records
),
cte2 AS (  
    SELECT 
        hospital, 
        COUNT(*) AS total_patients,
        AVG(billing_amount) AS avg_bill 
    FROM health_records
    GROUP BY hospital
),
cte3 AS ( 
    SELECT 
        h1.name, h1.age, h1.gender, h1.blood_type, h1.medical_condition, 
        h1.date_of_admission, h1.doctor, h1.hospital, h1.insurance_provider,
        h1.billing_amount, h1.room_number, h1.admission_type, h1.discharge_date,
        h2.medication, h2.test_results, 
        cte2.total_patients
    FROM health_records h1
    LEFT JOIN health_records h2 ON h1.name = h2.name AND h1.hospital = h2.hospital 
    LEFT JOIN cte2 ON h1.hospital = cte2.hospital 
)
SELECT 
    cte3.name, cte3.age, cte3.gender, cte3.blood_type, cte3.medical_condition, 
    cte3.date_of_admission, cte3.doctor, cte3.hospital, cte3.insurance_provider,
    cte3.billing_amount, cte3.room_number, cte3.admission_type, cte3.discharge_date,
    cte3.medication, cte3.test_results
FROM cte3
ORDER BY cte3.date_of_admission DESC;