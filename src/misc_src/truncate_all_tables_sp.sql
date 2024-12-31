-- CALL TRUNCATE_ALL_TABLES('RAW', 'TEST');

CREATE OR REPLACE PROCEDURE TRUNCATE_ALL_TABLES(DATABASE_NAME STRING, SCHEMA_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    
    var sql_command = 
        `SELECT table_name 
         FROM ${DATABASE_NAME}.INFORMATION_SCHEMA.TABLES 
         WHERE table_schema = '${SCHEMA_NAME}'
         AND table_type = 'BASE TABLE'`;
    
    var tables = snowflake.execute({ sqlText: sql_command });
    
    
    var truncated_count = 0;
    
    
    while (tables.next()) {
        var table_name = tables.getColumnValue(1);
        try {
            var truncate_command = `TRUNCATE TABLE ${DATABASE_NAME}.${SCHEMA_NAME}.${table_name}`;
            snowflake.execute({ sqlText: truncate_command });
            truncated_count++;
        } catch (err) {
            return `Error truncating table ${table_name}: ${err}`;
        }
    }
    
    return `Successfully truncated ${truncated_count} tables in ${SCHEMA_NAME} schema`;
$$;