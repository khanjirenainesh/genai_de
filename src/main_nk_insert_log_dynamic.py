
'''
this code is working with false data generation dynamically and inserted into tables
only problem is , it is also using logging table for this use case whihc we have to skip
we are not saving synthetic data anywhere here in this code and directly inserting into tables 
'''






from snowflake.connector.pandas_tools import write_pandas
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import PromptTemplate
import snowflake.connector
import pandas as pd
import os
import json
import random
import string
import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple

def create_snowflake_connection(env_vars: Dict[str, str]) -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        user=env_vars["SNOWFLAKE_USER"],
        password=env_vars["SNOWFLAKE_PASSWORD"],
        account=env_vars["SNOWFLAKE_ACCOUNT"],
        warehouse=env_vars["SNOWFLAKE_WAREHOUSE"],
        database=env_vars["SNOWFLAKE_DATABASE"],
        schema=env_vars["SNOWFLAKE_SCHEMA"],
    )

def create_error_log_table(cursor) -> None:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS TEST.INSERT_ERROR_LOG (
            error_id NUMBER IDENTITY(1,1),
            table_name VARCHAR(255),
            column_name VARCHAR(255),
            error_message VARCHAR(1000),
            failed_row_data VARCHAR(4000),
            error_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            error_type VARCHAR(100)
        )
    """)

def generate_invalid_data(data_type: str) -> Any:
    invalid_data = {
        "NUMBER": lambda: random.choice(['abc', '', '!@#', None, float('inf')]),
        "FLOAT": lambda: random.choice(['xyz', '', '###', None, 'NaN']),
        "VARCHAR": lambda: random.choice([
            ''.join(random.choices(string.printable, k=1000)),
            None,
            bytes([random.randint(0, 255) for _ in range(10)]),
        ]),
        "DATE": lambda: random.choice([
            'invalid_date',
            '2024-13-45',
            '0000-00-00',
            None,
            datetime.datetime.max
        ]),
        "TIMESTAMP_NTZ": lambda: random.choice([
            'invalid_timestamp',
            '2024-13-45 25:61:99',
            None,
            datetime.datetime.max
        ]),
        "BOOLEAN": lambda: random.choice(['maybe', 2, 'true', None, 'invalid'])
    }
    
    data_type = data_type.upper().split('(')[0]
    generator = invalid_data.get(data_type, lambda: None)
    return generator()

def generate_valid_data(data_type: str) -> Any:
    valid_data = {
        "NUMBER": lambda: random.randint(-1000000, 1000000),
        "FLOAT": lambda: random.uniform(-1000000, 1000000),
        "VARCHAR": lambda: ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(1, 50))),
        "DATE": lambda: datetime.date.today() + datetime.timedelta(days=random.randint(-1000, 1000)),
        "TIMESTAMP_NTZ": lambda: datetime.datetime.now() + datetime.timedelta(days=random.randint(-1000, 1000)),
        "BOOLEAN": lambda: random.choice([True, False])
    }
    
    data_type = data_type.upper().split('(')[0]
    generator = valid_data.get(data_type, lambda: None)
    return generator()

def generate_mixed_data(metadata: Dict[str, List[Dict]], num_rows: int = 100) -> Dict[str, List[Dict]]:
    mixed_data = {}
    
    for table_name, columns in metadata.items():
        table_rows = []
        for _ in range(num_rows):
            row = {}
            for column in columns:
                if random.random() < 0.3:  # 30% chance of invalid data
                    row[column['column_name']] = generate_invalid_data(column['data_type'])
                else:
                    row[column['column_name']] = generate_valid_data(column['data_type'])
            table_rows.append(row)
        mixed_data[table_name] = table_rows
    
    return mixed_data

def insert_with_error_logging(cursor, conn: snowflake.connector.SnowflakeConnection, table_name: str, data: List[Dict]) -> None:
    df = pd.DataFrame(data)
    
    for index, row in df.iterrows():
        try:
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=pd.DataFrame([row]),
                table_name=table_name,
                quote_identifiers=False
            )
            print(f"Inserted row {index} into {table_name}")
        except Exception as e:
            error_message = str(e)
            error_type = type(e).__name__
            failed_column = None
            
            # Try to identify which column caused the error
            for column, value in row.items():
                try:
                    cursor.execute(f"INSERT INTO {table_name} ({column}) VALUES (%s)", (value,))
                except Exception as column_e:
                    if str(column_e) == error_message:
                        failed_column = column
                        break
            
            cursor.execute("""
                INSERT INTO TEST.INSERT_ERROR_LOG (table_name, column_name, error_message, failed_row_data, error_type)
                VALUES (%s, %s, %s, %s, %s)
            """, (table_name, failed_column, error_message, str(row.to_dict())[:4000], error_type))
            
            print(f"Error inserting into {table_name}, column {failed_column}: {error_message}")

def main():
    env_vars = {
        "SNOWFLAKE_USER": os.environ["SNOWFLAKE_USER"],
        "SNOWFLAKE_PASSWORD": os.environ["SNOWFLAKE_PASSWORD"],
        "SNOWFLAKE_ACCOUNT": os.environ["SNOWFLAKE_ACCOUNT"],
        "SNOWFLAKE_WAREHOUSE": os.environ["SNOWFLAKE_WAREHOUSE"],
        "SNOWFLAKE_DATABASE": os.environ["SNOWFLAKE_DATABASE"],
        "SNOWFLAKE_SCHEMA": os.environ["SNOWFLAKE_SCHEMA"],
        "AZURE_OPENAI_ENDPOINT": os.environ["AZURE_OPENAI_ENDPOINT"],
        "AZURE_OPENAI_4o_DEPLOYMENT_NAME": os.environ["AZURE_OPENAI_4o_DEPLOYMENT_NAME"],
        "AZURE_OPENAI_API_VERSION": os.environ["AZURE_OPENAI_API_VERSION"],
        "AZURE_OPENAI_API_KEY": os.environ["AZURE_OPENAI_API_KEY"],
    }
    print("Environment variables loaded.")

    def get_table_names(cursor) -> List[str]:
        cursor.execute("SHOW TABLES")
        table_names = [row[1] for row in cursor.fetchall()]
        print(f"Retrieved table names: {table_names}")
        return table_names

    def get_table_metadata(cursor) -> Dict[str, List[Dict]]:
        metadata = {}
        for table_name in get_table_names(cursor):
            cursor.execute(f"DESCRIBE TABLE {table_name}")
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    'column_name': row[0],
                    'data_type': row[1],
                    'nullable': row[3] == 'Y'
                })
            metadata[table_name] = columns
        print(f"Metadata retrieved: {metadata}")
        return metadata

    conn = create_snowflake_connection(env_vars)
    print("Snowflake connection established.")
    cursor = conn.cursor()

    try:
        create_error_log_table(cursor)
        print("Error log table created.")

        table_names = get_table_names(cursor)
        metadata = get_table_metadata(cursor)

        mixed_data = generate_mixed_data(metadata, num_rows=200)
        print(f"Generated mixed data for tables: {list(mixed_data.keys())}")

        for table_name, data in mixed_data.items():
            print(f"Inserting data into {table_name}")
            insert_with_error_logging(cursor, conn, table_name, data)

        conn.commit()
        print("All changes committed.")
        
    finally:
        cursor.close()
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    main()