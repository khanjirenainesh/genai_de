import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import PromptTemplate
from datetime import datetime
from langchain_text_splitters import RecursiveJsonSplitter
import pandas as pd
import json
import csv
import os
import time
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Any
from azure.storage.filedatalake import DataLakeServiceClient
from io import StringIO

def process_tables():
    start_time = time.time()
    load_dotenv()

    conn = snowflake.connector.connect(
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA"),
    )

    model = AzureChatOpenAI(
        azure_endpoint=os.environ.get("AZURE_OPENAI_ENDPOINT"),
        azure_deployment=os.environ.get("AZURE_OPENAI_4o_DEPLOYMENT_NAME"),
        openai_api_version=os.environ.get("AZURE_OPENAI_API_VERSION"),
        openai_api_key=os.environ.get("AZURE_OPENAI_API_KEY"),
    )

    cursor = conn.cursor()

    azure_storage_connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    adls_client = DataLakeServiceClient.from_connection_string(azure_storage_connection_string)

    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema = 'TEST' AND table_type = 'BASE TABLE'
    """)
    tables = cursor.fetchall()
    table_names = [table[0] for table in tables]
    
    container_name = "genaicsvstore"
    file_system_client = adls_client.get_file_system_client(file_system=container_name)

    for table_name in table_names:
        table_start_time = time.time()
        print(f"Processing table: {table_name}")

        cursor.execute(f"""
        SELECT 
            COLUMN_NAME, 
            DATA_TYPE, 
            IS_NULLABLE, 
            CHARACTER_MAXIMUM_LENGTH 
        FROM 
            INFORMATION_SCHEMA.COLUMNS
        WHERE 
            TABLE_SCHEMA = 'TEST' AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """)
        metadata = cursor.fetchall()

        table_metadata = {
            table_name: [
                {
                    "column_name": column_name,
                    "data_type": data_type,
                    "is_nullable": is_nullable,
                    "character_maximum_length": character_maximum_length
                } for column_name, data_type, is_nullable,character_maximum_length in metadata
            ]
        }

        json_data = json.dumps(table_metadata, indent=4)
        splitter = RecursiveJsonSplitter(max_chunk_size=300)
        texts = splitter.split_text(json_data=json.loads(json_data))

        prompt_template = """
        Generate 50 rows of bad-quality data for the table based on the given metadata. 
        STRICTLY comply with table constraints and all column data types.
        Note: 
        - This bad data must be VALID in order to insert in respective tables without fail.
        - Don't put null in non-nullable fields and don't exceed character length limit from table metadata.
        - For TIMESTAMP fields, use the format 'YYYY-MM-DD HH:MM:SS' and ensure all timestamps are valid.
        - Ignore timestamp and date columns for bad data and always generate good data for them.
        While bad data should simulate realistic yet invalid scenarios violating constraints like:
        1. Negative or illogical values (e.g., negative age or weight).
        2. Duplicate primary keys.
        3. Logical inconsistencies (e.g., start date after end date, year > 9999).
        4. Missing values.
        
        Output format: 
        - Provide a JSON array containing serializable data for the table.
        - Generate JSON serializable data.
        - STRICTLY DO NOT provide any comments using // or descriptions for columns, only give me the JSON data.
        - Ensure all date and timestamp values are valid and within reasonable ranges.

        Here is the input table Metadata: 
        {metadata}
        """
        prompt = PromptTemplate(input_variables=["metadata"], template=prompt_template)
        formatted_prompt = prompt.format(metadata=texts[0])
        
        response = model.invoke(formatted_prompt)
        synthetic_data = response.content.replace("```json", "").replace("```", "").strip()

        try:
            parsed_data = json.loads(synthetic_data)
        except json.JSONDecodeError as e:
            print(f"Error: Failed to parse generated JSON data for table {table_name}. Error: {str(e)}")
            log_file = f"synthetic_data_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            try:
                with open(log_file, 'w') as f:
                    f.write(f"Table: {table_name}\n")
                    f.write(f"Error: {str(e)}\n")
                    f.write("Raw response:\n")
                    f.write(synthetic_data)
                print(f"Raw response logged to {log_file}")
            except IOError:
                current_dir = os.path.dirname(os.path.abspath(__file__))
                log_file = os.path.join(current_dir, log_file)
                with open(log_file, 'w') as f:
                    f.write(f"Table: {table_name}\n")
                    f.write(f"Error: {str(e)}\n")
                    f.write("Raw response:\n")
                    f.write(synthetic_data)
                print(f"Raw response logged to {log_file} in current directory")
            continue

        if parsed_data and isinstance(parsed_data, list) and len(parsed_data) > 0:
            csv_buffer = StringIO()
            if isinstance(parsed_data[0], dict):
                writer = csv.DictWriter(csv_buffer, fieldnames=parsed_data[0].keys())
                writer.writeheader()
                writer.writerows(parsed_data)
            else:
                writer = csv.writer(csv_buffer)
                writer.writerows(parsed_data)
            
            csv_content = csv_buffer.getvalue()
            file_client = file_system_client.get_file_client(f"{table_name}.csv")
            file_client.upload_data(csv_content, overwrite=True)
            print(f"Table '{table_name}' saved to ADLS container '{container_name}'")
        else:
            print(f"Table '{table_name}' has no valid data. No file created.")

        table_end_time = time.time()
        print(f"Time taken to process {table_name}: {table_end_time - table_start_time:.2f} seconds")

    cursor.close()
    conn.close()

    end_time = time.time()
    print(f"Total time taken: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    process_tables()
