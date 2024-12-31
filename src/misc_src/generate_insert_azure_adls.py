'''
This code generates synthetic data, saves it to Azure Data Lake Storage Gen2,
and then inserts it into Snowflake tables.
'''

from snowflake.connector.pandas_tools import write_pandas
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import PromptTemplate
import snowflake.connector
import pandas as pd
import json
import csv
import os
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Any
from azure.storage.filedatalake import DataLakeServiceClient
from io import StringIO

load_dotenv()

def create_snowflake_connection(env_vars: Dict[str, str]) -> snowflake.connector.SnowflakeConnection:
    """
    Create and return a Snowflake connection using environment variables.
    """
    try:
        return snowflake.connector.connect(
            user=env_vars.get("SNOWFLAKE_USER"),
            password=env_vars.get("SNOWFLAKE_PASSWORD"),
            account=env_vars.get("SNOWFLAKE_ACCOUNT"),
            warehouse=env_vars.get("SNOWFLAKE_WAREHOUSE"),
            database=env_vars.get("SNOWFLAKE_DATABASE"),
            schema=env_vars.get("SNOWFLAKE_SCHEMA"),
        )
    except Exception as e:
        print(f"Error creating Snowflake connection: {e}")
        raise

def initialize_azure_model(env_vars: Dict[str, str]) -> AzureChatOpenAI:
    """
    Initialize and return Azure OpenAI model instance.
    """
    try:
        return AzureChatOpenAI(
            azure_endpoint=env_vars.get("AZURE_OPENAI_ENDPOINT"),
            azure_deployment=env_vars.get("AZURE_OPENAI_4o_DEPLOYMENT_NAME"),
            openai_api_version=env_vars.get("AZURE_OPENAI_API_VERSION"),
            openai_api_key=env_vars.get("AZURE_OPENAI_API_KEY"),
        )
    except Exception as e:
        print(f"Error initializing Azure OpenAI model: {e}")
        raise

def get_table_names(cursor) -> List[str]:
    """
    Fetch all table names from the TEST schema.
    """
    try:
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables
            WHERE table_schema = 'TEST' AND table_type = 'BASE TABLE'
        """)
        return [table[0] for table in cursor.fetchall()]
    except Exception as e:
        print(f"Error fetching table names: {e}")
        return []

def fetch_table_data(cursor, table_names: List[str], limit: int = 1000) -> Dict[str, pd.DataFrame]:
    """
    Fetch data from all tables and return as dictionary of DataFrames.
    """
    table_data = {}
    for table_name in table_names:
        try:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=[col[0] for col in cursor.description])
            table_data[table_name] = df
        except Exception as e:
            print(f"Error fetching data for table {table_name}: {e}")
    return table_data

def get_table_metadata(cursor) -> Dict[str, List[Dict[str, str]]]:
    """
    Fetch and structure table metadata.
    """
    try:
        cursor.execute("""
            SELECT 
                TABLE_NAME, 
                COLUMN_NAME, 
                DATA_TYPE, 
                IS_NULLABLE, 
                COLUMN_DEFAULT 
            FROM 
                INFORMATION_SCHEMA.COLUMNS
            WHERE 
                TABLE_SCHEMA = 'TEST'
            ORDER BY table_name
        """)
        
        metadata = cursor.fetchall()
        tables = {}
        
        for table_name, column_name, data_type, is_nullable, _ in metadata:
            if table_name not in tables:
                tables[table_name] = []
            
            tables[table_name].append({
                "column_name": column_name,
                "data_type": data_type,
                "is_nullable": is_nullable
            })
        
        return tables
    except Exception as e:
        print(f"Error fetching table metadata: {e}")
        return {}

def get_synthetic_data_prompt() -> str:
    return """
    You are a data generator tasked with creating synthetic data. Based on the following JSON metadata describing table structure and data types, generate sample data rows for each column. 
    - Adhere to the specified types, constraints, and formats.
    - Provide 10 rows of sample data in JSON array format.
    - Ensure the data is realistic and coherent.

    Metadata:
    {metadata}

    Expected Output:
    Provide 10 rows of JSON data for each table. Use same format as metadata.
    Provide the output in pure json format which I can parse as a json data to various platforms.
    """

def generate_synthetic_data(model: AzureChatOpenAI, metadata: Dict) -> Dict:
    """
    Generate synthetic data using the AI model.
    """
    prompt = PromptTemplate(
        input_variables=["metadata"],
        template=get_synthetic_data_prompt()
    )
    
    formatted_prompt = prompt.format(metadata=json.dumps(metadata, indent=4))
    try:
        response = model.invoke(formatted_prompt)
        
        # Clean up the response
        cleaned_response = response.content.replace("```json", "").replace("```", "").strip()
        
        return json.loads(cleaned_response)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return {}
    except Exception as e:
        print(f"Error generating synthetic data: {e}")
        return {}

def save_to_adls(data: Dict[str, List[Dict]], adls_client: DataLakeServiceClient, container_name: str) -> None:
    """
    Save generated data to CSV files in Azure Data Lake Storage Gen2.
    
    Args:
        data: Dictionary containing table data
        adls_client: ADLS Gen2 service client
        container_name: Name of the container to store files
    """
    try:
        file_system_client = adls_client.get_file_system_client(file_system=container_name)
        
        for table_name, rows in data.items():
            if not rows:
                print(f"Table '{table_name}' is empty. No file created.")
                continue
                
            csv_content = StringIO()
            writer = csv.DictWriter(csv_content, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
            
            file_client = file_system_client.get_file_client(f"{table_name}.csv")
            file_client.upload_data(csv_content.getvalue(), overwrite=True)
            
            print(f"Table '{table_name}' saved to ADLS Gen2")
    except Exception as e:
        print(f"Error saving to ADLS Gen2: {e}")

def load_to_snowflake(conn: snowflake.connector.SnowflakeConnection, table_names: List[str], adls_client: DataLakeServiceClient, container_name: str) -> None:
    """
    Load CSV files from ADLS Gen2 into Snowflake tables.
    
    Args:
        conn: Snowflake connection object
        table_names: List of table names to load data into
        adls_client: ADLS Gen2 service client
        container_name: Name of the container storing files
    """
    try:
        file_system_client = adls_client.get_file_system_client(file_system=container_name)
        
        for table_name in table_names:
            file_client = file_system_client.get_file_client(f"{table_name}.csv")
            
            download_stream = file_client.download_file()
            file_contents = download_stream.readall().decode('utf-8')
            
            df = pd.read_csv(StringIO(file_contents))
            
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name
            )
            
            print(f"Loaded {nrows} rows into {table_name}")
    except Exception as e:
        print(f"Error loading to Snowflake: {e}")

def generate_synthetic_data_for_table(model: AzureChatOpenAI, table_metadata: Dict) -> Dict:
    """
    Generate synthetic data for a single table using the AI model.
    """
    prompt = PromptTemplate(
        input_variables=["metadata"],
        template=get_synthetic_data_prompt()
    )
    
    formatted_prompt = prompt.format(metadata=json.dumps({"table": table_metadata}, indent=4))
    #print(formatted_prompt)
    try:
        response = model.invoke(formatted_prompt)
        #print(response.content)
        
        # Clean up the response
        cleaned_response = response.content.replace("```json", "").replace("```", "").strip()
        #print(cleaned_response)
        
        return json.loads(cleaned_response)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return {}
    except Exception as e:
        print(f"Error generating synthetic data for table: {e}")
        return {}

def main():
    # Get environment variables
    env_vars = {
        "SNOWFLAKE_USER": os.environ.get("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD"),
        "SNOWFLAKE_ACCOUNT": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "SNOWFLAKE_DATABASE": os.environ.get("SNOWFLAKE_DATABASE"),
        "SNOWFLAKE_SCHEMA": os.environ.get("SNOWFLAKE_SCHEMA"),
        "AZURE_OPENAI_ENDPOINT": os.environ.get("AZURE_OPENAI_ENDPOINT"),
        "AZURE_OPENAI_4o_DEPLOYMENT_NAME": os.environ.get("AZURE_OPENAI_4o_DEPLOYMENT_NAME"),
        "AZURE_OPENAI_API_VERSION": os.environ.get("AZURE_OPENAI_API_VERSION"),
        "AZURE_OPENAI_API_KEY": os.environ.get("AZURE_OPENAI_API_KEY"),
    }
    
    # Initialize connections and clients
    try:
        conn = create_snowflake_connection(env_vars)
        model = initialize_azure_model(env_vars)
        cursor = conn.cursor()
        azure_storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        
        if not azure_storage_connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set")
        adls_client = DataLakeServiceClient.from_connection_string(azure_storage_connection_string)
    except Exception as e:
        print(f"Error initializing connections: {e}")
        return

    container_name = "genaicsvstore"
    
    try:
        # Get table names and metadata
        table_names = get_table_names(cursor)
        metadata = get_table_metadata(cursor)
        
        # Process each table iteratively
        for table_name in table_names:
            print(f"\nProcessing table: {table_name}")
            
            # Generate synthetic data for this table
            table_metadata = metadata.get(table_name, [])
            #print(table_metadata)

            if not table_metadata:
                print(f"No metadata found for table {table_name}, skipping...")
                continue
                
            print("Generating synthetic data...")
            synthetic_data = generate_synthetic_data_for_table(model, table_metadata)
            
            if not synthetic_data:
                print(f"No synthetic data generated for {table_name}, skipping...")
                continue
            
            # Save this table's data to ADLS Gen2
            print("Saving to ADLS Gen2...")
            save_to_adls({table_name: synthetic_data}, adls_client, container_name)
            
            # Load this table's data to Snowflake
            print("Loading to Snowflake...")
            load_to_snowflake(conn, [table_name], adls_client, container_name)
            
            print(f"Completed processing table: {table_name}")
        
    except Exception as e:
        print(f"Error in main process: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()