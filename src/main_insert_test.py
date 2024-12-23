'''
this code generates good data and insert into tables
'''

from snowflake.connector.pandas_tools import write_pandas
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import PromptTemplate
import snowflake.connector
import pandas as pd
import json
import csv
import os
from pathlib import Path
from typing import Dict, List, Any

def create_snowflake_connection(env_vars: Dict[str, str]) -> snowflake.connector.SnowflakeConnection:
    """
    Create and return a Snowflake connection using environment variables.
    """
    return snowflake.connector.connect(
        user=env_vars["SNOWFLAKE_USER"],
        password=env_vars["SNOWFLAKE_PASSWORD"],
        account=env_vars["SNOWFLAKE_ACCOUNT"],
        warehouse=env_vars["SNOWFLAKE_WAREHOUSE"],
        database=env_vars["SNOWFLAKE_DATABASE"],
        schema=env_vars["SNOWFLAKE_SCHEMA"],
    )

def initialize_azure_model(env_vars: Dict[str, str]) -> AzureChatOpenAI:
    """
    Initialize and return Azure OpenAI model instance.
    """
    return AzureChatOpenAI(
        azure_endpoint=env_vars["AZURE_OPENAI_ENDPOINT"],
        azure_deployment=env_vars["AZURE_OPENAI_4o_DEPLOYMENT_NAME"],
        openai_api_version=env_vars["AZURE_OPENAI_API_VERSION"],
        openai_api_key=env_vars["AZURE_OPENAI_API_KEY"],
    )

def get_table_names(cursor) -> List[str]:
    """
    Fetch all table names from the TEST schema.
    """
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema = 'TEST' AND table_type = 'BASE TABLE'
    """)
    return [table[0] for table in cursor.fetchall()]

def fetch_table_data(cursor, table_names: List[str], limit: int = 1000) -> Dict[str, pd.DataFrame]:
    """
    Fetch data from all tables and return as dictionary of DataFrames.
    """
    table_data = {}
    for table_name in table_names:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=[col[0] for col in cursor.description])
        table_data[table_name] = df
    return table_data

def get_table_metadata(cursor) -> Dict[str, List[Dict[str, str]]]:
    """
    Fetch and structure table metadata.
    """
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

def get_synthetic_data_prompt() -> str:
    """
    Return the prompt template for synthetic data generation.
    """
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
    Generate json serializable data.
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
    response = model.invoke(formatted_prompt)
    
    # Clean up the response
    cleaned_response = response.content.replace("```json", "").replace("```", "").strip()
    
    try:
        return json.loads(cleaned_response)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return {}

def save_to_csv(data: Dict[str, List[Dict]], base_dir: str) -> None:
    """
    Save generated data to CSV files in the specified output directory.
    
    Args:
        data: Dictionary containing table data
        base_dir: Base project directory path
    """
    output_dir = os.path.join(base_dir, "data", "csv_output")
    
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    for table_name, rows in data.items():
        if not rows:
            print(f"Table '{table_name}' is empty. No file created.")
            continue
            
        output_csv_file = os.path.join(output_dir, f"{table_name}.csv")
        column_names = rows[0].keys()
        
        with open(output_csv_file, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=column_names)
            writer.writeheader()
            writer.writerows(rows)
            
        print(f"Table '{table_name}' saved to {output_csv_file}")

def load_to_snowflake(conn: snowflake.connector.SnowflakeConnection, table_names: List[str], base_dir: str) -> None:
    """
    Load CSV files into Snowflake tables.
    
    Args:
        conn: Snowflake connection object
        table_names: List of table names to load data into
        base_dir: Base project directory path
    """
    output_dir = os.path.join(base_dir, "data", "csv_output")
    
    for table_name in table_names:
        csv_file = os.path.join(output_dir, f"{table_name}.csv")
        
        if os.path.exists(csv_file):
            # Read CSV into DataFrame
            df = pd.read_csv(csv_file)
            
            # Write DataFrame to Snowflake with target schema
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name
            )
            
            print(f"Loaded {nrows} rows into {table_name}")
        else:
            print(f"CSV file for {table_name} not found")

def main():
    # Get environment variables
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
    
    # Get the project root directory (one level up from src directory)
    project_root = str(Path(__file__).parent.parent)
    
    # Initialize connections and model
    conn = create_snowflake_connection(env_vars)
    model = initialize_azure_model(env_vars)
    cursor = conn.cursor()
    
    try:
        # Get table names and metadata
        table_names = get_table_names(cursor)
        table_data = fetch_table_data(cursor, table_names)
        metadata = get_table_metadata(cursor)
        
        # Generate and save synthetic data
        synthetic_data = generate_synthetic_data(model, metadata)
        save_to_csv(synthetic_data, project_root)
        
        # Load data into Snowflake
        load_to_snowflake(conn, table_names, project_root)
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()