from snowflake.connector.pandas_tools import write_pandas
from langchain_openai import AzureChatOpenAI
from sklearn.ensemble import IsolationForest
from langchain_core.prompts import PromptTemplate
from langchain_text_splitters import RecursiveJsonSplitter
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Any
from io import StringIO
import os
import json
import csv
import time
from datetime import datetime

load_dotenv()

class Config:
    """store environment variables"""
    def __init__(self):
        self.env_vars = {
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
        
        # Validate required environment variables
        missing_vars = [key for key, value in self.env_vars.items() if value is None]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

class DatabaseConnector:
    """Handle database connections and queries"""
    def __init__(self, config: Config):
        self.config = config
        self.conn = self._create_connection()
        print("Database connector initialized")

    def _create_connection(self):
        """Create Snowflake connection using native connector"""
        try:
            conn = snowflake.connector.connect(
                user=self.config.env_vars['SNOWFLAKE_USER'],
                password=self.config.env_vars['SNOWFLAKE_PASSWORD'],
                account=self.config.env_vars['SNOWFLAKE_ACCOUNT'],
                warehouse=self.config.env_vars['SNOWFLAKE_WAREHOUSE'],
                database=self.config.env_vars['SNOWFLAKE_DATABASE'],
                schema=self.config.env_vars['SNOWFLAKE_SCHEMA']
            )
            print("Successfully connected to Snowflake")
            return conn
        except Exception as e:
            print(f"Error connecting to Snowflake: {str(e)}")
            raise

    def get_table_metadata(self, database: str, schema: str, table: str = None) -> pd.DataFrame:
        """Fetch metadata for specified table(s)"""
        try:
            table_condition = f"and TABLE_NAME = '{table}'" if table else ""
            query = f"""
                SELECT 
                    TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                FROM {database}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}' {table_condition}
            """
            print(f"Fetching metadata for schema: {schema}" + (f", table: {table}" if table else ""))
            cursor = self.conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            cursor.close()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            print(f"Error fetching metadata: {str(e)}")
            raise

    def get_table_data(self, database: str, schema: str, table: str) -> pd.DataFrame:
        """Fetch data from specified table"""
        try:
            query = f"SELECT * FROM {database}.{schema}.{table}"
            print(f"Fetching data from table: {table}")
            cursor = self.conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            cursor.close()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            print(f"Error fetching table data: {str(e)}")
            raise

    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            print("Database connection closed")

class AnomalyDetector:
    """Handle anomaly detection in datasets"""
    @staticmethod
    def detect_anomalies(df: pd.DataFrame, table_name: str = "Unnamed Table") -> str:
        print(f"Starting anomaly detection for table: {table_name}")
        
        # Separate numeric and text columns
        numeric_columns = [col for col in df.columns 
                         if not pd.api.types.is_datetime64_any_dtype(df[col]) 
                         and pd.to_numeric(df[col], errors='coerce').notna().all()]
        
        if not numeric_columns:
            return f"No numeric data available for anomaly detection in table '{table_name}'."
        
        numeric_data = df[numeric_columns]
        
        # Initialize and fit Isolation Forest model
        model = IsolationForest(
            contamination=0.01,
            max_features=0.5,
            max_samples=0.5,
            n_estimators=50,
            random_state=42
        )
        
        anomalies = model.fit_predict(numeric_data)
        anomaly_indices = numeric_data.index[anomalies == -1]
        anomaly_rows = df.loc[anomaly_indices]
        
        if len(anomaly_indices) > 0:
            print(f"Found {len(anomaly_indices)} anomalies in {table_name}")
            return (
                f"Detected {len(anomaly_indices)} anomalies in the dataset of table '{table_name}'.\n"
                f"Anomalous rows:\n{anomaly_rows.to_string(index=False)}"
            )
        return f"No anomalies detected in table '{table_name}'."

class InsightGenerator:
    """Generate insights using Azure OpenAI"""
    def __init__(self, config: Config):
        self.model = AzureChatOpenAI(
            azure_endpoint=config.env_vars["AZURE_OPENAI_ENDPOINT"],
            azure_deployment=config.env_vars["AZURE_OPENAI_4o_DEPLOYMENT_NAME"],
            openai_api_version=config.env_vars["AZURE_OPENAI_API_VERSION"],
            openai_api_key=config.env_vars["AZURE_OPENAI_API_KEY"],
        )
        print("Insight generator initialized")

    def generate_insights(self, prompt: str) -> str:
        """Generate insights using the configured model"""
        print("Generating insights from prompt")
        response = self.model.invoke(prompt)
        return response.content

    @staticmethod
    def create_anomaly_prompt(issues: str) -> str:
        """Create prompt for anomaly analysis"""
        return f"""The following issues were detected in the database:\n\n{issues}\n.
                Give specific solution based on the anomalies.
                Dont add any extra line other than solution to the anomaly.
                Give tablewise solution.
                dont mix up solution for different tables.
                Ensure the format intact for every table same.
                Provide specific issue with wrong values.
                
                give solution in concise way.
                Also generate SQL query which is strictly snowflake friendly to get anomalies. 
                
                Sample output:
            
                table_name : <table name>
                solution : solution for issues provided.

                SQL Query:
                <sql query>

                """

    @staticmethod
    def create_semantic_prompt(data: pd.DataFrame, schema_details: pd.DataFrame, table_name: str) -> str:
        """Create prompt for semantic analysis"""
        return f""" 
                Given a column name, please provide a description of its likely semantic meaning, including what type of data it represents and its expected data type or format. Your response should focus on:
                
                Sample data: 
                {data}
                
                metadata: 
                {schema_details}

                1. Scan throught the records of each column to check if the data it holds aligns with its semantic meaning of its column name.
                2. Highlight errors ONLY IF the semantic meaning does not align with the column name.
                3. Skip the columns where the semantic meaning and the data it holds is valid.
                4. DONT SKIP text columns in the table.
                5. ONLY provide column names and its issues.
                6. Go through all the columns.
                7. Ensure the format intact .
                8. Please provide details of columns which has issues.
                Sample output:
                Issue: <issue>

                Please provide concise output
                """

def main():
    try:
        # Initialize components
        config = Config()
        db_connector = DatabaseConnector(config)
        anomaly_detector = AnomalyDetector()
        insight_generator = InsightGenerator(config)

        try:
            # Get database configuration
            database = config.env_vars["SNOWFLAKE_DATABASE"]
            schema = config.env_vars["SNOWFLAKE_SCHEMA"]

            # Get metadata for all tables
            metadata = db_connector.get_table_metadata(database, schema)
            total_tables = len(metadata['TABLE_NAME'].unique())
            print(f"Starting processing of {total_tables} tables")
            
            base_dir = str(Path(__file__).parent.parent)
            logs_dir = os.path.join(base_dir, "logs")
            os.makedirs(logs_dir, exist_ok=True)
            
            # Create timestamp for the report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = os.path.join(logs_dir, f"anomaly_detection_report_{timestamp}.txt")
            
            # Keep track of total processing time and tables processed
            total_start_time = time.time()
            tables_processed = 0
            
            # Process each table
            with open(report_path, "w") as report_file:
                # Write header with start time
                report_file.write(f"Analysis Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                report_file.write(f"Total Tables to Process: {total_tables}\n")
                report_file.write(f"{'-'*80}\n\n")
                
                for table in metadata['TABLE_NAME'].unique():
                    table_start_time = time.time()
                    print(f"\nProcessing table: {table}")
                    
                    # Get table data and metadata
                    df = db_connector.get_table_data(database, schema, table)
                    table_metadata = db_connector.get_table_metadata(database, schema, table)
                    
                    # Process data in chunks
                    chunk_size = 5000
                    chunks = [df[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]
                    
                    report_file.write(f"Table: {table}\n")
                    report_file.write(f"Processing Start Time: {datetime.now().strftime('%H:%M:%S')}\n")
                    report_file.write(f"Total Records: {len(df)}\n")
                    
                    for chunk_num, chunk in enumerate(chunks, 1):
                        chunk_start_time = time.time()
                        print(f"Processing chunk {chunk_num}/{len(chunks)} for table {table}")
                        
                        # Detect anomalies
                        anomaly_result = anomaly_detector.detect_anomalies(chunk, table_name=table)
                        
                        # Generate insights
                        if "Detected" in anomaly_result:
                            anomaly_prompt = insight_generator.create_anomaly_prompt(anomaly_result)
                            semantic_prompt = insight_generator.create_semantic_prompt(chunk, table_metadata, table)
                            
                            anomaly_insights = insight_generator.generate_insights(anomaly_prompt).replace("```plaintext", "").replace("```", "").strip()
                            symantic_issues = insight_generator.generate_insights(semantic_prompt).replace("```plaintext", "").replace("```", "").strip()
                            
                            # Write results to file
                            report_file.write(f"\nChunk {chunk_num}/{len(chunks)}:\n")
                            report_file.write(f"Anomaly Analysis:\n{anomaly_insights}\n")
                            report_file.write(f"Semantic Analysis:\n{symantic_issues}\n")
                        
                        chunk_time = time.time() - chunk_start_time
                        report_file.write(f"Chunk Processing Time: {chunk_time:.2f} seconds\n")
                    
                    # Calculate and write table processing time
                    table_time = time.time() - table_start_time
                    tables_processed += 1
                    
                    report_file.write(f"\nTable Processing Time: {table_time:.2f} seconds")
                    report_file.write(f"\nTables Processed: {tables_processed}/{total_tables}")
                    report_file.write(f"\n{'-'*80}\n\n")
                
                # Write summary at the end
                total_time = time.time() - total_start_time
                report_file.write(f"\nFinal Summary:\n")
                report_file.write(f"Total Tables Processed: {tables_processed}\n")
                report_file.write(f"Total Processing Time: {total_time:.2f} seconds\n")
                report_file.write(f"Average Time per Table: {(total_time/tables_processed):.2f} seconds\n")
                report_file.write(f"Analysis End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

            print(f"Analysis completed. Results written to {report_path}")
            print(f"Total processing time: {total_time:.2f} seconds")
            print(f"Total tables processed: {tables_processed}")

        finally:
            # Ensure database connection is closed
            db_connector.close()

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()