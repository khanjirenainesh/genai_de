from langchain_openai import AzureChatOpenAI
from sklearn.ensemble import IsolationForest
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import os
import time
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Tuple
import psycopg2
from pandas.api.types import is_numeric_dtype
import math

load_dotenv(override=True)

class Config:
    """Store environment variables and warehouse configuration"""
    SUPPORTED_WAREHOUSES = ['snowflake', 'redshift', 'bigquery']
    
    def __init__(self, warehouse_type):
        if warehouse_type.lower() not in self.SUPPORTED_WAREHOUSES:
            raise ValueError(f"Unsupported warehouse type. Supported types: {', '.join(self.SUPPORTED_WAREHOUSES)}")
        
        self.warehouse_type = warehouse_type.lower()
        self.env_vars = self._load_environment_variables()
        
    def _load_environment_variables(self) -> Dict[str, str]:
        """Load environment variables based on selected warehouse"""
        common_vars = {
            "AZURE_OPENAI_ENDPOINT": os.environ.get("AZURE_OPENAI_ENDPOINT"),
            "AZURE_OPENAI_4o_DEPLOYMENT_NAME": os.environ.get("AZURE_OPENAI_4o_DEPLOYMENT_NAME"),
            "AZURE_OPENAI_API_VERSION": os.environ.get("AZURE_OPENAI_API_VERSION"),
            "AZURE_OPENAI_API_KEY": os.environ.get("AZURE_OPENAI_API_KEY"),
        }
        
        warehouse_vars = {
            'snowflake': {
                "SNOWFLAKE_USER": os.environ.get("SNOWFLAKE_USER"),
                "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD"),
                "SNOWFLAKE_ACCOUNT": os.environ.get("SNOWFLAKE_ACCOUNT"),
                "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE"),
                "SNOWFLAKE_DATABASE": os.environ.get("SNOWFLAKE_DATABASE"),
                "SNOWFLAKE_SCHEMA": os.environ.get("SNOWFLAKE_SCHEMA"),
            },
            'redshift': {
                "REDSHIFT_HOST": os.environ.get("REDSHIFT_HOST"),
                "REDSHIFT_PORT": os.environ.get("REDSHIFT_PORT"),
                "REDSHIFT_DATABASE": os.environ.get("REDSHIFT_DATABASE"),
                "REDSHIFT_USER": os.environ.get("REDSHIFT_USER"),
                "REDSHIFT_PASSWORD": os.environ.get("REDSHIFT_PASSWORD"),
                "REDSHIFT_SCHEMA": os.environ.get("REDSHIFT_SCHEMA"),
            },
            'bigquery': {
                "BIGQUERY_PROJECT_ID": os.environ.get("BIGQUERY_PROJECT_ID"),
                "BIGQUERY_DATASET": os.environ.get("BIGQUERY_DATASET"),
                "BIGQUERY_CREDENTIALS": os.environ.get("BIGQUERY_CREDENTIALS"),
            }
        }
        
        env_vars = {**common_vars, **warehouse_vars[self.warehouse_type]}
        missing_vars = [key for key, value in env_vars.items() if value is None]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
            
        return env_vars



class DatabaseConnector(ABC):
    """Abstract base class for database connections"""
    @abstractmethod
    def _create_connection(self):
        pass
    
    @abstractmethod
    def get_table_metadata(self, database: str, schema: str, table: str = None) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def get_table_data(self, database: str, schema: str, table: str) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def close(self):
        pass

class SnowflakeConnector(DatabaseConnector):
    def __init__(self, config: Config):
        self.config = config
        self.conn = self._create_connection()
        print("Snowflake connector initialized")

    def _create_connection(self):
        try:
            return snowflake.connector.connect(
                user=self.config.env_vars['SNOWFLAKE_USER'],
                password=self.config.env_vars['SNOWFLAKE_PASSWORD'],
                account=self.config.env_vars['SNOWFLAKE_ACCOUNT'],
                warehouse=self.config.env_vars['SNOWFLAKE_WAREHOUSE'],
                database=self.config.env_vars['SNOWFLAKE_DATABASE'],
                schema=self.config.env_vars['SNOWFLAKE_SCHEMA']
            )
        except Exception as e:
            print(f"Error connecting to Snowflake: {str(e)}")
            raise

    def get_table_metadata(self, database: str, schema: str, table: str = None) -> pd.DataFrame:
        try:
            table_condition = f"and TABLE_NAME = '{table}'" if table else ""
            query = f"""
                SELECT 
                    TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                FROM {database}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}' {table_condition}
            """
            cursor = self.conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            cursor.close()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            print(f"Error fetching Snowflake metadata: {str(e)}")
            raise

    def get_table_data(self, database: str, schema: str, table: str) -> pd.DataFrame:
        try:
            query = f"SELECT * FROM {database}.{schema}.{table}"
            cursor = self.conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            cursor.close()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            print(f"Error fetching Snowflake table data: {str(e)}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            print("Snowflake connection closed")

class RedshiftConnector(DatabaseConnector):
    def __init__(self, config: Config):
        self.config = config
        self.conn = self._create_connection()
        print("Redshift connector initialized")

    def _create_connection(self):
        try:
            return psycopg2.connect(
                host=self.config.env_vars['REDSHIFT_HOST'],
                port=self.config.env_vars['REDSHIFT_PORT'],
                database=self.config.env_vars['REDSHIFT_DATABASE'],
                user=self.config.env_vars['REDSHIFT_USER'],
                password=self.config.env_vars['REDSHIFT_PASSWORD']
            )
        except Exception as e:
            print(f"Error connecting to Redshift: {str(e)}")
            raise

    def get_table_metadata(self, database: str, schema: str, table: str = None) -> pd.DataFrame:
        try:
            table_condition = f"AND tablename = '{table}'" if table else ""
            query = f"""
                SELECT 
                    tablename as TABLE_NAME,
                    column_name as COLUMN_NAME,
                    type as DATA_TYPE,
                    CASE WHEN is_nullable = 'YES' THEN 'TRUE' ELSE 'FALSE' END as IS_NULLABLE,
                    character_maximum_length as CHARACTER_MAXIMUM_LENGTH
                FROM pg_catalog.pg_tables t
                JOIN information_schema.columns c ON t.tablename = c.table_name
                WHERE schemaname = '{schema}' {table_condition}
            """
            return pd.read_sql(query, self.conn)
        except Exception as e:
            print(f"Error fetching Redshift metadata: {str(e)}")
            raise

    def get_table_data(self, database: str, schema: str, table: str) -> pd.DataFrame:
        try:
            query = f"SELECT * FROM {schema}.{table}"
            return pd.read_sql(query, self.conn)
        except Exception as e:
            print(f"Error fetching Redshift table data: {str(e)}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            print("Redshift connection closed")

class BigQueryConnector(DatabaseConnector):
    def __init__(self, config: Config):
        self.config = config
        self.client = self._create_connection()
        print("BigQuery connector initialized")

    def _create_connection(self):
        try:
            # Load credentials from the path specified in environment variables
            credentials = service_account.Credentials.from_service_account_file(
                self.config.env_vars['BIGQUERY_CREDENTIALS']
            )
            
            # Initialize BigQuery client
            client = bigquery.Client(
                project=self.config.env_vars['BIGQUERY_PROJECT_ID'],
                credentials=credentials
            )
            print("Successfully connected to BigQuery")
            return client
        except Exception as e:
            print(f"Error connecting to BigQuery: {str(e)}")
            raise

    def get_table_metadata(self, database: str, schema: str, table: str = None) -> pd.DataFrame:
        """
        Get metadata for BigQuery tables
        Note: In BigQuery, database is project_id and schema is dataset_id
        """
        try:
            dataset_ref = self.client.dataset(schema, project=database)
            
            metadata_rows = []
            if table:
                # Get metadata for specific table
                table_ref = dataset_ref.table(table)
                table_obj = self.client.get_table(table_ref)
                for field in table_obj.schema:
                    metadata_rows.append({
                        'TABLE_NAME': table,
                        'COLUMN_NAME': field.name,
                        'DATA_TYPE': field.field_type,
                        'IS_NULLABLE': 'YES' if field.is_nullable else 'NO',
                        'CHARACTER_MAXIMUM_LENGTH': None  # BigQuery doesn't specify max length
                    })
            else:
                # Get metadata for all tables in dataset
                tables = self.client.list_tables(dataset_ref)
                for table_list_item in tables:
                    table_obj = self.client.get_table(table_list_item.reference)
                    for field in table_obj.schema:
                        metadata_rows.append({
                            'TABLE_NAME': table_list_item.table_id,
                            'COLUMN_NAME': field.name,
                            'DATA_TYPE': field.field_type,
                            'IS_NULLABLE': 'YES' if field.is_nullable else 'NO',
                            'CHARACTER_MAXIMUM_LENGTH': None
                        })

            return pd.DataFrame(metadata_rows)
        except Exception as e:
            print(f"Error fetching BigQuery metadata: {str(e)}")
            raise

    def get_table_data(self, database: str, schema: str, table: str) -> pd.DataFrame:
        """
        Get data from BigQuery table
        Note: In BigQuery, database is project_id and schema is dataset_id
        """
        try:
            query = f"""
                SELECT *
                FROM `{database}.{schema}.{table}`
            """
            # Run the query and return results as a pandas DataFrame
            query_job = self.client.query(query)
            results = query_job.result()
            return results.to_dataframe()
        except Exception as e:
            print(f"Error fetching BigQuery table data: {str(e)}")
            raise

    def close(self):
        """
        Close the BigQuery client
        Note: BigQuery client doesn't need explicit closure, but we keep the method
        for consistency with the interface
        """
        if self.client:
            self.client.close()
            print("BigQuery connection closed")


def get_connector(config: Config) -> DatabaseConnector:
    """Factory function to create appropriate database connector"""
    connectors = {
        'snowflake': SnowflakeConnector,
        'redshift': RedshiftConnector,
        'bigquery': BigQueryConnector
    }
    
    connector_class = connectors.get(config.warehouse_type)
    if not connector_class:
        raise ValueError(f"No connector implementation for warehouse type: {config.warehouse_type}")
    
    return connector_class(config)

class ChunkProcessor:
    """Handle processing of large tables with many columns"""
    def __init__(self, max_columns_per_batch: int = 20):
        self.max_columns_per_batch = max_columns_per_batch
    
    def split_columns(self, df: pd.DataFrame) -> List[List[str]]:
        """Split columns into manageable batches"""
        columns = df.columns.tolist()
        return [columns[i:i + self.max_columns_per_batch] 
                for i in range(0, len(columns), self.max_columns_per_batch)]
    
    def get_numeric_columns(self, df: pd.DataFrame, columns: List[str]) -> List[str]:
        """Get numeric columns from a specific set of columns"""

        numeric_columns = []
        text_columns = []

        # Iterate through the columns to check if they are numeric or non-numeric
        for column in columns:
        # Check if the column is not datetime and all values can be converted to numeric
            if not pd.api.types.is_datetime64_any_dtype(df[column]) and pd.to_numeric(df[column], errors='coerce').notna().all():
                numeric_columns.append(column)
            else:
                text_columns.append(column)

        return numeric_columns

class AnomalyDetector:
    """Enhanced anomaly detector to handle large column counts"""
    def __init__(self):
        self.chunk_processor = ChunkProcessor()
    
    def detect_anomalies(self, df: pd.DataFrame, table_name: str = "Unnamed Table") -> str:
        print(f"Starting anomaly detection for table: {table_name} with {len(df.columns)} columns")
        
        all_anomalies = []
        column_batches = self.chunk_processor.split_columns(df)
        
        for batch_idx, column_batch in enumerate(column_batches, 1):
            print(f"Processing column batch {batch_idx}/{len(column_batches)} for table {table_name}")
            
            # Get numeric columns for this batch
            numeric_columns = self.chunk_processor.get_numeric_columns(df, column_batch)
            
            if not numeric_columns:
                continue
                
            numeric_data = df[numeric_columns]
            
            try:
                model = IsolationForest(
                    contamination=0.01,
                    max_features=min(1.0, 10/len(numeric_columns)),  # Adjust features based on column count
                    max_samples=min(1.0, 1000/len(df)),  # Adjust samples based on row count
                    n_estimators=100,
                    random_state=42
                )
                
                anomalies = model.fit_predict(numeric_data)
                anomaly_indices = numeric_data.index[anomalies == -1]
                
                if len(anomaly_indices) > 0:
                    batch_anomalies = df.loc[anomaly_indices, column_batch]
                    all_anomalies.append(batch_anomalies)
                    
            except Exception as e:
                print(f"Error processing batch {batch_idx} of {table_name}: {str(e)}")
                continue
        
        if all_anomalies:
            combined_anomalies = pd.concat(all_anomalies, axis=1)
            return (
                f"Detected anomalies in {len(combined_anomalies)} rows in table '{table_name}'.\n"
                f"Anomalous rows:\n{combined_anomalies.to_string(index=False)}"
            )
            
        return f"No anomalies detected in table '{table_name}'."

class InsightGenerator:
    """Generate insights using Azure OpenAI with warehouse-specific prompts"""
    def __init__(self, config: Config):
        self.config = config
        self.warehouse_type = config.warehouse_type
        self.model = AzureChatOpenAI(
            azure_endpoint=config.env_vars["AZURE_OPENAI_ENDPOINT"],
            azure_deployment=config.env_vars["AZURE_OPENAI_4o_DEPLOYMENT_NAME"],
            openai_api_version=config.env_vars["AZURE_OPENAI_API_VERSION"],
            openai_api_key=config.env_vars["AZURE_OPENAI_API_KEY"],
        )
        print("Insight generator initialized")

    def generate_insights(self, prompt: str) -> str:
        print("Generating insights from prompt")
        response = self.model.invoke(prompt)
        return response.content

    def create_anomaly_prompt(self, issues: str) -> str:
        base_prompt = f"""The following issues were detected in the {self.warehouse_type} database:\n\n{issues}\n
                Give specific solution based on the anomalies.
                Dont add any extra line other than solution to the anomaly.
                Ensure that the following steps are applied to every table and every column, without skipping any due to the number of columns or complexity
                dont mix up solution for different tables.
                Ensure the format intact for every table same.
                Provide specific issue with wrong values.
                
                give solution in concise way.
                Also generate SQL query which is strictly {self.warehouse_type} friendly to get anomalies. 
                
                Sample output:
            
                table_name : <table name>
                solution : solution for issues provided.
                
                Also 
                1. Highlight columns that should be masked or encrypted, with compliance standards as PII, HIPAA, GDPR, SOC2 and provide suggestions
                2. Suggest appropriate masking techniques for each sensitive field
                Sample output:
            
                column name : <column name>
                solution : solution for issues provided.
                """
                
        # Add warehouse-specific SQL hints
        sql_hints = {
            'snowflake': "Generate Snowflake-specific SQL query with appropriate window functions and Snowflake features.",
            'redshift': "Generate Redshift-specific SQL query considering distribution and sort keys.",
            'bigquery': "Generate BigQuery-specific SQL query using appropriate partitioning and clustering."
        }
        
        return base_prompt + f"\n{sql_hints.get(self.warehouse_type, '')}\n\nSample output:\n\ntable_name : <table name>\nsolution : solution for issues provided.\n\nSQL Query:\n<sql query>\n"

    def create_semantic_prompt(self, data: pd.DataFrame, schema_details: pd.DataFrame, table_name: str) -> str:
        warehouse_specific_hints = {
            'snowflake': "Consider Snowflake-specific data types and variant/array columns.",
            'redshift': "Consider Redshift compression encodings and distribution styles.",
            'bigquery': "Consider BigQuery nested and repeated fields."
        }
        
        return f""" 
                Analysis for {self.warehouse_type} table:
                {warehouse_specific_hints.get(self.warehouse_type, '')}
                
                Sample data: 
                {data}
                
                metadata: 
                {schema_details}

                1. Scan through the records of each column to check if the data aligns with its semantic meaning.
                2. Highlight errors ONLY IF the semantic meaning does not align with the column name.
                3. Skip the columns where the semantic meaning and the data it holds is valid.
                4. Check for {self.warehouse_type}-specific data type optimizations.
                5. ONLY provide column names and its issues.
                6. Go through all the columns and all the tables.
                7. Ensure the format intact.
                8. Please provide details of columns which has issues.
                
                Sample output:
                Issue: <issue>

                Please provide concise output
                """


def process_large_table(df: pd.DataFrame, table_metadata: pd.DataFrame, 
                       insight_generator: InsightGenerator, 
                       report_file, table_name: str,
                       max_chunk_rows: int = 5000):
    """Process large tables with many columns efficiently"""
    
    # Split data into row chunks for memory management
    total_rows = len(df)
    chunk_size = min(max_chunk_rows, total_rows)
    num_chunks = math.ceil(total_rows / chunk_size)
    
    anomaly_detector = AnomalyDetector()
    
    report_file.write(f"\nProcessing table {table_name} with {len(df.columns)} columns\n")
    report_file.write(f"Total rows: {total_rows}\n")
    
    for chunk_idx in range(num_chunks):
        start_idx = chunk_idx * chunk_size
        end_idx = min((chunk_idx + 1) * chunk_size, total_rows)
        
        print(f"Processing rows {start_idx} to {end_idx} of {total_rows} for table {table_name}")
        chunk = df.iloc[start_idx:end_idx]
        
        try:
            # Detect anomalies in chunks
            anomaly_result = anomaly_detector.detect_anomalies(chunk, table_name=table_name)
            
            if "Detected" in anomaly_result:
                # Instead of parsing the string, work directly with the anomalous data
                anomaly_parts = anomaly_result.split('\n', 1)
                if len(anomaly_parts) > 1:
                    header, anomaly_data = anomaly_parts
                    
                    # Process in smaller batches for insight generation
                    max_insight_rows = 100
                    anomaly_df = pd.DataFrame([row.split() for row in anomaly_data.strip().split('\n')])
                    
                    for i in range(0, len(anomaly_df), max_insight_rows):
                        batch = anomaly_df.iloc[i:i + max_insight_rows]
                        
                        anomaly_prompt = insight_generator.create_anomaly_prompt(
                            f"Detected anomalies in batch {i//max_insight_rows + 1}:\n{batch.to_string()}"
                        )
                        semantic_prompt = insight_generator.create_semantic_prompt(
                            batch, table_metadata, table_name
                        )
                        
                        # Generate and write insights
                        anomaly_insights = insight_generator.generate_insights(anomaly_prompt)
                        semantic_issues = insight_generator.generate_insights(semantic_prompt)
                        
                        report_file.write(f"\nChunk {chunk_idx + 1}/{num_chunks}, "
                                        f"Batch {i//max_insight_rows + 1}:\n")
                        report_file.write(f"Anomaly Analysis:\n{anomaly_insights}\n")
                        report_file.write(f"Semantic Analysis:\n{semantic_issues}\n")
                    
        except Exception as e:
            print(f"Error processing chunk {chunk_idx + 1} of table {table_name}: {str(e)}")
            report_file.write(f"\nERROR in chunk {chunk_idx + 1}: {str(e)}\n")
            continue


def get_warehouse_config(config: Config) -> dict:
    """Get database and schema configuration for the specific warehouse type"""
    warehouse_type = config.warehouse_type
    
    try:
        if warehouse_type == 'snowflake':
            return {
                'database': config.env_vars["SNOWFLAKE_DATABASE"],
                'schema': config.env_vars["SNOWFLAKE_SCHEMA"]
            }
        elif warehouse_type == 'redshift':
            return {
                'database': config.env_vars["REDSHIFT_DATABASE"],
                'schema': config.env_vars["REDSHIFT_SCHEMA"]
            }
        elif warehouse_type == 'bigquery':
            return {
                'database': config.env_vars["BIGQUERY_PROJECT_ID"],
                'schema': config.env_vars["BIGQUERY_DATASET"]
            }
        else:
            raise ValueError(f"Unsupported warehouse type: {warehouse_type}")
            
    except KeyError as e:
        raise KeyError(f"Missing required configuration for {warehouse_type}: {str(e)}")


def main():
    try:
        # Get warehouse type from environment variable or use default as os.environ.get("WAREHOUSE_TYPE", "snowflake").lower()
        warehouse_type = os.environ.get("WAREHOUSE_TYPE").lower()
        
        # Initialize components
        config = Config(warehouse_type)
        db_connector = get_connector(config)
        anomaly_detector = AnomalyDetector()
        insight_generator = InsightGenerator(config)

        try:
            # Get database configuration based on warehouse type
            warehouse_config = get_warehouse_config(config)
            database = warehouse_config['database']
            schema = warehouse_config['schema']

            # Get metadata for all tables
            metadata = db_connector.get_table_metadata(database, schema)
            total_tables = len(metadata['TABLE_NAME'].unique())
            print(f"Starting processing of {total_tables} tables in {warehouse_type}")
            
            # Set up logging directory
            base_dir = str(Path(__file__).parent.parent)
            logs_dir = os.path.join(base_dir, "logs", f"{warehouse_type}_anomaly_detection_reports")
            os.makedirs(logs_dir, exist_ok=True)
            
            # Create timestamp for the report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = os.path.join(logs_dir, f"{warehouse_type}_anomaly_detection_report_{timestamp}.txt")
            
            # Keep track of total processing time and tables processed
            total_start_time = time.time()
            tables_processed = 0
            
            # Process each table
            with open(report_path, "w") as report_file:
                # Write header with start time and warehouse info
                report_file.write(f"Data Warehouse: {warehouse_type.upper()}\n")
                report_file.write(f"Database/Project: {database}\n")
                report_file.write(f"Schema/Dataset: {schema}\n")
                report_file.write(f"Analysis Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                report_file.write(f"Total Tables to Process: {total_tables}\n")
                report_file.write(f"{'-'*80}\n\n")
                
                for table in metadata['TABLE_NAME'].unique():
                    table_start_time = time.time()
                    print(f"\nProcessing {warehouse_type} table: {table}")
                    
                    try:
                        # Get table data and metadata
                        df = db_connector.get_table_data(database, schema, table)
                        table_metadata = db_connector.get_table_metadata(database, schema, table)

                        process_large_table(
                            df=df,
                            table_metadata=table_metadata,
                            insight_generator=insight_generator,
                            report_file=report_file,
                            table_name=table
                        )
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
                                semantic_issues = insight_generator.generate_insights(semantic_prompt).replace("```plaintext", "").replace("```", "").strip()
                                
                                # Write results to file
                                report_file.write(f"\nChunk {chunk_num}/{len(chunks)}:\n")
                                report_file.write(f"Anomaly Analysis:\n{anomaly_insights}\n")
                                report_file.write(f"Semantic Analysis:\n{semantic_issues}\n")
                            
                            chunk_time = time.time() - chunk_start_time
                            report_file.write(f"Chunk Processing Time: {chunk_time:.2f} seconds\n")
                            
                    except Exception as table_error:
                        error_msg = f"Error processing table {table}: {str(table_error)}"
                        print(error_msg)
                        report_file.write(f"\nERROR: {error_msg}\n")
                        continue
                    
                    # Calculate and write table processing time
                    table_time = time.time() - table_start_time
                    tables_processed += 1
                    
                    report_file.write(f"\nTable Processing Time: {table_time:.2f} seconds")
                    report_file.write(f"\nTables Processed: {tables_processed}/{total_tables}")
                    report_file.write(f"\n{'-'*80}\n\n")
                
                # Write summary at the end
                total_time = time.time() - total_start_time
                report_file.write(f"\nFinal Summary:\n")
                report_file.write(f"Warehouse Type: {warehouse_type.upper()}\n")
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

