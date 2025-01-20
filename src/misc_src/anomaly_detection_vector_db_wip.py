from snowflake.connector.pandas_tools import write_pandas
from langchain_openai import AzureChatOpenAI
from sklearn.ensemble import IsolationForest
from langchain_core.prompts import PromptTemplate
from langchain_text_splitters import RecursiveJsonSplitter
import snowflake.connector
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Any, Tuple
from io import StringIO
import os
import json
import csv
import time
import faiss
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from concurrent.futures import ThreadPoolExecutor
import pickle
from typing import Optional, Dict, Any

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

class VectorDatabase:
    """Handle vector database operations for efficient similarity search"""
    def __init__(self):
        self.index = None
        self.scaler = StandardScaler()
        self.stored_vectors = []
        self.vector_metadata = []
        
    def build_index(self, data: np.ndarray, dimension: int):
        """Build FAISS index for fast similarity search"""
        self.index = faiss.IndexFlatL2(dimension)
        self.index.add(data)
        print(f"Built FAISS index with {self.index.ntotal} vectors")
        
    def add_vectors(self, vectors: np.ndarray, metadata: List[Dict]):
        """Add vectors and their metadata to storage"""
        if len(vectors) != len(metadata):
            raise ValueError("Number of vectors must match number of metadata entries")
        self.stored_vectors.extend(vectors)
        self.vector_metadata.extend(metadata)
        
    def search(self, query_vector: np.ndarray, k: int = 5) -> Tuple[np.ndarray, np.ndarray]:
        """Search for similar vectors"""
        if self.index is None:
            raise ValueError("Index not built yet")
        return self.index.search(query_vector, k)

class VectorDatabaseManager:
    """Enhanced vector database manager with caching and persistence"""
    def __init__(self, cache_dir: str = "vector_cache"):
        self.cache_dir = cache_dir
        self.vector_dbs: Dict[str, VectorDatabase] = {}
        os.makedirs(cache_dir, exist_ok=True)
        
    def get_cache_path(self, table_name: str) -> str:
        return os.path.join(self.cache_dir, f"{table_name}_vectors.pkl")
        
    def load_or_create_db(self, table_name: str) -> VectorDatabase:
        """Load vector database from cache or create new one"""
        cache_path = self.get_cache_path(table_name)
        
        if table_name in self.vector_dbs:
            return self.vector_dbs[table_name]
            
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'rb') as f:
                    self.vector_dbs[table_name] = pickle.load(f)
                print(f"Loaded vector database from cache for table: {table_name}")
                return self.vector_dbs[table_name]
            except Exception as e:
                print(f"Error loading cached vector database: {str(e)}")
                
        self.vector_dbs[table_name] = VectorDatabase()
        return self.vector_dbs[table_name]
        
    def save_db(self, table_name: str):
        """Save vector database to cache"""
        if table_name in self.vector_dbs:
            cache_path = self.get_cache_path(table_name)
            try:
                with open(cache_path, 'wb') as f:
                    pickle.dump(self.vector_dbs[table_name], f)
                print(f"Saved vector database to cache for table: {table_name}")
            except Exception as e:
                print(f"Error saving vector database to cache: {str(e)}")

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

class EnhancedDatabaseConnector(DatabaseConnector):
    """Enhanced database connector with vector database integration"""
    def __init__(self, config: Config):
        super().__init__(config)
        self.vector_manager = VectorDatabaseManager()
        self.data_cache: Dict[str, pd.DataFrame] = {}
        
    def get_table_data(self, database: str, schema: str, table: str) -> pd.DataFrame:
        """Enhanced data retrieval using vector database"""
        cache_key = f"{database}.{schema}.{table}"
        
        # Check if data is in memory cache
        if cache_key in self.data_cache:
            print(f"Retrieved data from memory cache for table: {table}")
            return self.data_cache[cache_key]
            
        try:
            # Get data from original method
            df = super().get_table_data(database, schema, table)
            
            # Store in memory cache
            self.data_cache[cache_key] = df
            
            # Initialize vector database for this table
            vector_db = self.vector_manager.load_or_create_db(table)
            
            # Process numeric columns for vector database
            numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
            if not numeric_columns.empty:
                numeric_data = df[numeric_columns].fillna(df[numeric_columns].mean())
                vectors = self.vector_manager.vector_dbs[table].scaler.fit_transform(numeric_data).astype('float32')
                
                # Build or update vector index
                vector_db.build_index(vectors, vectors.shape[1])
                
                # Store metadata
                metadata = [{"row_index": i, "original_index": df.index[i]} 
                          for i in range(len(df))]
                vector_db.add_vectors(vectors, metadata)
                
                # Save vector database to cache
                self.vector_manager.save_db(table)
                
            return df
            
        except Exception as e:
            print(f"Error in enhanced data retrieval: {str(e)}")
            raise
            
    def close(self):
        """Close database connection and save vector databases"""
        try:
            # Save all vector databases
            for table in self.vector_manager.vector_dbs:
                self.vector_manager.save_db(table)
        finally:
            super().close()

class EnhancedAnomalyDetector:
    """Enhanced anomaly detector with vectorized operations"""
    def __init__(self):
        self.vector_manager = VectorDatabaseManager()
        self.isolation_forest = IsolationForest(
            contamination=0.01,
            max_features=0.5,
            max_samples=0.5,
            n_estimators=50,
            random_state=42
        )
        
    def prepare_data(self, df: pd.DataFrame) -> Tuple[np.ndarray, List[str]]:
        """Prepare data for vector database"""
        numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
        
        if numeric_columns.empty:
            raise ValueError("No numeric data available for processing")
            
        numeric_data = df[numeric_columns].copy()
        numeric_data = numeric_data.fillna(numeric_data.mean())
        
        # Scale the data
        scaled_data = StandardScaler().fit_transform(numeric_data)
        return scaled_data.astype('float32'), numeric_columns.tolist()
        
    def detect_anomalies(self, df: pd.DataFrame, table_name: str = "Unnamed Table") -> str:
        """Enhanced anomaly detection using vector database"""
        try:
            # Get or create vector database for this table
            vector_db = self.vector_manager.load_or_create_db(table_name)
            
            # Prepare data
            vectors, columns = self.prepare_data(df)
            
            # Use vector database for fast similarity search
            k = 5
            distances, indices = vector_db.search(vectors, k)
            
            # Calculate anomaly scores using distances
            anomaly_scores = np.mean(distances, axis=1)
            threshold = np.percentile(anomaly_scores, 95)
            anomaly_indices = np.where(anomaly_scores > threshold)[0]
            
            if len(anomaly_indices) == 0:
                return f"No anomalies detected in table '{table_name}'."
                
            # Get anomalous rows
            anomaly_rows = df.iloc[anomaly_indices]
            
            return (
                f"Detected {len(anomaly_indices)} verified anomalies in table '{table_name}'.\n"
                f"Anomalous rows:\n{anomaly_rows.to_string(index=False)}"
            )
            
        except Exception as e:
            print(f"Error in anomaly detection: {str(e)}")
            return f"Error processing table '{table_name}': {str(e)}"


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
        config = Config()
        db_connector = EnhancedDatabaseConnector(config)  # Use enhanced connector
        anomaly_detector = EnhancedAnomalyDetector()
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