import os
import logging
import glob
import snowflake.connector
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import PromptTemplate
from datetime import datetime
import time
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Any
import json
import pandas as pd

load_dotenv(override=True)

# Set up directory structure
base_dir = str(Path(__file__).parent.parent)
logs_dir = os.path.join(base_dir, "logs", "code_optimizations")
input_dir = os.path.join(base_dir, "data", "test")  # "input" or "test" folder inside data folder
os.makedirs(logs_dir, exist_ok=True)

def get_snowflake_connection():
    """Create a Snowflake connection using environment variables."""
    return snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
        database=os.environ.get('SNOWFLAKE_DATABASE'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA')
    )

def get_view_definitions(conn) -> List[Dict[str, str]]:
    """Retrieve all view definitions from the specified schema."""
    cursor = conn.cursor()
    try:
        # Query to get view definitions
        query = """
        SELECT table_name, view_definition
        FROM information_schema.views
        WHERE table_schema = CURRENT_SCHEMA()
        """
        cursor.execute(query)
        views = []
        for row in cursor.fetchall():
            views.append({
                'name': row[0],
                'definition': row[1]
            })
        return views
    finally:
        cursor.close()

def get_local_sql_files() -> List[Dict[str, str]]:
    """Read SQL files from the input directory and its subdirectories."""
    sql_files = []
    
    # Use glob to recursively find all .sql files
    for sql_file in glob.glob(os.path.join(input_dir, "**/*.sql"), recursive=True):
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Get relative path for cleaner naming
            relative_path = os.path.relpath(sql_file, input_dir)
            sql_files.append({
                'name': relative_path,
                'definition': sql_content
            })
        except Exception as e:
            logging.error(f"Error reading file {sql_file}: {str(e)}")
    
    return sql_files

def get_sql_sources() -> List[Dict[str, str]]:
    """Get SQL sources based on the source type specified in env file."""
    source_type = os.environ.get('SQL_SOURCE_TYPE', 'local').lower()
    
    if source_type not in ['local', 'snowflake', 'both']:
        raise ValueError("SQL_SOURCE_TYPE must be 'local', 'snowflake', or 'both'")
    
    sources = []
    source_info = {}
    
    if source_type in ['local', 'both']:
        local_files = get_local_sql_files()
        sources.extend(local_files)
        source_info['local'] = len(local_files)
    
    if source_type in ['snowflake', 'both']:
        if not all(env_var in os.environ for env_var in ['SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT']):
            raise ValueError("Snowflake credentials not found in environment variables")
        
        conn = get_snowflake_connection()
        try:
            views = get_view_definitions(conn)
            sources.extend(views)
            source_info['snowflake'] = len(views)
        finally:
            conn.close()
    
    return sources, source_info

def flatten_sensitive_analysis(sensitive_analysis_result: str) -> Dict[str, Any]:
    """
    Flatten the sensitive_analysis_result JSON string into a dictionary.
    If the JSON is invalid, return an empty dictionary.
    """
    try:
        # Parse the JSON string into a dictionary
        sensitive_data = json.loads(sensitive_analysis_result)
        
        # Ensure all required keys are present, even if empty
        required_keys = [
            "hardcoded_value_identification",
            "query_structure_optimization",
            "join_analysis",
            "performance_enhancement_recommendations"
        ]
        
        for key in required_keys:
            if key not in sensitive_data:
                sensitive_data[key] = ""  # Default to empty string if key is missing
        
        return sensitive_data
    except json.JSONDecodeError:
        # If JSON is invalid, return a dictionary with empty values for required keys
        return {
            "hardcoded_value_identification": "",
            "query_structure_optimization": "",
            "join_analysis": "",
            "performance_enhancement_recommendations": ""
        }


def main():
    # Load environment variables
    env_vars = {
        "AZURE_OPENAI_ENDPOINT": os.environ.get("AZURE_OPENAI_ENDPOINT"),
        "AZURE_OPENAI_4o_DEPLOYMENT_NAME": os.environ.get("AZURE_OPENAI_4o_DEPLOYMENT_NAME"),
        "AZURE_OPENAI_API_VERSION": os.environ.get("AZURE_OPENAI_API_VERSION"),
        "AZURE_OPENAI_API_KEY": os.environ.get("AZURE_OPENAI_API_KEY"),
    }

    model = AzureChatOpenAI(
        azure_endpoint=env_vars.get("AZURE_OPENAI_ENDPOINT"),
        azure_deployment=env_vars.get("AZURE_OPENAI_4o_DEPLOYMENT_NAME"),
        openai_api_version=env_vars.get("AZURE_OPENAI_API_VERSION"),
        openai_api_key=env_vars.get("AZURE_OPENAI_API_KEY"),
    )

    # Detailed prompts for each analysis category
    hardcoded_value_prompt = """Identify and analyze hardcoded values in the SQL query:
    - List all hardcoded values and For each hardcoded value:
    - Explain potential security and maintenance risks
    - Suggest parameterization strategies
        
    - Suggest parameterization or dynamic alternatives.
    - Provide sample code for implementation.
    
    SQL Query:
    {code}"""

    query_structure_prompt = """Analyze the query structure and optimization opportunities:
    - Review nested queries and subqueries:
    - Map the query execution flow.
    - Identify performance bottlenecks in nested operations.
    - Suggest opportunities for flattening or restructuring.
    - Provide alternative query structures with explanations.
    - Analyze column usage:
    - Flag any *'SELECT ' statements.
    - List unused columns in JOIN conditions - [columns list].
    - Identify columns fetched but not used in the final output.
    - Provide optimized SELECT statements with specific columns.
    - Provide optimized SELECT statements with specific columns.
    - List specific unused column names.
    
    SQL Query:
    {code}"""

    join_analysis_prompt = """Comprehensive join analysis:
    - Review all JOIN operations:
    - Evaluate join conditions for their necessity.
    - Identify unused columns from joined tables.
    - Suggest removal of unnecessary joins.
    - Recommend appropriate join types (LEFT, INNER, etc.)
    
    SQL Query:
    {code}"""

    performance_prompt = """Performance enhancement recommendations:
    - Suggest specific improvements in coding standards:
    - Use of table aliases and naming conventions.
    - Ensure proper indentation and formatting.
    - Recommend use of appropriate indexes.
    - Discuss when to use temporary tables vs CTEs.
    - Explore opportunities for materialized views.
    - Provide performance-focused recommendations:
    - Discuss potential partitioning strategies.
    - Suggest suitable clustering keys.
    - Explain benefits of query result caching.
    - Recommend strategies for execution plan optimization.
    
    SQL Query:
    {code}"""

    # Set up analysis report file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(logs_dir, f"sql_analysis_report_{timestamp}.xlsx")

    try:
        # Get SQL sources based on configuration
        all_sql_sources, source_info = get_sql_sources()
        total_sources = len(all_sql_sources)
        
        analysis_data = []

        for source in all_sql_sources:
            try:
                # Perform detailed analysis for each category
                hardcoded_result = model.invoke(
                    PromptTemplate(input_variables=["code"], template=hardcoded_value_prompt)
                    .format(code=source['definition'])
                ).content

                structure_result = model.invoke(
                    PromptTemplate(input_variables=["code"], template=query_structure_prompt)
                    .format(code=source['definition'])
                ).content

                join_result = model.invoke(
                    PromptTemplate(input_variables=["code"], template=join_analysis_prompt)
                    .format(code=source['definition'])
                ).content

                performance_result = model.invoke(
                    PromptTemplate(input_variables=["code"], template=performance_prompt)
                    .format(code=source['definition'])
                ).content

                # Append analysis results
                analysis_data.append({
                    "source_name": source['name'],
                    "hardcoded_value_identification": hardcoded_result,
                    "query_structure_optimization": structure_result,
                    "join_analysis": join_result,
                    "performance_enhancement_recommendations": performance_result
                })
                
            except Exception as e:
                error_message = f"Error analyzing {source['name']}: {str(e)}"
                logging.error(error_message)
                analysis_data.append({
                    "source_name": source['name'],
                    "error": error_message
                })

        # Create DataFrame and save to Excel
        df = pd.DataFrame(analysis_data)
        df.to_excel(report_path, index=False)
        print(f"Analysis report saved to: {report_path}")

    except Exception as e:
        logging.error(f"Error during analysis: {str(e)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Script execution failed: {str(e)}")