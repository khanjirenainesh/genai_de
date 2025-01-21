'''
This code currently supports snowflake view connection and local SQL file analysis
'''

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

load_dotenv(override=True)
# Set up directory structure

base_dir = str(Path(__file__).parent.parent)
logs_dir = os.path.join(base_dir, "logs", "code_optimizations")
input_dir = os.path.join(base_dir, "data", "test") # "input" or "test" folder inside data folder
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

    prompt = """Analyze the following SQL code for potential issues. 
    Please check for:

    - Syntax errors (missing semicolons, commas after column names, incorrect keywords, etc.)
    - Missing JOIN/WHERE columns, table names
    - check for commas, FROM/where clause, proper join and case statements in nested subqueries.

    For each issue found, provide:
    - The specific line or section where the issue occurs, only mention the issue header if the issue is found.

    SQL Code to analyze:
    {code}
    strictly ignore code that is enclosed in these symbols /* */ or code that is commented out.
    strictly Don't give any other information apart from above mentioned issues. Ignore comments and irrelevant information from the code. 
    Please format your response as a structured analysis with clear sections for each type of issue found."""

    sensitive_prompt = """
    Please analyze the following Snowflake SQL query focusing on these key areas:

        1. Hardcoded Values Identification
            - List all hardcoded values in the SQL code.
            For each hardcoded value:
            - Explain potential risks associated with its use.
            - Suggest parameterization or dynamic alternatives.
            - Provide sample code for implementation.
        2. Query Structure Optimization
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
        3. Join Analysis
            - Review all JOIN operations:
            - Evaluate join conditions for their necessity.
            - Identify unused columns from joined tables.
            - Suggest removal of unnecessary joins.
            - Recommend appropriate join types (LEFT, INNER, etc.)
        4. Performance Enhancement Recommendations
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
        Output Requirements
            - Include clear before/after code examples where applicable.
            - Indicate the expected performance impact of each suggestion.
            - Prioritize changes as High, Medium, or Low based on urgency and impact.
            - Mention any potential risks or dependencies to consider.

    Provide Solution with specific case or Provide Optimized SQL query.
    Original Query:
    {sensitive_code}
    """

    # Set up analysis report file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(logs_dir, f"sql_analysis_report_{timestamp}.txt")
    # os.makedirs(report_path, exist_ok=True) 
    print(f"Analysis report will be saved to: {report_path}")

    try:
        # Get SQL sources based on configuration
        print("\n=== Getting SQL sources ===")
        source_type = os.environ.get("SQL_SOURCE_TYPE").lower()
        print(f"Source type: {source_type}")
        
        all_sql_sources, source_info = get_sql_sources()
        total_sources = len(all_sql_sources)
        
        # Print source information
        for source_type, count in source_info.items():
            print(f"Found {count} {source_type} SQL sources")
        print(f"Total SQL sources to analyze: {total_sources}")

        # Open report file for writing
        with open(report_path, 'w') as report_file:
            report_file.write(f"SQL Analysis Report - Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            for source_type, count in source_info.items():
                report_file.write(f"{source_type.capitalize()} SQL sources: {count}\n")
            report_file.write("="*80 + "\n\n")

            processed_count = 0
            for source in all_sql_sources:
                print(f"\nAnalyzing: {source['name']}")
                
                try:
                    prompt_template = PromptTemplate(
                        input_variables=["code"],
                        template=prompt
                    )
                    sensitive_prompt_template = PromptTemplate(
                        input_variables=["sensitive_code"],
                        template=sensitive_prompt
                    )
                    
                    start_time = time.time()
                    result = model.invoke(prompt_template.format(code=source['definition']))
                    sensitive_result = model.invoke(sensitive_prompt_template.format(sensitive_code=source['definition']))
                    analysis_text = result.content
                    sensitive_text = sensitive_result.content
                    end_time = time.time()
                    print(f"Analysis completed in {end_time - start_time:.2f} seconds")
                    
                    # Write to report file
                    report_file.write(f"Source: {source['name']}\n")
                    report_file.write(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    report_file.write("-"*80 + "\n")
                    report_file.write(analysis_text + "\n")
                    report_file.write(sensitive_text + "\n")
                    report_file.write("="*80 + "\n\n")
                    
                    processed_count += 1
                    print(f"Progress: {processed_count}/{total_sources} analyzed")
                        
                except Exception as e:
                    error_message = f"Error analyzing {source['name']}: {str(e)}"
                    logging.error(error_message)
                    report_file.write(f"ERROR analyzing {source['name']}: {str(e)}\n")
                    report_file.write("="*80 + "\n\n")

    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        logging.error(f"Error during analysis: {str(e)}")

    print(f"\n=== Analysis completed ===")
    print(f"Total sources analyzed: {processed_count}/{total_sources}")
    print(f"Report file location: {os.path.abspath(report_path)}")

if __name__ == "__main__":
    try:
        main()
        print("\nScript executed successfully")
    except Exception as e:
        print(f"\nScript execution failed: {str(e)}")
        logging.error(f"Script execution failed: {str(e)}")