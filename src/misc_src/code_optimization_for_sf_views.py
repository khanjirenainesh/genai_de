'''
This code currently supports snowflake view connection and it's analysis
'''

import os
import logging
import snowflake.connector
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import PromptTemplate
from datetime import datetime
import time
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Any

# Set up directory structure
base_dir = str(Path(__file__).parent.parent)
logs_dir = os.path.join(base_dir, "logs")
os.makedirs(logs_dir, exist_ok=True)

load_dotenv()

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
    report_path = os.path.join(logs_dir, f"sql_view_logic_analysis_report_{timestamp}.txt")
    print(f"Analysis report will be saved to: {report_path}")

    try:
        # Connect to Snowflake
        print("\n=== Connecting to Snowflake ===")
        conn = get_snowflake_connection()
        
        # Get view definitions
        print("\n=== Retrieving view definitions ===")
        views = get_view_definitions(conn)
        views_count = len(views)
        print(f"Found {views_count} views to analyze")

        # Open report file for writing
        with open(report_path, 'w') as report_file:
            report_file.write(f"SQL View Analysis Report - Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            report_file.write("="*80 + "\n\n")

            processed_views_count = 0
            for view in views:
                print(f"\nAnalyzing view: {view['name']}")
                
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
                    result = model.invoke(prompt_template.format(code=view['definition']))
                    sensitive_result = model.invoke(sensitive_prompt_template.format(sensitive_code=view['definition']))
                    analysis_text = result.content
                    sensitive_text = sensitive_result.content
                    end_time = time.time()
                    print(f"Analysis completed in {end_time - start_time:.2f} seconds")
                    
                    # Write to report file
                    report_file.write(f"View: {view['name']}\n")
                    report_file.write(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    report_file.write("-"*80 + "\n")
                    report_file.write(analysis_text + "\n")
                    report_file.write(sensitive_text + "\n")
                    report_file.write("="*80 + "\n\n")
                    
                    processed_views_count += 1
                    print(f"Progress: {processed_views_count}/{views_count} views analyzed")
                        
                except Exception as e:
                    error_message = f"Error analyzing view {view['name']}: {str(e)}"
                    logging.error(error_message)
                    report_file.write(f"ERROR analyzing {view['name']}: {str(e)}\n")
                    report_file.write("="*80 + "\n\n")

    finally:
        if 'conn' in locals():
            conn.close()

    print(f"\n=== Analysis completed ===")
    print(f"Total views analyzed: {processed_views_count}/{views_count}")
    print(f"Report file location: {os.path.abspath(report_path)}")

if __name__ == "__main__":
    try:
        main()
        print("\nScript executed successfully")
    except Exception as e:
        print(f"\nScript execution failed: {str(e)}")
        logging.error(f"Script execution failed: {str(e)}")