import streamlit as st
import pandas as pd
import os
import logging
import snowflake.connector
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import PromptTemplate
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import json
import time
import glob
from typing import Dict, List, Any

load_dotenv(override=True)

# Set up directory structure
base_dir = str(Path(__file__).parent.parent)
logs_dir = os.path.join(base_dir, "logs", "code_optimizations")
input_dir = os.path.join(base_dir, "data", "test")
os.makedirs(logs_dir, exist_ok=True)

def initialize_azure_chat_model() -> AzureChatOpenAI:
    """Initialize and return an AzureChatOpenAI model instance."""
    required_env_vars = [
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_4o_DEPLOYMENT_NAME",
        "AZURE_OPENAI_API_VERSION",
        "AZURE_OPENAI_API_KEY"
    ]
    
    # Verify all required environment variables are present
    missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return AzureChatOpenAI(
        azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
        azure_deployment=os.environ["AZURE_OPENAI_4o_DEPLOYMENT_NAME"],
        api_version=os.environ["AZURE_OPENAI_API_VERSION"],
        api_key=os.environ["AZURE_OPENAI_API_KEY"],
        temperature=0
    )

# [Previous functions remain unchanged: get_snowflake_connection, get_view_definitions, 
# get_local_sql_files, get_sql_sources, flatten_sensitive_analysis, perform_analysis]

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

def get_view_definitions(conn) -> list[dict[str, str]]:
    """Retrieve all view definitions from the specified schema."""
    cursor = conn.cursor()
    try:
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

def get_local_sql_files() -> list[dict[str, str]]:
    """Read SQL files from the input directory and its subdirectories."""
    sql_files = []
    for sql_file in glob.glob(os.path.join(input_dir, "**/*.sql"), recursive=True):
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            relative_path = os.path.relpath(sql_file, input_dir)
            sql_files.append({
                'name': relative_path,
                'definition': sql_content
            })
        except Exception as e:
            logging.error(f"Error reading file {sql_file}: {str(e)}")
    return sql_files

def get_sql_sources(source_type: str) -> tuple[list[dict[str, str]], dict[str, int]]:
    """Get SQL sources based on the source type specified."""
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

def flatten_sensitive_analysis(sensitive_analysis_result: str) -> dict[str, Any]:
    """Flatten the sensitive_analysis_result JSON string into a dictionary."""
    try:
        sensitive_data = json.loads(sensitive_analysis_result)
        required_keys = [
            "hardcoded_value_identification",
            "query_structure_optimization",
            "join_analysis",
            "performance_enhancement_recommendations"
        ]
        for key in required_keys:
            if key not in sensitive_data:
                sensitive_data[key] = ""
        return sensitive_data
    except json.JSONDecodeError:
        return {
            "hardcoded_value_identification": "",
            "query_structure_optimization": "",
            "join_analysis": "",
            "performance_enhancement_recommendations": ""
        }

def perform_analysis(source: dict[str, str], model: AzureChatOpenAI) -> dict[str, str]:
    """Perform analysis on a single SQL source."""
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

    try:
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

        return {
            "source_name": source['name'],
            "hardcoded_value_identification": hardcoded_result,
            "query_structure_optimization": structure_result,
            "join_analysis": join_result,
            "performance_enhancement_recommendations": performance_result
        }
    except Exception as e:
        return {
            "source_name": source['name'],
            "error": str(e)
        }

def main():
    st.title("ViewLogic AI")
    st.sidebar.title("Configuration")

    try:
        # Initialize the Azure Chat model
        model = initialize_azure_chat_model()
        
        # Source type selection
        source_type = st.sidebar.selectbox("Select SQL Source Type", ["local", "snowflake", "both"])

        if st.sidebar.button("Run Analysis"):
            with st.spinner("Running analysis..."):
                try:
                    # Step 1: Fetching SQL sources
                    st.write("Fetching SQL sources...")
                    all_sql_sources, source_info = get_sql_sources(source_type)
                    total_sources = len(all_sql_sources)
                    st.success(f"Found {total_sources} SQL sources to analyze.")

                    # Step 2: Perform analysis
                    analysis_data = []
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    for i, source in enumerate(all_sql_sources):
                        status_text.text(f"Analyzing source {i + 1} of {total_sources}: {source['name']}")
                        analysis_result = perform_analysis(source, model)
                        analysis_data.append(analysis_result)
                        progress_bar.progress((i + 1) / total_sources)
                        time.sleep(0.1)  # Simulate some delay for better UX

                    # Step 3: Display results
                    st.write("Analysis Results:")
                    df = pd.DataFrame(analysis_data)
                    st.dataframe(df)

                    # Step 4: Save report to Excel
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    report_path = os.path.join(logs_dir, f"sql_analysis_report_{timestamp}.xlsx")
                    df.to_excel(report_path, index=False)
                    st.success(f"Analysis report saved to: {report_path}")

                    # Step 5: Provide download link
                    with open(report_path, "rb") as file:
                        btn = st.download_button(
                            label="Download Report",
                            data=file,
                            file_name=f"sql_analysis_report_{timestamp}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                except Exception as e:
                    st.error(f"Error during analysis: {str(e)}")
    except Exception as e:
        st.error(f"Error initializing Azure OpenAI model: {str(e)}")

if __name__ == "__main__":
    main()