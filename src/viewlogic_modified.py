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
from typing import Dict, List, Any, Literal
from dataclasses import dataclass

load_dotenv(override=True)

# Set up directory structure
base_dir = str(Path(__file__).parent.parent)
logs_dir = os.path.join(base_dir, "logs", "code_optimizations")
input_dir = os.path.join(base_dir, "data", "test")
os.makedirs(logs_dir, exist_ok=True)

@dataclass
class PromptConfig:
    hardcoded_value: str
    structure: str
    performance: str
    optimization: str
    optimized_python_code: str = ""  # For language-specific analysis

# Prompt configurations for different languages
SQL_PROMPTS = PromptConfig(
    hardcoded_value="""Identify and analyze hardcoded values in the SQL query:
    - List all hardcoded values and For each hardcoded value:
    - Explain potential security and maintenance risks
    - Suggest parameterization strategies
    - Suggest parameterization or dynamic alternatives.
    - Provide sample code for implementation.
    - Don't provide unnecessary comments if the issues don't exist
    - Don't provide any other unnecessary information apart from above mentioned issues.
    
    SQL Query:
    {code}""",
    
    structure="""Analyze the query structure and optimization opportunities:
    - Review nested queries and subqueries:
    - Map the query execution flow.
    - Identify performance bottlenecks in nested operations.
    - Suggest opportunities for flattening or restructuring.
    - Analyze column usage:
    - Flag any *'SELECT ' statements.
    - List unused columns in JOIN conditions - [columns list].
    - Identify columns fetched but not used in the final output.
    - List specific unused column names.
    - Don't provide unnecessary comments if the issues don't exist
    - Don't provide any other unnecessary information apart from above mentioned issues.
    
    SQL Query:
    {code}""",
    
    performance="""Performance enhancement recommendations:
    - Review all JOIN operations:
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
    - Recommend appropriate join types (LEFT, INNER, etc.)
    - Recommend strategies for execution plan optimization.
    - Don't provide unnecessary comments if the issues don't exist
    - Don't provide any other unnecessary information apart from above mentioned issues.

    SQL Query:
    {code}""",
    
    optimization="""
    - Provide me an optimzed SQL query based on your analysis.
    - Rewrite the provided Snowflake SQL query for better performance without altering the query logic or additional columns.
    - Ensure the following optimizations:
    - use proper group by clause.
    - use proper join clause.
    - Use of Table Aliases: Apply clear and concise aliases for readability and maintainability.
    - Execution Plan Optimization: Recommend changes that reduce scan time, optimize joins, and eliminate redundant operations.
    - Don't provide any other unnecessary information apart from above mentioned issues.
    - Don't provide unnecessary comments if the issues don't exist
     Strictly focus on performance improvements ,execution time reduction and do not alter the query logic or additional columns and do not return additional information.
    
    
    SQL Query:
    {code}"""
)

PYTHON_PROMPTS = PromptConfig(
    hardcoded_value="""Comprehensive Analysis of Hardcoded Values

                1. Value Identification:
                - List all numerical constants, string literals, and fixed configurations
                - Flag critical business logic constants
                - Identify magic numbers and strings
                - Document array/collection size constraints

                2. Configuration Recommendations:
                - Design a configuration management strategy
                - Suggest environment variable implementations
                - Propose constants file organization
                - Recommend configuration validation approaches

                3. Risk Assessment:
                - Evaluate deployment flexibility limitations
                - Analyze cross-environment compatibility
                - Consider scaling implications
                - Document security considerations

                Python Code:
                {code}""",
    
    structure="""Code Architecture and Organization Analysis

                1. Structural Review:
                - Evaluate class hierarchy and relationships
                - Analyze function modularity and cohesion
                - Assess interface design patterns
                - Review naming conventions and documentation

                2. Code Quality Assessment:
                - Identify repeated logic patterns
                - Evaluate function complexity
                - Analyze code coupling
                - Review inheritance patterns

                3. Architecture Recommendations:
                - Suggest design pattern implementations
                - Propose modularization strategies
                - Recommend SOLID principle applications
                - Outline refactoring priorities

                4. Dependency Analysis:
                - Map module interactions
                - Evaluate circular dependencies
                - Assess external package usage
                - Review import organization

                Python Code:
                {code}""",
    
    performance="""Performance Optimization Analysis

                1. Performance Analysis:
                - Profile critical code sections
                - Measure time complexity
                - Identify resource-intensive operations
                - Analyze I/O patterns

                2. Algorithm Enhancement:
                - Suggest optimization techniques
                - Recommend caching strategies
                - Propose parallel processing options
                - Evaluate time-space tradeoffs

                3. Data Structure Optimization:
                - Review collection type choices
                - Suggest efficient alternatives
                - Analyze memory patterns
                - Recommend indexing strategies

                4. Resource Management:
                - Evaluate memory usage patterns
                - Suggest garbage collection optimization
                - Review connection handling
                - Analyze resource cleanup

                Python Code:
                {code}""",
    
    optimization="""Code Optimization and Best Practices

                1. Python Optimization:
                - Apply language-specific optimizations
                - Implement performance enhancements
                - Optimize import statements
                - Review generator usage

                2. Code Quality:
                - Apply consistent formatting
                - Implement clear documentation
                - Use descriptive naming
                - Optimize function signatures

                3. PEP 8 Compliance:
                - Format according to style guide
                - Review spacing and indentation
                - Check naming conventions
                - Verify docstring formats

                4. Readability Improvements:
                - Enhance code organization
                - Add meaningful comments
                - Improve variable naming
                - Structure logical flows

                Python Code:
                {code}""",
    
    optimized_python_code="""Python Code Optimization:

                - Give me an optimized version of below python script.
                - Don't add any other information other than the optimized version of code
                - Don't modify any business logic, function names, variable names, comments, symbols, definitions, structs, tests, or arbitrary lines of code that are 'above' or 'below' in their instruction.

                Python Code:
                {code}"""
)

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

def get_snowflake_view_details() -> List[Dict[str, Any]]:
    """Retrieve detailed information about Snowflake views."""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT table_schema, table_name, view_definition
        FROM information_schema.views
        WHERE table_schema = CURRENT_SCHEMA()
        """
        
        cursor.execute(query)
        
        # Fetch column names
        columns = [desc[0].lower() for desc in cursor.description]
        
        # Convert results to list of dictionaries
        views_details = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return views_details
    except Exception as e:
        st.error(f"Error retrieving Snowflake view details: {e}")
        return []

def get_local_files(file_type: Literal['sql', 'python']) -> list[dict[str, str]]:
    """Read SQL or Python files from the input directory and its subdirectories."""
    extension = '.sql' if file_type == 'sql' else '.py'
    files = []
    for file_path in glob.iglob(os.path.join(input_dir, f"**/*{extension}"), recursive=True):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            relative_path = os.path.relpath(file_path, input_dir)
            files.append({'name': relative_path, 'definition': content})
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {e}")
    return files

def get_code_sources(source_type: str, language: Literal['sql', 'python']) -> tuple[list[dict[str, str]], dict[str, int]]:
    """Get code sources based on the source type and language specified."""
    if source_type not in ['local', 'snowflake']:
        raise ValueError("SOURCE_TYPE must be 'local', 'snowflake'")
    
    sources = []
    source_info = {}
    
    if source_type in ['local']:
        local_files = get_local_files(language)
        sources.extend(local_files)
        source_info['local'] = len(local_files)
    
    if language == 'sql' and source_type in ['snowflake']:
        if not all(env_var in os.environ for env_var in ['SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT']):
            raise ValueError("Snowflake credentials not found in environment variables")
        
        conn = get_snowflake_connection()
        try:
            views = get_snowflake_view_details()
            sources.extend([{'name': view['table_name'], 'definition': view['view_definition']} for view in views])
            source_info['snowflake'] = len(views)
        finally:
            conn.close()
    
    return sources, source_info

def perform_analysis(source: dict[str, str], model: AzureChatOpenAI, language: Literal['sql', 'python']) -> dict[str, str]:
    """Perform analysis on a single source file."""
    prompts = SQL_PROMPTS if language == 'sql' else PYTHON_PROMPTS
    
    try:
        analysis_results = {
            "source_name": source['name'],
            "hardcoded_value_identification": model.invoke(
                PromptTemplate(input_variables=["code"], template=prompts.hardcoded_value)
                .format(code=source['definition'])
            ).content,
            "structure_analysis": model.invoke(
                PromptTemplate(input_variables=["code"], template=prompts.structure)
                .format(code=source['definition'])
            ).content,
            "performance_recommendations": model.invoke(
                PromptTemplate(input_variables=["code"], template=prompts.performance)
                .format(code=source['definition'])
            ).content,
            "optimized_code": model.invoke(
                PromptTemplate(input_variables=["code"], template=prompts.optimization)
                .format(code=source['definition'])
            ).content
        }
        
        if language == 'python' and prompts.optimized_python_code:
            analysis_results["optimized_python_code"] = model.invoke(
                PromptTemplate(input_variables=["code"], template=prompts.optimized_python_code)
                .format(code=source['definition'])
            ).content
            
        return {k: v.replace("```", "").replace("**", "").replace("###", "") for k, v in analysis_results.items()}
    except Exception as e:
        return {
            "source_name": source['name'],
            "error": str(e)
        }

def get_available_files(file_type: Literal['sql', 'python']) -> List[str]:
    """Get list of available files of specified type from input directory."""
    extension = '.sql' if file_type == 'sql' else '.py'
    files = []
    
    try:
        # Validate input directory exists
        if not os.path.exists(input_dir):
            logging.error(f"Input directory does not exist: {input_dir}")
            return []

        # Use glob to find all matching files
        files = glob.glob(os.path.join(input_dir, "**/*" + extension), recursive=True)
        
        if not files:
            logging.warning(f"No {extension} files found in {input_dir}")
        
        # Get relative paths from input_dir
        files = [os.path.relpath(file, input_dir) for file in files]
        
        return sorted(files)
    
    except Exception as e:
        logging.error(f"Error discovering files: {e}")
        return []


def main():
    st.title("ViewLogic AI")
    st.sidebar.title("Configuration")

    # Initialize session state for file selection
    if 'selected_files' not in st.session_state:
        st.session_state.selected_files = []

    # Language selection with custom labels
    language = st.sidebar.radio(
        "Select Language",
        ["Python", "SQL"],
        format_func=lambda x: f"{x} Analysis"
    ).lower()

    # Source type selection based on language
    if language == 'sql':
        source_type = st.sidebar.radio(
            "Select Source Type",
            ["Local Files", "Snowflake"],
            format_func=lambda x: x.replace('_', ' ').title()
        ).lower().replace(' ', '_')
    else:
        source_type = "local"
        st.sidebar.info("Python analysis uses local files only")

    # Normalize source type to ensure consistency
    source_type = source_type.replace('_files', '').replace(' ', '')

    # Universal local file selection logic
    if source_type == 'local':
        # Determine file type based on language
        file_type = 'sql' if language == 'sql' else 'python'
        
        # Get available files
        available_files = get_available_files(file_type)
    

        if not available_files:
            st.sidebar.warning(f"No {file_type.upper()} files found in the input directory")
        else:
            # Multi-select for files
            st.session_state.selected_files = st.sidebar.multiselect(
                f"Select {file_type.upper()} Files to Analyze",
                available_files,
                default=[],
                help=f"Choose {file_type.upper()} files for optimization analysis"
            )

    elif source_type == 'snowflake' and language == 'sql':
        # Get Snowflake views
        snowflake_view_details = get_snowflake_view_details()
        
        if not snowflake_view_details:
            st.sidebar.warning("No views found in Snowflake")
            return

        # Create list of view names for selection
        view_names = [view['table_name'] for view in snowflake_view_details]
        
        # Multi-select for Snowflake views
        st.session_state.selected_files = st.sidebar.multiselect(
            "Select Snowflake Views to Analyze",
            view_names,
            default=[],
            help="Choose Snowflake views for optimization analysis"
        )

    # Display selected files
    if st.session_state.selected_files:
        st.sidebar.subheader("Selected Files:")
        for file in st.session_state.selected_files:
            st.sidebar.text(f"• {file}")

    # Load environment variables and initialize model
    model = AzureChatOpenAI(
        azure_endpoint=os.environ.get("AZURE_OPENAI_ENDPOINT"),
        azure_deployment=os.environ.get("AZURE_OPENAI_4o_DEPLOYMENT_NAME"),
        openai_api_version=os.environ.get("AZURE_OPENAI_API_VERSION"),
        openai_api_key=os.environ.get("AZURE_OPENAI_API_KEY"),
        temperature=0.7
    )

    # Run Analysis button
    if st.sidebar.button("Run Analysis", disabled=not st.session_state.selected_files):
        with st.spinner("Running analysis..."):
            try:
                # Step 1: Fetching code sources
                st.write(f"Fetching {language.upper()} sources...")
                all_sources, source_info = get_code_sources(source_type, language)
                
                # Filter sources based on selected files
                selected_sources = [
                    source for source in all_sources 
                    if source['name'] in st.session_state.selected_files
                ]
                
                total_sources = len(selected_sources)
                st.success(f"Analyzing {total_sources} selected {language.upper()} files")

                # Step 2: Perform analysis
                analysis_data = []
                progress_bar = st.progress(0)
                status_text = st.empty()

                # Create two columns for progress display
                col1, col2 = st.columns(2)
                
                for i, source in enumerate(selected_sources):
                    # Update progress in first column
                    with col1:
                        status_text.text(f"Analyzing file {i + 1} of {total_sources}")
                    
                    # Show current file in second column
                    with col2:
                        st.text(f"Current file: {source['name']}")
                    
                    analysis_result = perform_analysis(source, model, language)
                    analysis_data.append(analysis_result)
                    progress_bar.progress((i + 1) / total_sources)
                    time.sleep(0.1)

                # Step 3: Display results
                st.subheader("Analysis Results")
                
                # Create tabs for different views
                tab1, tab2 = st.tabs(["Summary View", "Detailed View"])
                
                with tab1:
                    df_summary = pd.DataFrame(analysis_data)[['source_name', 'optimized_code']]
                    st.dataframe(df_summary, use_container_width=True)
                
                with tab2:
                    df = pd.DataFrame(analysis_data)
                    st.dataframe(df, use_container_width=True)

                # Step 4: Save report
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                if language == 'python':
                    report_filename = f"python_script_analysis_{timestamp}.xlsx"
                elif language == 'sql':
                    report_filename = f"sql_query_analysis_{timestamp}.xlsx"
                else:
                    report_filename = f"{language}_analysis_report_{timestamp}.xlsx"

                report_path = os.path.join(logs_dir, report_filename)
                df.to_excel(report_path, index=False)
                st.success(f"Analysis report saved to: {report_path}")

                # Step 5: Provide download link
                with open(report_path, "rb") as file:
                    st.download_button(
                    label="📥 Download Full Report",
                    data=file,
                    file_name=report_filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                    
            except Exception as e:
                st.error(f"Error during analysis: {str(e)}")
    else:
        if not st.session_state.selected_files:
            st.info("Please select files to analyze")

if __name__ == "__main__":
    main()