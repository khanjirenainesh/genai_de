import os
import glob
import logging
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import PromptTemplate
from datetime import datetime
from langchain_text_splitters import RecursiveJsonSplitter
import pandas as pd
import json
import csv
import time
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Any
from io import StringIO

print("Starting script execution...")

# Set up directory structure
base_dir = str(Path(__file__).parent.parent)
logs_dir = os.path.join(base_dir, "logs")
os.makedirs(logs_dir, exist_ok=True)
print(f"Created logs directory at: {logs_dir}")

load_dotenv()
print("Environment variables loaded")

def main():
    print("\n=== Initializing main function ===")
    
    # Load environment variables
    env_vars = {
        "AZURE_OPENAI_ENDPOINT": os.environ.get("AZURE_OPENAI_ENDPOINT"),
        "AZURE_OPENAI_4o_DEPLOYMENT_NAME": os.environ.get("AZURE_OPENAI_4o_DEPLOYMENT_NAME"),
        "AZURE_OPENAI_API_VERSION": os.environ.get("AZURE_OPENAI_API_VERSION"),
        "AZURE_OPENAI_API_KEY": os.environ.get("AZURE_OPENAI_API_KEY"),
    }
    
    # Check if all environment variables are present
    missing_vars = [key for key, value in env_vars.items() if value is None]
    if missing_vars:
        print(f"WARNING: Missing environment variables: {missing_vars}")
    else:
        print("All required environment variables found")

    print("Initializing Azure OpenAI model...")
    model = AzureChatOpenAI(
        azure_endpoint=env_vars.get("AZURE_OPENAI_ENDPOINT"),
        azure_deployment=env_vars.get("AZURE_OPENAI_4o_DEPLOYMENT_NAME"),
        openai_api_version=env_vars.get("AZURE_OPENAI_API_VERSION"),
        openai_api_key=env_vars.get("AZURE_OPENAI_API_KEY"),
    )
    print("Azure OpenAI model initialized successfully")

    prompt =  """Analyze the following SQL code for potential issues. 
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

    # Set up directory paths
    input_dir = os.path.join(base_dir, "data", "test")
    print(f"\nBase directory: {base_dir}")
    print(f"Input directory: {input_dir}")

    # Set up analysis report file
    report_path = os.path.join(logs_dir, "sql_analysis_report.txt")
    print(f"Analysis report will be saved to: {report_path}")

    # Check if input directory exists
    if not os.path.exists(input_dir):
        print(f"ERROR: Input directory not found: {input_dir}")
        return

    print("\n=== Starting SQL file processing ===")
    sql_files_count = 0
    processed_files_count = 0

    # Open report file for writing
    with open(report_path, 'w') as report_file:
        report_file.write(f"SQL Analysis Report - Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        report_file.write("="*80 + "\n\n")

        for root, dirs, files in os.walk(input_dir):
            sql_files = [f for f in files if f.endswith('.sql')]
            sql_files_count = len(sql_files)
            print(f"Found {sql_files_count} SQL files to process")

            for file in files:
                if file.endswith(".sql"):
                    file_path = os.path.join(root, file)
                    print(f"\nProcessing file: {file_path}")
                    
                    try:
                        print(f"Reading file contents...")
                        with open(file_path, 'r') as file:
                            sql_code = file.read()
                            print(f"File size: {len(sql_code)} characters")

                            print("Creating prompt template...")
                            prompt_template = PromptTemplate(
                                input_variables=["code"],
                                template=prompt
                            )
                            
                            print("Generating analysis...")
                            start_time = time.time()
                            result = model.invoke(prompt_template.format(code=sql_code))
                            analysis_text = result.content
                            end_time = time.time()
                            print(f"Analysis completed in {end_time - start_time:.2f} seconds")
                            
                            # Write to report file
                            report_file.write(f"File: {file_path}\n")
                            report_file.write(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                            report_file.write("-"*80 + "\n")
                            report_file.write(analysis_text + "\n")
                            report_file.write("="*80 + "\n\n")
                            
                            # Log to log file
                            log_message = f"""
File Analysis: {file_path}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*50}
Analysis Result:
{analysis_text}
{'='*50}
"""
                            if "error" in analysis_text.lower() or "issue" in analysis_text.lower():
                                print("Issues found in analysis")
                                logging.warning(log_message)
                                print(f"Issues found in file {file_path}. Check logs for details.")
                            else:
                                print("No issues found in analysis")
                                logging.info(log_message)
                                print(f"No issues found in file {file_path}")
                                
                            processed_files_count += 1
                            print(f"Progress: {processed_files_count}/{sql_files_count} files processed")
                                
                    except Exception as e:
                        error_message = f"Error processing file {file_path}: {str(e)}"
                        print(f"ERROR: {error_message}")
                        logging.error(error_message)
                        report_file.write(f"ERROR processing {file_path}: {str(e)}\n")
                        report_file.write("="*80 + "\n\n")

    print(f"\n=== Processing completed ===")
    print(f"Total files processed: {processed_files_count}/{sql_files_count}")
    print(f"Report file location: {os.path.abspath(report_path)}")

if __name__ == "__main__":
    try:
        main()
        print("\nScript executed successfully")
    except Exception as e:
        print(f"\nScript execution failed: {str(e)}")
        logging.error(f"Script execution failed: {str(e)}")