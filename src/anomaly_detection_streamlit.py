import streamlit as st
import pandas as pd
from datetime import datetime
import time
import json
from pathlib import Path
from anomaly_detection_load_excel import Config, SnowflakeConnector, AnomalyDetector, InsightGenerator, ExcelReportGenerator

# Initialize components
config = Config()
db_connector = SnowflakeConnector(config)
anomaly_detector = AnomalyDetector()
insight_generator = InsightGenerator(config)

# Streamlit UI
st.title("Data Anomaly Detection")

# Initialize session state variables
if 'selected_tables' not in st.session_state:
    st.session_state.selected_tables = []
if 'select_all' not in st.session_state:
    st.session_state.select_all = False

# Sidebar for additional user input
st.sidebar.header("Settings")
chunk_size = st.sidebar.slider("Select chunk size for processing:", min_value=1000, max_value=10000, value=5000, step=1000)
enable_semantic_analysis = st.sidebar.checkbox("Enable Semantic Analysis", value=True)
enable_anomaly_detection = st.sidebar.checkbox("Enable Anomaly Detection", value=True)

# Get table metadata
metadata = db_connector.get_table_metadata()
table_names = metadata['table_name'].unique().tolist()

# Functions to handle state changes
def handle_select_all():
    if st.session_state.select_all:
        st.session_state.selected_tables = table_names
    else:
        st.session_state.selected_tables = []

def handle_table_selection():
    # This ensures the selected_tables state is updated when individual tables are selected
    st.session_state.select_all = False

# Add a "Select All" option with on_change handler
select_all = st.checkbox(f"Select All Tables from {config.env_vars['SNOWFLAKE_SCHEMA']} Schema", 
                        value=st.session_state.select_all,
                        key='select_all',
                        on_change=handle_select_all)

# Table selection with on_change handler
selected_tables = st.multiselect(
    "Select tables to analyze:",
    table_names,
    default=st.session_state.selected_tables,
    key='selected_tables',
    on_change=handle_table_selection
)

# Button to start analysis
if st.button("Start Analysis"):
    if not st.session_state.selected_tables:
        st.warning("Please select at least one table to analyze.")
    else:
        # Set up reporting
        reports_dir = Path(__file__).parent.parent / "logs" / "snowflake_data_identification_reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = reports_dir / f"snowflake_anomaly_report_{timestamp}.xlsx"

        report_generator = ExcelReportGenerator(
            config.env_vars['SNOWFLAKE_DATABASE'],
            config.env_vars['SNOWFLAKE_SCHEMA']
        )

        # Process tables
        start_time = datetime.now()
        total_tables = len(st.session_state.selected_tables)
        tables_processed = 0

        # Progress bar for overall processing
        overall_progress_bar = st.progress(0)
        overall_status_text = st.empty()

        # Real-time logs
        log_container = st.expander("Real-Time Logs", expanded=True)
        log_area = log_container.empty()  # Create an empty placeholder
        log_messages = []

        # Process tables
        for table in st.session_state.selected_tables:
            log_messages.append(f"Processing table: {table}")
            # Update the single log area with all messages
            log_area.text_area("Logs", value="\n".join(log_messages), height=200)       

            table_start = time.time()
            anomalous_records_count = 0

            # Progress bar for table processing
            table_progress_bar = st.progress(0)
            table_status_text = st.empty()

            try:
                # Get table data and metadata
                df = db_connector.get_table_data(table)
                table_metadata = db_connector.get_table_metadata(table)

                # Process in chunks
                num_chunks = (len(df) // chunk_size) + 1
                for i in range(0, len(df), chunk_size):
                    chunk = df[i:i + chunk_size]

                    # Update table progress bar
                    table_progress_bar.progress(min((i + chunk_size) / len(df), 1.0))
                    table_status_text.text(f"Processing chunk {i // chunk_size + 1} of {num_chunks}")

                    # Detect anomalies and generate insights
                    if enable_anomaly_detection:
                        anomaly_result = anomaly_detector.detect_anomalies(chunk, table)
                        anomalous_records_count += len(anomaly_detector.anomalous_records)
                        if "Detected" in anomaly_result:
                            anomaly_insights = insight_generator.generate_insights(
                                insight_generator.create_anomaly_prompt(anomaly_result)
                            ).replace("plaintext", "").replace("json", "").replace("```", "").strip()

                            anomaly_insights_json = json.loads(anomaly_insights)

                            # Display results in Streamlit
                            with st.expander(f"Anomaly Insights for Table: {table}", expanded=False):
                                st.write(f"Anomalies detected in table: {table}")
                                st.write(anomaly_result)
                                st.write("Anomaly Insights:")
                                st.json(anomaly_insights_json)

                            # Add to report
                            processing_time = time.time() - table_start
                            report_generator.add_entry('anomaly', [
                                table, f"{processing_time:.2f}", len(df),
                                anomalous_records_count,
                                ", ".join(df.columns),
                                str(anomaly_insights_json.get("anomaly_solution", "")),
                                str(anomaly_insights_json.get("SQL_query", "")),
                                str(anomaly_insights_json.get("Sensitive_Data_Compliance_Suggestions", ""))
                            ]) 

                    # Semantic analysis
                    if enable_semantic_analysis:
                        semantic_insights = insight_generator.generate_insights(
                            insight_generator.create_semantic_prompt(chunk, table_metadata, table)
                        ).replace("plaintext", "").replace("json", "").replace("```", "").strip()

                        # Display results in Streamlit
                        with st.expander(f"Semantic Insights for Table: {table}", expanded=False):
                            st.write("Semantic Insights:")
                            st.write(semantic_insights)

                        # Add to report
                        report_generator.add_entry('semantic', [
                            table, "", "", "Semantic Analysis",
                            semantic_insights, ""
                        ])

                tables_processed += 1

                # Update overall progress bar
                overall_progress_bar.progress(tables_processed / total_tables)
                overall_status_text.text(f"Processed {tables_processed} of {total_tables} tables")

            except Exception as e:
                log_messages.append(f"Error processing table {table}: {str(e)}")
                log_container.text_area("Logs", value="\n".join(log_messages), height=200)
                continue

        # Update summary
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()

        report_generator.add_entry('summary', [
            config.env_vars['SNOWFLAKE_DATABASE'],
            config.env_vars['SNOWFLAKE_SCHEMA'],
            total_tables,
            tables_processed,
            start_time.strftime('%Y-%m-%d %H:%M:%S'),
            end_time.strftime('%Y-%m-%d %H:%M:%S'),
            f"{total_time:.2f}",
            "Completed"
        ])

        # Save report
        report_generator.save(str(report_path))
        st.success(f"\nAnalysis completed. Results written to {report_path}")
        st.write(f"Total processing time: {total_time:.2f} seconds")
        st.write(f"Tables processed: {tables_processed}/{total_tables}")

        # Download button for the report
        with open(report_path, "rb") as file:
            st.download_button(
                label="Download Report",
                data=file,
                file_name=report_path.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# Close the database connection
if 'db_connector' in locals():
    db_connector.close()