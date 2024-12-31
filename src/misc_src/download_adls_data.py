from azure.storage.filedatalake import DataLakeServiceClient
import os

def download_csv_files_from_adls(connection_string, container_name, local_path):
    try:
        # Create a DataLakeServiceClient
        service_client = DataLakeServiceClient.from_connection_string(connection_string)
        
        # Get a file system client
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        
        # List all paths in the container
        paths = file_system_client.get_paths()
        
        for path in paths:
            if path.name.endswith('.csv'):
                # Get a file client
                file_client = file_system_client.get_file_client(path.name)
                
                # Download the file
                download = file_client.download_file()
                downloaded_bytes = download.readall()
                
                # Ensure the local directory exists
                os.makedirs(local_path, exist_ok=True)
                
                # Write the downloaded content to a local file
                local_file_path = os.path.join(local_path, os.path.basename(path.name))
                with open(local_file_path, 'wb') as local_file:
                    local_file.write(downloaded_bytes)
                
                print(f"Downloaded: {path.name}")
        
        print("All CSV files downloaded successfully.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Usage
connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
container_name = "genaicsvstore"
local_path = r"C:\Users\kanil01\genai_de\data\adls_download"

download_csv_files_from_adls(connection_string, container_name, local_path)