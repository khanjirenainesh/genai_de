
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the Azure Storage connection string
azure_storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

if azure_storage_connection_string:
    print("Azure Storage connection string:", azure_storage_connection_string)
else:
    print("Error: AZURE_STORAGE_CONNECTION_STRING not found in environment variables.")
