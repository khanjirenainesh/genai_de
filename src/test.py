import os
from dotenv import load_dotenv

load_dotenv(override=True)
env_vars = {
        "AZURE_OPENAI_ENDPOINT": os.environ.get("AZURE_OPENAI_ENDPOINT"),
        "AZURE_OPENAI_4o_DEPLOYMENT_NAME": os.environ.get("AZURE_OPENAI_4o_DEPLOYMENT_NAME"),
        "AZURE_OPENAI_API_VERSION": os.environ.get("AZURE_OPENAI_API_VERSION"),
        "AZURE_OPENAI_API_KEY": os.environ.get("AZURE_OPENAI_API_KEY"),
    }
warehouse_type = os.environ.get("WAREHOUSE_TYPE").lower()
source_type = os.environ.get("SQL_SOURCE_TYPE").lower()


print(env_vars.get("AZURE_OPENAI_ENDPOINT"))
print(source_type)
print(warehouse_type)



