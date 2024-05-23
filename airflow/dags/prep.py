import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Access the environment variables
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
VALIDATION_RESULT_URI = os.getenv('VALIDATION_RESULT_URI')
TEAMS_WEBHOOK = os.getenv('TEAMS_WEBHOOK')

RAW_DATA_DIR = os.getenv('RAW_DATA_DIR')
GOOD_DATA_DIR = os.getenv('GOOD_DATA_DIR')
BAD_DATA_DIR = os.getenv('BAD_DATA_DIR')
TEMP_DATA_DIR = os.getenv('TEMP_DATA_DIR')
INGESTION_LOCK_FILE = os.getenv('INGESTION_LOCK_FILE')

print(DB_HOST, DB_NAME, DB_PASSWORD)
print(TEAMS_WEBHOOK)