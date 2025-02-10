from dotenv import load_dotenv
import base64
import os

env_file_path = os.getcwd() + "/.env"

env_loaded = load_dotenv(dotenv_path=env_file_path, override=True)

LOG_LEVEL = os.getenv("LOG_LEVEL", "info")
FILE_INGESTION_PATH = os.getenv("FILE_INGESTION_PATH", "./ingestion")
FILE_OUTPUT_PATH = os.getenv("FILE_OUTPUT_PATH", "./output")