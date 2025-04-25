import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    @property
    def STORAGE_BACKEND(self):
        return os.getenv('STORAGE_BACKEND', 'local')

    @property
    def AZURE_STORAGE_CONNECTION_STRING(self):
        return os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    @property
    def AZURE_STORAGE_CONTAINER(self):
        return os.getenv('AZURE_STORAGE_CONTAINER')

config = Config()

# Usage example:
# from src.config import config
# backend = config.STORAGE_BACKEND
