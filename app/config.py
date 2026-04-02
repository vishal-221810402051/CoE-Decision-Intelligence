import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    APP_ENV = os.getenv("APP_ENV", "development")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    DATA_PATH = os.getenv("DATA_PATH", "./data")

config = Config()