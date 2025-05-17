# app/core/config.py
import os

from google import genai
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # SECRET_KEY: str
    # ALGORITHM: str = "HS256"
    # ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    # REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # DATABASE_URL: str

    # POSTGRES_DB: str
    # POSTGRES_USER: str
    # POSTGRES_PASSWORD: str

    # REDIS_HOST: str = "redis"
    # REDIS_PORT: int = 6379
    # REDIS_URL: str = f"redis://{REDIS_HOST}:{REDIS_PORT}"

    # SERVER_IP: str = "0.0.0.0" 

    GOOGLE_PROJECT_ID: str
    GOOGLE_REGION: str
    GOOGLE_API_KEY: str
    DEBUG: bool = False
    
    class Config:
        env_file = ".env"
        extra = "allow"


settings = Settings()

def load_gemini_api_key():
    """
    Loads the Gemini API key from the secrets file.
    """
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        secrets_path = os.path.join(base_dir, "secrets", "Google-ai-studio-gemini-key.txt")
        with open(secrets_path, "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        raise RuntimeError(f"API key file not found at {secrets_path}. Please check the file path.")
    except Exception as e:
        raise RuntimeError(f"Error reading API key file: {e}")

GEMINI_API_KEY = load_gemini_api_key()
gemini_client = genai.Client(api_key=GEMINI_API_KEY)
