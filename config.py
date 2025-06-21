# config.py
import os

from google import genai
from pydantic import field_validator
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    GOOGLE_PROJECT_ID: str
    GOOGLE_REGION: str
    GOOGLE_API_KEY: str
    GMAIL_USERNAME: str
    GMAIL_APP_PASSWORD: str
    RECEIVER_EMAIL: list[str]
    DEBUG: bool = False
    TUSHARE_API_TOKEN: str
    ALPHA_VANTAGE_API_KEY: str
    FINNHUB_API_KEY: str
    
    @field_validator("RECEIVER_EMAIL", mode="before")
    @classmethod
    def split_receiver_emails(cls, v: str) -> list[str]:
        if isinstance(v, str):
            # remove brackets and split by comma
            return [email.strip() for email in v.strip("[]").split(",")]
        return v
    
    class Config:
        env_file = ".env"
        extra = "allow"


settings = Settings()

def load_gemini_api_key():
    """
    Loads the Gemini API key from the secrets file.
    """
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        secrets_path = os.path.join(base_dir, "secrets", "Google-ai-studio-gemini-key.txt")
        with open(secrets_path, "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        raise RuntimeError(f"API key file not found at {secrets_path}. Please check the file path.")
    except Exception as e:
        raise RuntimeError(f"Error reading API key file: {e}")

GEMINI_API_KEY = load_gemini_api_key()
gemini_client = genai.Client(api_key=GEMINI_API_KEY)
print(settings.model_dump())
