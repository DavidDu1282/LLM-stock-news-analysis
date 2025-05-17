# app/core/startup.py
from datetime import datetime  # Corrected import

from google import genai
from config import GEMINI_API_KEY, settings
from sessions import chat_sessions

llm_clients = {}

async def startup_event():
    """
    Initialize resources on application startup.
    """
    global llm_clients
    global chat_sessions

    try:
        llm_clients["gemini"] = genai.Client(api_key=GEMINI_API_KEY)
        llm_clients["vertex"] = genai.Client(vertexai=True, project=settings.GOOGLE_PROJECT_ID, location=settings.GOOGLE_REGION)

        chat_sessions["dummy_session"] = {
            "chat_session": llm_clients["gemini"].chats.create(model="gemini-2.0-flash-lite"),
            "last_used": datetime.now(),
            "user_id": "dummy_user_id"
        }

    except Exception as e:
        print(f"Failed to startup: {e}")
        raise
