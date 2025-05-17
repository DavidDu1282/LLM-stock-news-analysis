# app/services/llm/llm_utils.py
import asyncio
from datetime import datetime, timedelta
from typing import AsyncGenerator, Optional

from google import genai
from google.api_core.exceptions import (
    GoogleAPIError,
    InternalServerError,
    ResourceExhausted,
    ServiceUnavailable,
)
from google.genai import types

from sessions import chat_sessions
from startup import llm_clients
from llm_models import ChatRequest

GEMINI_MODELS = {
    # Vertex AI Models
    "gemini-2.0-flash-exp": {"rpm": 10, "type": "vertex"},
    # "gemini-2.5-pro-exp-03-25": {"rpm": 10, "type": "vertex"},
    # Gemini API Models
    "gemini-2.5-pro-exp-03-25": {"rpm": 2, "type": "gemini"},
    "gemini-1.5-pro-latest": {"rpm": 2, "type": "gemini"},
    "gemini-1.5-flash-latest": {"rpm": 15, "type": "gemini"},
    "gemini-1.5-flash-8b-latest": {"rpm": 15, "type": "gemini"},
    "gemini-2.0-flash": {"rpm": 15, "type": "gemini"},
    "gemini-2.0-flash-lite": {"rpm": 30, "type": "gemini"},
    "gemini-2.0-pro-exp": {"rpm": 2, "type": "gemini"},
    "gemini-2.5-pro-exp-03-25": {"rpm": 2, "type": "gemini"},
}

SESSION_EXPIRY_TIME = timedelta(hours=1)

last_request_times = {}
request_counts = {}

async def query_genai_api(request: ChatRequest) -> AsyncGenerator[str, None]:
    """
    Queries the Gemini API (or Vertex AI), handling rate limiting.

    Args:
        request: The ChatRequest object.
    """
    now = datetime.now()
    if request.model in last_request_times:
        time_since_last_request = (now - last_request_times[request.model]).total_seconds()
        if time_since_last_request < 60:
            if request_counts[request.model] >= GEMINI_MODELS[request.model]["rpm"]:
                yield "Rate limit exceeded. Please wait and try again."
                return
        else:
            request_counts[request.model] = 0

    if request.model not in request_counts:
        request_counts[request.model] = 0
    if request.model not in last_request_times:
        last_request_times[request.model] = now

    try:
        request_counts[request.model] += 1
        last_request_times[request.model] = now
        # responses = chat_sessions[request.session_id]["chat_session"].send_message_stream(request.prompt, config=types.GenerateContentConfig(system_instruction=request.system_instruction))
        responses = chat_sessions[request.session_id]["chat_session"].send_message(request.prompt, config=types.GenerateContentConfig(system_instruction=request.system_instruction))

        for chunk in responses:
            await asyncio.sleep(0)
            yield chunk.text

    except ResourceExhausted:
        yield "Rate limit exceeded by underlying API. Please wait and try again."
    except (InternalServerError, ServiceUnavailable):
        yield "The LLM service is temporarily unavailable.  Please try again later."
    except GoogleAPIError as e:
        print(f"Google API Error: {e}")
        yield f"Google API Error: {e}"
    except Exception as e:
        print(f"Unexpected Error: {e}")
        yield f"An unexpected error occurred: {e}"


async def _llm_query_helper(prompt: str, model: Optional[str] = None) -> str:
    """Helper function to query LLMs, reusing chat_logic's model selection."""

    request = ChatRequest(session_id="dummy_session", prompt=prompt, model=model)
    response_generator = query_genai_api(request=request)

    full_response = ""
    async for chunk in response_generator:
        full_response += chunk

    return full_response