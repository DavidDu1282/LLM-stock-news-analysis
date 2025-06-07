import os
import sys
import time
from config import settings
script_start_time = time.time()
print(f"{script_start_time:.2f}: Script started (using google-genai SDK pattern with client.models.generate_content).")

try:
    import_start_genai = time.time()
    from google import genai # Using the newer google-genai package
    print(f"{time.time():.2f}: Imported google.genai (took {time.time() - import_start_genai:.2f}s)")
except ImportError:
    print("The 'google-genai' library is not found. Please install it by running: 'pip install google-genai'")
    print("(You might also need to uninstall 'google-generativeai' if it was previously installed.)")
    sys.exit(1)

# --- Configuration ---
config_start_time = time.time()
# GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
# GOOGLE_PROJECT_ID = os.environ.get("GOOGLE_PROJECT_ID")
# GOOGLE_REGION = os.environ.get("GOOGLE_REGION", "us-central1")
GOOGLE_API_KEY = settings.GOOGLE_API_KEY
GOOGLE_PROJECT_ID = settings.GOOGLE_PROJECT_ID
GOOGLE_REGION = settings.GOOGLE_REGION

print(f"{time.time():.2f}: Environment variables loaded (took {time.time() - config_start_time:.2f}s)")
print(f"    GOOGLE_API_KEY: {'Set' if GOOGLE_API_KEY else 'Not Set'}")
print(f"    GOOGLE_PROJECT_ID (from GOOGLE_PROJECT_ID): {GOOGLE_PROJECT_ID}")
print(f"    GOOGLE_REGION (from GOOGLE_REGION): {GOOGLE_REGION}")

GEMINI_MODELS_CONFIG = {
    "gemini-2.5-flash-exp": {"rpm": 10, "type": "vertex_via_genai", "name_override": "gemini-2.5-flash-preview-04-17"},
    "gemini-2.5-flash-latest-studio": {"rpm": 10, "type": "studio_via_genai", "name_override": "gemini-2.5-flash-preview-04-17"},
    "gemini-2.0-flash-latest-studio": {"rpm": 2, "type": "studio_via_genai", "name_override": "gemini-2.0-flash-latest"},
}

def create_clients() -> tuple[genai.Client, genai.Client]:
    studio_client = None
    vertex_client = None

    # Initialize Google AI Studio client

    if GOOGLE_API_KEY:
        client_init_start = time.time()
        try:
            studio_client = genai.Client(api_key=GOOGLE_API_KEY)
            print(f"{time.time():.2f}: Google AI Studio Client (genai.Client with api_key) initialized. (took {time.time() - client_init_start:.2f}s)")
        except Exception as e:
            print(f"{time.time():.2f}: Error initializing Google AI Studio Client: {e}. 'studio_via_genai' type models may fail.")
    else:
        print(f"{time.time():.2f}: Warning: GOOGLE_API_KEY not set. Cannot initialize Google AI Studio Client.")

    # Initialize Vertex AI client
    if GOOGLE_PROJECT_ID and GOOGLE_REGION:
        client_init_start = time.time()
        try:
            # The vertexai=True flag might or might not be needed/supported by your genai.Client version.
            # If it causes an error, remove it. Providing project & location usually suffices for ADC mode.
            client_args = {
                "vertexai": True,
                "project": GOOGLE_PROJECT_ID,
                "location": GOOGLE_REGION
            }
            # Add vertexai=True if you are sure your SDK version uses it and it helps
            # client_args["vertexai"] = True # As per your recollection
            
            vertex_client = genai.Client(**client_args)
            print(f"{time.time():.2f}: Vertex AI Client (genai.Client with project/location) initialized for project '{GOOGLE_PROJECT_ID}'. (took {time.time() - client_init_start:.2f}s)")
        except TypeError as te: # Catch if 'vertexai' is an unexpected keyword
            if 'vertexai' in str(te):
                print(f"{time.time():.2f}: Note: 'vertexai=True' might not be a valid parameter for your genai.Client version. Trying without it.")
                try:
                    vertex_client = genai.Client(project=GOOGLE_PROJECT_ID, location=GOOGLE_REGION)
                    print(f"{time.time():.2f}: Vertex AI Client (genai.Client project/location only) initialized. (took {time.time() - client_init_start:.2f}s)")
                except Exception as e_inner:
                    print(f"{time.time():.2f}: Error initializing Vertex AI Client (fallback attempt): {e_inner}.")
            else:
                print(f"{time.time():.2f}: Error initializing Vertex AI Client: {te}.")
        except Exception as e:
            print(f"{time.time():.2f}: Error initializing Vertex AI Client: {e}. 'vertex_via_genai' type models may fail.")
    else:
        print(f"{time.time():.2f}: Warning: GOOGLE_PROJECT_ID or GOOGLE_REGION not set. Cannot initialize Vertex AI Client.")

    return studio_client, vertex_client

# --- API Call Function using genai.Client.models.generate_content ---
def call_model_via_genai_client(
    client_type_str: str,
    client_instance: genai.Client,
    model_name_key: str,
    model_config: dict,
    query: str
) -> tuple[bool, str]:

    actual_model_name = model_config.get("name_override", model_name_key)
    model_path_for_call = actual_model_name

    print(f"{time.time():.2f}: Attempting API call via {client_type_str} client.models.generate_content for model: {model_path_for_call}")
    call_api_start_time = time.time()
    try:
        # Using client.models.generate_content directly
        response = client_instance.models.generate_content(
            model=model_path_for_call, # Pass the model name string here
            contents=[query]           # Contents typically as a list; plain string might also work for simple text
        )
        print(f"{time.time():.2f}:   client.models.generate_content() for {model_path_for_call} completed (took {time.time() - (call_api_start_time):.2f}s)") # Corrected timer base

        if hasattr(response, 'prompt_feedback') and response.prompt_feedback and response.prompt_feedback.block_reason:
            error_msg = f"Query blocked for model {actual_model_name} ({client_type_str}) due to: {response.prompt_feedback.block_reason}"
            print(f"{time.time():.2f}:   Failure: {error_msg}")
            return False, error_msg

        if hasattr(response, 'text') and response.text is not None:
            print(f"{time.time():.2f}:   Success getting text from {actual_model_name}")
            return True, response.text
        elif response.candidates: # Fallback if .text isn't directly available or is None
            full_text = [part.text for candidate in response.candidates if candidate.content and candidate.content.parts for part in candidate.content.parts if hasattr(part, 'text')]
            if full_text:
                print(f"{time.time():.2f}:   Success getting text from candidates for {actual_model_name}")
                return True, "".join(full_text)
        
        error_msg = f"Model {actual_model_name} ({client_type_str}) returned an empty or unexpected response structure."
        if hasattr(response, 'candidates') and response.candidates:
            try:
                finish_reason = response.candidates[0].finish_reason
                safety_ratings = response.candidates[0].safety_ratings
                error_msg += f" Finish Reason: {finish_reason}. Safety Ratings: {safety_ratings}."
            except (IndexError, AttributeError):
                pass # Keep original error_msg
        print(f"{time.time():.2f}:   Failure: {error_msg}")
        return False, error_msg

    except Exception as e:
        error_msg = f"API call via {client_type_str} to {actual_model_name} (path: {model_path_for_call}) failed: {type(e).__name__} - {e}"
        print(f"{time.time():.2f}:   Exception: {error_msg} (total API call function time: {time.time() - call_api_start_time:.2f}s)")
        return False, error_msg
    finally:
        print(f"{time.time():.2f}: Exiting call_model_via_genai_client for {actual_model_name} ({client_type_str}) (total duration: {time.time() - call_api_start_time:.2f}s)")

def send_query_to_first_available_model(query: str, studio_client: genai.Client, vertex_client: genai.Client, models_config: dict) -> tuple[str, str] | None:
    func_start_time = time.time()
    print(f"{time.time():.2f}: Entered send_query_to_first_available_model (using genai.Client with client.models.generate_content)")
    if not models_config:
        print(f"{time.time():.2f}: Error: No models configured.")
        return None

    
    for model_name_key, model_details in models_config.items():
        model_type = model_details.get("type")
        print(f"\n{time.time():.2f}: Trying model: {model_name_key} (Type: {model_type})")

        success = False
        api_response_message = "Model type not specified or client not configured for this type."
        attempt_loop_start_time = time.time()
        
        client_to_use = None
        client_type_str = "Unknown"

        if model_type == "studio_via_genai":
            client_to_use = studio_client
            client_type_str = "Google AI Studio"
            if not client_to_use:
                api_response_message = "Google AI Studio client not available/initialized."
        elif model_type == "vertex_via_genai":
            client_to_use = vertex_client
            client_type_str = "Vertex AI"
            if not client_to_use:
                api_response_message = "Vertex AI client not available/initialized."
        else:
            api_response_message = f"Unsupported model type '{model_type}' for {model_name_key}."
            print(f"{time.time():.2f}: {api_response_message} Skipping.")
            continue 

        if client_to_use:
            success, api_response_message = call_model_via_genai_client(
                client_type_str, client_to_use, model_name_key, model_details, query
            )
        else:
            print(f"{time.time():.2f}: Skipping {model_name_key}: {api_response_message}")

        if success:
            print(f"{time.time():.2f}: Query successfully processed by {model_name_key} (Type: {model_type}). (Attempt took {time.time() - attempt_loop_start_time:.2f}s)")
            print(f"{time.time():.2f}: Exiting send_query_to_first_available_model (SUCCESS after {time.time() - func_start_time:.2f}s)")
            return model_name_key, api_response_message
        else:
            print(f"{time.time():.2f}: Failed with {model_name_key}. Trying next model... (Attempt took {time.time() - attempt_loop_start_time:.2f}s)")

    print(f"\n{time.time():.2f}: All models were tried, but none successfully processed the query.")
    print(f"{time.time():.2f}: Exiting send_query_to_first_available_model (ALL FAILED after {time.time() - func_start_time:.2f}s)")
    return None

# --- Main Execution Block (remains the same) ---
if __name__ == "__main__":
    main_start_time = time.time()
    print(f"{time.time():.2f}: --- Gemini Model API Call Script (using google-genai Client with client.models.generate_content) ---")
    print("This script will attempt to call Gemini models using genai.Client objects from 'google-genai' SDK.")
    print("Ensure you have:\n"
        "1. Installed the 'google-genai' library: pip install google-genai\n"
        "   (Recommended: pip uninstall google-generativeai)\n"
        "2. Set environment variables:\n"
        "   - GOOGLE_API_KEY (for 'studio_via_genai' type models)\n"
        "   - GOOGLE_PROJECT_ID (script uses this for Vertex AI)\n"
        "   - GOOGLE_REGION (script uses this for Vertex AI, defaults to 'us-central1')\n"
        "3. Authenticated with Google Cloud (e.g., `gcloud auth application-default login`) for Vertex AI calls.")
    print("-" * 40)

    if not GOOGLE_PROJECT_ID and any(details.get("type") == "vertex_via_genai" for details in GEMINI_MODELS_CONFIG.values()):
        print(f"{time.time():.2f}: NOTICE: GOOGLE_PROJECT_ID (for Vertex AI) resolved to: {GOOGLE_PROJECT_ID if GOOGLE_PROJECT_ID else 'Not Set'}")

    print(f"{time.time():.2f}: Effective GEMINI_MODELS_CONFIG being used:")
    if not GEMINI_MODELS_CONFIG:
        print(f"{time.time():.2f}: No models defined in GEMINI_MODELS_CONFIG. Exiting.")
        sys.exit(1)
        
    for name, details in GEMINI_MODELS_CONFIG.items():
        print(f"- {name}: {details}")
    print("-" * 40)

    studio_client, vertex_client = create_clients()
    user_query_example = "What are the main differences between a llama and an alpaca? Be concise."

    print(f"{time.time():.2f}: Attempting to send query: \"{user_query_example}\"")
    send_query_call_start_time = time.time()
    result_tuple = send_query_to_first_available_model(user_query_example, studio_client, vertex_client, GEMINI_MODELS_CONFIG)
    print(f"{time.time():.2f}: send_query_to_first_available_model call completed (took {time.time() - send_query_call_start_time:.2f}s)")

    print(f"\n{time.time():.2f}: --- Query Sending Attempt Summary ---")
    if result_tuple:
        successful_model_key, response_text = result_tuple
        print(f"The query was successfully handled by model key: {successful_model_key}")
        print(f"Response:\n{response_text}")
    else:
        print("The query could not be processed by any of the configured models.")
    print("-" * 40)
    print(f"{time.time():.2f}: Script finished. Total execution time: {time.time() - script_start_time:.2f}s")