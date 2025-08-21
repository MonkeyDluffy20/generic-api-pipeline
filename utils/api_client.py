import json
import requests
from pathlib import Path
from utils.token_manager import get_access_token
from utils.logger import get_logger

_logger = get_logger(__name__)
CONFIG_FILE = Path("endpointconfig.json")

def load_config():
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)

def get_endpoint(entity):
    """
    Look up a single endpoint definition by its 'entity' name.
    Example: entity='invoices' → returns that dict from config["apiEndpoints"].
    Returns None if not found.
    """
    config = load_config()
    # Iterate through apiEndpoints and return the first match where entity equals the requested one
    return next((e for e in config["apiEndpoints"] if e["entity"] == entity), None)


def call_api(entity, api_override=None, **kwargs):
    """
    Dynamically call an API endpoint defined in the config.

    Arguments:
      - entity: the logical name of the endpoint (used to look up URL + HTTP method).
      - api_override: optional string to override the 'api' template in config (useful for list/detail variants).
      - **kwargs: values that will be substituted into {placeholders} in the URL template, e.g. id=123.

    Behavior:
      1) Fetch an access token (Bearer).
      2) Resolve the endpoint definition from config via 'entity'.
      3) Build the request URL by formatting the API template with kwargs (handles {id}, etc.).
      4) Make the HTTP request using the configured HTTP method.
      5) If status != 200 → log error and raise; else return parsed JSON.
    """

    # 1) Get a fresh access token (from your utils)
    token = get_access_token()

    # 2) Load the endpoint definition (method + api template) for the given entity
    endpoint = get_endpoint(entity)

    # If the entity isn't defined in your JSON, fail fast with a clear error
    if not endpoint:
        raise ValueError(f"No API endpoint found for entity '{entity}'")

    # 3) Decide which URL template to use: override wins; otherwise the one from config
    api_template = api_override if api_override else endpoint["api"]

    try:
        # Substitute placeholders in the URL template with values from kwargs.
        # Example: api_template="https://api/x/{id}" and kwargs={"id": 42} → "https://api/x/42"
        url = api_template.format(**kwargs)
    except KeyError as e:
        # If a placeholder is missing (e.g., {id} not provided), log a helpful warning and re-raise
        _logger.warning(f"Missing placeholder for {e} in API {api_template} → kwargs={kwargs}")
        raise

    # Build Authorization header using the bearer token
    headers = {"Authorization": f"Bearer {token}"}

    # Log the full URL (good for traceability; avoid logging secrets)
    _logger.info(f"Calling API: {url}")

    # 4) Fire the HTTP request using the method from config (GET/POST/PUT/DELETE, etc.)
    # Note: No body or query params are being sent here—just headers.
    response = requests.request(endpoint["method"], url, headers=headers)

    # 5) Basic error handling: only 200 is considered success
    if response.status_code != 200:
        # Log both code and body for debugging (be careful if body may contain sensitive data)
        _logger.error(f"API call failed: {response.status_code} {response.text}")
        # Raise a generic exception upward so callers can handle it
        raise Exception(f"API call failed: {response.status_code}")

    # On success, parse and return the response JSON as a Python object
    return response.json()