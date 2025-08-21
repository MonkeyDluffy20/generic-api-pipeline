"""
Generic OAuth2 Token Manager (Python)

This module handles:
- Loading OAuth2 config from JSON file
- Requesting new access tokens using refresh_token
- Persisting new refresh_token values
- Caching tokens in memory with expiry tracking
- Returning a valid token (auto-refresh before expiry)

Supports any OAuth2 API (Employment Hero, KeyPay, Xero, etc.)
"""

import requests
import time
import json
from pathlib import Path

# Path to token configuration JSON
TOKEN_FILE = Path("token_config.json")

# Buffer (in seconds) to refresh before token actually expires
TOKEN_EXPIRY_BUFFER = 60  # refresh 1 minute early

# In-memory token cache
token_cache = {
    "access_token": None,
    "expires_at": 0  # UNIX timestamp of expiry time
}


def load_token_config():
    """
    Load OAuth2 token configuration from JSON file.

    Returns:
        dict: Token configuration containing:
              - tokenUrl (str): OAuth endpoint
              - headers (dict): Request headers
              - body (dict): Request body (grant_type, client_id, client_secret, refresh_token)
    """
    with open(TOKEN_FILE, "r") as f:
        data = json.load(f)

    # Support both naming conventions for flexibility
    return data.get("tokenConfig") or data.get("token_config")


def save_token_config(config):
    """
    Save updated token configuration to JSON file.
    Used when a new refresh_token is issued by OAuth server.

    Args:
        config (dict): Updated configuration with new refresh_token
    """
    with open(TOKEN_FILE, "w") as f:
        json.dump({"tokenConfig": config}, f, indent=2)


def fetch_new_token():
    """
    Request a new access token using the refresh_token.

    Steps:
    1. Load current configuration from token_config.json
    2. Make POST request to OAuth token endpoint
    3. Parse response and extract access_token + expires_in
    4. Update refresh_token in config if new one is provided
    5. Store token and expiry time in cache

    Returns:
        str: Freshly obtained access_token
    """
    config = load_token_config()

    response = requests.post(
        config["tokenUrl"],
        headers=config["headers"],
        data=config["body"],
        timeout=30
    )

    if response.status_code != 200:
        raise Exception(f"Token request failed: {response.text}")

    token_data = response.json()

    # If server provides a new refresh_token, update config
    if "refresh_token" in token_data:
        config["body"]["refresh_token"] = token_data["refresh_token"]
        save_token_config(config)

    # Calculate expiry time with buffer
    # expires_in = seconds until expiry returned by API
    # expires_at = current time + expires_in - buffer
    expires_in_seconds = token_data["expires_in"]
    expires_at = time.time() + expires_in_seconds - TOKEN_EXPIRY_BUFFER

    # Update in-memory cache
    token_cache["access_token"] = token_data["access_token"]
    token_cache["expires_at"] = expires_at

    return token_cache["access_token"]


def get_access_token():
    """
    Get a valid access_token for API requests.

    Logic:
    1. Check if cached token exists and is still valid (time < expires_at)
    2. If valid, return cached token
    3. If expired or missing, request new token and update cache

    Returns:
        str: Valid access_token ready for Authorization header
    """
    current_time = time.time()

    # If cached token exists and is still valid, return it
    if token_cache["access_token"] and current_time < token_cache["expires_at"]:
        return token_cache["access_token"]

    # Otherwise, fetch new token from OAuth server
    return fetch_new_token()
