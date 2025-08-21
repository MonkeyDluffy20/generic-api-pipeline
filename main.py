
from utils.api_client import call_api, load_config  
from utils.db_client import DBClient               
from utils.logger import get_logger               

# Initialize a logger for this module
_logger = get_logger(__name__)
# Load config
config = load_config()

# Extract DB settings
db_server = config["database"]["server"]
db_name = config["database"]["database"]


def fetch_ids_for_entity(entity_config):
    """
    Try to fetch list endpoint for the entity (i.e., a URL without {id}).
    - If successful, return a list of IDs from the response.
    - If it fails or returns nothing, return an empty list.
    """

    entity = entity_config["entity"]  # Logical name of the entity, e.g., 'business'
    
    # Derive base API by removing the '{id}' placeholder from the API template
    # Example: "/business/{id}" → "/business"
    base_api = entity_config["api"].split("{id}")[0].rstrip("/")
    
    # Determine which field in each record to treat as the ID (defaults to "id")
    parent_key = entity_config.get("parentKey", "id")

    try:
        # Log what we're trying to fetch
        _logger.info(f"Trying to fetch list for {entity} at {base_api}")
        
        # Call the API using an override so we hit the list endpoint (without {id})
        data = call_api(entity, api_override=base_api)

        # If no data returned, log a warning and return empty list
        if not data:
            _logger.warning(f"No data returned from list endpoint {base_api}")
            return []

        # If the API response is wrapped in {"items": [...]}, extract the items list
        if isinstance(data, dict) and "items" in data:
            data = data["items"]

        # Collect all IDs where parent_key exists in the record
        ids = [record[parent_key] for record in data if parent_key in record]

        # Log the number of IDs found
        _logger.info(f"Found {len(ids)} IDs for {entity}")
        return ids

    except Exception as e:
        # Any failure here means we cannot fetch the list → log and return []
        _logger.error(f"Failed to fetch list for {entity}: {e}")
        return []


def process_entity(db_client, entity_config):
    """
    Process a single entity by:
    - Checking if its API needs an ID ({id} placeholder).
    - If yes → fetch all IDs and call API for each ID.
    - If no → just call the single endpoint once.
    - Store the API responses in the database via db_client.
    """

    entity = entity_config["entity"]
    
    # Check if the endpoint has {id} in its API template
    requires_id = "{id}" in entity_config["api"]

    if requires_id:
        # Get list of IDs by calling fetch_ids_for_entity
        ids = fetch_ids_for_entity(entity_config)

        if not ids:
            # No IDs found → log and skip
            _logger.warning(f"No IDs discovered for {entity}, skipping {entity}/{{id}} calls.")
            return

        # Loop through each discovered ID and fetch its details
        for pid in ids:
            try:
                response = call_api(entity, id=pid)  # Pass id for URL substitution
                if response:
                    db_client.upsert_data(entity, response)  # Insert or update DB
            except Exception as e:
                _logger.error(f"Failed to process {entity} with id={pid}: {e}")

    else:
        # If endpoint does not need ID → single API call
        try:
            response = call_api(entity)
            if response:
                _logger.info(f"Inserting data for {entity}")
                db_client.upsert_data(entity, response)
        except Exception as e:
            _logger.error(f"Failed to process {entity}: {e}")


def main():
    """
    Main workflow:
    - Load configuration (endpoint details from JSON).
    - Create DB client connection.
    - For each endpoint in config → process it using process_entity.
    """

    config = load_config()  # Load JSON config (apiEndpoints, etc.)
    
    # Initialize DB client for your target database (keyPay)
    db_client = DBClient(server=db_server, database=db_name)

    # Loop through all configured API endpoints and process them
    for endpoint in config["apiEndpoints"]:
        process_entity(db_client, endpoint)


# Standard Python entry point check
if __name__ == "__main__":
    main()
