import pyodbc
import logging
from utils.api_client import load_config
from utils.logger import get_logger

_logger = get_logger(__name__)
config = load_config()

class DBClient:
    """
    Database client class for handling connections and performing upserts.
    Works specifically with SQL Server using ODBC and config-driven mappings.
    """

    def __init__(self, server: str, database: str):
        # These parameters are currently not used in the connection string below,
        # but could be dynamically injected if needed.
        self.server = server
        self.database = database

    def _create_connection(self) -> pyodbc.Connection:
        """
        Create and return a SQL Server database connection.
        Uses hardcoded DSN settings in this version.
        """
        try:
            conn_str = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                "Trusted_Connection=yes;"
            )

            # Attempt to establish connection
            return pyodbc.connect(conn_str)
        except pyodbc.Error as e:
            # Log and re-raise if connection fails
            _logger.error(f"Failed to create database connection: {e}")
            raise

    def upsert_data(self, entity: str, data: dict | list[dict]):
        """
        Upsert API response data into a SQL Server table.
        
        Steps:
        1. Load table mapping from config (maps API fields → DB columns).
        2. Prepare a MERGE statement for UPSERT logic.
        3. Exclude non-updatable columns like timestamp/rowversion.
        4. Execute the SQL for each record, handling errors gracefully.
        """

        # Load full config to get table mapping for the entity
        config = load_config()
        mapping = config["tableMapping"].get(entity)

        if not mapping:
            # If mapping is missing → no way to insert data
            raise ValueError(f"No table mapping defined for entity '{entity}'")

        # Schema name (default = dbo) pulled from config
        schema = config.get("database", {}).get("schema", "dbo")

        # Fully qualified table name, e.g., [dbo].[business]
        table_name = f"[{schema}].[{entity}]"

        # Create a new DB connection and cursor
        conn = self._create_connection()
        cursor = conn.cursor()

        # If single dict passed, wrap in list for consistency
        if isinstance(data, dict):
            data = [data]

        row_count = 0  # Track affected rows for logging

        # Columns to exclude (cannot be updated or inserted)
        excluded_cols = {"timestamp", "rowversion", "dateCreated"}

        for record in data:
            # Build a mapped dictionary: {DB_col: record[API_field]}
            mapped_values = {col: record.get(api_field) for col, api_field in mapping.items()}

            # Remove excluded columns from mapped data
            skipped_cols = [col for col in mapped_values if col in excluded_cols]
            for col in skipped_cols:
                mapped_values.pop(col)

            if skipped_cols:
                _logger.info(f"Skipped columns for {entity}: {', '.join(skipped_cols)}")

            # Prepare SQL fragments for MERGE
            columns = ", ".join(mapped_values.keys())                 # "col1, col2, col3"
            values = ", ".join(["?"] * len(mapped_values))           # "?, ?, ?"
            updates = ", ".join([f"{col}=?" for col in mapped_values.keys() if col != 'id'])  # Exclude ID from updates

            # Build the MERGE SQL
            sql = f"""
            MERGE INTO {table_name} AS target
            USING (SELECT {', '.join(['?' for _ in mapped_values])}) AS source ({columns})
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET {updates}
            WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values});
            """

            # Prepare parameters for MERGE
            # Order matters: first SELECT params, then UPDATE params, then INSERT params
            source_params = list(mapped_values.values())
            update_params = [v for k, v in mapped_values.items() if k != 'id']
            insert_params = list(mapped_values.values())

            params = source_params + update_params + insert_params

            try:
                # Execute SQL with all params
                cursor.execute(sql, params)
                # rowcount: number of rows affected (-1 if not available)
                row_count += cursor.rowcount if cursor.rowcount != -1 else 0
            except Exception as e:
                # Log details for debugging
                _logger.error(f"Failed to upsert record into {entity}: {e}")
                _logger.error(f"SQL: {sql}")
                _logger.error(f"Params count: {len(params)}")

        # Commit changes to DB
        conn.commit()
        cursor.close()
        conn.close()

        # Log the total number of rows processed
        _logger.info(f"Upsert complete for {entity}: {row_count} rows affected")