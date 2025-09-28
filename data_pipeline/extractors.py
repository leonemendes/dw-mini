"""
Data extraction utilities for the data warehouse pipeline.
Handles extraction from various sources and conversion to Arrow format.
"""
import pyarrow as pa
import psycopg2
import pandas as pd
from typing import Dict, Any, Optional
import logging
from django.conf import settings

logger = logging.getLogger(__name__)


def extract_to_arrow(source_config: Dict[str, Any]) -> pa.Table:
    """
    Extract data from PostgreSQL source and convert to Apache Arrow format.
    
    Args:
    ---
        source_config: Dictionary containing connection parameters
                      (host, database, user, password, table_name, query)
    
    Returns:
    ---
        pa.Table: Arrow table with extracted data
        
    Raises:
    ---
        psycopg2.Error: Database connection or query errors
        ValueError: Invalid source configuration
    """
    if not source_config.get('database'):
        raise ValueError("Database name is required in source_config")
    
    connection = None
    
    try:
        # Connect to PostgreSQL
        connection = psycopg2.connect(
            host=source_config.get('host', 'localhost'),
            port=source_config.get('port', 5432),
            database=source_config['database'],
            user=source_config.get('user', 'postgres'),
            password=source_config.get('password', 'postgres')
        )
        
        # Use custom query or default table select
        if 'query' in source_config:
            query = source_config['query']
        elif 'table_name' in source_config:
            query = f"SELECT * FROM {source_config['table_name']}"
        else:
            raise ValueError("Either 'query' or 'table_name' must be provided")
        
        logger.info(f"Executing query: {query}")
        
        # Read data using pandas for efficient Arrow conversion
        df = pd.read_sql(query, connection)
        
        if df.empty:
            logger.warning("Query returned no results")
            return pa.Table.from_pandas(pd.DataFrame())
        
        # Convert to Apache Arrow format
        arrow_table = pa.Table.from_pandas(df)
        
        logger.info(f"Successfully extracted {len(arrow_table)} rows to Arrow format")
        return arrow_table
        
    except psycopg2.Error as e:
        logger.error(f"PostgreSQL error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during extraction: {str(e)}")
        raise
    finally:
        if connection:
            connection.close()


def get_table_schema(source_config: Dict[str, Any], table_name: str) -> Dict[str, str]:
    """
    Get schema information for a specific table.
    
    Args:
    ---
        source_config: Database connection parameters
        table_name: Name of the table to analyze
        
    Returns:
    ---
        Dict mapping column names to their data types
    """
    connection = None
    
    try:
        connection = psycopg2.connect(
            host=source_config.get('host', 'localhost'),
            port=source_config.get('port', 5432),
            database=source_config['database'],
            user=source_config.get('user', 'postgres'),
            password=source_config.get('password', 'postgres')
        )
        
        # Query PostgreSQL information_schema for table schema
        schema_query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = %s
        ORDER BY ordinal_position;
        """
        
        cursor = connection.cursor()
        cursor.execute(schema_query, (table_name,))
        columns = cursor.fetchall()
        
        schema = {}
        for column_name, data_type, is_nullable in columns:
            schema[column_name] = {
                'type': data_type,
                'nullable': is_nullable == 'YES'
            }
        
        logger.info(f"Retrieved schema for table '{table_name}': {len(schema)} columns")
        return schema
        
    finally:
        if connection:
            connection.close()


def list_tables(source_config: Dict[str, Any]) -> list:
    """
    List all tables available in the database.
    
    Args:
    ---
        source_config: Database connection parameters
        
    Returns:
    ---
        List of table names
    """
    connection = None
    
    try:
        connection = psycopg2.connect(
            host=source_config.get('host', 'localhost'),
            port=source_config.get('port', 5432),
            database=source_config['database'],
            user=source_config.get('user', 'postgres'),
            password=source_config.get('password', 'postgres')
        )
        
        # Query for all user tables
        tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """
        
        cursor = connection.cursor()
        cursor.execute(tables_query)
        tables = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Found {len(tables)} tables in database")
        return tables
        
    finally:
        if connection:
            connection.close()