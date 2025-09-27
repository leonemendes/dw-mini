"""
Data loading utilities for the data warehouse pipeline.
Handles loading Arrow data into ClickHouse and other destinations.
"""
import pyarrow as pa
from clickhouse_driver import Client
import logging
from typing import Optional, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)


def load_to_clickhouse(arrow_table: pa.Table, 
                      table_name: str,
                      clickhouse_host: str = 'localhost',
                      clickhouse_port: int = 9000,
                      database: str = 'default',
                      drop_if_exists: bool = True) -> bool:
    """
    Load Apache Arrow table data into ClickHouse database.
    
    Args:
    ---
        arrow_table: Arrow table containing data to load
        table_name: Target table name in ClickHouse
        clickhouse_host: ClickHouse server host
        clickhouse_port: ClickHouse server port
        database: Target database name
        drop_if_exists: Whether to drop existing table
        
    Returns:
    ---
        bool: True if successful, False otherwise
        
    Raises:
    ---
        Exception: ClickHouse connection or insertion errors
    """
    
    if arrow_table.num_rows == 0:
        logger.warning(f"Arrow table is empty, skipping load to {table_name}")
        return True
        
    client = None
    
    try:
        # Connect to ClickHouse
        client = Client(host=clickhouse_host, port=clickhouse_port, database=database)
        
        # Test connection
        client.execute('SELECT 1')
        logger.info(f"Connected to ClickHouse at {clickhouse_host}:{clickhouse_port}")
        
        # Convert Arrow to pandas for ClickHouse insertion
        df = arrow_table.to_pandas()
        
        # Drop existing table if requested
        if drop_if_exists:
            client.execute(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"Dropped existing table: {table_name}")
        
        # Auto-generate CREATE TABLE statement
        create_table_sql = _generate_create_table_sql(df, table_name)
        client.execute(create_table_sql)
        logger.info(f"Created table: {table_name}")
        
        # Insert data using ClickHouse's efficient insertion
        client.insert_dataframe(f'INSERT INTO {table_name} VALUES', df)
        
        # Verify insertion
        result = client.execute(f'SELECT COUNT(*) FROM {table_name}')
        row_count = result[0][0]
        
        logger.info(f"Successfully loaded {row_count} rows into {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to load data to ClickHouse: {str(e)}")
        raise
    finally:
        if client:
            client.disconnect()


def _generate_create_table_sql(df: pd.DataFrame, table_name: str) -> str:
    """
    Generate CREATE TABLE SQL based on pandas DataFrame dtypes.
    
    Args:
        df: Pandas DataFrame to analyze
        table_name: Name of the table to create
        
    Returns:
        str: CREATE TABLE SQL statement
    """
    # Enhanced dtype mapping for better ClickHouse compatibility
    dtype_mapping = {
        'int8': 'Int8',
        'int16': 'Int16', 
        'int32': 'Int32',
        'int64': 'Int64',
        'uint8': 'UInt8',
        'uint16': 'UInt16',
        'uint32': 'UInt32', 
        'uint64': 'UInt64',
        'float32': 'Float32',
        'float64': 'Float64',
        'bool': 'UInt8',
        'object': 'String',
        'string': 'String',
        'datetime64[ns]': 'DateTime',
        'datetime64[ns, UTC]': 'DateTime',
        'timedelta64[ns]': 'Int64'
    }
    
    columns = []
    for col, dtype in df.dtypes.items():
        # Clean column name for ClickHouse
        clean_col = col.replace(' ', '_').replace('-', '_')
        
        # Map pandas dtype to ClickHouse type
        clickhouse_type = dtype_mapping.get(str(dtype), 'String')
        
        # Handle nullable types
        if df[col].isna().any():
            clickhouse_type = f"Nullable({clickhouse_type})"
            
        columns.append(f"`{clean_col}` {clickhouse_type}")
    
    # Generate CREATE TABLE statement
    create_sql = f"""
    CREATE TABLE {table_name} (
        {', '.join(columns)}
    ) ENGINE = MergeTree() 
    ORDER BY tuple()
    """
    
    return create_sql


def get_clickhouse_table_info(table_name: str,
                             clickhouse_host: str = 'localhost',
                             clickhouse_port: int = 9000,
                             database: str = 'default') -> Dict[str, Any]:
    """
    Get information about a ClickHouse table.
    
    Args:
        table_name: Name of the table
        clickhouse_host: ClickHouse server host
        clickhouse_port: ClickHouse server port
        database: Database name
        
    Returns:
        Dict containing table information (columns, row count, etc.)
    """
    client = None
    
    try:
        client = Client(host=clickhouse_host, port=clickhouse_port, database=database)
        
        # Get table schema
        schema_query = f"DESCRIBE TABLE {table_name}"
        schema_result = client.execute(schema_query)
        
        columns = []
        for row in schema_result:
            columns.append({
                'name': row[0],
                'type': row[1],
                'default_type': row[2] if len(row) > 2 else None,
                'default_expression': row[3] if len(row) > 3 else None
            })
        
        # Get row count
        count_result = client.execute(f'SELECT COUNT(*) FROM {table_name}')
        row_count = count_result[0][0]
        
        # Get table size (approximate)
        size_query = f"""
        SELECT 
            formatReadableSize(sum(bytes_on_disk)) as size
        FROM system.parts 
        WHERE table = '{table_name}' AND database = '{database}'
        """
        
        size_result = client.execute(size_query)
        table_size = size_result[0][0] if size_result else 'Unknown'
        
        return {
            'table_name': table_name,
            'columns': columns,
            'row_count': row_count,
            'table_size': table_size
        }
        
    finally:
        if client:
            client.disconnect()