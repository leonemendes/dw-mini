"""
Offline tests for data pipeline functionality.
Uses mocks to avoid external dependencies like PostgreSQL/ClickHouse.
"""
from django.test import TestCase
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import pyarrow as pa
import psycopg2

from data_pipeline.extractors import extract_to_arrow, get_table_schema, list_tables
from data_pipeline.loaders import load_to_clickhouse, get_clickhouse_table_info


class ExtractorTestCase(TestCase):
    """Test data extraction functionality without external dependencies."""
    
    def setUp(self):
        """Set up test data and configurations."""
        self.source_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'postgres',
            'password': 'postgres',
            'query': 'SELECT 1 as id, \'test\' as name'
        }
        
        # Sample data that would come from PostgreSQL
        self.sample_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['test1', 'test2', 'test3'],
            'value': [100.5, 200.0, 300.25]
        })
    
    @patch('data_pipeline.extractors.psycopg2.connect')
    @patch('data_pipeline.extractors.pd.read_sql')
    def test_extract_to_arrow_success(self, mock_read_sql, mock_connect):
        """Test successful data extraction to Arrow format."""
        # Mock database connection and data reading
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        mock_read_sql.return_value = self.sample_data
        
        # Execute extraction
        arrow_table = extract_to_arrow(self.source_config)
        
        # Verify calls
        mock_connect.assert_called_once_with(
            host='localhost',
            port=5432,
            database='test_db',
            user='postgres',
            password='postgres'
        )
        mock_read_sql.assert_called_once_with(
            'SELECT 1 as id, \'test\' as name',
            mock_connection
        )
        mock_connection.close.assert_called_once()
        
        # Verify Arrow table
        self.assertIsInstance(arrow_table, pa.Table)
        self.assertEqual(arrow_table.num_rows, 3)
        self.assertEqual(arrow_table.num_columns, 3)
        
        # Convert back to pandas to verify data
        result_df = arrow_table.to_pandas()
        pd.testing.assert_frame_equal(result_df, self.sample_data)
    
    @patch('data_pipeline.extractors.psycopg2.connect')
    @patch('data_pipeline.extractors.pd.read_sql')
    def test_extract_with_table_name(self, mock_read_sql, mock_connect):
        """Test extraction using table_name instead of custom query."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        mock_read_sql.return_value = self.sample_data
        
        # Config with table_name instead of query
        config = self.source_config.copy()
        del config['query']
        config['table_name'] = 'users'
        
        arrow_table = extract_to_arrow(config)
        
        # Verify the generated query
        mock_read_sql.assert_called_once_with(
            'SELECT * FROM users',
            mock_connection
        )
    
    @patch('data_pipeline.extractors.psycopg2.connect')
    @patch('data_pipeline.extractors.pd.read_sql')
    def test_extract_empty_result(self, mock_read_sql, mock_connect):
        """Test extraction when query returns no results."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        mock_read_sql.return_value = pd.DataFrame()  # Empty DataFrame
        
        arrow_table = extract_to_arrow(self.source_config)
        
        self.assertEqual(arrow_table.num_rows, 0)
    
    @patch('data_pipeline.extractors.psycopg2.connect')
    def test_extract_database_error(self, mock_connect):
        """Test handling of database connection errors."""
        # Mock connection failure
        mock_connect.side_effect = psycopg2.Error("Connection failed")
        
        with self.assertRaises(psycopg2.Error):
            extract_to_arrow(self.source_config)
    
    def test_extract_invalid_config(self):
        """Test extraction with invalid configuration."""
        invalid_config = {'host': 'localhost'}  # Missing required fields
        
        with self.assertRaises(ValueError):
            extract_to_arrow(invalid_config)
    
    @patch('data_pipeline.extractors.psycopg2.connect')
    def test_get_table_schema(self, mock_connect):
        """Test table schema discovery."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock schema query results
        mock_cursor.fetchall.return_value = [
            ('id', 'integer', 'NO'),
            ('name', 'character varying', 'YES'),
            ('created_at', 'timestamp without time zone', 'NO')
        ]
        
        schema = get_table_schema(self.source_config, 'test_table')
        
        expected_schema = {
            'id': {'type': 'integer', 'nullable': False},
            'name': {'type': 'character varying', 'nullable': True},
            'created_at': {'type': 'timestamp without time zone', 'nullable': False}
        }
        
        self.assertEqual(schema, expected_schema)
        mock_cursor.execute.assert_called_once()
    
    @patch('data_pipeline.extractors.psycopg2.connect')
    def test_list_tables(self, mock_connect):
        """Test listing available tables."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        mock_cursor.fetchall.return_value = [
            ('users',),
            ('products',),
            ('orders',)
        ]
        
        tables = list_tables(self.source_config)
        
        expected_tables = ['users', 'products', 'orders']
        self.assertEqual(tables, expected_tables)


class LoaderTestCase(TestCase):
    """Test data loading functionality without external dependencies."""
    
    def setUp(self):
        """Set up test Arrow data."""
        self.sample_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['test1', 'test2', 'test3'],
            'value': [100.5, 200.0, 300.25],
            'active': [True, False, True]
        })
        self.arrow_table = pa.Table.from_pandas(self.sample_data)
    
    @patch('data_pipeline.loaders.Client')
    def test_load_to_clickhouse_success(self, mock_client_class):
        """Test successful loading to ClickHouse."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock ClickHouse responses
        mock_client.execute.side_effect = [
            None,  # Connection test
            None,  # DROP TABLE
            None,  # CREATE TABLE
            [[3]]  # COUNT query result
        ]
        
        success = load_to_clickhouse(
            self.arrow_table, 
            'test_table',
            clickhouse_host='localhost',
            clickhouse_port=9000
        )
        
        self.assertTrue(success)
        
        # Verify ClickHouse client calls
        mock_client_class.assert_called_once_with(
            host='localhost', 
            port=9000, 
            database='default'
        )
        
        # Verify table operations
        mock_client.execute.assert_any_call('SELECT 1')  # Connection test
        mock_client.execute.assert_any_call('DROP TABLE IF EXISTS test_table')
        mock_client.execute.assert_any_call('SELECT COUNT(*) FROM test_table')
        
        # Verify data insertion
        mock_client.insert_dataframe.assert_called_once()
    
    @patch('data_pipeline.loaders.Client')
    def test_load_empty_table(self, mock_client_class):
        """Test loading empty Arrow table."""
        empty_df = pd.DataFrame()
        empty_arrow = pa.Table.from_pandas(empty_df)
        
        success = load_to_clickhouse(empty_arrow, 'empty_table')
        
        # Should return True but skip actual loading
        self.assertTrue(success)
        # ClickHouse client should not be called for empty tables
        mock_client_class.assert_not_called()
    
    @patch('data_pipeline.loaders.Client')
    def test_load_clickhouse_error(self, mock_client_class):
        """Test handling of ClickHouse errors."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock ClickHouse connection failure
        mock_client.execute.side_effect = Exception("ClickHouse connection failed")
        
        with self.assertRaises(Exception):
            load_to_clickhouse(self.arrow_table, 'test_table')
    
    def test_generate_create_table_sql(self):
        """Test CREATE TABLE SQL generation."""
        from data_pipeline.loaders import _generate_create_table_sql
        
        # Test with various data types
        test_df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.1, 2.2, 3.3],
            'str_col': ['a', 'b', 'c'],
            'bool_col': [True, False, True],
            'nullable_col': [1, None, 3]
        })
        
        sql = _generate_create_table_sql(test_df, 'test_table')
        
        self.assertIn('CREATE TABLE test_table', sql)
        self.assertIn('`int_col` Int64', sql)
        self.assertIn('`float_col` Float64', sql)
        self.assertIn('`str_col` String', sql)
        self.assertIn('`bool_col` UInt8', sql)
        self.assertIn('Nullable(Int64)', sql)  # nullable column
        self.assertIn('ENGINE = MergeTree()', sql)
    
    @patch('data_pipeline.loaders.Client')
    def test_get_clickhouse_table_info(self, mock_client_class):
        """Test getting ClickHouse table information."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock ClickHouse responses
        mock_client.execute.side_effect = [
            # DESCRIBE TABLE response
            [
                ('id', 'Int64', '', ''),
                ('name', 'String', '', ''),
                ('value', 'Float64', '', '')
            ],
            [[1000]],  # COUNT response
            [['500KB']]  # Size response
        ]
        
        info = get_clickhouse_table_info('test_table')
        
        expected_info = {
            'table_name': 'test_table',
            'columns': [
                {'name': 'id', 'type': 'Int64', 'default_type': '', 'default_expression': ''},
                {'name': 'name', 'type': 'String', 'default_type': '', 'default_expression': ''},
                {'name': 'value', 'type': 'Float64', 'default_type': '', 'default_expression': ''}
            ],
            'row_count': 1000,
            'table_size': '500KB'
        }
        
        self.assertEqual(info, expected_info)


class IntegrationTestCase(TestCase):
    """Integration tests for complete pipeline without external services."""
    
    @patch('data_pipeline.extractors.psycopg2.connect')
    @patch('data_pipeline.extractors.pd.read_sql')
    @patch('data_pipeline.loaders.Client')
    def test_full_pipeline_offline(self, mock_clickhouse, mock_read_sql, mock_pg_connect):
        """Test complete pipeline: Extract -> Load without external services."""
        
        # Mock PostgreSQL
        mock_pg_connection = Mock()
        mock_pg_connect.return_value = mock_pg_connection
        
        sample_data = pd.DataFrame({
            'user_id': [1, 2, 3],
            'username': ['alice', 'bob', 'charlie'],
            'score': [95.5, 87.2, 92.1]
        })
        mock_read_sql.return_value = sample_data
        
        # Mock ClickHouse
        mock_ch_client = Mock()
        mock_clickhouse.return_value = mock_ch_client
        mock_ch_client.execute.side_effect = [None, None, None, [[3]]]
        
        # Source configuration
        source_config = {
            'host': 'localhost',
            'database': 'test_db',
            'user': 'postgres',
            'password': 'postgres',
            'table_name': 'users'
        }
        
        # Execute pipeline
        arrow_table = extract_to_arrow(source_config)
        success = load_to_clickhouse(arrow_table, 'users_warehouse')
        
        # Verify results
        self.assertTrue(success)
        self.assertEqual(arrow_table.num_rows, 3)
        self.assertEqual(arrow_table.num_columns, 3)
        
        # Verify all mocks were called appropriately
        mock_pg_connect.assert_called_once()
        mock_read_sql.assert_called_once()
        mock_clickhouse.assert_called_once()
        mock_ch_client.insert_dataframe.assert_called_once()