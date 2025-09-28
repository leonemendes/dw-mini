"""
Offline tests for data pipeline functionality.
"""
from django.test import TestCase
import pandas as pd
import pyarrow as pa
import psycopg2

from data_pipeline.extractors import extract_to_arrow, get_table_schema, list_tables
from data_pipeline.loaders import load_to_clickhouse, get_clickhouse_table_info

class ExtractorsTest(TestCase):
    
    def setUp(self):
        self.source_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_backend_db',
            'user': 'postgres',
            'password': 'postgres',
            'query': 'SELECT 1 as id, \'test\' as name'
        }

    def test_list_tables(self):
        """Test list tables"""

        tables = list_tables(self.source_config)
        self.assertTrue(set(['data_pipeline_datasource', 'data_pipeline_importjob']).issubset(tables))
    
    def test_get_table_schema(self):
        """Test get table schema"""

        table_schema = get_table_schema(self.source_config, 'data_pipeline_datasource')

        self.assertTrue('name' in table_schema)
    
    def test_extract_to_arrow(self):

        pa_table = extract_to_arrow(self.source_config)

        print(pa_table)

