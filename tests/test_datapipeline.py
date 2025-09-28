# test_pipeline.py (na raiz do projeto)
import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')
django.setup()

from data_pipeline.extractors import extract_to_arrow
from data_pipeline.loaders import load_to_clickhouse

def test_basic_pipeline():
    """Test basic PostgreSQL -> Arrow -> ClickHouse pipeline"""
    
    # Source config for your Docker PostgreSQL
    source_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'backend_db',  # Your database name
        'user': 'postgres',
        'password': 'postgres',
        'query': 'SELECT 1 as id, \'test\' as name'  # Simple test query
    }
    
    try:
        # Extract to Arrow
        print("Extracting data to Arrow format...")
        arrow_table = extract_to_arrow(source_config)
        print(f"Extracted {arrow_table.num_rows} rows")
        
        # Load to ClickHouse
        print("Loading to ClickHouse...")
        success = load_to_clickhouse(arrow_table, 'test_table')
        
        if success:
            print("Pipeline completed successfully!")
        else:
            print("Pipeline failed")
            
    except Exception as e:
        print(f"Pipeline error: {str(e)}")

if __name__ == "__main__":
    test_basic_pipeline()