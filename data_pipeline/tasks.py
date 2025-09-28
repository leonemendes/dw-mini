"""
Celery tasks for data pipeline operations.
These tasks run asynchronously in the background via Redis.
"""
from celery import shared_task
from celery.utils.log import get_task_logger
from django.utils import timezone
import pyarrow as pa
from typing import Dict, Any

from .extractors import extract_to_arrow, get_table_schema
from .loaders import load_to_clickhouse
from .models import ImportJob, DataSource

logger = get_task_logger(__name__)


@shared_task(bind=True, max_retries=3)
def extract_data_task(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Async task to extract data from source and convert to Arrow format.
    
    Args:
    ---
        source_config: Database connection and query parameters
        
    Returns:
    ---
        Dict with task results and metadata
    """
    try:
        logger.info(f"Starting data extraction from {source_config.get('database')}")
        
        # Update task state
        self.update_state(
            state='PROGRESS',
            meta={'status': 'Connecting to source database'}
        )
        
        # Extract data to Arrow format
        arrow_table = extract_to_arrow(source_config)
        
        self.update_state(
            state='PROGRESS', 
            meta={'status': f'Extracted {arrow_table.num_rows} rows'}
        )
        
        # Serialize Arrow table for passing to next task
        arrow_bytes = arrow_table.serialize()
        
        logger.info(f"Successfully extracted {arrow_table.num_rows} rows")
        
        return {
            'status': 'SUCCESS',
            'rows_extracted': arrow_table.num_rows,
            'columns': arrow_table.num_columns,
            'arrow_data': arrow_bytes.to_pybytes(),  # Serialize for Redis
            'schema': str(arrow_table.schema)
        }
        
    except Exception as exc:
        logger.error(f"Data extraction failed: {str(exc)}")
        
        # Retry with exponential backoff
        countdown = 2 ** self.request.retries
        self.retry(countdown=countdown, exc=exc)


@shared_task(bind=True, max_retries=3)
def load_data_task(self, arrow_data_bytes: bytes, table_name: str, 
                   clickhouse_config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Async task to load Arrow data into ClickHouse.
    
    Args:
    ---
        arrow_data_bytes: Serialized Arrow table data
        table_name: Target table name
        clickhouse_config: ClickHouse connection parameters
        
    Returns:
    ---
        Dict with load results
    """
    try:
        logger.info(f"Starting data load to table: {table_name}")
        
        self.update_state(
            state='PROGRESS',
            meta={'status': 'Deserializing Arrow data'}
        )
        
        # Deserialize Arrow data
        arrow_table = pa.ipc.open_stream(arrow_data_bytes).read_all()
        
        self.update_state(
            state='PROGRESS',
            meta={'status': f'Loading {arrow_table.num_rows} rows to ClickHouse'}
        )
        
        # Load to ClickHouse
        clickhouse_config = clickhouse_config or {}
        success = load_to_clickhouse(
            arrow_table, 
            table_name,
            **clickhouse_config
        )
        
        if success:
            logger.info(f"Successfully loaded data to {table_name}")
            return {
                'status': 'SUCCESS',
                'table_name': table_name,
                'rows_loaded': arrow_table.num_rows
            }
        else:
            raise Exception("Load operation returned False")
            
    except Exception as exc:
        logger.error(f"Data loading failed: {str(exc)}")
        
        countdown = 2 ** self.request.retries
        self.retry(countdown=countdown, exc=exc)


@shared_task(bind=True)
def full_pipeline_task(self, source_id: int) -> Dict[str, Any]:
    """
    Complete pipeline task: Extract -> Transform -> Load.
    
    Args:
    ---
        source_id: ID of the DataSource model
        
    Returns:
    ---
        Dict with pipeline results
    """
    job = None
    
    try:
        # Get source configuration
        data_source = DataSource.objects.get(id=source_id)
        
        # Create import job record
        job = ImportJob.objects.create(
            data_source=data_source,
            status='running',
            started_at=timezone.now()
        )
        
        logger.info(f"Starting full pipeline for source: {data_source.name}")
        
        self.update_state(
            state='PROGRESS',
            meta={'status': 'Starting extraction phase'}
        )
        
        # Phase 1: Extract
        source_config = data_source.connection_config
        arrow_table = extract_to_arrow(source_config)
        
        self.update_state(
            state='PROGRESS',
            meta={'status': 'Starting load phase'}
        )
        
        # Phase 2: Load
        table_name = f"{data_source.name}_{job.id}"
        success = load_to_clickhouse(arrow_table, table_name)
        
        if success:
            # Update job status
            job.status = 'success'
            job.rows_processed = arrow_table.num_rows
            job.completed_at = timezone.now()
            job.save()
            
            logger.info(f"Pipeline completed successfully for {data_source.name}")
            
            return {
                'status': 'SUCCESS',
                'job_id': job.id,
                'rows_processed': arrow_table.num_rows,
                'table_name': table_name
            }
        else:
            raise Exception("Pipeline failed during load phase")
            
    except Exception as exc:
        logger.error(f"Pipeline failed: {str(exc)}")
        
        if job:
            job.status = 'failed'
            job.completed_at = timezone.now()
            job.save()
        
        raise


@shared_task
def cleanup_old_jobs():
    """
    Periodic task to clean up old import jobs.
    """
    from datetime import timedelta
    
    cutoff_date = timezone.now() - timedelta(days=30)
    old_jobs = ImportJob.objects.filter(completed_at__lt=cutoff_date)
    
    deleted_count = old_jobs.count()
    old_jobs.delete()
    
    logger.info(f"Cleaned up {deleted_count} old import jobs")
    return {'deleted_jobs': deleted_count}


@shared_task
def discover_schema_task(source_config: Dict[str, Any], table_name: str):
    """
    Async task to discover and cache table schema.
    """
    try:
        schema = get_table_schema(source_config, table_name)
        logger.info(f"Discovered schema for {table_name}: {len(schema)} columns")
        return schema
        
    except Exception as exc:
        logger.error(f"Schema discovery failed: {str(exc)}")
        raise