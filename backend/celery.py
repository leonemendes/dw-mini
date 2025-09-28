"""
Celery configuration for Django project.
"""
import os
from celery import Celery
from django.conf import settings

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')

app = Celery('dw_mini')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

# Celery configuration
app.conf.update(
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    
    # Task routing - different queues for different types of work
    task_routes={
        'data_pipeline.tasks.extract_data_task': {'queue': 'extraction'},
        'data_pipeline.tasks.load_data_task': {'queue': 'loading'},
        'data_pipeline.tasks.full_pipeline_task': {'queue': 'pipeline'},
    },
    
    # Task result expiration
    result_expires=3600,
    
    # Worker settings
    worker_prefetch_multiplier=1,
    task_acks_late=True,
)