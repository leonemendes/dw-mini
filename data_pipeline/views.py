# data_pipeline/views.py
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .tasks import full_pipeline_task

@api_view(['POST'])
def start_pipeline(request):
    """Start data pipeline asynchronously"""
    source_id = request.data.get('source_id')
    
    # Queue the task
    task = full_pipeline_task.delay(source_id)
    
    return Response({
        'task_id': task.id,
        'status': 'queued'
    })

@api_view(['GET']) 
def task_status(request, task_id):
    """Check task status"""
    from celery.result import AsyncResult
    
    result = AsyncResult(task_id)
    
    return Response({
        'task_id': task_id,
        'status': result.status,
        'result': result.result
    })