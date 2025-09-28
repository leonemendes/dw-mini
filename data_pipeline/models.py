from django.db import models

class DataSource(models.Model):
    """Configuration for data sources"""
    name = models.CharField(max_length=100)
    source_type = models.CharField(max_length=50)  # postgresql, mysql, etc
    connection_config = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)

class ImportJob(models.Model):
    """Track data import jobs"""
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'), 
        ('success', 'Success'),
        ('failed', 'Failed')
    ]
    
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES)
    rows_processed = models.IntegerField(default=0)
    started_at = models.DateTimeField(null=True)
    completed_at = models.DateTimeField(null=True)