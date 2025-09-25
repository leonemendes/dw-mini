from django.db import models
from django.utils import timezone

class Event(models.Model):
    name = models.CharField(max_length=255)
    user_id = models.IntegerField()
    timestamp = models.DateTimeField(default=timezone.now)
    properties = models.JSONField(default=dict)

    def __str__(self):
        return f"{self.name} by {self.user_id} at {self.timestamp}"

