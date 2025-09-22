from django.db import models

class UserEvent(models.Model):
    event_name = models.CharField(max_length=100)
    user_id = models.IntegerField()
    timestamp = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.event_name} (user {self.user_id})"
