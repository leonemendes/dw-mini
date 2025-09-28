from rest_framework import generics
from .models import Event
from .serializers import EventSerializer
from .redis_client import redis_client
import json

class EventListCreateView(generics.ListCreateAPIView):
    queryset = Event.objects.all().order_by("-timestamp")
    serializer_class = EventSerializer

    def perform_create(self, serializer):
        event_data = serializer.validated_data
        redis_client.lpush("events_queue", json.dumps(event_data, default=str))