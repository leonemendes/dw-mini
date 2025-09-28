from rest_framework import generics
from .models import Event
from .serializers import EventSerializer

class EventListCreateView(generics.ListCreateAPIView):
    """Standard CRUD operations for events"""
    queryset = Event.objects.all().order_by("-timestamp")
    serializer_class = EventSerializer

class EventDetailView(generics.RetrieveDestroyAPIView):
    """Read/Delete single instance endpoints"""
    queryset = Event.objects.all().order_by("-timestamp")
    serializer_class = EventSerializer