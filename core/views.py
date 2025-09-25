from rest_framework import generics
from .models import Event
from .serializers import EventSerializer

class EventListCreateView(generics.ListCreateAPIView):
    queryset = Event.objects.all().order_by("-timestamp")
    serializer_class = EventSerializer