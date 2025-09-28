from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from core.models import Event

class EventModelTest(TestCase):
    def test_create_event(self):
        """Test basic event creation"""
        event = Event.objects.create(
            name="test_event",
            user_id=123,
            properties={"action": "click"}
        )
        self.assertIsNotNone(event.id)  # foi salvo no banco
        self.assertEqual(event.name, "test_event")
        self.assertEqual(event.user_id, 123)
        self.assertIsNotNone(event.timestamp)  # timestamp gerado automaticamente

class EventAPITest(APITestCase):
    def test_create_event_via_api(self):
        """Test API event creation"""
        url = reverse('event-list-create')
        payload = {
            "name": "TEST",
            "user_id": 23,
            "properties": {"source": "core_test"}
        }

        response = self.client.post(url, payload, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Event.objects.count(), 1)
        ev = Event.objects.first()
        self.assertEqual(ev.name, "TEST")
        self.assertEqual(ev.user_id, 23)

    def test_list_events(self):
        """Test sequential events creation"""
        Event.objects.create(name="TEST", user_id=23, properties={"source": "core_test"})
        Event.objects.create(name="TEST", user_id=23, properties={"source": "core_test"})
        url = reverse('event-list-create')
        response = self.client.get(url, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.json()), 2)
