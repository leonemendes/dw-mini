from django.test import TestCase
from core.models import Event

class EventModelTest(TestCase):
    def test_create_event(self):
        event = Event.objects.create(
            name="test_event",
            user_id=123,
            properties={"action": "click"}
        )
        self.assertIsNotNone(event.id)  # foi salvo no banco
        self.assertEqual(event.name, "test_event")
        self.assertEqual(event.user_id, 123)
        self.assertIsNotNone(event.timestamp)  # timestamp gerado automaticamente
