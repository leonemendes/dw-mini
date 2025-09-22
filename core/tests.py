from django.test import TestCase
from core.models import UserEvent

class UserEventModelTest(TestCase):
    def test_create_event(self):
        event = UserEvent.objects.create(
            event_name="page_view",
            user_id=123
        )
        self.assertIsNotNone(event.id)  # foi salvo no banco
        self.assertEqual(event.event_name, "page_view")
        self.assertEqual(event.user_id, 123)
        self.assertIsNotNone(event.timestamp)  # timestamp gerado automaticamente

    def test_str_representation(self):
        event = UserEvent.objects.create(event_name="login", user_id=456)
        self.assertEqual(str(event), "login (user 456)")
