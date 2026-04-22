"""
Tests for the WhatsApp notification service (utils/whatsapp.py).

Run from the backend directory:
    python test_whatsapp.py

No external calls are made — all HTTP is intercepted with unittest.mock.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch, call
from fastapi import BackgroundTasks

sys.path.insert(0, os.path.dirname(__file__))

from app.utils.whatsapp import _build_message, send_whatsapp_notification


# ---------------------------------------------------------------------------
# _build_message tests
# ---------------------------------------------------------------------------

class TestBuildMessage(unittest.TestCase):

    def test_contains_module_and_status(self):
        msg = _build_message("BC", "SUBMITTED - PENDING L1", {})
        self.assertIn("BC", msg)
        self.assertIn("SUBMITTED - PENDING L1", msg)

    def test_known_detail_fields_appear(self):
        details = {
            "id": "BC-2025-001",
            "project": "Rabat-Nord",
            "pm": "Ahmed Alaoui",
            "total": "50,000.00 MAD",
        }
        msg = _build_message("BC", "FULLY APPROVED", details)
        self.assertIn("BC-2025-001", msg)
        self.assertIn("Rabat-Nord", msg)
        self.assertIn("Ahmed Alaoui", msg)
        self.assertIn("50,000.00 MAD", msg)

    def test_none_detail_fields_are_skipped(self):
        details = {"id": "EXP-001", "pm": None, "project": ""}
        msg = _build_message("EXP", "APPROVED", details)
        self.assertIn("EXP-001", msg)
        # None and empty string should not produce a label line
        self.assertNotIn("Project Manager:", msg)
        self.assertNotIn("Project:", msg)

    def test_link_appended_with_base_url(self):
        with patch.dict(os.environ, {"FRONTEND_URL": "https://po.sib.co.ma"}):
            msg = _build_message("BC", "APPROVED", {}, link="/configuration/bc/detail/42")
        self.assertIn("https://po.sib.co.ma/configuration/bc/detail/42", msg)

    def test_link_uses_default_when_env_missing(self):
        env = {k: v for k, v in os.environ.items() if k != "FRONTEND_URL"}
        with patch.dict(os.environ, env, clear=True):
            msg = _build_message("BC", "APPROVED", {}, link="/some/path")
        self.assertIn("https://po.sib.co.ma/some/path", msg)

    def test_no_link_produces_no_portal_line(self):
        msg = _build_message("BC", "APPROVED", {})
        self.assertNotIn("Portal:", msg)

    def test_remark_field_appears(self):
        msg = _build_message("EXP", "REJECTED", {"remark": "Missing invoice"})
        self.assertIn("Missing invoice", msg)


# ---------------------------------------------------------------------------
# send_whatsapp_notification tests
# ---------------------------------------------------------------------------

class TestSendWhatsappNotification(unittest.TestCase):

    def _make_bg(self):
        bg = MagicMock(spec=BackgroundTasks)
        return bg

    def test_no_api_key_skips_send(self):
        env = {k: v for k, v in os.environ.items() if k != "WASENDER_API_KEY"}
        with patch.dict(os.environ, env, clear=True):
            bg = self._make_bg()
            send_whatsapp_notification(["+212600000001"], "BC", "TEST", {}, bg)
        bg.add_task.assert_not_called()

    def test_empty_number_list_skips_send(self):
        with patch.dict(os.environ, {"WASENDER_API_KEY": "fake-key"}):
            bg = self._make_bg()
            send_whatsapp_notification([], "BC", "TEST", {}, bg)
        bg.add_task.assert_not_called()

    def test_none_in_number_list_skips_send(self):
        with patch.dict(os.environ, {"WASENDER_API_KEY": "fake-key"}):
            bg = self._make_bg()
            send_whatsapp_notification([None, None], "BC", "TEST", {}, bg)
        bg.add_task.assert_not_called()

    def test_task_is_enqueued_with_valid_input(self):
        with patch.dict(os.environ, {"WASENDER_API_KEY": "fake-key"}):
            bg = self._make_bg()
            send_whatsapp_notification(["+212600000001"], "BC", "SUBMITTED", {}, bg)
        bg.add_task.assert_called_once()

    def _run_enqueued_task(self, bg: BackgroundTasks):
        """Execute the background task synchronously (for testing)."""
        task = bg.tasks[0]
        task.func(*task.args, **task.kwargs)

    def test_http_post_called_per_number(self):
        with patch.dict(os.environ, {"WASENDER_API_KEY": "fake-key"}):
            bg = BackgroundTasks()
            send_whatsapp_notification(
                ["+212600000001", "+212600000002"],
                "EXP", "APPROVED", {"id": "EXP-01"}, bg
            )
            with patch("httpx.Client") as mock_client_cls:
                mock_client = MagicMock()
                mock_client_cls.return_value.__enter__.return_value = mock_client
                self._run_enqueued_task(bg)

        self.assertEqual(mock_client.post.call_count, 2)
        numbers_sent = [c.kwargs["json"]["to"] for c in mock_client.post.call_args_list]
        self.assertIn("+212600000001", numbers_sent)
        self.assertIn("+212600000002", numbers_sent)

    def test_correct_bearer_header_sent(self):
        with patch.dict(os.environ, {"WASENDER_API_KEY": "test-secret-key"}):
            bg = BackgroundTasks()
            send_whatsapp_notification(["+212600000001"], "BC", "APPROVED", {}, bg)
            with patch("httpx.Client") as mock_client_cls:
                mock_client = MagicMock()
                mock_client_cls.return_value.__enter__.return_value = mock_client
                self._run_enqueued_task(bg)

        _, kwargs = mock_client.post.call_args
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer test-secret-key")

    def test_empty_strings_in_number_list_are_skipped(self):
        with patch.dict(os.environ, {"WASENDER_API_KEY": "fake-key"}):
            bg = BackgroundTasks()
            send_whatsapp_notification(["", "+212600000001", ""], "BC", "APPROVED", {}, bg)
            with patch("httpx.Client") as mock_client_cls:
                mock_client = MagicMock()
                mock_client_cls.return_value.__enter__.return_value = mock_client
                self._run_enqueued_task(bg)

        self.assertEqual(mock_client.post.call_count, 1)
        _, kwargs = mock_client.post.call_args
        self.assertEqual(kwargs["json"]["to"], "+212600000001")

    def test_message_body_contains_status(self):
        with patch.dict(os.environ, {"WASENDER_API_KEY": "fake-key"}):
            bg = BackgroundTasks()
            send_whatsapp_notification(["+212600000001"], "CAISSE", "PD VALIDATED", {"id": "REQ-001"}, bg)
            with patch("httpx.Client") as mock_client_cls:
                mock_client = MagicMock()
                mock_client_cls.return_value.__enter__.return_value = mock_client
                self._run_enqueued_task(bg)
                body = mock_client.post.call_args.kwargs["json"]["text"]

        self.assertIn("PD VALIDATED", body)
        self.assertIn("REQ-001", body)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestBuildMessage))
    suite.addTests(loader.loadTestsFromTestCase(TestSendWhatsappNotification))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
