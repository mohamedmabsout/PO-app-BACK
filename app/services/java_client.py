import requests
import logging
from typing import Dict, Optional

from ..config import settings

logger = logging.getLogger(__name__)


class JavaApiClient:
    def __init__(self):
        self.base_url = settings.java_api_base_url.rstrip("/")
        self.username = settings.java_api_username
        self.password = settings.java_api_password
        self.token = None

    def _authenticate(self) -> bool:
        """Authenticates with Java WebApp and stores the JWT token."""
        try:
            url = f"{self.base_url}/api/authenticate"
            payload = {"username": self.username, "password": self.password}
            response = requests.post(url, json=payload, timeout=10)

            if response.status_code == 200:
                self.token = response.json().get("id_token") or response.json().get("jwtToken")
                if not self.token:
                    logger.error(f"Auth succeeded but no token key found in response: {response.json().keys()}")
                    return False
                return True
            else:
                logger.error(f"Java API Auth Failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Failed to connect to Java API: {str(e)}")
            return False

    def get_monthly_closing_data(self, year: int, month: int) -> Optional[Dict]:
        """
        Fetches the grouped Labor and Expense data for the specified month.
        Endpoint: GET /api/erp-integration/monthly-closing-data/{year}/{month}
        """
        if not self.token and not self._authenticate():
            raise ConnectionError("Cannot authenticate with Java WebApp.")

        url = f"{self.base_url}/api/erp-integration/monthly-closing-data/{year}/{month}"
        headers = {"Authorization": f"Bearer {self.token}"}

        try:
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 401:  # Token expired, retry once
                if not self._authenticate():
                    raise ConnectionError("Re-authentication failed.")
                headers = {"Authorization": f"Bearer {self.token}"}
                response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to fetch closing data: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            logger.error(f"Error fetching from Java: {str(e)}")
            return None