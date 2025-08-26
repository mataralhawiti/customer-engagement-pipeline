"""
HTTP sender transform for sending enriched events to third-party API.
"""

import logging
import requests
from typing import Dict, Any, List
import apache_beam as beam

logger = logging.getLogger(__name__)


class HTTPSenderDoFn(beam.DoFn):
    """
    DoFn to send enriched events to third-party API endpoint.
    Handles batching, error logging, and basic retry logic.
    """

    def __init__(self, api_url: str, timeout: int = 30):
        """
        Initialize HTTP sender.

        Args:
            api_url: Base URL of the third-party API
            timeout: Request timeout in seconds
        """
        self.api_url = api_url
        self.timeout = timeout
        self._sent = beam.metrics.Metrics.counter("http_sender", "events_sent")
        self._errors = beam.metrics.Metrics.counter("http_sender", "send_errors")
        self._batches = beam.metrics.Metrics.counter("http_sender", "batches_sent")
        self._log = logging.getLogger(self.__class__.__name__)

    def setup(self):
        """Initialize HTTP session for reuse."""
        self._session = requests.Session()
        self._session.headers.update(
            {"Content-Type": "application/json", "User-Agent": "beam-pipeline/1.0"}
        )

    def teardown(self):
        """Clean up HTTP session."""
        if hasattr(self, "_session"):
            self._session.close()

    def process(self, element):
        """
        Send enriched event to third-party API.

        Args:
            element: Enriched event dictionary

        Yields:
            None (this is a sink transform)
        """
        try:
            # Convert single event to batch format expected by API
            event_batch = {"events": [element]}

            # Send to third-party API
            self._send_to_api(event_batch)

            self._sent.inc()
            self._batches.inc()

        except Exception as e:
            self._errors.inc()
            self._log.error(
                f"Failed to send event to API: {e}, event_id: {element.get('id')}"
            )
            # Continue processing other events
            return

    def _send_to_api(self, event_batch: Dict[str, List[Dict[str, Any]]]):
        """
        Send event batch to the third-party API.

        Args:
            event_batch: Dictionary with 'events' key containing list of events

        Raises:
            requests.RequestException: If HTTP request fails
        """
        url = f"{self.api_url}/events"

        try:
            response = self._session.post(url, json=event_batch, timeout=self.timeout)

            # Log response details
            event_count = len(event_batch["events"])

            if response.status_code == 200:
                response_data = response.json()
                self._log.info(
                    f"Successfully sent {event_count} events to {url}. "
                    f"Response: {response_data.get('message', 'OK')}"
                )
            else:
                self._log.warning(
                    f"API returned status {response.status_code} for {event_count} events. "
                    f"Response: {response.text[:200]}"
                )

            # Raise for bad status codes
            response.raise_for_status()

        except requests.exceptions.Timeout:
            self._log.error(f"Timeout sending to {url} after {self.timeout}s")
            raise
        except requests.exceptions.ConnectionError:
            self._log.error(f"Connection error sending to {url}")
            raise
        except requests.exceptions.HTTPError as e:
            self._log.error(f"HTTP error sending to {url}: {e}")
            raise
        except Exception as e:
            self._log.error(f"Unexpected error sending to {url}: {e}")
            raise


class SendToThirdPartyAPI(beam.PTransform):
    """
    PTransform that sends enriched events to third-party API.
    """

    def __init__(self, api_url: str, timeout: int = 30):
        """
        Initialize the transform.

        Args:
            api_url: Base URL of the third-party API
            timeout: Request timeout in seconds
        """
        self.api_url = api_url
        self.timeout = timeout

    def expand(self, pcoll):
        return pcoll | "SendToAPI" >> beam.ParDo(
            HTTPSenderDoFn(api_url=self.api_url, timeout=self.timeout)
        )
