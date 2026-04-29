import logging
import time
import random
from curl_cffi import requests

logger = logging.getLogger(__name__)


class NetworkService:
    """Handles HTTP requests and evades DataDome bot protection."""

    def __init__(self):
        self.session = requests.Session(impersonate="chrome120")

    def _request(self, method: str, url: str, max_retries: int = 3, delay_range: tuple = (6.0, 10.0), **kwargs):
        """Internal method to handle HTTP requests, proactive delays, bot detection, and retries."""

        response = None

        for attempt in range(1, max_retries + 1):
            # 1. Proactive human-like delay before every request
            delay = random.uniform(delay_range[0], delay_range[1])
            logger.info(f"Delaying {method} request by {delay:.2f} seconds... (Attempt {attempt}/{max_retries})")
            time.sleep(delay)

            try:
                response = self.session.request(method, url, **kwargs)

                # 2. Reactive DataDome Block Handling
                if response.status_code in [403, 405, 429]:
                    page_num = None
                    for key in ['params', 'json', 'data']:
                        payload = kwargs.get(key)
                        if isinstance(payload, dict) and "page" in payload:
                            page_num = payload["page"]
                            break

                    page_info = f" on page {page_num}" if page_num else ""
                    cooldown = random.uniform(600.0, 660.0)

                    logger.warning(
                        f"DATADOME BLOCK on {method}{page_info} (URL: {url}). Sleeping {cooldown / 60:.2f}min...")
                    time.sleep(cooldown)

                    self.rotate_session()
                    continue  # Loop around and retry with the new session

                # 3. If successful (or a normal 404), return the response immediately
                return response

            except Exception as e:
                logger.error(f"Network error on {method} {url}: {e}")
                if attempt == max_retries:
                    raise  # Re-raise the exception if we're completely out of retries

        # Fallback if we exhaust all retries and never successfully returned inside the loop
        return response

    def get(self, url: str,delay_range: tuple = (6.0, 10.0), **kwargs):
        """Sends a GET request with automatic DataDome handling."""
        kwargs.setdefault('timeout', 15)
        return self._request("GET", url, delay_range=delay_range, **kwargs)

    def post(self, url: str,delay_range: tuple = (6.0, 10.0), **kwargs):
        """Sends a POST request with automatic DataDome handling."""
        kwargs.setdefault('timeout', 15)
        return self._request("POST", url, delay_range=delay_range, **kwargs)

    def rotate_session(self):
        """Drops the current session cookies and generates a fresh browser fingerprint."""
        logger.info("\n[ANTI-BOT] Rotating session to clear velocity history...")

        if self.session:
            self.session.close()

        # Take a long breather to reset the IP trust score
        cooldown = random.uniform(35.0, 60.0)
        logger.info(f"[ANTI-BOT] IP cooling down for {cooldown:.2f} seconds...")
        time.sleep(cooldown)

        # Start a brand new session with a modern browser profile
        self.session = requests.Session(impersonate="chrome120")
        logger.info("[ANTI-BOT] New session acquired. Resuming scrape...\n")