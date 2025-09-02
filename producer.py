""""

What does this script do?

- Fetches a single user with status 'pending' from Postgres and marks them as 'in_progress'
- Calls GitHub GraphQL, retrieves followers/following (with pagination), and pushes their logins into a Redis queue
- Enforces a fixed delay between requests to GitHub, respects rate limits, and can wait until reset if necessary
- Deduplicates logins globally (Redis SET), so the queue doesn't bloat with duplicates
- Stores pagination progress in the 'scraper_progress' table to resume exactly where it left off after a restart
- Logs events in a readable way (without leaking secrets)

"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional
from datetime import datetime, timezone

import json
import logging
import os
import socket
import sys
import time

@dataclass
class Settings:
  # Postgres
  POSTGRES_DSN: str = os.getenv(
    "POSTGRES_DSN",
    "postgresql://postgres:postgres@localhost:5432/postgres",
  )

  # Redis
  REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
  REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
  REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))

  # GitHub API
  GITHUB_TOKEN: Optional[str] = os.getenv("GITHUB_API_TOKEN")
  GITHUB_GRAPHQL_URL: str = os.getenv("GITHUB_API_ENDPOINT", "https://api.github.com/graphql")
  GITHUB_RATELIMIT_URL: str = os.getenv("GITHUB_RATE_LIMIT_ENDPOINT", "https://api.github.com/rate_limit")

  MIN_SECONDS_BETWEEN_REQUESTS: float = float(os.getenv("MIN_SECONDS_BETWEEN_REQUESTS", "1.9"))
  REQUEST_TIMEOUT_SECONDS: float = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
  RESET_SAFETY_MARGIN_SEC: int = int(os.getenv("RESET_SAFETY_MARGIN_SEC", "2"))

  # Queue and deduplication
  QUEUE_NAME: str = os.getenv("QUEUE_NAME", "github_logins_queue")
  SEEN_SET: str = os.getenv("SEEN_SET", "github_seen_logins")
  MAX_QUEUE_SIZE: int = int(os.getenv("MAX_QUEUE_SIZE", "8000"))
  ENQUEUE_BLOCK_UNTIL_BELOW: int = int(os.getenv("ENQUEUE_BLOCK_UNTIL_BELOW", "40"))

  # Start login if no pending in DB
  INITIAL_PROFILE_LOGIN: str = os.getenv("INITIAL_PROFILE_LOGIN", "jakubgania")

  # Health check services
  CHECK_SERVICES: bool = os.getenv("CHECK_SERVICES", "1") == "1"
  SERVICES: tuple[tuple[str, str, int], ...] = (
    ("postgres", os.getenv("PG_HOST", "localhost"), int(os.getenv("PG_PORT", "5432"))),
    ("redis", os.getenv("REDIS_HOST", "localhost"), int(os.getenv("REDIS_PORT", "6379"))),
  )

  LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

SETTINGS = Settings()

# ----------------------------------------------------------------------------
# LOGS
# ----------------------------------------------------------------------------

def setup_logging() -> None:
  level = getattr(logging, SETTINGS.LOG_LEVEL.upper(), logging.INFO)
  logging.basicConfig(
    level=level,
    format="%(asctime)sZ | %(levelname)-8s | %(message)s",
  )

  logging.Formatter.converter = time.gmtime

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------------

def format_duration(seconds: float) -> str:
  if seconds < 60:
    return f"{seconds:.2f}s"
  if seconds < 3600:
    m = seconds / 60
    return f"{m:.2f}m"
  h = seconds / 3600
  return f"{h:.2f}h"

def now_utc() -> datetime:
  return datetime.now(timezone.utc)

# ----------------------------------------------------------------------------
# GITHUB — client + rate limiting with intervals between requests
# ----------------------------------------------------------------------------

class GitHubError(RuntimeError):
  pass

class RateLimiter:
  """" Maintains the minimum interval between requests and respects the limit reset """

  def __init__(self, min_interval: float):
    self.min_interval = max(0.0, float(min_interval))
    self._last_request_ts: float | None = None
  
  def wait_before_request(self) -> None:
    """  """
    if self._last_request_ts is None:
      return
    elasped = time.time() - self._last_request_ts
    remaining = self.min_interval - elasped
    if remaining > 0:
      time.sleep(remaining)

  def mark_request_done(self) -> None:
    self._last_request_ts = time.time()

  def wait_until_reset_if_needed(self, remaining: Optional[int], reset_unix: Optional[int]) -> None:
    if remaining is None or reset_unix is None:
      return
    if int(remaining) > 0:
      return
    sleep_for = max(0, int(reset_unix) - int(time.time())) + SETTINGS.RESET_SAFETY_MARGIN_SEC
    if sleep_for > 0:
      logger.warning("Rate limit exhausted. Sleeping until reset: %ss (at %s)", sleep_for, datetime.fromtimestamp(int(reset_unix)))
      time.sleep(sleep_for)


# ----------------------------------------------------------------------------
# MAIN LOOP
# ----------------------------------------------------------------------------

def main():
  setup_logging()
  print("start")

  if not SETTINGS.GITHUB_TOKEN:
    logger.error("GITHUB_API_TOKEN not set. Export and retry.")
    logger.error("Set it e.g. `export GITHUB_API_TOKEN=your_token` and try again.")
    sys.exit(1)

if __name__ == "__main__":
  main()