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

import psycopg
import redis
import requests

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

class GitHubClient:
  def __init__(self, token: str, graphql_url: str, timeout: float, rate_limiter: RateLimiter):
    self.session = requests.Session()
    self.session.headers.update({
      "Authorization": f"Bearer {token}",
      "Accept": "application/json",
      "Content-Type": "application/json",
      "User-Agent": "queue-feeder/1.0",
    })
    self.graphql_url = graphql_url
    self.timeout = timeout
    self.rate_limiter = rate_limiter

  def graphql(self, query: str, variables: dict) -> dict:
    """ Sends a GraphQL query, respecting spacing and limits. Returns JSON.
        Throws GitHubError on HTTP/GraphQL errors."""
    
    self.rate_limiter.wait_before_request()

    try:
      resp = self.session.post(
        self.graphql_url,
        data=json.dumps({"query": query, "variables": variables}),
        timeout=self.timeout,
      )
    except requests.RequestException as e:
      raise GitHubError(f"Network error: {e}") from e
    finally:
      # mark the execution of the request regardless of success - we keep the distance
      self.rate_limiter.mark_request_done()

    # Read the limit headers and possibly wait for a reset
    try:
      remaining = int(resp.headers.get("X-RateLimit-Remaining", "1"))
    except ValueError:
      remaining = None
    
    try:
      reset_unix = int(resp.headers.get("X-RateLimit-Reset", "0")) or None
    except ValueError:
      reset_unix = None

    if resp.status_code == 401:
      raise GitHubError("Unauthorized (401): invalid or expired token")

    if resp.status_code == 403:
      #403 may indicate a secondary rate limit - take a break to reset
      self.rate_limiter.wait_until_reset_if_needed(remaining, reset_unix)
      raise GitHubError("Forbidden (403): possibly secondary rate limit")
    
    if resp.status_code >= 400:
      raise GitHubError(f"HTTP {resp.status_code}: {resp.text[:500]}")
    
    try:
      payload = resp.json()
    except ValueError as e:
      raise GitHubError(f"Invalid JSON: {e}") from e
    
    if "errors" in payload and payload["errors"]:
      msg = payload["errors"][0].get("message", "Unknow GraphQL error")
      # If limits errors - wait
      self.rate_limiter.wait_until_reset_if_needed(remaining, reset_unix)
      raise GitHubError(f"GraphQL error: {msg}")
    
    # If you have reached your limits, please wait until the reset occurs before we refund your money.
    self.rate_limiter.wait_until_reset_if_needed(remaining, reset_unix)
    return payload
  
  def validate_token(self, ratelimit_url: str) -> None:
    try:
      r = self.session.get(ratelimit_url, timeout=SETTINGS.REQUEST_TIMEOUT_SECONDS)
    except requests.RequestException as e:
      raise GitHubError(f"Cannot reach GitHub API: {e}") from e
    
    if r.status_code == 401:
      raise GitHubError("Invalid or expired GitHub token (401)")
    
    if r.status_code >= 400:
      raise GitHubError(f"GitHub rate_limit error {r.status_code}: {r.text}")
    
    data = r.json()
    rem = data.get("resources", {}).get("graphql", {}).get("remaining")
    reset = data.get("resources", {}).get("graphql", {}).get("reset")
    logger.info("GitHub token OK — remaining=%s, reset_unix=%s", rem, reset)

# ----------------------------------------------------------------------------
# SQL — Progress
# ----------------------------------------------------------------------------

CREATE_POSTGRES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS scraper_progress (
  id INT PRIMARY KEY,
  current_login TEXT,
  current_mode TEXT,
  current_cursor TEXT,
  last_update TIMESTAMPTZ DEFAULT NOW()
);
"""

def ensure_progress_table():
  with psycopg.connect(SETTINGS.POSTGRES_DSN) as conn:
    with conn.cursor() as cur:
      cur.execute(CREATE_POSTGRES_TABLE_SQL)
      conn.commit()

def save_progress(login: Optional[str], mode: Optional[str], cursor: Optional[str]) -> None:
  """ Saves (overwrites) the only progress record with id=1 """
  with psycopg.connect(SETTINGS.POSTGRES_DSN) as conn:
    with conn.cursor() as cur:
      cur.execute(
        """
        INSERT INTO scraper_progress (id, current_login, current_mode, current_cursor, last_update)
        VALUES (1, %s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE
        SET current_login = EXCLUDED.current_login,
        current_mode = EXCLUDED.current_mode,
        current_cursor = EXCLUDED.current_cursor,
        last_update = NOW();
        """,
        (login, mode, cursor),
      )
      conn.commit()

def load_progress() -> Optional[tuple[str, Optional[str], Optional[str]]]:
  """ Returns (login, mode, cursor) or None if missing """
  with psycopg.connect(SETTINGS.POSTGRES_DSN) as conn:
    with conn.cursor() as cur:
      cur.execute(
        "SELECT current_login, current_mode, current_cursor FROM scraper_progress WHERE id = 1;"
      )
      row = cur.fetchone()
      if row and row[0]:
        return row[0], row[1], row[2]
      return None
    
def claim_pending_user() -> Optional[str]:
  """ Takes the oldest `pending` and marks it as `in_progress` atomically. Returns login or None if none """
  with psycopg.connect(SETTINGS.POSTGRES_DSN) as conn:
    conn.execute("BEGIN")
    with conn.cursor() as cur:
      cur.execute(
        """
        SELECT login FROM users
        WHERE status = 'pending'
        ORDER BY created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1;
        """
      )
      row = cur.fetchone()
      if not row:
        conn.commit()
        return None
      login = row[0]
      cur.execute("UPDATE users SET status = 'in_progress' WHERE login = %s;", (login,))
      conn.commit()
      return login
    
def mark_user_status(login: str, status: str) -> None:
  if status not in {"pending", "in_progress", "done", "failed"}:
    raise ValueError("Invalid status")
  with psycopg.connect(SETTINGS.POSTGRES_DSN) as conn:
    with conn.cursor() as cur:
      cur.execute("UPDATE users SET status = %s WHERE login = %s;", (status, login))
      conn.commit()

# ----------------------------------------------------------------------------
# REDIS — queue and deduplication
# ----------------------------------------------------------------------------

class Queue:
  def __init__(self, client: redis.Redis, list_name: str, seen_set: str):
    self.client = client
    self.list_name = list_name
    self.seen_set = seen_set

  def size(self) -> int:
    return int(self.client.llen(self.list_name))
  
  def _wait_for_space(self, batch_len: int) -> None:
    """ Waits until the queue drops below the threshold to push the batch without overflow """
    while True:
      qsize = self.size()
      if qsize + batch_len <= SETTINGS.MAX_QUEUE_SIZE:
        return
      
      # If too high, wait until it drops below the unlock level
      if qsize <= SETTINGS.ENQUEUE_BLOCK_UNTIL_BELOW:
        return
      
      logger.info("Queue=%s too big (>%s). Waiting...", qsize, SETTINGS.MAX_QUEUE_SIZE)
      time.sleep(2)

  def enqueue_unique(self, logins: Iterable[str]) -> int:
    """ Adds only new logins to the queue (based on Redis SET). Returns how many were added """
    normed = [l.strip().lower() for l in logins if l and l.strip()]
    if not normed:
      return 0
    
    # Add to SET and check which are new (SADD returns the number of new ones; but we want a list)
    new_items = []
    pipe = self.client.pipeline()
    for l in normed:
      pipe.sadd(self.seen_set, l)

    sadd_results = pipe.execute()
    for l, added in zip(normed, sadd_results):
      if int(added) == 1:
        new_items.append(1)

    if not new_items:
      return 0
    
    self._wait_for_space(len(new_items))
    self.client.rpush(self.list_name, *new_items)
    logger.info("Enqueued %s new logins. Queue size=%s", len(new_items), self.size())
    return len(new_items)



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