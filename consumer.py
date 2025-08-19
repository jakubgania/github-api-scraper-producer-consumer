"""
GitHub user consumer with:
- robust cofing via env vars
- proper logging
- persistent DB connection (with auto-reconnect) and schema init
- UPSERT without pre-check
- requests.Session with User-Agent, timeouts, retries & rate-limit handling
- dead-letter table to record failures (username, error type, status, details, timestap)
- graceful shutdown (SIGINT/SIGTERM)

Environment variables:
  POSTGRES_DSN             (default: postgresql://postgres:postgres@localhost:5432/postgres)
  REDIS_HOST               (default: localhost)
  REDIS_PORT               (default: 6379)
  REDIS_DB                 (default: 0)
  REDIS_QUEUE_NAME         (default: github_logins_queue)
  GITHUB_API_TOKEN         (required)
  LOG_LEVEL                (default: INFO)
  HTTP_CONNECT_TIMEOUT_S   (default: 3)
  HTTP_READ_TIMEOUT_S      (default: 10)
  HTTP_MAX_RETRIES         (default: 3)

Usage:
  uv run consumer.py
"""

from __future__ import annotations

import logging
import json
import time
import sys
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import psycopg
import redis
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# -------------------- Configuration --------------------

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/postgres"
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
QUEUE_NAME = os.getenv("REDIS_QUEUE_NAME", "github_logins_queue")

GITHUB_API_ENDPOINT = "https://api.github.com/graphql"
GITHUB_API_TOKEN = os.environ.get("GITHUB_API_TOKEN")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

HTTP_CONNECT_TIMEOUT_S = float(os.getenv("HTTP_CONNECT_TIMEOUT_S", "3"))
HTTP_READ_TIMEOUT_S = float(os.getenv("HTTP_READ_TIMEOUT_S", "10"))
HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "3"))

# BLPOP timeout in seconds (lets us periodically check for shutdown)
REDIS_BLPOP_TIMEOUT_S = 5

# Maximum seconds to wait for Postgres at start (0/negative for unlimited)
POSTGRES_WAIT_MAX_S = int(os.getenv("POSTGRES_WAIT_MAX_S", "120"))

# -------------------- Logging --------------------

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("consumer")

# -------------------- SQL --------------------

SQL_INIT = """
CREATE TABLE IF NOT EXISTS users (
  login TEXT PRIMARY KEY,
  name TEXT,
  bio TEXT,
  company TEXT,
  location TEXT,
  created_at TIMESTAMPTZ,
  followers_count INT,
  following_count INT,
  status TEXT DEFAULT 'pending'
);
CREATE INDEX IF NOT EXISTS idx_users_company ON users (company);
CREATE INDEX IF NOT EXISTS idx_users_location ON users (location);

-- Dead-letter table for failures
CREATE TABLE IF NOT EXISTS dead_letters (
    id BIGSERIAL PRIMARY KEY,
    username TEXT,
    error_type TEXT,
    http_status INT,
    error_detail JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);
"""

SQL_INSERT_USER = """
INSERT INTO users (login, name, bio, company, location, created_at, followers_count, following_count, status)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending')
ON CONFLICT (login) DO NOTHING
RETURNING login;
"""

SQL_DEAD_LETTER = """
INSERT INTO dead_letters (username, error_type, http_status, error_detail)
VALUES (%s, %s, %s, %s);
"""

def get_redis_connection() -> redis.Redis:
  return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def wait_for_postgres(dsn: str, max_wait_s: int) -> None:
  start = time.monotonic()
  attempt = 0
  backoff = 1.0

  while True:
    try:
      with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
          cur.execute("SELECT 1;")
          logger.info("✅ Postgres is ready")
          return
    except Exception as e:
      attempt += 1
      if max_wait_s > 0 and (time.monotonic() - start) > max_wait_s:
        logger.error("Postgres not available after %ss: %s", max_wait_s, e)
        raise
      logger.warning("Waiting for Postgres (attempt %d): %s", attempt, e)
      time.sleep(backoff)
      backoff = min(backoff * 2, 8)

def init_db(dsn: str) -> None:
  with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
      cur.execute(SQL_INIT)
      conn.commit()
  logger.info("✅ Database schema ensured")

def create_session() -> requests.Session:
    if not GITHUB_API_TOKEN:
        logger.critical("💥 GITHUB_API_TOKEN is not set")
        raise SystemExit(2)

    session = requests.Session()

    # Retries for transient errors
    retry = Retry(
        total=HTTP_MAX_RETRIES,
        connect=HTTP_MAX_RETRIES,
        read=HTTP_MAX_RETRIES,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("POST",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update(
        {
            "Authorization": f"Bearer {GITHUB_API_TOKEN}",
            "User-Agent": "github-consumer/1.0 (+https://example.local)",
            "Accept": "application/json",
        }
    )
    return session

# -------------------- GitHub API --------------------

# login - string
# name - String
# email - String
# bio - String
# company - String
# location - String
# createdAt - DateTime - An ISO-8601 encoded UTC date string
# isHireable - Boolean
# repositories -> totalCount - Int
# followers -> totalCount - Int
# following -> totalCount - Int
# socialAccounts -> SocialAccountEdge -> SocialAccount -
#                                                        displayName - String
#                                                        provider - SocialAccountProvider
#                                                        url - URI
# twitterUsername - String
# websiteUrl - URI

BASIC_USER_QUERY = """
query($username: String!) {
    user(login: $username) {
        login
        name
        email
        bio
        company
        location
        createdAt
        isHireable
        repositories {
          totalCount
        }
        followers {
            totalCount
        }
        following {
            totalCount
        }
        twitterUsername
        websiteUrl
    }
}
"""

def _respect_rate_limit(resp: requests.Response) -> None:
  # If we're out of rate limit, sleep until reset
  try:
    remaining = int(resp.headers.get("X-RateLimit-Remaining", "1"))
    if remaining <= 0:
      reset = int(resp.headers.get("X-RateLimit-Reset", "0"))
      now = int(time.time())
      sleep_for = max(0, reset - now) + 1
      logger.warning("🛑 Rate limit exhausted. Sleeping %ss until reset...", sleep_for)
      time.sleep(sleep_for)
  except Exception:
    # Be conservative if headers are missing/malformed
    pass

def fetch_github_user(session: requests.Session, username: str) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], int]:
  payload = {"query": BASIC_USER_QUERY, "variables": {"username": username}}
  try:
    response = session.post(
      GITHUB_API_ENDPOINT,
      json=payload,
      timeout=(HTTP_CONNECT_TIMEOUT_S, HTTP_READ_TIMEOUT_S),
    )
    status = response.status_code

     # Log rate limit headers
    logger.info(f"Rate Limit Remaining: {response.headers.get('X-RateLimit-Remaining')}")
    # logger.info(f"Rate Limit Reset: {response.headers.get('X-RateLimit-Reset')}")

    # Fetch and convert the reset timestamp
    reset_timestamp = int(response.headers.get('X-RateLimit-Reset'))
    current_time = int(time.time())  # current time in seconds since the epoch
    time_left = reset_timestamp - current_time  # Calculate the time left in seconds
    
    # Calculate minutes and seconds
    minutes_left = time_left // 60
    seconds_left = time_left % 60
    
    logger.info(f"Rate Limit Reset: {minutes_left} minutes, {seconds_left} seconds remaining")

    # Respect rate limit based on headers
    _respect_rate_limit(response)

    # Handle secondary rate limit / abuse detection
    if status == 403 and "rate limit" in response.text.lower():
        logger.warning("Secondary rate limit hit for %s; backing off", username)
        time.sleep(2)

    response.raise_for_status()
    data = response.json()

    if "errors" in data and data["errors"]:
        return None, data, status
    return data.get("data", {}).get("user"), data, status
  
  except requests.RequestException as e:
        # Return None with synthetic error payload
        err = {"exception": type(e).__name__, "message": str(e)}
        return None, err, getattr(e.response, "status_code", 0) or 0


# -------------------- Persistence --------------------

class DB:
  def __init__(self, dsn: str):
    self.dsn = dsn
    self.conn: Optional[psycopg.Connection] = None

  def connect(self) -> None:
    if self.conn is not None:
      try:
        self.conn.close()
      except Exception:
        pass
    self.conn = psycopg.connect(self.dsn, autocommit=False)

  def ensure_connection(self) -> None:
      if self.conn is None or self.conn.closed:
          self.connect()

  def insert_user(self, user: Dict[str, Any]) -> bool:
    self.ensure_connection()
    try:
      with self.conn.cursor() as cur:
        cur.execute(
          SQL_INSERT_USER,
          (
            user.get("login"),
            user.get("name"),
            user.get("bio"),
            user.get("company"),
            user.get("location"),
            user.get("createdAt"),
            (user.get("followers") or {}).get("totalCount"),
            (user.get("following") or {}).get("totalCount"),
          ),
        )
        inserted = cur.fetchone() is not None
      self.conn.commit()
      return inserted
    except Exception as e:
      self.conn.rollback()
      logger.exception("Failed to insert user %s: %s", user.get("login"), e)
      logger.exception("Failed to insert user %s: %s", user.get("login"), e)
      raise

  def record_dead_letter(
      self,
      username: str,
      error_type: str,
      http_status: int,
      error_detail: Dict[str, Any] | None = None,
  ) -> None:
    self.ensure_connection()
    try:
      with self.conn.cursor() as cur:
        cur.execute(
          SQL_DEAD_LETTER,
          (
            username,
            error_type,
            http_status,
            json.dumps(error_detail or {})
          ),
        )
      self.conn.commit()
    except Exception as e:
      self.conn.rollback()
      logger.exception("Failed to record dead-letter for %s: %s", username, e)
      raise
    
  def close(self) -> None:
    if self.conn is not None:
        try:
          self.conn.close()
        except Exception:
          pass

def process_username(db: DB, session: requests.Session, username: str) -> None:
  user, raw, status = fetch_github_user(session, username)

  if user is None:
    # Determine error type
    error_type = "unknown"
    if isinstance(raw, dict):
      # GraphQL errors present
      if "errors" in raw and raw["errors"]:
        # Concatenate error types/messages for reference
        first = raw["errors"][0]
        error_type = first.get("type") or first.get("message", "graphql_error")[:64]
      elif raw.get("exception"):
        error_type = raw["exception"]
      elif status == 404:
        error_type = "not_found"
      elif status == 403:
        error_type = "forbidden_or_rate_limited"
    logger.warning("No data for user %s (status %s; %s)", username, status, error_type)
    db.record_dead_letter(username=username, error_type=error_type, http_status=status, error_detail=raw if isinstance(raw, dict) else {})
    return
  
  # Insert user (UPSERT guards duplicates)
  try:
    inserted = db.insert_user(user)
    if inserted:
      logger.info("✅ Saved %s to DB", username)
    else:
      logger.info("⚠️ User %s already in DB, skipped", username)
  except Exception:
    # If DB insert fails, record as dead-letter for later reprocessing
    db.record_dead_letter(
      username=username,
      error_type="db_insert_failed",
      http_status=status,
      error_detail={"user": user},
    )

stop_flag = False

def consumer() -> None:
  logger.info("✅ Starting GitHub consumer...")

  wait_for_postgres(POSTGRES_DSN, POSTGRES_WAIT_MAX_S)
  init_db(POSTGRES_DSN)

  db = DB(POSTGRES_DSN)
  db.connect()

  redis_client = get_redis_connection()
  session = create_session()

  processed = 0
  started = time.time()

  try:
    while not stop_flag:
      try:
        result = redis_client.blpop(QUEUE_NAME, timeout=REDIS_BLPOP_TIMEOUT_S)
      except redis.exceptions.RedisError as e:
        logger.error("Redis error: %s", e)
        time.sleep(1)
        continue

      if result is None:
        # Timeout tick: loop and check stop flag
        continue

      _, username = result
      now = datetime.now(timezone.utc).strftime("%H:%M:%S")
      logger.info("[%s] 📡 Got login from Redis: %s", now, username)

      # Process one username
      try:
          process_username(db, session, username)
          processed += 1
      except Exception:
          # process_username already logs and dead-letters; still continue
          pass

      # Simple adaptive throttle based on remaining rate limit header is inside fetch; here a small pause
      # 3600 / 0.8 = 4500 
      # 4500 from 5000 = 90%
      time.sleep(0.8)

      # 3600 / 0.75 = 4800
      # 4800 from 5000 = 96%
      # time.sleep(0.75)

      print(" ")
  
  finally:
    elapsed = time.time() - started
    logger.info("Shutting down. Processed=%s Elapsed=%.1fs", processed, elapsed)
    try:
      db.close()
    except Exception:
      pass

if __name__ == "__main__":
  consumer()