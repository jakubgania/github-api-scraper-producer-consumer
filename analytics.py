from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone, timedelta
import time
import os

import redis
import psycopg

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REQUEST_COUNTER_KEY = os.getenv("REQUEST_COUNTER_KEY", "github_requests_total")
MINUTE_REQUEST_COUNTER_KEY: str = os.getenv("MINUTE_REQUEST_COUNTER_KEY", "minute_requests_counter")

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

POSTGRES_DSN: str = os.getenv(
  "POSTGRES_DSN",
  "postgresql://postgres:postgres@localhost:5433/postgres",
)

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5433")
PG_USER = os.getenv("PG_USER", "analytics")
PG_PASSWORD = os.getenv("PG_PASSWORD", "analytics")
PG_DATABASE = os.getenv("PG_DATABASE", "analytics")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS requests_metrics (
  minute TIMESTAMPTZ NOT NULL,
  request_count INT NOT NULL,
  PRIMARY KEY (minute)
);
"""

INSERT_METRICS_SQL = """
INSERT INTO requests_metrics (minute, request_count)
VALUES (%s, %s)
ON CONFLICT (minute) DO NOTHING
RETURNING minute;
"""

CREATE_WORKERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS workers_state (
  container_id TEXT PRIMARY KEY,
  start_time TIMESTAMPTZ
);
"""

def get_pg_connection():
  return psycopg.connect(
    host=PG_HOST,
    port=PG_PORT,
    user=PG_USER,
    password=PG_PASSWORD,
    dbname=PG_DATABASE
  )

def ensure_table():
  with get_pg_connection() as conn:
    with conn.cursor() as cur:
      cur.execute(CREATE_TABLE_SQL)
      cur.execute(CREATE_WORKERS_TABLE_SQL)
      conn.commit()

def insert_metrics(minute: datetime, requests: int):
    try:
      with get_pg_connection() as conn:
        with conn.cursor() as cur:
          cur.execute(
            INSERT_METRICS_SQL,
            (minute, requests),
          )
        conn.commit()
    except Exception as e:
        # conn.rollback()
        # logger.exception("Failed to insert requests_per_minute for %s: %s", bucket_minute, e)
        raise
    
def replace_workers_snapshot(workers: list[dict]):
  with get_pg_connection() as conn:
    with conn.cursor() as cur:
      cur.execute("TRUNCATE workers_state;")
      for w in workers:
        raw_ts = w.get("start_time")
        try:
          start_time = datetime.fromtimestamp(float(raw_ts), tz=timezone.utc)
        except Exception:
          start_time = None

        cur.execute(
          "INSERT INTO workers_state (container_id, start_time) VALUES (%s, %s)",
          (w.get("container_id"), start_time)
        )

    conn.commit()


# step 1 - get data from redis
# step 2 - send data to cloud

def run_task1():
  print(">>>1 task - trigger at", datetime.now().strftime("%H:%M:%S"))

def run_task2():
  print(">>>2 task - trigger at", datetime.now().strftime("%H:%M:%S"))

def run_task3():
  try:
    count = redis_client.get(REQUEST_COUNTER_KEY) or 0

    prev_minute = (datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=1))
    minute_key = f"{MINUTE_REQUEST_COUNTER_KEY}:{prev_minute.strftime('%Y-%m-%d %H:%M')}"
    # minute_key = f"{MINUTE_REQUEST_COUNTER_KEY}:{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}"
    minute_count = int(redis_client.get(minute_key) or 0)

    worker_keys = redis_client.keys("worker:*")
    workers = []
    for key in worker_keys:
      data = redis_client.hgetall(key)
      workers.append(data)

    print(f">>>data from redis - {datetime.now().strftime('%H:%M:%S')} | total GitHub requests = {count}")
    print(f">>>requests per minute: {minute_count}")
    print(f"   active workers = {len(workers)}")

    for w in workers:
      raw_ts = w.get('start_time')
      if raw_ts:
        try:
          ts = float(raw_ts)
          formatted = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
          formatted = raw_ts
      else:
        formatted = "N/A"

      print(f"      - id={w.get('container_id')} start_time={formatted}")

    insert_metrics(prev_minute, minute_count)
    replace_workers_snapshot(workers)
    
    print(" ")

    # here you can add e.g. sending to the cloud
  except Exception as e:
    print(f"Redis error: {e}")

scheduler = BackgroundScheduler()
# scheduler.add_job(run_task1, 'cron', second=0, id="task1")
# scheduler.add_job(run_task2, 'cron', second=0, id="task2")
scheduler.add_job(run_task3, 'cron', second=0, id="task3")
scheduler.start()

print("Scheduler started...")

try:
  ensure_table()

  while True:
    time.sleep(1)
except KeyboardInterrupt:
  print("Schutting down scheduler...")
  scheduler.shutdown()