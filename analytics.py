from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import time
import json
import os

import redis
import psycopg

LOCAL_TZ = ZoneInfo("Europe/Berlin")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REQUEST_COUNTER_KEY = os.getenv("REQUEST_COUNTER_KEY", "github_requests_total")
MINUTE_REQUEST_COUNTER_KEY: str = os.getenv("MINUTE_REQUEST_COUNTER_KEY", "minute_requests_counter")

redis_client = redis.Redis(
  host=REDIS_HOST,
  port=REDIS_PORT,
  db=REDIS_DB,
  decode_responses=True
)

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

CREATE_GLOBAL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS global_request_counter (
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  total_requests BIGINT NOT NULL,
  PRIMARY KEY (ts)
);
"""

INSERT_GLOBAL_SQL = """
INSERT INTO global_request_counter (ts, total_requests)
VALUES (%s, %s)
ON CONFLICT (ts) DO NOTHING;
"""

TELEMETRY_MINUTELY_SQL = """
CREATE TABLE IF NOT EXISTS telemetry_minutely (
minute        TIMESTAMPTZ NOT NULL,
metric        TEXT        NOT NULL,        -- np. 'api_response_time'
container_id  TEXT        NOT NULL,        -- worker id
count         INT         NOT NULL,
sum           DOUBLE PRECISION NOT NULL,
avg           DOUBLE PRECISION NOT NULL,
p50           DOUBLE PRECISION,
p95           DOUBLE PRECISION,
p99           DOUBLE PRECISION,
min           DOUBLE PRECISION,
max           DOUBLE PRECISION,
PRIMARY KEY (minute, metric, container_id)
);
"""

TELEMETRY_MINUTELY_GLOBAL_SQL = """
CREATE TABLE IF NOT EXISTS telemetry_minutely_global (
minute        TIMESTAMPTZ NOT NULL,
metric        TEXT        NOT NULL,
count         INT         NOT NULL,
sum           DOUBLE PRECISION NOT NULL,
avg           DOUBLE PRECISION NOT NULL,
p50           DOUBLE PRECISION,
p95           DOUBLE PRECISION,
p99           DOUBLE PRECISION,
min           DOUBLE PRECISION,
max           DOUBLE PRECISION,
PRIMARY KEY (minute, metric)
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
      cur.execute(CREATE_GLOBAL_TABLE_SQL)
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

def insert_global_counter(ts: datetime, total: int):
  with get_pg_connection() as conn:
    with conn.cursor() as cur:
      cur.execute(INSERT_GLOBAL_SQL, (ts, total))
      conn.commit()

def estimate_milestones(current_total: int, step: int = 100000, window: int = 10):
  since = datetime.now(timezone.utc) - timedelta(minutes=window)

  sql = """
  SELECT AVG(request_count)::float
  FROM requests_metrics
  WHERE minute > %s;
  """
  with get_pg_connection() as conn:
      with conn.cursor() as cur:
          cur.execute(sql, (since,))
          avg_per_min = cur.fetchone()[0] or 0

  if avg_per_min <= 0:
    print("No data or average = 0")
    return []
  
  milestones = []
  now_ts = datetime.now(timezone.utc)

  next_target = ((current_total // step) + 1) * step

  for target in [next_target, next_target + step]:
    remaining = target - current_total
    minutes_needed = remaining / avg_per_min
    eta = now_ts + timedelta(minutes=minutes_needed)
    eta_local = eta.astimezone(LOCAL_TZ)
    milestones.append({
      "target": target,
      "remaining": remaining,
      "minutes_needed": minutes_needed,
      "eta": eta_local
    })

  return milestones

def get_last_requests_metrics(limit: int = 60) -> list[dict]:
  sql = """
  SELECT minute, request_count
  FROM requests_metrics
  ORDER BY minute DESC
  LIMIT %s
  """

  with get_pg_connection() as conn:
    with conn.cursor() as cur:
      cur.execute(sql, (limit,))
      rows = cur.fetchall()

  rows.reverse()

  return [
    {"timestamp": r[0].isoformat(), "requests_count": r[1]}
    for r in rows
  ]

def get_last_24h_metrics() -> list[dict]:
  since = datetime.now(timezone.utc) - timedelta(hours=24)
  sql = """
  SELECT minute, request_count
  FROM requests_metrics
  WHERE minute >= %s
  ORDER BY minute ASC
  """

  with get_pg_connection() as conn:
    with conn.cursor() as cur:
      cur.execute(sql, (since,))
      rows = cur.fetchall()

  return [
    {"timestamp": r[0].isoformat(), "requests_count": r[1]}
    for r in rows
  ]

def get_last_24h_metrics_hourly() -> list[dict]:
  sql = """
  SELECT date_trunc('hour', minute) AS hour, SUM(request_count) AS requests_count
  FROM requests_metrics
  WHERE minute >= NOW() - interval '24 hours'
  GROUP BY hour
  ORDER BY hour ASC;
  """
  with get_pg_connection() as conn:
    with conn.cursor() as cur:
      cur.execute(sql)
      rows = cur.fetchall()

  return [
    {"timestamp": r[0].isoformat(), "requests_count": int(r[1])}
    for r in rows
  ]

def get_last_ingest_metrics_60min() -> list[dict]:
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    results = []
    for i in range(60):
        minute = now - timedelta(minutes=(59 - i))
        minute_str = minute.strftime("%Y-%m-%d %H:%M")

        inserted  = int(redis_client.get(f"github:inserted_minute:{minute_str}") or 0)
        skipped   = int(redis_client.get(f"github:skipped_minute:{minute_str}") or 0)
        processed = int(redis_client.get(f"github:processed_minute:{minute_str}") or 0)

        results.append({
            "timestamp": minute.isoformat(),
            "inserted": inserted,
            "skipped": skipped,
            "processed": processed,
        })

    return results[:-1]

def get_telemetry_data(redis_client: redis.Redis, container_id: str, metric: str, last_minutes: int = 60):
  now = datetime.now(timezone.utc)
  data = []

  for i in range(last_minutes):
    minute_key = (now - timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M")
    key = f"metrics:{container_id}:{metric}:{minute_key}"

    values = redis_client.lrange(key, 0, -1)
    data.extend([float(v) for v in values])

  return data

def calculate_average(data: list) -> float:
  """ avg """
  if not data:
    return 0.0
  return sum(data) / len(data)

# def collect_minute_samples(prev_minute_dt: datetime):

# def persist_minute_stats(perv_minute_dt: datetime, buckets: dict):

# step 1 - get data from redis
# step 2 - send data to cloud

def run_task1():
  print(">>>1 task - trigger at", datetime.now().strftime("%H:%M:%S"))

def run_task2():
  print(">>>2 task - trigger at", datetime.now().strftime("%H:%M:%S"))

def run_task3():
  try:
    count = int(redis_client.get(REQUEST_COUNTER_KEY) or 0)

    worker_keys = redis_client.keys("metrics:*")
    worker_ids = set()

    for key in worker_keys:
      parts = key.split(":")
      if len(parts) >= 3:
        worker_ids.add(parts[1])

    print(f">>> Found {len(worker_ids)} unique worker IDs")

    prev_minute = (datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=1))
    minute_key = f"{MINUTE_REQUEST_COUNTER_KEY}:{prev_minute.strftime('%Y-%m-%d %H:%M')}"
    # minute_key = f"{MINUTE_REQUEST_COUNTER_KEY}:{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}"
    prev_minute_str = prev_minute.strftime("%Y-%m-%d %H:%M")

    inserted_minute  = int(redis_client.get(f"github:inserted_minute:{prev_minute_str}") or 0)
    skipped_minute   = int(redis_client.get(f"github:skipped_minute:{prev_minute_str}") or 0)
    processed_minute = int(redis_client.get(f"github:processed_minute:{prev_minute_str}") or 0)

    print(f">>> Ingest per minute: inserted={inserted_minute}, skipped={skipped_minute}, processed={processed_minute}")

    minute_count = int(redis_client.get(minute_key) or 0)

    # --- GLOBAL INGEST COUNTERS (inserted/skipped/dead/processed) ---
    g_inserted, g_skipped, g_dead, g_processed = redis_client.mget(
      "github:inserted_total",
      "github:skipped_total",
      "github:dead_total",
      "github:processed_total",
    )
    inserted_total  = int(g_inserted  or 0)
    skipped_total   = int(g_skipped   or 0)
    dead_total      = int(g_dead      or 0)
    processed_total = int(g_processed or 0)

    workers_telemetry = []
    for worker_id in worker_ids:
      worker_metrics = {}

      metrics = ["loop_time", "api_response_time", "db_insert_user_time", "db_insert_org_time", "redis_fetch_time"]
      for metric in metrics:
        data = get_telemetry_data(redis_client, worker_id, metric)
        avg_value = calculate_average(data)
        worker_metrics[metric] = round(avg_value, 6)

      workers_telemetry.append({
        "container_id": worker_id,
        "metrics": worker_metrics
      })

    worker_state_keys = redis_client.keys("worker:*")
    workers_state = []
    for key in worker_state_keys:
      data = redis_client.hgetall(key)
      workers_state.append({
        "container_id": data.get("container_id"),
        "start_time": data.get("start_time"),
        "inserted": int(data.get("inserted") or 0),
        "skipped": int(data.get("skipped") or 0),
        "dead": int(data.get("dead") or 0),
        "processed": int(data.get("processed") or 0),
      })

    print(f">>>data from redis - {datetime.now().strftime('%H:%M:%S')} | total GitHub requests = {count}")
    print(f"    ingest totals: inserted={inserted_total} skipped={skipped_total} dead={dead_total} processed={processed_total}")
    print(f">>>requests per minute: {minute_count}")
    print(f"   active workers = {len(workers_state)}")

    state_by_id = {w["container_id"]: w for w in workers_state if w.get("container_id")}
    for rec in workers_telemetry:
      wid = rec["container_id"]
      st = state_by_id.get(wid)
      if st:
        rec["start_time"]      = st["start_time"]
        rec["inserted_total"]  = st["inserted"]
        rec["skipped_total"]   = st["skipped"]
        rec["dead_total"]      = st["dead"]
        rec["processed_total"] = st["processed"]

    for w in workers_telemetry:
      container_id = w.get('container_id')
      raw_ts = redis_client.hget(f"worker:{container_id}", "start_time")
      if raw_ts:
        try:
          ts = float(raw_ts)
          formatted = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
          formatted = raw_ts
      else:
        formatted = "N/A"
      print(f"      - id={container_id} start_time={formatted}")

    milestones = estimate_milestones(count)
    for m in milestones:
      print(f"Milestone {m['target']:,} → za {m['minutes_needed']:.1f} min, około {m['eta'].strftime('%Y-%m-%d %H:%M:%S')}")

    insert_metrics(prev_minute, minute_count)
    # collect minute samples
    # persist minute stats
    replace_workers_snapshot(workers_state)
    insert_global_counter(datetime.now(timezone.utc), count)
    
    print(" ")
    print(milestones[0])

    snapshot = {
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "total_requests": count,
      "requests_last_minute": minute_count,
      "workers": workers_telemetry,
      "next_milestone": {
        "target": milestones[0]['target'],
        "minutes_needed": milestones[0]['minutes_needed'],
        "estimated_time_of_arrival": milestones[0]['eta'].isoformat()
      },
      "ingest_totals": {
        "inserted": inserted_total,
        "skipped": skipped_total,
        "dead": dead_total,
        "processed": processed_total,
      },
      "ingest_60min": get_last_ingest_metrics_60min(),
      "requests_metrics": get_last_requests_metrics(60),
      "requests_24h_minute": get_last_24h_metrics(),
      "requests_24h_hour": get_last_24h_metrics_hourly()
    }

    output_path = Path("dashboard-analytics/aws/data.json")
    with output_path.open("w", encoding="utf-8") as f:
      json.dump(snapshot, f, indent=2)

    # print("")

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