from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import time

# import redis

# redis_client = redis.Redis(host='localhost', port=6379, db=0)

def run_task():
  print(">>> Trigger at", datetime.now().strftime("%H:%M:%S"))

scheduler = BackgroundScheduler()
scheduler.add_job(run_task, 'cron', second=0)
scheduler.start()

print("Scheduler started...")

try:
  while True:
    time.sleep(1)
except KeyboardInterrupt:
  print("Schutting down scheduler...")
  scheduler.shutdown()