import os

from celery import Celery
from kombu import Queue

BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
QUEUE_NAME = os.environ.get("CELERY_QUEUE", "transcribe")
TASK_NAME = "stt.transcribe_file"

TASK_SOFT_TIME_LIMIT = int(os.environ.get("TASK_SOFT_TIME_LIMIT", "300"))
TASK_TIME_LIMIT = int(os.environ.get("TASK_TIME_LIMIT", "360"))

# Redis hands a task to another worker once visibility_timeout expires, so it
# must stay well above TASK_TIME_LIMIT or a long call gets transcribed twice.
VISIBILITY_TIMEOUT = int(os.environ.get("BROKER_VISIBILITY_TIMEOUT", "900"))

MAX_RETRIES = int(os.environ.get("TASK_MAX_RETRIES", "3"))
RETRY_BACKOFF = int(os.environ.get("TASK_RETRY_BACKOFF", "30"))
# Keep the longest retry delay below VISIBILITY_TIMEOUT: delayed tasks are held
# by the worker and a longer delay would let Redis re-deliver them.
RETRY_BACKOFF_MAX = int(os.environ.get("TASK_RETRY_BACKOFF_MAX", "300"))

# Marks a file whose results are already in the database, so a task re-delivered
# after a worker crash finishes the cleanup instead of transcribing it again.
DONE_MARKER_TTL = int(os.environ.get("DONE_MARKER_TTL", "86400"))

app = Celery("stt", include=["tasks"])

app.conf.update(
    broker_url=BROKER_URL,
    task_default_queue=QUEUE_NAME,
    task_queues=(Queue(QUEUE_NAME, durable=True),),
    task_serializer="json",
    accept_content=["json"],
    timezone=os.environ.get("TZ", "Europe/Moscow"),
    enable_utc=False,
    # The producer never reads results; storing them would only grow the broker.
    task_ignore_result=True,
    # Acknowledge after the work is done, so a killed worker returns the file to
    # the queue instead of dropping it.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=TASK_SOFT_TIME_LIMIT,
    task_time_limit=TASK_TIME_LIMIT,
    task_track_started=True,
    broker_transport_options={
        "visibility_timeout": VISIBILITY_TIMEOUT,
        "socket_keepalive": True,
        "health_check_interval": 30,
        "retry_on_timeout": True,
    },
    broker_connection_retry_on_startup=True,
    # A pool process sets up a database connection before it reports readiness.
    # The 4 second default leaves no room for a slow network and makes the pool
    # kill and refork every process in a loop.
    worker_proc_alive_timeout=float(os.environ.get("WORKER_PROC_ALIVE_TIMEOUT", "30")),
    worker_max_tasks_per_child=int(os.environ.get("WORKER_MAX_TASKS_PER_CHILD", "200")),
    # Required for Flower to see tasks.
    worker_send_task_events=True,
    task_send_sent_event=True,
)
