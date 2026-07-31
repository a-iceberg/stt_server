import os

import redis
from celery import Celery

BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
QUEUE_NAME = os.environ.get("CELERY_QUEUE", "transcribe")
TASK_NAME = "stt.transcribe_file"

app = Celery("stt_producer")

app.conf.update(
    broker_url=BROKER_URL,
    task_default_queue=QUEUE_NAME,
    task_serializer="json",
    accept_content=["json"],
    task_ignore_result=True,
    timezone=os.environ.get("TZ", "Europe/Moscow"),
    enable_utc=False,
    broker_transport_options={
        "visibility_timeout": int(os.environ.get("BROKER_VISIBILITY_TIMEOUT", "900")),
        "socket_keepalive": True,
        "health_check_interval": 30,
        "retry_on_timeout": True,
    },
    broker_connection_retry_on_startup=True,
)


def send_transcribe_task(payload):
    """Publishes one file to the worker pools. Raises if the broker is down."""
    return app.send_task(
        TASK_NAME,
        kwargs=payload,
        queue=QUEUE_NAME,
        retry=True,
        retry_policy={
            "max_retries": 3,
            "interval_start": 0.5,
            "interval_step": 0.5,
            "interval_max": 3,
        },
    )


def purge_queue():
    """Drops every pending task. Returns the number of removed messages."""
    return app.control.purge()


_redis = None


def pending_tasks():
    """Counts the task messages still waiting to be picked up by a worker.

    The Redis transport keeps them in a list named after the queue.
    """
    global _redis
    if _redis is None:
        _redis = redis.Redis.from_url(BROKER_URL)
    return _redis.llen(QUEUE_NAME)
