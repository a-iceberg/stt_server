import datetime
import os
import shutil
import time

import redis
from celery import Task
from celery.exceptions import SoftTimeLimitExceeded
from celery.signals import worker_process_init
from celery.utils.log import get_task_logger

from celery_app import (
    BROKER_URL,
    DONE_MARKER_TTL,
    MAX_RETRIES,
    RETRY_BACKOFF,
    RETRY_BACKOFF_MAX,
    TASK_NAME,
    app,
)
from init_server import SaveResultError, TranscriptionError, stt_server
from worker_slot import cpu_slot

logger = get_task_logger(__name__)

FAILED_FILES_PATH = os.environ.get('FAILED_FILES_PATH', 'audio/failed/')

_server = None
_redis = None


def get_server():
    global _server
    if _server is None:
        _server = stt_server(cpu_id=cpu_slot())
        _server.logger.info(
            f'celery worker process ready: cpu_id={_server.cpu_id} uri={_server.gpu_uri}'
        )
    return _server


def get_redis():
    global _redis
    if _redis is None:
        _redis = redis.Redis.from_url(BROKER_URL)
    return _redis


@worker_process_init.connect
def init_worker_process(**_kwargs):
    get_server()


def done_marker_key(original_file_name):
    return 'stt:done:' + original_file_name


def mark_done(original_file_name):
    try:
        get_redis().setex(done_marker_key(original_file_name), DONE_MARKER_TTL, 1)
    except Exception as e:
        logger.warning(f'unable to set done marker for {original_file_name}: {e}')


def is_done(original_file_name):
    try:
        return bool(get_redis().exists(done_marker_key(original_file_name)))
    except Exception as e:
        logger.warning(f'unable to read done marker for {original_file_name}: {e}')
        return False


def parse_record_date(record_date):
    if isinstance(record_date, str):
        return datetime.datetime.strptime(record_date, '%Y-%m-%d %H:%M:%S')
    return record_date


class TranscribeTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Dead letter: keep the recording instead of losing it.

        Reached only after all retries are exhausted. The file is moved out of
        the scanned directory so the producer stops re-queueing it, and the
        registry row is dropped so the queue table does not grow.
        """
        filepath = kwargs.get('filepath', '')
        filename = kwargs.get('filename', '')
        linkedid = kwargs.get('linkedid', '')

        if not filename:
            logger.error(f'task {task_id} failed without a filename: {exc}')
            return

        server = get_server()
        source_file = filepath + filename

        try:
            os.makedirs(FAILED_FILES_PATH, exist_ok=True)
            if os.path.isfile(source_file):
                shutil.move(source_file, os.path.join(FAILED_FILES_PATH, filename))
                logger.error(f'{filename} moved to {FAILED_FILES_PATH} after {exc}')
        except Exception as move_error:
            logger.error(f'unable to move {source_file} to failed dir: {move_error}')

        try:
            server.ensure_connection()
            server.delete_current_queue(filename, linkedid)
        except Exception as sql_error:
            logger.error(f'unable to clean queue row for {filename}: {sql_error}')

        server.send_to_telegram(
            f'transcription failed after {MAX_RETRIES} retries: {filename} ({exc})'
        )


@app.task(
    base=TranscribeTask,
    bind=True,
    name=TASK_NAME,
    autoretry_for=(TranscriptionError, SaveResultError, SoftTimeLimitExceeded),
    retry_backoff=RETRY_BACKOFF,
    retry_backoff_max=RETRY_BACKOFF_MAX,
    retry_jitter=True,
    max_retries=MAX_RETRIES,
)
def transcribe_file(
    self,
    filepath,
    filename,
    duration,
    source_id,
    record_date,
    src,
    dst,
    linkedid,
    queue_date,
):
    queue_start = time.time()

    server = get_server()
    server.ensure_connection()
    server.source_id = source_id
    server.temp_file_path = filepath
    server.temp_file_name = filename

    source_file = filepath + filename
    rec_date = parse_record_date(record_date)

    # The registry row is deleted once a file is fully handled, so a missing row
    # means this task was already completed and got re-delivered.
    if not server.queue_row_exists(filename):
        server.logger.info(f'{filename} has no queue row, already handled - skipping')
        return 'skipped'

    server.assign_queue_row(filename, server.cpu_id)

    if not os.path.isfile(source_file):
        server.logger.info(f'File not found: {source_file}\nRemoving from queue..')
        server.delete_current_queue(filename, linkedid)
        server.perf_log(0, queue_start, time.time(), duration, linkedid)
        return 'file_not_found'

    if dst == 'main' and server.is_duplicate_conversation(linkedid, rec_date, filename):
        server.logger.info(
            f'File {source_file} contains already recognized conversation'
            '\nRemoving from queue..'
        )
        server.delete_current_queue(filename, linkedid)
        server.delete_source_file(filepath, filename, linkedid)
        server.perf_log(0, queue_start, time.time(), duration, linkedid)
        return 'duplicate'

    side = 0 if filename[-6:] == 'in.wav' else 1
    files_converted = 0

    if is_done(filename):
        server.logger.warning(
            f'{filename} is already transcribed, finishing cleanup only'
        )
    elif duration > 5:
        server.transcribe_to_sql(
            duration,
            side,
            filename,
            rec_date,
            src,
            dst,
            linkedid,
            queue_date,
        )
        mark_done(filename)
        files_converted = 1
    else:
        server.logger.info(f'{filename} duration {duration}')
        server.save_result(
            duration,
            '',
            '0',
            '0',
            side,
            datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            0,
            filename,
            rec_date,
            src,
            dst,
            linkedid,
            queue_date,
            0,
        )
        mark_done(filename)

    server.delete_current_queue(filename, linkedid)
    server.logger.info(f'files_converted: {files_converted}')
    server.delete_source_file(filepath, filename, linkedid)
    server.perf_log(0, queue_start, time.time(), duration, linkedid)

    return 'ok'
