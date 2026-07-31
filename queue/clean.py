from celery_client import purge_queue
from init_queue import stt_server
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    logging.info("Initializing queue cleaning")

    # Redis first: dropping registry rows while tasks are still pending would
    # let those tasks run against rows that no longer exist.
    purged = purge_queue()
    logging.info("Purged %s pending tasks from the broker", purged)

    server = stt_server()
    server.clean_queue()
    logging.info("Queue has been cleaned successfully")
