import datetime
import logging
import os
import time

from init_queue import stt_server

logging.basicConfig(level=logging.INFO)


def main():
    logging.info('Starting')
    batch_size = int(os.environ.get('SCAN_BATCH_SIZE', '1000'))
    sleep_time = int(os.environ.get('SCAN_INTERVAL_SECONDS', '3'))
    server_object = stt_server()

    while True:
        server_object.release_stuck_rows()

        for source_id in server_object.sources:  # ['call', 'master']
            server_object.source_id = server_object.get_source_id(source_id)
            complete_files = server_object.get_sql_complete_files()
            candidates = 0
            queued = 0
            for (
                filepath,
                filename,
                rec_date,
                src,
                dst,
                linkedid,
                version,
                file_size
            ) in server_object.get_fs_files_list(complete_files):
                if server_object.add_queue(
                    filepath, filename, rec_date, src, dst, linkedid, version, file_size
                ):
                    queued += 1
                candidates += 1
                if candidates > batch_size:
                    logging.info('batch size reached. break')
                    break

            logging.info(
                f'id {source_id} candidates: {candidates} sent to queue: {queued}'
            )

        logging.info(
            datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            + ' sleeping ' + str(sleep_time) + 's..'
        )
        time.sleep(sleep_time)


if __name__ == '__main__':
    main()
