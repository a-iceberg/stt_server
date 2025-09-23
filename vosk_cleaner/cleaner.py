import datetime
import psycopg
import time
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def connect_sql():
	con = psycopg.connect(
		dbname='voice_ai',
		user=os.environ.get("POSTGRESQL_LOGIN", ""),
		password=os.environ.get("POSTGRESQL_PASSWORD", ""),
		host=os.environ.get("POSTGRESQL_SERVER", ""),
		port=os.environ.get("POSTGRESQL_PORT", "")
	)
	logging.info('Connected to SQL')
	return con

def clean_calls(conn, bottom_limit):
	cursor = conn.cursor()
	sql_query = "delete from calls where call_date<'"+bottom_limit+"';"
	cursor.execute(sql_query)
	conn.commit()
	logging.info('calls cleaned')

def clean_transcribations(conn, bottom_limit):
	cursor = conn.cursor()
	sql_query = "delete from transcribations where record_date<'"+bottom_limit+"';"
	cursor.execute(sql_query)
	conn.commit()
	logging.info('transcribations cleaned')

def clean_perf_log(conn, bottom_limit):
	cursor = conn.cursor()
	sql_query = "delete from perf_log where event_date<'"+bottom_limit+"';"
	cursor.execute(sql_query)
	conn.commit()
	logging.info('perf_log cleaned')

logging.info('Start')
conn = connect_sql()
logging.info('waiting for 15 min')

time.sleep(15 * 60)

while True:
    bottom_limit = str(
        (datetime.datetime.now() - datetime.timedelta(days=366)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
    )
    logging.info("Deleting before %s", bottom_limit)

    clean_calls(conn, bottom_limit)
    clean_transcribations(conn, bottom_limit)
    clean_perf_log(conn, bottom_limit)
    logging.info("waiting for 24h")

    time.sleep(24 * 60 * 60)
