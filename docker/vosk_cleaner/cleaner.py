import datetime
import pymssql
import time
import os
import logging

# init logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def connect_sql():

	con = pymssql.connect(
		server=os.environ.get('MSSQL_SERVER', ''),
		user=os.environ.get('MSSQL_LOGIN', ''),
		password=os.environ.get('MSSQL_PASSWORD', ''),
		database='voice_ai',
		#autocommit=True			
	)
	logging.info('Connected to MSSQL')
	return con

def clean_transcribations(conn, bottom_limit):

	cursor = conn.cursor()
	sql_query = "delete from transcribations where record_date<'"+bottom_limit+"';"
	cursor.execute(sql_query)
	conn.commit() # autocommit
	logging.info('transcribations cleaned')


def clean_perf_log(conn, bottom_limit):

	cursor = conn.cursor()
	sql_query = "delete from perf_log where event_date<'"+bottom_limit+"';"
	cursor.execute(sql_query)
	conn.commit() # autocommit
	logging.info('perf_log cleaned')

def clean_summarization_queue(conn, bottom_limit):

	cursor = conn.cursor()
	sql_query = "delete from summarization_queue where record_date<'"+bottom_limit+"';"
	cursor.execute(sql_query)
	conn.commit() # autocommit
	logging.info('summarization_queue cleaned')

logging.info('Start')

conn = connect_sql()

logging.info('waiting for 10 min')

time.sleep(10 * 60)

while True:

	bottom_limit = str((datetime.datetime.now() - datetime.timedelta(days=180)).strftime('%Y-%m-%dT%H:%M:%S'))
	logging.info('Deleting before %s', bottom_limit)
	clean_transcribations(conn, bottom_limit)
	clean_perf_log(conn, bottom_limit)
	clean_summarization_queue(conn, bottom_limit)
	logging.info('waiting for 24h')
	time.sleep(24 * 60 * 60)
	