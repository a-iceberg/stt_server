import datetime
import pymssql
import time
import os

def connect_sql():

	return pymssql.connect(
		server=os.environ.get('MSSQL_SERVER', ''),
		user=os.environ.get('MSSQL_LOGIN', ''),
		password=os.environ.get('MSSQL_PASSWORD', ''),
		database='voice_ai',
		#autocommit=True			
	)

def clean_transcribations(conn, bottom_limit):

	cursor = conn.cursor()
	sql_query = "delete from transcribations where record_date<'"+bottom_limit+"';"
	cursor.execute(sql_query)
	conn.commit() # autocommit
	print(datetime.datetime.now(), 'transcribations cleaned')

def clean_perf_log(conn, bottom_limit):

	cursor = conn.cursor()
	sql_query = "delete from perf_log where event_date<'"+bottom_limit+"';"
	cursor.execute(sql_query)
	conn.commit() # autocommit
	print(datetime.datetime.now(), 'perf_log cleaned')

def clean_summarization_queue(conn, bottom_limit):

	cursor = conn.cursor()
	sql_query = "delete from summarization_queue where record_date<'"+bottom_limit+"';"
	cursor.execute(sql_query)
	conn.commit() # autocommit
	print(datetime.datetime.now(), 'summarization_queue cleaned')

print(datetime.datetime.now(), 'Start')

conn = connect_sql()

print(datetime.datetime.now(), 'Connected. Waiting 10 min..')

time.sleep(10 * 60)

print(datetime.datetime.now(), 'Go')

while True:

	bottom_limit = str((datetime.datetime.now() - datetime.timedelta(days=180)).strftime('%Y-%m-%dT%H:%M:%S'))
	print(datetime.datetime.now(), 'Deleting before', bottom_limit)
	clean_transcribations(conn, bottom_limit)
	clean_perf_log(conn, bottom_limit)
	clean_summarization_queue(conn, bottom_limit)
	time.sleep(24 * 60 * 60)
	