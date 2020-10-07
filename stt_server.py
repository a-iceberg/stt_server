import sys
import os
import time
import datetime
from stt_miner import get_file_splitted
from stt_miner import transcribe_to_sql
from init_server import server_settings
from init_server import connect_sql
from init_server import delete_queue

cpu_id = sys.argv[1]
settings = server_settings()
conn = connect_sql(settings)
cursor = conn.cursor()
sql_query =		"select filepath, filename, date_y, date_m, date_d from queue where cpu_id='"+cpu_id+"' order by date;"

while True:
	cursor.execute(sql_query)
	for row in cursor.fetchall():
		filepath	= row[0]
		filename	= row[1]
		date_y		= row[2]
		date_m		= row[3]
		date_d		= row[4]
		# split channels
		splitted_file_name = get_file_splitted(filepath+filename, settings.script_path)
		temp_storage_path = settings.script_path+'files/'
		# transcribe
		transcribe_to_sql(temp_storage_path, splitted_file_name+'_l.wav', conn, settings, 0, date_y, date_m, date_d, filename)	
		transcribe_to_sql(temp_storage_path, splitted_file_name+'_r.wav', conn, settings, 1, date_y, date_m, date_d, filename)
		# delete from queue
		delete_queue(conn,filename)
		# remove temporary splitted audio files
		os.remove(temp_storage_path+splitted_file_name+'_l.wav')
		os.remove(temp_storage_path+splitted_file_name+'_r.wav')
		print('=== === ===',filename)
	print(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'sleeping 10s..')
	time.sleep(10)