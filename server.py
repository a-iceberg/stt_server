import sys
import time
import datetime
from init_server import stt_server
import re

print('cpu', sys.argv[1])
server_object = stt_server(sys.argv[1])
cursor = server_object.conn.cursor()
sql_query = "select filepath, filename, date_y, date_m, date_d, duration, source_id, "
sql_query += "record_date, src, dst, linkedid from queue "
sql_query += "where cpu_id='"+str(server_object.cpu_id)+"'"
sql_query += " order by record_date;"
processed = 0
cursor.execute(sql_query)
for row in cursor.fetchall():

	server_object.original_file_path = row[0]
	server_object.original_file_name = row[1]
	server_object.date_y = row[2]
	server_object.date_m = row[3]
	server_object.date_d = row[4]
	server_object.original_file_duration = row[5]
	server_object.source_id = row[6]
	server_object.rec_date = row[7]
	server_object.src = row[8]
	server_object.dst = row[9]
	server_object.linkedid = row[10]

	"""rec_source_date = re.findall(r'\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}', server_object.original_file_name)[0]
	server_object.rec_date = rec_source_date[:10] + ' ' + rec_source_date[11:].replace('-', ':')
	if len(re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', server_object.rec_date)) == 0:
		print('Unable to extract date from filename', server_object.original_file_name)
		server_object.rec_date = 'Null'"""

	if server_object.original_file_duration>5:

		files_converted = 0

		if server_object.make_file_splitted(0):
			server_object.transcribe_to_sql(0)
			server_object.remove_temporary_file()
			files_converted += 1

		if server_object.make_file_splitted(1):
			server_object.transcribe_to_sql(1)
			server_object.remove_temporary_file()
			files_converted += 1

		if files_converted == 0:
			sys.exit( str(server_object.cpu_id)+': '+datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')+\
					  ' No converted files found. exit..')
		else:
			print('files_converted', files_converted)
		server_object.delete_current_queue()
		server_object.delete_source_file()
		break
	else:

		print(server_object.original_file_name, 'duration', server_object.original_file_duration)
		trans_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		server_object.save_result('', '0', '0', 0, trans_date, 0)
		server_object.save_result('', '0', '0', 1, trans_date, 0)
		server_object.delete_current_queue()
		server_object.delete_source_file()
	processed += 1

print(server_object.cpu_id,datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'exit to next job..')
if processed == 0:
	time.sleep(3)
