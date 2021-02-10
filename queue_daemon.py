# call centre queue
import sys
import datetime
import time
from init_server import stt_server
server_object = stt_server(0)

if len(sys.argv)==4:
	server_object.date_y = sys.argv[1]
	server_object.date_m = sys.argv[2]
	server_object.date_d = sys.argv[3]
else:
	server_object.set_today_ymd()

while True:
	for source_id in server_object.sources: # ['call', 'master']
		#server_object.source_id = server_object.sources['call']
		server_object.source_id = server_object.get_source_id(source_id)
		complete_files	= server_object.get_sql_complete_files()
		incomplete_count = 0
		complete_count = 0
		for filename, rec_date in server_object.get_fs_files_list():
			#filename = fd['filename']
			#server_object.rec_date = fd['rec_date']
			server_object.rec_date = rec_date
			if not filename in complete_files:
				server_object.set_shortest_queue_cpu()
				server_object.original_file_name = filename
				server_object.add_queue()
				# print('id', source_id, 'incomplete', incomplete_count, 'cpu', server_object.cpu_id, 'file', filename)
				incomplete_count += 1
			else:
				complete_count += 1

		print('id', source_id)
		print('incomplete_count',incomplete_count)
		print('complete_count',complete_count)
	
	if len(sys.argv)==4:
		break
	else:
		print(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'sleeping 10s..')
		time.sleep(10)
		server_object.set_today_ymd()