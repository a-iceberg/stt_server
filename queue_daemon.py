import datetime
import time
from init_server import stt_server
#from init_server import connect_sql
#from init_server import get_today_ymd
	
#settings = server_settings()
server_object = stt_server(0)
#conn = connect_sql(settings)


while True:
	#date_y, date_m, date_d	= get_today_ymd()
	server_object.set_today_ymd()
	# get filenames in today's queue
	complete_files	= server_object.get_sql_complete_files()

	# list files
	#filepath, fs_files_list	= get_fs_files_list(settings, date_y, date_m, date_d)
	#filepath, fs_files_list	= server_object.get_fs_files_list()
	for filename in server_object.get_fs_files_list():
		if not filename in complete_files:
			server_object.set_shortest_queue_cpu()
			server_object.original_file_name = filename
			server_object.add_queue()
			#conn, filepath, filename, cpu_id, date_y, date_m, date_d

	print(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'sleeping 10s..')
	time.sleep(10)