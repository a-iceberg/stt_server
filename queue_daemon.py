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
	complete_files	= server_object.get_sql_complete_files()
	# cpu_id = 0
	incomplete_count = 0
	complete_count = 0
	for filename in server_object.get_fs_files_list():	
		if not filename in complete_files:
			server_object.set_shortest_queue_cpu()
			#server_object.cpu_id = cpu_id
			server_object.original_file_name = filename
			server_object.add_queue()
			print(incomplete_count, 'incomplete', server_object.cpu_id, filename)
			"""cpu_id += 1
			if cpu_id>len(server_object.cpu_cores)-1:
				cpu_id = 0"""
			incomplete_count += 1
			# debug ++
			# break
			# debug --
		else:
			print(complete_count,'complete:',filename)
			complete_count += 1
	
	print('incomplete_count',incomplete_count)
	print('complete_count',complete_count)
	
	if len(sys.argv)==4:
		break
	else:
		print(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'sleeping 10s..')
		time.sleep(10)
		server_object.set_today_ymd()