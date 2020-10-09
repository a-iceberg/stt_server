import datetime
import time
from init_server import stt_server
server_object = stt_server(0)

#while True:
	
server_object.set_today_ymd()
#complete_files	= server_object.get_sql_complete_files()
cpu_id = 0
for filename in server_object.get_fs_files_list():	
	#	if not filename in complete_files:
	#server_object.set_shortest_queue_cpu()
	server_object.cpu_id = cpu_id
	server_object.original_file_name = filename
	server_object.add_queue()
	print(cpu_id,filename)
	cpu_id += 1
	if cpu_id>len(server_object.cpu_cores)-1:
		cpu_id = 0

#print(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'sleeping 10s..')
#time.sleep(10)