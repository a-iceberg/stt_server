import datetime
import time
from init_queue import stt_server

exit()

batch_size = 1000

server_object = stt_server()

for source_id in server_object.sources: # ['call', 'master']

	#if source_id == 'call': # debug
	server_object.source_id = server_object.get_source_id(source_id)
	complete_files = server_object.get_sql_complete_files()
	incomplete_count = 0
	for filepath, filename, rec_date, src, dst, linkedid, version in server_object.get_fs_files_list(complete_files):
		server_object.set_shortest_queue_cpu()
		server_object.add_queue(filepath, filename, rec_date, src, dst, linkedid, version)
		incomplete_count += 1
		if incomplete_count>batch_size:
			print('batch size reached. break')
			break

	print('id', source_id)
	print(incomplete_count, 'files sent to queue', server_object.sources[source_id])

sleep_time = 3
print(
	datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
	'sleeping '+str(sleep_time)+'s..'
)
time.sleep(sleep_time)
