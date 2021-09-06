import os
import time
import datetime
import pendulum
#import sys
#sys.path.insert(0,'../')
from init_server import stt_server

server_object = stt_server()

print('cpu:', server_object.cpu_id)

batch_size = 10

cursor = server_object.conn.cursor()
past_in_minutes = pendulum.now().add(minutes=-6).strftime('%Y-%m-%d %H:%M:%S')
sql_query = "select filepath, filename, duration, source_id, "
sql_query += "record_date, src, dst, linkedid, file_size, date from queue "
sql_query += "where cpu_id='" + str(server_object.cpu_id) + "' "
sql_query += "and ( (source_id = '2' and record_date < '" + past_in_minutes + "') or not source_id = '2' ) "
sql_query += "order by record_date, filename;"
processed = 0
cursor.execute(sql_query)
linkedid = ''
fetched = cursor.fetchall()
print('fetched', len(fetched))
for row in fetched:

	queue_start = time.time()

	original_file_path = row[0]
	original_file_name = row[1]
	original_file_duration = row[2]
	server_object.source_id = row[3]
	rec_date = row[4]
	src = row[5]
	dst = row[6]
	linkedid = row[7]
	file_size = row[8]
	queue_date = row[9]

	files_converted = 0

	if not os.path.isfile(original_file_path + original_file_name):
		msg = 'File not found: ' + original_file_path + original_file_name
		msg += '\nRemoving from queue..'
		print(msg)
		server_object.delete_current_queue(original_file_name, linkedid)

		queue_end = time.time()
		server_object.perf_log(0, queue_start, queue_end, original_file_duration, linkedid)
		continue

	if original_file_duration>5:

		if server_object.source_id == server_object.sources['master']:
			server_object.temp_file_path = original_file_path
			server_object.temp_file_name = original_file_name
			if os.path.isfile(server_object.temp_file_path + server_object.temp_file_name):
				side = 0 if 'in' in original_file_name else 1
				server_object.transcribe_to_sql(
					original_file_duration,
					side,
					original_file_name,
					rec_date,
					src,
					dst,
					linkedid,
					file_size,
					queue_date
				)
				files_converted += 1				
			else:
				print('mrm file not found', server_object.temp_file_path + server_object.temp_file_name)
			
			server_object.delete_current_queue(original_file_name, linkedid)


		elif server_object.source_id == server_object.sources['call']:

			if server_object.make_file_splitted(
					0,
					original_file_path,
					original_file_name,
					linkedid,
					original_file_duration
			):
				server_object.transcribe_to_sql(
					original_file_duration,
					0,
					original_file_name,
					rec_date,
					src,
					dst,
					linkedid,
					file_size,
					queue_date
				)
				server_object.remove_temporary_file()
				files_converted += 1

			if server_object.make_file_splitted(
					1,
					original_file_path,
					original_file_name,
					linkedid,
					original_file_duration
			):
				server_object.transcribe_to_sql(
					original_file_duration,
					1,
					original_file_name,
					rec_date,
					src,
					dst,
					linkedid,
					file_size,
					queue_date
				)
				server_object.remove_temporary_file()
				files_converted += 1

			if files_converted == 0:
				print('file not found:', linkedid, 'removing from queue..')
				server_object.delete_current_queue(original_file_name, linkedid)

		print('files_converted', files_converted)		
		
		server_object.delete_source_file(original_file_path, original_file_name, linkedid)

	else:
		print(original_file_name, 'duration', original_file_duration)
		trans_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		if server_object.source_id == server_object.sources['master']:
			side = 0 if 'in' in original_file_name else 1
			server_object.save_result(
				original_file_duration,
				'',
				'0',
				'0',
				side,
				trans_date,
				0,
				original_file_name,
				rec_date,
				src,
				dst,
				linkedid,
				file_size,
				queue_date
			)
		elif server_object.source_id == server_object.sources['call']:
			server_object.save_result(
				original_file_duration,
				'',
				'0',
				'0',
				0,
				trans_date,
				0,
				original_file_name,
				rec_date,
				src,
				dst,
				linkedid,
				file_size,
				queue_date
			)
			server_object.save_result(
				original_file_duration,
				'',
				'0',
				'0',
				1,
				trans_date,
				0,
				original_file_name,
				rec_date,
				src,
				dst,
				linkedid,
				file_size,
				queue_date
			)

		server_object.delete_current_queue(original_file_name, linkedid)
		server_object.delete_source_file(original_file_path, original_file_name, linkedid)

	queue_end = time.time()
	server_object.perf_log(0, queue_start, queue_end, original_file_duration, linkedid)

	processed += 1
	print(' === processed:', processed, '/', len(fetched), 'files_converted:', files_converted, '===')
	if processed > batch_size:
		print('processed > batch_size')
		break

print(server_object.cpu_id,datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'exit to next job..')
if processed == 0:
	time.sleep(3)

print('exit')
