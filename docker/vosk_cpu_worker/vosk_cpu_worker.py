import os
import time
import datetime
import pendulum
from init_server import stt_server

def main():
	server_object = stt_server()

	# print('started', server_object.cpu_id, server_object.gpu_uri)
	server_object.logger.info(f'CPU {server_object.cpu_id} started: {server_object.gpu_uri}')

	cursor = server_object.conn.cursor()

	while True:

		past_in_minutes = pendulum.now().add(minutes=-6).strftime('%Y-%m-%d %H:%M:%S')
		server_object.logger.info(f'past_in_minutes: {past_in_minutes}')
		sql_query = "select top 3 filepath, filename, duration, source_id, "
		sql_query += "record_date, src, dst, linkedid, file_size, date from queue "
		sql_query += "where cpu_id='" + str(server_object.cpu_id) + "' "
		sql_query += "and record_date < '" + past_in_minutes + "' "
		sql_query += "order by record_date desc, filename;"
		processed = 0
		cursor.execute(sql_query)
		linkedid = ''
		fetched = cursor.fetchall()
		# print('fetched', len(fetched))
		server_object.logger.info(f'fetched: {len(fetched)}')
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
				# print(msg)
				server_object.logger.info(msg)
				server_object.delete_current_queue(original_file_name, linkedid)

				queue_end = time.time()
				server_object.perf_log(0, queue_start, queue_end, original_file_duration, linkedid)
				continue

			side = 0 if original_file_name[-6:]=='in.wav' else 1

			if original_file_duration>5:			
				server_object.temp_file_path = original_file_path
				server_object.temp_file_name = original_file_name
				if os.path.isfile(server_object.temp_file_path + server_object.temp_file_name):
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
					"""print(
						'id: '+str(server_object.source_id)+' file not found', 
						server_object.temp_file_path + server_object.temp_file_name
						)"""
					server_object.logger.info(
						f'id: {server_object.source_id} file not found {server_object.temp_file_path + server_object.temp_file_name}'
					)
				server_object.delete_current_queue(original_file_name, linkedid)
				# print('files_converted', files_converted)			
				server_object.logger.info(f'files_converted: {files_converted}')
				server_object.delete_source_file(original_file_path, original_file_name, linkedid)

			else:
				# print(original_file_name, 'duration', original_file_duration)
				server_object.logger.info(f'{original_file_name} duration {original_file_duration}')
				trans_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
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
					queue_date,
					0
				)

				server_object.delete_current_queue(original_file_name, linkedid)
				server_object.delete_source_file(original_file_path, original_file_name, linkedid)

			queue_end = time.time()
			server_object.perf_log(0, queue_start, queue_end, original_file_duration, linkedid)

			processed += 1
			# print(' === processed:', processed, '/', len(fetched), 'files_converted:', files_converted, '===')
			server_object.logger.info(f'processed: {processed}/{len(fetched)} files_converted: {files_converted}')

		# print(server_object.cpu_id,datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'queue reached..')
		server_object.logger.info(f'{server_object.cpu_id} {datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")} queue reached..')
		if processed == 0:
			time.sleep(10)

if __name__ == "__main__":
	main()
