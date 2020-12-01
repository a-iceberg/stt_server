import sys
import time
import datetime
from init_server import stt_server

server_object = stt_server(sys.argv[1])
cursor = server_object.conn.cursor()
sql_query =		"select top 1 filepath, filename, date_y, date_m, date_d, duration from queue where cpu_id='"+server_object.cpu_id+"' order by date;"
#last_file_name = ''
#while True:
if True:
	cursor.execute(sql_query)
	for row in cursor.fetchall():

		server_object.original_file_path	= row[0]
		server_object.original_file_name	= row[1]
		#if last_file_name != '' and last_file_name != server_object.original_file_name:
		#	print(server_object.cpu_id,'next filename. exit')
		#last_file_name = server_object.original_file_name
		server_object.date_y				= row[2]
		server_object.date_m				= row[3]
		server_object.date_d				= row[4]
		server_object.original_file_duration= row[5]
		
		linkedid = server_object.linkedid_by_filename()
		
		if server_object.original_file_duration>5:
		
			files_converted = 0
		
			if server_object.make_file_splitted(0):
				server_object.transcribe_to_sql(0,linkedid)
				server_object.remove_temporary_file()
				files_converted+=1

			if server_object.make_file_splitted(1):
				server_object.transcribe_to_sql(1,linkedid)
				server_object.remove_temporary_file()			
				files_converted+=1
				
			if files_converted==0:
				sys.exit( str(server_object.cpu_id)+': '+datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')+' No converted files found. exit..')
			
		else:
			
			trans_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
			server_object.save_result('', '0', '0', 0, trans_date, 0, linkedid)
			server_object.save_result('', '0', '0', 1, trans_date, 0, linkedid)
			
		server_object.delete_current_queue()
	# last_file_name = ''
	print(server_object.cpu_id,datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'exit to next job..')
	#time.sleep(10)
