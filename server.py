import sys
import time
import datetime
from init_server import stt_server

server_object = stt_server(sys.argv[1])
cursor = server_object.conn.cursor()
sql_query =		"select filepath, filename, date_y, date_m, date_d, duration from queue where cpu_id='"+server_object.cpu_id+"' order by date;"

while True:
	cursor.execute(sql_query)
	for row in cursor.fetchall():

		server_object.original_file_path	= row[0]			
		server_object.original_file_name	= row[1]
		server_object.date_y				= row[2]
		server_object.date_m				= row[3]
		server_object.date_d				= row[4]
		server_object.original_file_duration= row[5]
		
		if server_object.original_file_duration>5:
		
			files_converted = 0
		
			if server_object.make_file_splitted(0):
				server_object.transcribe_to_sql(0)
				server_object.remove_temporary_file()
				files_converted+=1

			if server_object.make_file_splitted(1):
				server_object.transcribe_to_sql(1)
				server_object.remove_temporary_file()			
				files_converted+=1
				
			if files_converted==0:
				exit();
			
		else:
			
			trans_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
			server_object.save_result('', '0', 0, trans_date)
			server_object.save_result('', '0', 1, trans_date)
			
		server_object.delete_current_queue()
			
	print(server_object.cpu_id,datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'sleeping 10s..')
	time.sleep(10)