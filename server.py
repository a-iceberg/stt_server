import sys
#import os
import time
import datetime
#from stt_miner import get_file_splitted
#from stt_miner import transcribe_to_sql
from init_server import stt_server
#from init_server import connect_sql
#from init_server import delete_queue

#cpu_id = sys.argv[1]
server_object = stt_server(sys.argv[1])
#conn = server_object.connect_sql()
cursor = server_object.conn.cursor()
sql_query =		"select filepath, filename, date_y, date_m, date_d from queue where cpu_id='"+server_object.cpu_id+"' order by date;"

while True:
	cursor.execute(sql_query)
	for row in cursor.fetchall():

		server_object.original_file_path	= row[0]			
		server_object.original_file_name	= row[1]
		server_object.date_y				= row[2]
		server_object.date_m				= row[3]
		server_object.date_d				= row[4]
		# split channels
		#splitted_file_name = server_object.make_file_splitted()
		
		if server_object.make_file_splitted(0):
			server_object.transcribe_to_sql(0)
			server_object.remove_temporary_file()
			
		if server_object.make_file_splitted(1):
			server_object.transcribe_to_sql(1)
			server_object.remove_temporary_file()
			
		server_object.delete_current_queue()
			#temp_storage_path = settings.script_path+'files/'
			#if os.path.isfile(temp_storage_path+splitted_file_name+'_l.wav') and os.path.isfile(temp_storage_path+splitted_file_name+'_r.wav'):
			#if os.path.isfile(temp_storage_path+splitted_file_name+'_l.wav') and os.path.isfile(temp_storage_path+splitted_file_name+'_r.wav'):
			# transcribe
			#try:
				#print('a',temp_storage_path, splitted_file_name, filename_original)
				#transcribe_to_sql(temp_storage_path, splitted_file_name+'_l.wav', server_object.conn, settings, 0, date_y, date_m, date_d, filename_original)	
				
				#print('b',temp_storage_path, splitted_file_name, filename_original)
				#transcribe_to_sql(temp_storage_path, splitted_file_name+'_r.wav', server_object.conn, settings, 1, date_y, date_m, date_d, filename_original)
				
				#print('c',temp_storage_path, splitted_file_name, filename_original)
				# delete from queue
				
				# remove temporary splitted audio files
				
			#except Exception as e:
				#print("error:",str(e),'file:',server_object.temp_file_name)
			#print('+++',server_object.temp_file_name)
		#else:
		#	print('---',server_object.temp_file_path + server_object.temp_file_name)
			#exit()
			
	print(server_object.cpu_id,datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'sleeping 10s..')
	time.sleep(10)