import datetime
from os import walk
import pymssql
from init_server import server_settings

def connect_sql(settings):
	
	return pymssql.connect(
		server		= settings.db_server, 
		user		= settings.db_login, 
		password	= settings.db_pass,
		database	= settings.db_name
	)

def get_fs_files_list(settings, date_y, date_m, date_d):

	today_path = settings.audio_storage_path + settings.audio_path_prefix + date_y + '-' + date_m + '/'	+ date_d + '/'	
	files_list = []
	for (dirpath, dirnames, filenames) in walk(today_path):
		files_list.extend(filenames)
		break

	return today_path, files_list

def get_today_ymd():	
	
	date_y	= datetime.datetime.today().strftime('%Y')
	date_m	= datetime.datetime.today().strftime('%m')
	date_d	= datetime.datetime.today().strftime('%d')
	
	return date_y, date_m, date_d

def get_sql_complete_files(conn): 				# <<< === TODO: union recognized filenames
	
	cursor = conn.cursor()
	query = "select filename from queue where date_y='"+date_y+"' and date_m='"+date_m+"' and date_d='"+date_d+"' order by filename;"
	cursor.execute(query)
	complete_files = []
	for row in cursor.fetchall():
		complete_files.append(row[0])
	
	return complete_files

def add_queue(conn, settings, filepath, filename, date_y, date_m, date_d):
		
	cpu_id   = str(settings.cpu_id)
	cursor = conn.cursor()
	current_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
	sql_query = "insert into queue (filepath, filename, cpu_id, date, date_y, date_m, date_d) "
	sql_query += "values ('"+filepath+"','"+filename+"','"+cpu_id+"','"+current_date+"','"+date_y+"','"+date_m+"','"+date_d+"');"
	cursor.execute(sql_query)
	conn.commit()

settings = server_settings(0)					# <<< === TODO: get cpuid from param

date_y, date_m, date_d	= get_today_ymd()

# get filenames in today's queue
conn = connect_sql(settings)
complete_files	= get_sql_complete_files(conn)

# list files
filepath, fs_files_list	= get_fs_files_list(settings, date_y, date_m, date_d)
for filename in fs_files_list:
	if not filename in complete_files:
		print('new',filename)
		add_queue(conn, settings, filepath, filename, date_y, date_m, date_d)
	else:
		print('completed',filename)
	break

print('k')