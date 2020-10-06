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

def get_files_list(settings, date_y, date_m, date_d):

	today_path = settings.audio_storage_path + settings.audio_path_prefix + date_y + '-' + date_m + '/'	+ date_d + '/'	
	files_list = []
	for (dirpath, dirnames, filenames) in walk(today_path):
		files_list.extend(filenames)
		break

	return files_list

def get_today_ymd():	
	
	date_y	= datetime.datetime.today().strftime('%Y')
	date_m	= datetime.datetime.today().strftime('%m')
	date_d	= datetime.datetime.today().strftime('%d')
	
	return date_y, date_m, date_d

settings = server_settings()

date_y, date_m, date_d	= get_today_ymd()

# get filenames in today's queue 				<<< === TODO: union recognized filenames
conn = connect_sql(settings)
query = "select filename from queue where date_y='"+date_y+"' and date_m='"+date_m+"' and date_y='"+date_d+"';"
cursor.execute(query)
complete_files = []
for row in cursor.fetchall():
	complete_files.append(row[0])

# list files
files_list	= get_files_list(settings, date_y, date_m, date_d)
for filename in files_list:
	if not filename in complete_files:
		print('new',filename)
	else:
		print('completed',filename)
	break

print('k')