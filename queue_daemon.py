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

settings = server_settings()

# list files
date_y = datetime.datetime.today().strftime('%Y')
date_m = datetime.datetime.today().strftime('%m')
date_d = datetime.datetime.today().strftime('%d')

today_path = ''+
	settings.audio_storage_path + 
	settings.audio_path_prefix + 
	date_y + '-'
	date_m + '/'
	date_d + '/'
	
files_list = []
for (dirpath, dirnames, filenames) in walk(today_path):
    files_list.extend(filenames)
    break

for filename in files_list:
	print(len(files_list),filename)
	break
	
#conn = connect_sql(settings)
print('k')