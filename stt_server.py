import sys
from stt_miner import get_file_splitted
from stt_miner import transcribe_to_sql
from init_server import server_settings
from init_server import connect_sql
#from init_server import get_today_ymd

cpu_id = sys.argv[1]
settings = server_settings()
conn = connect_sql(settings)
cursor = conn.cursor()
sql_query =		"select filepath, filename, date_y, date_m, date_d from queue where cpu_id='"+cpu_id+"' order by date;"

# while ++
cursor.execute(sql_query)
for row in cursor.fetchall():
	filepath	= row[0]
	filename	= row[1]
	date_y		= row[2]
	date_m		= row[3]
	date_d		= row[4]
	# split channels
	splitted_file_name = get_file_splitted(filepath+filename, settings.script_path)
	temp_storage_path = settings.script_path+'files/'
	# transcribe
	#transcribation = mine_task(splitted_file_path)
	#print(transcribation)
	transcribe_to_sql(temp_storage_path, splitted_file_name+'_l.wav', conn, settings, 0, date_y, date_m, date_d)
	# delete from queue
	# remove splitted_file_path+'_l.wav'
	# remove splitted_file_path+'_r.wav'
	
	break # TODO: remove this breakpoint

# sleep
# while --
	
print('k')