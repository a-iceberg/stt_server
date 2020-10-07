from stt_miner import get_file_splitted
from init_server import server_settings, connect_sql

cpu_id = sys.argv[1]
settings = server_settings()
conn = connect_sql(settings)

cursor = conn.cursor()
sql_query =		"select filepath, filename, date_y, date_m, date_d from queue where cpu_id='"+cpu_id+"' order by date;"
cursor.execute(sql_query)
for row in cursor.fetchall():
	filepath	= row[0]
	filename	= row[1]
	date_y		= row[2]
	date_m		= row[3]
	date_d		= row[4]
	# split channels
	splitted_file_path = get_file_splitted(filepath+filename,settings.script_path)
	# transcribe
	transcribation = mine_task(splitted_file_path)
	print(transcribation)
	# save to transcribations
	# delete from queue
	# remove splitted_file_path+'_l.wav'
	# remove splitted_file_path+'_r.wav'

print('k')