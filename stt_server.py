import pymssql
from stt_miner import get_file_splitted

script_path = '/home/alex/projects/call_centre_stt_server/'
db_name = 'voice_ai'
db_server = '10.2.4.124'

#audio_path = '/mnt/share/audio_call/RXTX_2020-10/01/in_4953621465_2020-10-01-09-41-34rxtx.wav'
#splitted_file_path = get_file_splitted(audio_path,script_path)

def connect_sql(script_path, server, database):
	
	with open(script_path+'sql.login','r') as file:
		sql_login=file.read().replace('\n', '')
		file.close()
	
	with open(script_path+'sql.password','r') as file:
		sql_pass=file.read().replace('\n', '')
		file.close()
		
	return pymssql.connect(server = server, user = sql_login, password = sql_pass, database = database)

conn = connect_sql(script_path,db_name)
print('k')

'''
cursor = conn.cursor()	
query = "SELECT lat,lon,info,color,fill_color,fill_opacity FROM geo_map.dbo.requests where request_id='"+request_id+"';"
cursor.execute(query)
data = []
for row in cursor.fetchall():
	record	= {
		'lat':row[0],
		'lon':row[1],
		'info':row[2],
		'color':row[3],
		'fill_color':row[4],
		'fill_opacity':row[5]
		}
	data.append(record)
#conn.close()
query ="delete from geo_map.dbo.requests where request_id='"+request_id+"';"
cursor.execute(query)
conn.commit()
'''