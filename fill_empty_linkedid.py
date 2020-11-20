import pymssql as ms_sql
import pymysql as my_sql

ms_sql_name		= 'voice_ai'
ms_sql_server	= '10.2.4.124'
ms_sql_login	= 'ICECORP\\1c_sql'

my_sql_name		= 'MICO_96'
my_sql_server	= '10.2.4.146'
my_sql_login	= 'asterisk'

with open(self.script_path+'sql.pass','r') as file:
	ms_sql_pass		= file.read().replace('\n', '')
	file.close()

with open(self.script_path+'mysql.pass','r') as file:
	my_sql_pass		= file.read().replace('\n', '')
	file.close()
	
ms_sql_conn	= connect_sql()
my_sql_conn	= connect_mysql()

filename = ''
# read filename
with ms_sql_conn:
	query = 'select top 1 audio_file_name, date_y, date_m, date_d from transcribations where text!='' and linkedid is null and audio_file_name!='' order by date_y,date_m,date_d,transcribation_date;'
	cursor = ms_sql_conn.cursor()
	cursor.execute(query)
	for row in cursor.fetchall():
		filename = row[0]

if filename=='':
	print('empty filename. exit')
	exit()

# read linkedid
filename = file_name.replace('rxtx.wav','')
date_from = datetime.datetime(int(self.date_y),int(self.date_m),int(self.date_d))
date_toto = date_from+datetime.timedelta(days=1)
date_from = datetime.datetime.strptime(str(date_from), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
date_toto = datetime.datetime.strptime(str(date_toto), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
with my_sql_conn:
	cursor = my_sql_conn.cursor()

### continue there..
	query = """
	select
		linkedid
		from PT1C_cdr_MICO as PT1C_cdr_MICO
		where 
			calldate>'"""+date_from+"""' and 
			calldate<'"""+date_toto+"""' and 
			PT1C_cdr_MICO.recordingfile LIKE '%"""+filename+"""%' 
			limit 1
	"""

	cursor = self.mysql_conn.cursor()
	cursor.execute(query)
	for row in cursor.fetchall():
		return row[0]
		

# save
with my_sql_conn:
	cursor = my_sql_conn.cursor()
	sql_query = "insert into transcribations (audio_file_name, transcribation_date, date_y, date_m, date_d, text, start, end_time, side, conf, linkedid) "
	sql_query += "values ('"+self.original_file_name+"','"+transcribation_date+"','"+self.date_y+"','"+self.date_m+"','"+self.date_d+"','"+accept_text+"','"+str(accept_start)+"','"+str(accept_end)+"',"+str(side)+","+str(conf_mid)+","+str(linkedid)+");"
	cursor.execute(sql_query)
	self.conn.commit()