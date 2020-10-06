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
	sql_query = "select filename from queue where date_y='"+date_y+"' and date_m='"+date_m+"' and date_d='"+date_d+"' order by filename;"
	cursor.execute(sql_query)
	complete_files = []
	for row in cursor.fetchall():
		complete_files.append(row[0])
	
	return complete_files

def add_queue(conn, filepath, filename, cpu_id, date_y, date_m, date_d):
			
	cursor = conn.cursor()
	current_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
	sql_query = "insert into queue (filepath, filename, cpu_id, date, date_y, date_m, date_d) "
	sql_query += "values ('"+filepath+"','"+filename+"','"+str(cpu_id)+"','"+current_date+"','"+date_y+"','"+date_m+"','"+date_d+"');"
	cursor.execute(sql_query)
	conn.commit()

def shortest_queue_cpu(conn, settings):
			
	cursor = conn.cursor()
	sql_query = '''
	IF OBJECT_ID('tempdb..#tmp_cpu_queue_len') IS NOT NULL
    DROP TABLE #tmp_cpu_queue_len;

	CREATE TABLE #tmp_cpu_queue_len
	(
	cpu_id INT,
	files_count int
	);

	INSERT INTO #tmp_cpu_queue_len 
	'''
	for i in range( 0, len(settings.cpu_cores_count) ):
		if i==0:
			sql_query += 'select 0 as cpu_id, 0 as files_count '
		else:
			sql_query += 'union all select '+str(i)+',0 '
	sql_query += 'union all	select cpu_id, count(filename) from queue group by cpu_id; '
	sql_query += 'select top 1 cpu_id, max(files_count)  from #tmp_cpu_queue_len group by cpu_id order by max(files_count);'	
	#print(sql_query) # debug
	#return 0
	cursor.execute(sql_query)
	result = -1
	for row in cursor.fetchall():
		result = int(row[0])
	if result==-1:
		print('error: unable to get shortest_queue_cpu')
		result = 0
	return result
	
settings = server_settings()

date_y, date_m, date_d	= get_today_ymd()

# get filenames in today's queue
conn = connect_sql(settings)
complete_files	= get_sql_complete_files(conn)

# list files
filepath, fs_files_list	= get_fs_files_list(settings, date_y, date_m, date_d)
for filename in fs_files_list:
	if not filename in complete_files:
		print('new',filename)
		cpu_id	= shortest_queue_cpu(conn, settings);
		print('cpu_id',cpu_id)	# debug
		add_queue(conn, filepath, filename, cpu_id, date_y, date_m, date_d)
		break
	else:
		print('completed',filename)	

print('k')