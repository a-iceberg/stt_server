#from requests import Session as requests_session
import pymssql
import pymysql as mysql
import datetime
import os
import wave
import contextlib
import re
import pandas as pd
#import sys
import time
import shutil
#from shutil import copyfile
#import asyncio
#import websockets
#import socket


class stt_server:

	def __init__(self):
		# settings ++
		#self.cpu_id = self.get_worker_id()
		cores_count = int(os.environ.get('WORKERS_COUNT', '0'))
		self.cpu_cores = [i for i in range(0, cores_count)]

		#self.gpu_uri = os.environ.get('VOSK_SERVER_DEFAULT', '')
		
		# ms sql
		self.sql_name = 'voice_ai'

		# mysql
		self.mysql_name = {
			1: 'MICO_96',
			2: 'asterisk',
		}

		self.source_id = 0
		self.sources = {
			'call': 1,
			'master': 2,
		}

		self.original_storage_path = {
			1: 'audio/stereo/', # call centre records path
			2: 'audio/mono/' # masters records path
		}
		self.saved_for_analysis_path = 'audio/saved_for_analysis/'
		self.confidence_of_file = 0
		# settings --

		self.temp_file_path = ''
		self.temp_file_name = ''		
			
		self.conn = self.connect_sql()
		self.mysql_conn = {
			1: self.connect_mysql(1),
			2: self.connect_mysql(2),
		}

		"""if self.gpu_uri == '':
			self.model = Model(self.model_path)"""


	def log_deletion(self, filename):
		current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		connector = mysql.connect(
			host = '10.2.4.87',
			user = 'root',
			passwd = 'root'
		)
		connector.autocommit(True)
		cursor = connector.cursor()
		cursor.execute("use ml")
		cursor.execute("INSERT INTO deletions(date, filename) VALUES ('"+current_date+"', '"+filename+"');")


	def send_to_telegram(self, message):
		import requests
		current_date = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
		token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
		chat_id = os.environ.get('TELEGRAM_CHAT', '')
		session = requests.Session()
		get_request = 'https://api.telegram.org/bot' + token
		get_request += '/sendMessage?chat_id=' + chat_id
		get_request += '&parse_mode=Markdown&text=' + current_date + ' vosk_queue: ' + message
		session.get(get_request)
			

	def connect_sql(self):
		return pymssql.connect(
			server=os.environ.get('MSSQL_SERVER', ''),
			user=os.environ.get('MSSQL_LOGIN', ''),
			password=os.environ.get('MSSQL_PASSWORD', ''),
			database=self.sql_name,
			#autocommit=True			
		)		


	def connect_mysql(self, source_id):
		return mysql.connect(
			host=os.environ.get('MYSQL_SERVER', ''),
			user=os.environ.get('MYSQL_LOGIN', ''),
			passwd=os.environ.get('MYSQL_PASSWORD', ''),
			db=self.mysql_name[source_id],
			# autocommit = True
			# cursorclass=mysql.cursors.DictCursor,
		)
	

	def linkedid_by_filename(self, filename, date_y, date_m, date_d):
		# filename = filename.replace('rxtx.wav', '')
		filename = filename.replace('rxtx-in.wav', '.wav')
		filename = filename.replace('rxtx-out.wav', '.wav')
		filename = filename.replace('in_', '')
		filename = filename.replace('out_', '')

		date_from = datetime.datetime(int(date_y), int(date_m), int(date_d))
		date_toto = date_from+datetime.timedelta(days=1)
		date_from = datetime.datetime.strptime(str(date_from), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
		date_toto = datetime.datetime.strptime(str(date_toto), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')

		mysql_conn = self.connect_mysql(self.source_id)

		with mysql_conn:
			query = """
			select				
				linkedid,
				SUBSTRING(dstchannel, 5, 4),
				src
				from PT1C_cdr_MICO as PT1C_cdr_MICO
				where 
					calldate>'"""+date_from+"""' and 
					calldate<'"""+date_toto+"""' and 
					PT1C_cdr_MICO.recordingfile LIKE '%"""+filename+"""%' 
					limit 1;"""

			cursor = mysql_conn.cursor()
			cursor.execute(query)
			for row in cursor.fetchall():
				linkedid, dstchannel, src = row[0], row[1], row[2]
				return linkedid, dstchannel, src
		return '', '', ''


	def get_sql_complete_files(self):
		cursor = self.conn.cursor()
		sql_query = "select distinct filename from queue where"
		sql_query += " source_id='" + str(self.source_id) + "'"
		sql_query += " order by filename;"
		cursor.execute(sql_query)
		complete_files = []
		for row in cursor.fetchall():
			complete_files.append(row[0])

		return complete_files


	def copy_file(self, src, dst):
		if not os.path.exists(src):
			print("copy_file error: source file not exist")
			self.log('copy_file error: source file not exist '+src)
			return
		#if not os.path.exists(dst):
		#	os.makedirs(dst)
		self.log('copying '+src+' to '+dst)
		shutil.copy(src, dst)


	def log(self, text):
		current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		text = str(current_date) + ' ' + text
		print('log:', text)
		with open(self.saved_for_analysis_path+'debug/log.txt', 'a') as f:
			f.write(text + '\n')


	def get_fs_files_list(self, queue):
		fd_list = []

		if self.source_id == self.sources['master']:
			files_list = []
			print('master path', self.original_storage_path[self.source_id])
			for (dirpath, dirnames, filenames) in os.walk(self.original_storage_path[self.source_id]):
				# append if '.wav' in filename or '.WAV' in filename
				for filename in filenames:
					#files_list.extend(filenames)
					if '.wav' in filename or '.WAV' in filename:
						files_list.append(filename)					

			files_extracted = 0
			files_withoud_cdr_data = 0

			# get record date
			for filename in files_list:
				if not filename in queue:
					if os.environ.get('SAVE_FOR_ANALYSIS','0')=='1':
						# debug ++										
						dst_file = self.saved_for_analysis_path+'debug/master/'+filename
						if not os.path.exists(dst_file):
							self.copy_file(
								self.original_storage_path[self.source_id]+filename,
								self.saved_for_analysis_path+'debug/master/'
							)
						# debug --
					try:
						file_stat = os.stat(self.original_storage_path[self.source_id] + filename)
						# f_size = file_stat.st_size
						file_age = time.time() - file_stat.st_mtime
					except Exception as e:
						print("get_fs_files_list / file_stat Error:", str(e))
						file_age = 0
					if "h.wav" in filename:
						try:
							if file_age > 3600:
								os.remove(self.original_storage_path[self.source_id] + filename)
								self.log_deletion(self.original_storage_path[self.source_id] + filename)
								# debug ++
								# self.send_to_telegram('min. get_fs_files_list. removed: ' + str(filename))
								# debug --
								print(str(round(file_age/60)), 'min. get_fs_files_list. Removed:', filename)
							else:
								print(str(round(file_age/60)), 'min. get_fs_files_list. Skipped: ', filename)
							continue
						except OSError as e:  ## if failed, report it back to the user ##
							print("Error: %s - %s." % (e.filename, e.strerror))
							self.send_to_telegram('get_fs_files_list file delete error:\n' + str(e))

					rec_date = 'Null'
					version = 0
					r_d = re.findall(r'a.*b', filename)
					if len(r_d) and len(r_d[0]) == 21:
						try:
							rec_date = r_d[0][1:][:-1].replace('t', ' ')
							#print('v.1 date', rec_date)
							src = re.findall(r'c.*d', filename)[0][1:][:-1]
							dst = re.findall(r'e.*f', filename)[0][1:][:-1]
							linkedid = re.findall(r'g.*h', filename)[0][1:][:-1]
							version = 1
						except Exception as e:
							print("Error:", str(e))
							#self.send_to_telegram('v1 filename parse error: '+ filename +'\n' + str(e))

					if version == 0:


						rec_date = 'Null'
						uniqueid = re.findall(r'\d*\.\d*', filename)[0]
						cursor = self.mysql_conn[self.source_id].cursor()
						query = "select calldate, src, dst from cdr where uniqueid = '" + uniqueid + "' limit 1;"
						cursor.execute(query)  # cycled query
						src = ''
						dst = ''
						linkedid = uniqueid

						for row in cursor.fetchall():
							rec_date = str(row[0])
							print('v.0 date', rec_date)
							src = str(row[1])
							dst = str(row[2])

						if len(re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', rec_date)) == 0:
							print('u:', uniqueid, 'r:', rec_date, 'Unable to extract date from filename:', filename)
							rec_date = 'Null'
							files_withoud_cdr_data += 1

					if not rec_date == 'Null':

						fd_list.append({
							'filepath': self.original_storage_path[self.source_id],
							'filename': filename,
							'rec_date': rec_date,
							'src': src,
							'dst': dst,
							'linkedid': linkedid,
							'version': version,
						})
						files_extracted += 1

			print('master extracted:', files_extracted, 'without cdr data:', files_withoud_cdr_data)

		elif self.source_id == self.sources['call']:
			print('call path', self.original_storage_path[self.source_id])
			for root, dirs, files in os.walk(self.original_storage_path[self.source_id]):
				for filename in files:
					# continue if filename is not .wav and not .WAV
					if not filename.endswith('.wav') and not filename.endswith('.WAV'):
						continue
					
					# ToDo: remove this after upgrade audio records preparing method
					if filename[-11:]!='rxtx-in.wav' and filename[-12:]!='rxtx-out.wav':
						if 'wav' in filename or 'WAV' in filename:
							# log information about removed file and his path
							with open(self.saved_for_analysis_path+'debug/removed.csv', 'a') as f:
								f.write(root + ';' + filename + '\n')
							print('removed', root + '/' + filename)
							# remove file
							os.remove(os.path.join(root, filename))
						continue

					file_in_queue = filename in queue
					if os.environ.get('SAVE_FOR_ANALYSIS', '0')=='1':
						self.log('call check file '+filename)
						try:
							
							# debug ++
							if not file_in_queue:
								dst_file = self.saved_for_analysis_path+'debug/call/'+filename
								if not os.path.exists(dst_file):
									self.copy_file(									
										os.path.join(root, filename),
										self.saved_for_analysis_path+'debug/call/'
									)
								else:
									self.log('copying canceled. file exists: '+dst_file)
							#else: 
							#	self.log(filename+' in queue')
						except Exception as e:
							self.log('call debug error: '+str(e))
						# debug --
					if not file_in_queue and filename[-4:] == '.wav':
						rec_source_date = re.findall(r'\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}', filename)
						if len(rec_source_date) and len(rec_source_date[0]):
							rec_date = rec_source_date[0][:10] + ' ' + rec_source_date[0][11:].replace('-', ':')

							if len(re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', rec_date)) == 0:
								rec_date = 'Null'
								print('0 Unable to extract date:', root, filename)

							date_string = re.findall(r'\d{4}-\d{2}-\d{2}', filename)
							if len(date_string):
								date_y = date_string[0][:4]
								date_m = date_string[0][5:-3]
								date_d = date_string[0][-2:]
								linkedid, dst, src = self.linkedid_by_filename(filename, date_y, date_m, date_d)  # cycled query

								fd_list.append({
									'filepath': root+'/',
									'filename': filename,
									'rec_date': rec_date,
									'src': src,
									'dst': dst,
									'linkedid': linkedid,
									'version': 0,
								})
						else:
							print('1 Unable to extract date:', root, filename)
							self.send_to_telegram('1 Unable to extract date: ' + str(root) + ' ' + str(filename))		

		df = pd.DataFrame(fd_list, columns=['filepath', 'filename', 'rec_date', 'src', 'dst', 'linkedid', 'version'])
		df.sort_values(['rec_date', 'filename'], ascending=True, inplace=True)

		return df.values

	
	def set_shortest_queue_cpu(self):		
		cursor = self.conn.cursor()
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
		for i in self.cpu_cores:
			if i==0:
				sql_query += 'select 0 as cpu_id, 0 as files_count '
			else:
				sql_query += 'union all select '+str(i)+',0 '
		sql_query += 'union all	select cpu_id, count(filename) from queue group by cpu_id; '
		sql_query += 'select top 1 cpu_id, max(files_count)  from #tmp_cpu_queue_len group by cpu_id order by max(files_count), cpu_id;'	
		cursor.execute(sql_query)
		#self.conn.commit()  # autocommit
		result = 0
		for row in cursor.fetchall():
			result += 1
			self.cpu_id = int(row[0])
			# print('selected', self.cpu_id, 'cpu')
		if result == 0:
			print('error: unable to get shortest_queue_cpu')
			self.cpu_id = 0


	def get_source_id(self, source_name):
		for source in self.sources.items():
			if source[0] == source_name:
				return source[1]
		return 0


	def get_source_name(self, source_id):
		for source in self.sources.items():
			if source[1] == source_id:
				return source[0]
		return 0


	def add_queue(self, filepath, filename, rec_date, src, dst, linkedid, naming_version):
		try:
			file_stat = os.stat(filepath + filename)
			f_size = file_stat.st_size
			st_mtime = file_stat.st_mtime
		except Exception as e:
			f_size = -1
			st_mtime = 0
			print('file stat error:', str(e))
			self.send_to_telegram(str(e))

		if time.time() - st_mtime > 600:
			file_duration = self.calculate_file_length(filepath, filename)

			if file_duration == 0:
				message = 'zero file in queue: t[' + str(time.time() - st_mtime) + ']  '
				message += 's[' + str(f_size) + ']  '
				message += 'd[' + str(file_duration) + ']  '
				message += str(filename)
				# self.save_file_for_analysis(filepath, filename, file_duration)
				print(message)
				# self.send_to_telegram(message)
				#else:
				#self.save_file_for_analysis(filepath, filename, file_duration)

			cursor = self.conn.cursor()
			current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

			sql_query = "insert into queue "
			sql_query += "(filepath, filename, cpu_id, date, "
			sql_query += "duration, record_date, source_id, src, dst, linkedid, version, file_size) "
			sql_query += "values ('"
			sql_query += filepath + "','"
			sql_query += filename + "','"
			sql_query += str(self.cpu_id) + "','"
			sql_query += current_date + "','"
			sql_query += str(file_duration) + "',"
			sql_query += rec_date if rec_date == 'Null' else "'"+rec_date+"'"
			sql_query += ",'"
			sql_query += str(self.source_id) + "','"
			sql_query += str(src) + "','"
			sql_query += str(dst) + "','"
			sql_query += str(linkedid) + "',"
			sql_query += str(naming_version) + ","
			sql_query += str(f_size) + ");"

			try:
				cursor.execute(sql_query)
				self.conn.commit() # autocommit
			except Exception as e:
				print('add queue error. query: '+sql_query)
				print(str(e))
		else:
			message = 'queue skipped: t[' + str(time.time() - file_stat.st_mtime) + ']  '
			message += 's[' + str(file_stat.st_size) + ']  '
			# message += 'd[' + str(file_duration) + ']  '
			message += str(filename)
			# self.save_file_for_analysis(filepath, filename, file_duration)
			# print(message)
			# self.send_to_telegram(message)


	def calculate_file_length(self, filepath, filename):
		file_duration = 0
		try:
			fname = filepath + filename
			with contextlib.closing(wave.open(fname, 'r')) as f:
				frames = f.getnframes()
				rate = f.getframerate()
				file_duration = frames / float(rate)
		except Exception as e:
			print('file length calculate error:', str(e))
			# self.save_file_for_analysis(filepath, filename, file_duration)
			# self.send_to_telegram('file length calculate error:\n'+fname+'\n'+str(e))
		return file_duration
