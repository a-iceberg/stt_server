from vosk import Model, KaldiRecognizer, SetLogLevel
import json
import pymssql
import pymysql as mysql
import datetime
import os
import wave
import contextlib
import re
import pandas as pd

class stt_server:

	def __init__(self, cpu_id):

		# settings ++
		self.cpu_id = cpu_id
		self.cpu_cores = [i for i in range(0,31)]
		
		# ms sql
		self.sql_name = 'voice_ai'
		self.sql_server = '10.2.4.124'
		self.sql_login = 'ICECORP\\1c_sql'
		
		# mysql
		# mysql
		self.mysql_name = {
			1: 'MICO_96',
			2: 'asterisk',
		}
		self.mysql_server = '10.2.4.146'
		self.mysql_login = 'asterisk'

		self.script_path = '/home/alex/projects/call_centre_stt_server/'
		self.model_path = '/home/alex/projects/vosk-api/python/example/model'
		self.source_id = 0
		self.sources = {
			'call': 1,
			'master': 2,
		}
		self.original_storage_path = {
			1: '/mnt/share/audio_call/',
			2: '/mnt/share/audio_master/REC_IN_OUT/',
		}
		self.original_storage_prefix = {
			1: 'RXTX_',
			2: '',
		}
		self.source_file_deletion = {
			1: False,
			2: True,
		}
		self.temp_file_path = self.script_path+'files/'
		# settings --
		
		self.temp_file_name = ''
		self.original_file_path = ''
		self.original_file_name = ''
		self.original_file_duration	= 0
		self.date_y = ''
		self.date_m = ''
		self.date_d = ''
		self.rec_date = ''
		self.src = ''
		self.dst = ''
		self.linkedid = ''

		#store pass in file, to prevent pass publication on gitdelete_current_queue
		with open(self.script_path+'sql.pass','r') as file:
			self.sql_pass = file.read().replace('\n', '')
			file.close()
			
		with open(self.script_path+'mysql.pass','r') as file:
			self.mysql_pass = file.read().replace('\n', '')
			file.close()
			
		self.conn = self.connect_sql()
		self.mysql_conn = {
			1: self.connect_mysql(1),
			2: self.connect_mysql(2),
		}
			
	def connect_sql(self):

		return pymssql.connect(
			server = self.sql_server,
			user = self.sql_login,
			password = self.sql_pass,
			database = self.sql_name,
			#autocommit=True
		)

	def connect_mysql(self, source_id):

		return mysql.connect(
			host=self.mysql_server,
			user=self.mysql_login,
			passwd=self.mysql_pass,
			db=self.mysql_name[source_id],
			# autocommit = True
			# cursorclass=mysql.cursors.DictCursor,
		)
	
	def linkedid_by_filename(self):
		
		filename = self.original_file_name.replace('rxtx.wav', '')
		
		date_from = datetime.datetime(int(self.date_y), int(self.date_m), int(self.date_d))
		date_toto = date_from+datetime.timedelta(days=1)
		date_from = datetime.datetime.strptime(str(date_from), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
		date_toto = datetime.datetime.strptime(str(date_toto), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')

		mysql_conn = self.connect_mysql(self.source_id)

		with mysql_conn:
		#with self.mysql_conn[self.source_id]:
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

			cursor = mysql_conn[self.source_id].cursor()
			cursor.execute(query)
			for row in cursor.fetchall():
				linkedid, dstchannel, src = row[0], row[1], row[2]
				#print('linkedid, dstchannel', linkedid, dstchannel)
				return linkedid, dstchannel, src
		return '', '', ''
	
	def make_file_splitted(self,side):
			
		# crop '.wav' & append postfix
		self.temp_file_name = self.original_file_name[:-4]+('_R' if side else '_L')+'.wav'

		os_cmd 	= 'ffmpeg -y -i '
		os_cmd += self.original_file_path
		os_cmd += self.original_file_name
		os_cmd += ' -ar 16000 -af "pan=mono|c0=F'		
		os_cmd += 'R' if side else 'L'
		os_cmd += '" '
		os_cmd += self.temp_file_path
		os_cmd += self.temp_file_name

		try:
			os.system(os_cmd)
		except Exception as e:
			print('make_file_splitted error:',str(e))
		return os.path.isfile(self.temp_file_path + self.temp_file_name)

	def set_today_ymd(self):

		self.date_y	= datetime.datetime.today().strftime('%Y')
		self.date_m	= datetime.datetime.today().strftime('%m')
		self.date_d	= datetime.datetime.today().strftime('%d')

	def delete_current_queue(self):

		cursor = self.conn.cursor()
		sql_query = "delete from queue where filename = '"+self.original_file_name+"';"	
		cursor.execute(sql_query)
		self.conn.commit() # autocommit

	def delete_source_file(self):

		if self.source_file_deletion[self.source_id]:
			myfile = self.original_file_path + self.original_file_name
			try:
				os.remove(myfile)
				print('succesfully removed', myfile)
			except OSError as e:  ## if failed, report it back to the user ##
				print("Error: %s - %s." % (e.filename, e.strerror))
		
	def transcribe_to_sql(self,side,linkedid, dst):

		transcribation_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

		# read file
		wf = wave.open(self.temp_file_path + self.temp_file_name, "rb")

		# read model
		model = Model(self.model_path)
		rec = KaldiRecognizer(model, wf.getframerate())

		# recognizing
		phrases_count = 0

		while True:
			
			conf_score = []
			
			data = wf.readframes(4000)
			if len(data) == 0:
				break

			if rec.AcceptWaveform(data):
				accept = json.loads(rec.Result())
				if accept['text'] !='':

					accept_start	= str(accept['result'][0]['start'])
					accept_end   	= accept['result'][-1:][0]['end']
					accept_text		= str(accept['text'])
					
					for result_rec in accept['result']:
						conf_score.append(float(result_rec['conf']))
					conf_mid = str(sum(conf_score)/len(conf_score))
					conf_score = []
					
					self.save_result(accept_text, accept_start, accept_end, side, transcribation_date, conf_mid, linkedid, dst)
					
					phrases_count+=1

		if phrases_count == 0:
			self.save_result('', '0', '0', side, transcribation_date, 0, linkedid, dst)

	def save_result(self, accept_text, accept_start, accept_end, side, transcribation_date, conf_mid, linkedid, dst):
	
		cursor = self.conn.cursor()
		sql_query = """insert into transcribations(
		audio_file_name,
		transcribation_date,
		date_y,
		date_m,
		date_d,
		text,
		start,
		end_time,
		side,
		conf,
		linkedid,
		dst,
		record_date,
		source_id) """
		sql_query += "values ('" + \
			self.original_file_name + "','" + \
			transcribation_date + "','" + \
			self.date_y + "','" + \
			self.date_m + "','" + \
			self.date_d + "','" + \
			accept_text + "','" + \
			str(accept_start) + "','" + \
			str(accept_end) + "'," + \
			str(side) + "," + \
			str(conf_mid) + "," + \
			str(linkedid) + ",'" + \
			str(dst) + "','" + \
			str(self.rec_date) + "','" + \
			str(self.source_id) + \
			"');"
		# print('query',sql_query) # DEBUG
		cursor.execute(sql_query)
		self.conn.commit() # autocommit
			
	def remove_temporary_file(self):
		print('removing',self.temp_file_path + self.temp_file_name)
		os.remove(self.temp_file_path + self.temp_file_name)

	def get_sql_complete_files(self):

		cursor = self.conn.cursor()

		if self.source_id == self.sources['call']:
			sql_query = "select distinct filename from queue where "
			sql_query += "source_id='" + str(self.source_id) + "' and "
			sql_query += "date_y='" + self.date_y + "' and "
			sql_query += "date_m='" + self.date_m + "' and "
			sql_query += "date_d='" + self.date_d + "' "
			sql_query += "union all "
			sql_query += "select distinct audio_file_name from transcribations where "
			sql_query += "date_y='" + self.date_y + "' and "
			sql_query += "date_m='" + self.date_m + "' and "
			sql_query += "date_d='" + self.date_d + "' "
			sql_query += "order by filename;"

		elif self.source_id == self.sources['master']:
			sql_query = "select distinct filename from queue "
			sql_query += "where source_id='" + str(self.source_id) + "' "
			sql_query += "order by filename;"

		cursor.execute(sql_query)
		complete_files = []
		for row in cursor.fetchall():
			complete_files.append(row[0])

		return complete_files

	def get_fs_files_list(self):

		self.original_file_path = self.original_storage_path[self.source_id]
		self.original_file_path += self.original_storage_prefix[self.source_id]
		if self.source_id == self.sources['call']:
			self.original_file_path += self.date_y + '-' + self.date_m + '/' + self.date_d + '/'
		files_list = []
		for (dirpath, dirnames, filenames) in os.walk(self.original_file_path):
			files_list.extend(filenames)
			break

		# get record date
		fd_list = []
		for filename in files_list:

			rec_date = 'Null'

			if self.source_id == self.sources['call']:
				rec_source_date = re.findall(r'\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}', filename)[0]
				if len(rec_source_date):
					rec_date = rec_source_date[:10] + ' ' + rec_source_date[11:].replace('-', ':')
				if len(re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', rec_date)) == 0:
					rec_date = 'Null'
					print('Unable to extract date from filename', filename)
				linkedid, dst, src = self.linkedid_by_filename()

			elif self.source_id == self.sources['master']:
				uniqueid = re.findall(r'^\d*.\d*', filename)[0]
				cursor = self.mysql_conn[self.source_id].cursor()
				query = "select calldate, src, dst from cdr where uniqueid = '" + uniqueid + "' limit 1;"
				cursor.execute(query)
				for row in cursor.fetchall():
					rec_date = str(row[0])
					src = str(row[1])
					dst = str(row[2])
					linkedid = uniqueid

			fd_list.append({
				'filename': filename,
				'rec_date': rec_date,
				'src': src,
				'dst': dst,
				'linkedid': linkedid,
			})

		# break  # todo: REMOVE

		df = pd.DataFrame(fd_list)
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

	def add_queue(self):
		
		self.calculate_file_length()
		
		cursor = self.conn.cursor()
		current_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')		

		"""rec_source_date = re.findall(r'\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}', self.original_file_name)[0]
		self.rec_date = 'Null'
		if len(rec_source_date):
			self.rec_date = rec_source_date[:10] + ' ' + rec_source_date[11:].replace('-', ':')
			if len(re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', self.rec_date)) == 0:
				print('1: Unable to extract date from filename', self.original_file_name)
		else:
			print('0: Unable to extract date from filename', self.original_file_name)"""

		# debug ++
		# print(self.cpu_id, 'self.rec_date', self.rec_date, self.original_file_name)
		# debug --

		sql_query = "insert into queue "
		sql_query += "(filepath, filename, cpu_id, date, date_y, date_m, date_d, "
		sql_query += "duration, record_date, source_id, src, dst, linkedid) "
		sql_query += "values ('"
		sql_query += self.original_file_path + "','"
		sql_query += self.original_file_name + "','"
		sql_query += str(self.cpu_id) + "','"
		sql_query += current_date + "','"
		sql_query += self.date_y + "','"
		sql_query += self.date_m + "','"
		sql_query += self.date_d + "','"
		sql_query += str(self.original_file_duration) + "','"
		sql_query += self.rec_date + "','"
		sql_query += str(self.source_id) + "','"
		sql_query += str(self.src) + "','"
		sql_query += str(self.dst) + "','"
		sql_query += str(self.linkedid) + "');"
		
		try:
			cursor.execute(sql_query)
			self.conn.commit() # autocommit
		except Exception as e:
			print('add queue error. query: '+sql_query)
			print(str(e))
		
	def calculate_file_length(self):
		self.original_file_duration = 0
		try:
			fname = self.original_file_path + self.original_file_name
			with contextlib.closing(wave.open(fname,'r')) as f:
				frames = f.getnframes()
				rate = f.getframerate()
				self.original_file_duration = frames / float(rate)
		except Exception as e:
			print('file length calculate error:', str(e))

	def delete_old_results(self):

		cur_date = datetime.datetime.now()
		DD = datetime.timedelta(days=int(365 / 2))
		crop_date = str(cur_date - DD)
		crop_date = re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', crop_date)[0]
		bottom_limit = datetime.datetime.strptime(str(crop_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
		cursor = self.conn.cursor()
		sql_query = "delete from transcribations where record_date<'"+str(bottom_limit)+"';"
		cursor.execute(sql_query)
		self.conn.commit() # autocommit

