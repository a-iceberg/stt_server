from vosk import Model, KaldiRecognizer, SetLogLevel
import wave
import json

import pymssql
import datetime
import os
import os.path

class stt_server:

	def __init__(self, cpu_id):

		# settings ++
		self.cpu_id					= cpu_id
		self.cpu_cores				= [i for i in range(0,1)]
		self.db_name				= 'voice_ai'
		self.db_server				= '10.2.4.124'
		self.db_login				= 'ICECORP\\1c_sql'
		self.script_path			= '/home/alex/projects/call_centre_stt_server/'
		self.model_path				= '/home/alex/projects/vosk-api/python/example/model'
		self.original_storage_path	= '/mnt/share/audio_call/'
		self.original_storage_prefix= 'RXTX_'
		self.temp_file_path			= self.script_path+'files/'
		# settings --
		
		self.temp_file_name			= ''
		self.original_file_path		= ''
		self.original_file_name		= ''		
		self.date_y					= ''
		self.date_m					= ''
		self.date_d					= ''		

		#store pass in file, to prevent pass publication on git
		with open(self.script_path+'sql.pass','r') as file:
			self.db_pass		= file.read().replace('\n', '')
			file.close()
			
		self.conn				= self.connect_sql()
			
	def connect_sql(self):

		return pymssql.connect(
			server		= self.db_server, 
			user		= self.db_login, 
			password	= self.db_pass,
			database	= self.db_name
		)
	
	def make_file_splitted(self,side):
			
		# crop '.wav' & append postfix
		self.temp_file_name = self.original_file_name[:-4]+('_R' if side else '_L')+'.wav'

		#temp_storage_path = script_path+'files/'
		#filename = str(uuid.uuid4())
		#print('***',audio_path,'***',temp_storage_path+filename+'_l.wav')
		os_cmd 	= 'ffmpeg -i '
		os_cmd += self.original_file_path
		os_cmd += self.original_file_name
		os_cmd += ' -ar 16000 -af "pan=mono|c0=F'		
		os_cmd += 'R' if side else 'L'
		os_cmd += '" '
		os_cmd += self.temp_file_path
		os_cmd += self.temp_file_name

		#os.system('ffmpeg -i '+audio_path+' -ar 16000 -af "pan=mono|c0=FL" '+temp_storage_path+filename+'_l.wav')
		#os.system('ffmpeg -i '+audio_path+' -ar 16000 -af "pan=mono|c0=FR" '+temp_storage_path+filename+'_r.wav')
		try:
			os.system(os_cmd)
			#return filename
		except Exception as e:
			print('make_file_splitted error:',str(e))
		return os.path.isfile(self.temp_file_path + self.temp_file_name)

	def set_today_ymd(self):

		self.date_y	= datetime.datetime.today().strftime('%Y')
		self.date_m	= datetime.datetime.today().strftime('%m')
		self.date_d	= datetime.datetime.today().strftime('%d')

		#return date_y, date_m, date_d

	def delete_current_queue(self):

		cursor = self.conn.cursor()
		sql_query = "delete from queue where filename = '"+self.original_file_name+"';"	
		cursor.execute(sql_query)
		self.conn.commit()
		
	def transcribe_to_sql(self,side):
		#filepath, filename, conn, settings, side, date_y, date_m, date_d, file_name_original

		transcribation_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

		# read file
		wf = wave.open(self.original_file_path + self.original_file_name, "rb")

		# read model
		model = Model(self.model_path)
		rec = KaldiRecognizer(model, wf.getframerate())

		# recognizing
		phrases_count = 0

		while True:

			data = wf.readframes(4000)
			if len(data) == 0:
				break

			if rec.AcceptWaveform(data):
				accept = json.loads(rec.Result())
				if accept['text'] !='':

					accept_start	= str(accept['result'][0]['start'])
					accept_text		= str(accept['text'])

					#save_result(conn, file_name_original, transcribation_date, date_y, date_m, date_d, accept_text, accept_start, side)
					self.save_result(accept_text, accept_start, side, transcribation_date)
					phrases_count+=1

		if phrases_count == 0:			
			#save_result(conn, file_name_original, transcribation_date, date_y, date_m, date_d, '', '0', side)
			self.save_result('', '0', side, transcribation_date)

	def save_result(self, accept_text, accept_start, side, transcribation_date):
		#conn, file_name_original, transcribation_date, date_y, date_m, date_d, accept_text, accept_start, side
	
		cursor = self.conn.cursor()				
		sql_query = "insert into transcribations (audio_file_name, transcribation_date, date_y, date_m, date_d, text, start, side) "
		sql_query += "values ('"+self.original_file_name+"','"+transcribation_date+"','"+self.date_y+"','"+self.date_m+"','"+self.date_d+"','"+accept_text+"','"+accept_start+"',"+str(side)+");"
		cursor.execute(sql_query)
		self.conn.commit()
			
	def remove_temporary_file(self):
		print('removing',self.temp_file_path + self.temp_file_name)
		os.remove(self.temp_file_path + self.temp_file_name)
		
	def get_sql_complete_files(self):
	
		cursor = self.conn.cursor()
		sql_query =		"select filename from queue where date_y='"+self.date_y+"' and date_m='"+self.date_m+"' and date_d='"+self.date_d+"' union all "
		sql_query +=	"select audio_file_name as filename from transcribations where date_y='"+self.date_y+"' and date_m='"+self.date_m+"' and date_d='"+self.date_d+"' "
		sql_query +=	"order by filename;"
		cursor.execute(sql_query)
		complete_files = []
		for row in cursor.fetchall():
			complete_files.append(row[0])

		return complete_files
	
	def get_fs_files_list(self):

		self.original_file_path = self.original_storage_path + self.original_storage_prefix + self.date_y + '-' + self.date_m + '/'	+ self.date_d + '/'	
		files_list = []
		for (dirpath, dirnames, filenames) in walk(self.original_file_path):
			files_list.extend(filenames)
			break

		return files_list
	
	def set_shortest_queue_cpu(self):
		# conn, settings
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
		result = 0
		for row in cursor.fetchall():
			result+=1
			server_object.cpu_id = int(row[0])
		if result==0:
			print('error: unable to get shortest_queue_cpu')
			server_object.cpu_id = 0
	
	def add_queue(self):
		#conn, filepath, filename, cpu_id, date_y, date_m, date_d			
		cursor = self.conn.cursor()
		current_date = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		sql_query = "insert into queue (filepath, filename, cpu_id, date, date_y, date_m, date_d) "
		sql_query += "values ('"+self.original_file_path+"','"+self.original_file_name+"','"+str(self.cpu_id)+"','"+current_date+"','"+self.date_y+"','"+self.date_m+"','"+self.date_d+"');"
		cursor.execute(sql_query)
		self.conn.commit()